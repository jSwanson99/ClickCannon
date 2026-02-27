package metrics

import (
	"context"
	"fmt"
	"log/slog"
	"otelspam/internal/block"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

type Worker struct {
	log *slog.Logger

	runID     string
	conn      driver.Conn
	insertSQL string

	dataType             string
	targetBytesPerSecond uint64

	blockPool  *block.StructPool[block.SharedColumns]
	blockQueue chan block.SharedColumns

	metricsQueue chan Entry
	mu           sync.Mutex
	metrics      map[Name]uint64
	pointMetrics []Entry
}

func NewWorker(log *slog.Logger, runID, dataType string, targetBytesPerSecond uint64, cfg *Config, blockPool *block.StructPool[block.SharedColumns], blockQueue chan block.SharedColumns) (*Worker, error) {
	w := Worker{
		log:                  log.With("component", "metrics_worker", "data_type", dataType),
		runID:                runID,
		dataType:             dataType,
		targetBytesPerSecond: targetBytesPerSecond,

		blockPool:  blockPool,
		blockQueue: blockQueue,

		metricsQueue: make(chan Entry, 10_000),
		metrics:      make(map[Name]uint64),
		pointMetrics: make([]Entry, 0, 10_000),
	}

	if cfg.ClickHouseDSN != "" {
		opt, err := clickhouse.ParseDSN(cfg.ClickHouseDSN)
		if err != nil {
			return nil, fmt.Errorf("failed to parse DSN: %w", err)
		}

		w.conn, err = clickhouse.Open(opt)
		if err != nil {
			return nil, fmt.Errorf("failed to connect: %w", err)
		}
		w.log.Info("clickhouse connected")

		err = w.conn.Exec(context.Background(), fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %q", cfg.Database))
		if err != nil {
			return nil, fmt.Errorf("failed to create metrics database: %w", err)
		}

		runDDL := fmt.Sprintf(`
			CREATE TABLE IF NOT EXISTS %q.%q (
				run_id String,
				timestamp DateTime64(3),
			    data_type LowCardinality(String),
			    target_bytes_per_second UInt64
			) Engine = MergeTree()
			ORDER BY (run_id, timestamp)
		`, cfg.Database, cfg.RunTable)

		err = w.conn.Exec(context.Background(), runDDL)
		if err != nil {
			return nil, fmt.Errorf("failed to create run table: %w", err)
		}

		insertRunSQL := fmt.Sprintf(`INSERT INTO %q.%q VALUES (?, ?, ?, ?)`, cfg.Database, cfg.RunTable)
		err = w.conn.Exec(context.Background(), insertRunSQL, runID, time.Now(), dataType, targetBytesPerSecond)
		if err != nil {
			return nil, fmt.Errorf("failed to insert run: %w", err)
		}

		metricsDDL := fmt.Sprintf(`
			CREATE TABLE IF NOT EXISTS %q.%q (
				run_id String,
				metric_name LowCardinality(String),
				metric_meta String,
				timestamp DateTime64(3),
				value UInt64
			) Engine = MergeTree()
			ORDER BY (run_id, metric_name, timestamp)
		`, cfg.Database, cfg.MetricsTable)

		err = w.conn.Exec(context.Background(), metricsDDL)
		if err != nil {
			return nil, fmt.Errorf("failed to create metrics table: %w", err)
		}

		w.insertSQL = fmt.Sprintf(`INSERT INTO %q.%q VALUES (?, ?, ?, ?, ?)`, cfg.Database, cfg.MetricsTable)
	}

	return &w, nil
}

func (w *Worker) Run(ctx context.Context) error {
	w.log.Info("started")
	defer w.log.Info("stopped")

	go w.processMetrics(ctx)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(1 * time.Second):
			// this should be set from somewhere else maybe, but this works fine for now
			blockPoolCount, blockPoolCapacity := w.blockPool.Stats()
			w.SetMetric(BlockPoolCount, uint64(blockPoolCount))
			w.SetMetric(BlockPoolCapacity, uint64(blockPoolCapacity))
			w.SetMetric(BlockQueueLength, uint64(len(w.blockQueue)))

			// this should be dynamically adjustable in the future, but for now we set it constantly
			w.SetMetric(TargetBytesPerSecond, w.targetBytesPerSecond)

			if w.insertSQL != "" {
				err := w.pushMetrics(context.Background())
				if err != nil {
					w.log.Error("failed to push metrics", "err", err)
				}
			}

			w.resetMetrics()
		}
	}
}

func (w *Worker) processMetrics(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case m := <-w.metricsQueue:
			w.applyMetricEntry(m)
		}
	}
}

func (w *Worker) applyMetricEntry(m Entry) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if _, ok := w.metrics[m.Name]; !ok && m.Mode != EntryModePoint {
		w.metrics[m.Name] = 0
	}

	switch m.Mode {
	case EntryModeIncrement:
		w.metrics[m.Name] += m.Value
	case EntryModeDecrement:
		if m.Value >= w.metrics[m.Name] {
			w.metrics[m.Name] = 0
		} else {
			w.metrics[m.Name] -= m.Value
		}
	case EntryModeSet:
		w.metrics[m.Name] = m.Value
	case EntryModePoint:
		w.pointMetrics = append(w.pointMetrics, m)
	}
}

func (w *Worker) resetMetrics() {
	w.mu.Lock()
	defer w.mu.Unlock()

	for name := range w.metrics {
		// Skip resetting these. They should probably go in their own table or something
		switch name {
		case TargetBytesPerSecond:
		case TotalRows:
		case TotalBytesCompressed:
		case TotalBytesUncompressed:
		case ActiveReaders:
		case ActiveInserters:
		case ActiveUsers:
		case BlockPoolCount:
		case BlockPoolCapacity:
		case BlockQueueLength:
		default:
			w.metrics[name] = 0
		}
	}

	w.pointMetrics = w.pointMetrics[:0]
}

func (w *Worker) pushMetrics(ctx context.Context) error {
	batch, err := w.conn.PrepareBatch(ctx, w.insertSQL)
	if err != nil {
		return fmt.Errorf("failed to prepare metrics batch: %w", err)
	}
	defer func(batch driver.Batch) {
		batchErr := batch.Close()
		if batchErr != nil {
			w.log.Error("failed to close batch", "err", batchErr)
		}
	}(batch)

	w.mu.Lock()
	now := time.Now()
	for name, value := range w.metrics {
		err = batch.Append(w.runID, string(name), "", now, value)
		if err != nil {
			w.mu.Unlock()
			return fmt.Errorf("failed to append metric (%s/%d) to batch: %w", name, value, err)
		}
	}

	for _, m := range w.pointMetrics {
		err = batch.Append(w.runID, string(m.Name), m.Meta, m.Timestamp, m.Value)
		if err != nil {
			w.mu.Unlock()
			return fmt.Errorf("failed to append point metric (%s/%d) to batch: %w", m.Name, m.Value, err)
		}
	}

	w.mu.Unlock()

	err = batch.Send()
	if err != nil {
		return fmt.Errorf("failed to send metrics: %w", err)
	}

	w.log.Debug("pushed metrics", "count", batch.Rows())

	return nil
}

func (w *Worker) IncrementMetric(name Name, delta uint64) {
	w.metricsQueue <- Entry{
		Mode:  EntryModeIncrement,
		Name:  name,
		Value: delta,
	}
}

func (w *Worker) DecrementMetric(name Name, delta uint64) {
	w.metricsQueue <- Entry{
		Mode:  EntryModeDecrement,
		Name:  name,
		Value: delta,
	}
}

func (w *Worker) SetMetric(name Name, value uint64) {
	w.metricsQueue <- Entry{
		Mode:  EntryModeSet,
		Name:  name,
		Value: value,
	}
}

func (w *Worker) GetMetric(name Name) uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.metrics[name]
}

func (w *Worker) AddMetricPoint(name Name, meta string, value uint64) {
	w.metricsQueue <- Entry{
		Mode:      EntryModePoint,
		Timestamp: time.Now(),
		Meta:      meta,
		Name:      name,
		Value:     value,
	}
}
