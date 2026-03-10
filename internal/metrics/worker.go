package metrics

import (
	"context"
	"fmt"
	"log/slog"
	"otelspam/internal/block"
	"runtime"
	"sync"
	"syscall"
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

	blockPool  block.Pool
	blockQueue chan block.SharedColumns

	metricsQueue chan Entry
	mu           sync.Mutex
	metrics      map[Name]uint64
	pointMetrics []Entry
}

func NewWorker(log *slog.Logger, runID, configName, dataType string, targetBytesPerSecond uint64, runAttr map[string]string, cfg *Config, blockPool block.Pool, blockQueue chan block.SharedColumns) (*Worker, error) {
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
				name String,
				timestamp DateTime64(3),
			    data_type LowCardinality(String),
			    target_bytes_per_second UInt64,
			    attributes Map(LowCardinality(String), String)
			) Engine = MergeTree()
			ORDER BY (run_id, timestamp)
		`, cfg.Database, cfg.RunTable)

		err = w.conn.Exec(context.Background(), runDDL)
		if err != nil {
			return nil, fmt.Errorf("failed to create run table: %w", err)
		}

		insertRunSQL := fmt.Sprintf(`INSERT INTO %q.%q VALUES (?, ?, ?, ?, ?, ?)`, cfg.Database, cfg.RunTable)
		err = w.conn.Exec(context.Background(), insertRunSQL, runID, configName, time.Now(), dataType, targetBytesPerSecond, runAttr)
		if err != nil {
			return nil, fmt.Errorf("failed to insert run: %w", err)
		}
		w.log.Info("inserted run info")

		metricsDDL := fmt.Sprintf(`
			CREATE TABLE IF NOT EXISTS %q.%q (
				run_id String,
				metric_name LowCardinality(String),
				timestamp DateTime64(3),
				value UInt64,
				attributes Map(LowCardinality(String), String)
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

			w.collectRuntimeMetrics()

			if w.insertSQL != "" {
				err := w.pushMetrics(ctx)
				if err != nil {
					w.log.Error("failed to push metrics", "err", err)
					continue
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

func (w *Worker) collectRuntimeMetrics() {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)

	w.SetMetric(ProgramHeapAllocBytes, ms.HeapAlloc)
	w.SetMetric(ProgramSysBytes, ms.Sys)
	w.SetMetric(ProgramNumGoroutines, uint64(runtime.NumGoroutine()))
	w.SetMetric(ProgramNumGC, uint64(ms.NumGC))
	w.SetMetric(ProgramPauseTotalNs, ms.PauseTotalNs)
	w.SetMetric(ProgramNextGCBytes, ms.NextGC)

	w.SetMetric(ProgramNumCPU, uint64(runtime.NumCPU()))

	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err == nil {
		w.SetMetric(ProgramCPUUserNs, uint64(ru.Utime.Nano()))
		w.SetMetric(ProgramCPUSysNs, uint64(ru.Stime.Nano()))
	}
}

func (w *Worker) resetMetrics() {
	w.mu.Lock()
	defer w.mu.Unlock()

	for name := range w.metrics {
		// Skip resetting these. They should probably go in their own table or something
		switch name {
		case TotalRows:
		case TotalBytesCompressed:
		case TotalBytesUncompressed:
		case TargetBytesPerSecond:
		case TargetWorkerBytesPerSecond:
		case ActiveReaders:
		case ActiveInserters:
		case ActiveUsers:
		case BlockPoolCount:
		case BlockPoolCapacity:
		case BlockQueueLength:
		case ProgramHeapAllocBytes:
		case ProgramSysBytes:
		case ProgramNumGoroutines:
		case ProgramNumGC:
		case ProgramPauseTotalNs:
		case ProgramNextGCBytes:
		case ProgramCPUUserNs:
		case ProgramCPUSysNs:
		case ProgramNumCPU:
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
		err = batch.Append(w.runID, string(name), now, value, map[string]string{})
		if err != nil {
			w.mu.Unlock()
			return fmt.Errorf("failed to append metric (%s/%d) to batch: %w", name, value, err)
		}
	}

	for _, m := range w.pointMetrics {
		attr := m.Attributes
		if attr == nil {
			attr = map[string]string{}
		}

		err = batch.Append(w.runID, string(m.Name), m.Timestamp, m.Value, attr)
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
	select {
	case w.metricsQueue <- Entry{
		Mode:  EntryModeIncrement,
		Name:  name,
		Value: delta,
	}:
	default:
	}
}

func (w *Worker) DecrementMetric(name Name, delta uint64) {
	select {
	case w.metricsQueue <- Entry{
		Mode:  EntryModeDecrement,
		Name:  name,
		Value: delta,
	}:
	default:
	}
}

func (w *Worker) SetMetric(name Name, value uint64) {
	select {
	case w.metricsQueue <- Entry{
		Mode:  EntryModeSet,
		Name:  name,
		Value: value,
	}:
	default:
	}
}

func (w *Worker) GetMetric(name Name) uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.metrics[name]
}

func (w *Worker) AddMetricPoint(name Name, value uint64) {
	select {
	case w.metricsQueue <- Entry{
		Mode:      EntryModePoint,
		Timestamp: time.Now(),
		Name:      name,
		Value:     value,
	}:
	default:
	}
}

func (w *Worker) AddMetricPointWithAttributes(name Name, value uint64, attributes map[string]string) {
	select {
	case w.metricsQueue <- Entry{
		Mode:       EntryModePoint,
		Timestamp:  time.Now(),
		Name:       name,
		Attributes: attributes,
		Value:      value,
	}:
	default:
	}
}
