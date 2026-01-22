package main

import (
	"context"
	"fmt"
	"log"
	"sync"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"time"
)

type metricsWorker struct {
	runID     string
	conn      driver.Conn
	insertSQL string

	dataType             string
	targetBytesPerSecond uint64

	metricsQueue chan MetricEntry
	mu           sync.Mutex
	metrics      map[MetricName]uint64
}

type MetricsStore interface {
	IncrementMetric(name MetricName, delta uint64)
	DecrementMetric(name MetricName, delta uint64)
	SetMetric(name MetricName, value uint64)
	GetMetric(name MetricName) uint64
}

type MetricName string

const (
	MetricNameReadRowsPerSecond              MetricName = "read_rows_per_second"
	MetricNameReadCompressedBytesPerSecond   MetricName = "read_compressed_bytes_per_second"
	MetricNameReadUncompressedBytesPerSecond MetricName = "read_uncompressed_bytes_per_second"
	MetricNameInsertRowsPerSecond            MetricName = "insert_rows_per_second"
	MetricNameInsertBytesPerSecond           MetricName = "insert_bytes_per_second"
	MetricNameActiveReaders                  MetricName = "active_readers"
	MetricNameActiveInserters                MetricName = "active_inserters"
	MetricNameActiveUsers                    MetricName = "active_users"
	MetricNameUserQueriesPerSecond           MetricName = "user_queries_per_second"

	MetricNameTargetBytesPerSecond   MetricName = "target_bytes_per_second"
	MetricNameTotalRows              MetricName = "total_rows"
	MetricNameTotalBytesCompressed   MetricName = "total_bytes_compressed"
	MetricNameTotalBytesUncompressed MetricName = "total_bytes_uncompressed"
)

type MetricEntryMode int

const (
	MetricEntryModeIncrement MetricEntryMode = iota
	MetricEntryModeDecrement
	MetricEntryModeSet
)

type MetricEntry struct {
	Mode  MetricEntryMode
	Name  MetricName
	Value uint64
}

func newMetricsWorker(runID, dataType string, targetBytesPerSecond uint64, clickhouseDSN, metricsDatabase, runTable, metricsTable string) (*metricsWorker, error) {
	w := metricsWorker{
		runID:                runID,
		dataType:             dataType,
		targetBytesPerSecond: targetBytesPerSecond,

		metricsQueue: make(chan MetricEntry, 10_000),
		metrics:      make(map[MetricName]uint64),
	}

	if clickhouseDSN != "" {
		opt, err := clickhouse.ParseDSN(clickhouseDSN)
		if err != nil {
			return nil, fmt.Errorf("failed to parse DSN: %w", err)
		}

		w.conn, err = clickhouse.Open(opt)
		if err != nil {
			return nil, fmt.Errorf("failed to connect: %w", err)
		}
		log.Println("metrics clickhouse connected")

		err = w.conn.Exec(context.Background(), fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %q", metricsDatabase))
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
		`, metricsDatabase, runTable)

		err = w.conn.Exec(context.Background(), runDDL)
		if err != nil {
			return nil, fmt.Errorf("failed to create run table: %w", err)
		}

		insertRunSQL := fmt.Sprintf(`INSERT INTO %q.%q VALUES (?, ?, ?, ?)`, metricsDatabase, runTable)
		err = w.conn.Exec(context.Background(), insertRunSQL, runID, time.Now(), dataType, targetBytesPerSecond)
		if err != nil {
			return nil, fmt.Errorf("failed to insert run: %w", err)
		}

		metricsDDL := fmt.Sprintf(`
			CREATE TABLE IF NOT EXISTS %q.%q (
				run_id String,
				metric_name LowCardinality(String),
				timestamp DateTime64(3),
				value UInt64
			) Engine = MergeTree()
			ORDER BY (run_id, metric_name, timestamp)
		`, metricsDatabase, metricsTable)

		err = w.conn.Exec(context.Background(), metricsDDL)
		if err != nil {
			return nil, fmt.Errorf("failed to create metrics table: %w", err)
		}

		w.insertSQL = fmt.Sprintf(`INSERT INTO %q.%q VALUES (?, ?, ?, ?)`, metricsDatabase, metricsTable)
	}

	return &w, nil
}

func (w *metricsWorker) start(ctx context.Context) {
	go w.processMetrics(ctx)

	for ctx.Err() == nil {
		select {
		case <-time.After(1 * time.Second):
			w.mu.Lock()
			activeReaders := w.metrics[MetricNameActiveReaders]
			activeInserters := w.metrics[MetricNameActiveInserters]
			activeUsers := w.metrics[MetricNameActiveUsers]
			userQueriesPerSecond := w.metrics[MetricNameUserQueriesPerSecond]
			readRowsPerSecond := w.metrics[MetricNameReadRowsPerSecond]
			readCompressedBytesPerSecond := w.metrics[MetricNameReadCompressedBytesPerSecond]
			readUncompressedBytesPerSecond := w.metrics[MetricNameReadUncompressedBytesPerSecond]
			insertRowsPerSecond := w.metrics[MetricNameInsertRowsPerSecond]
			insertBytesPerSecond := w.metrics[MetricNameInsertBytesPerSecond]
			totalRows := w.metrics[MetricNameTotalRows]
			totalCompressedBytes := w.metrics[MetricNameTotalBytesCompressed]
			totalUncompressedBytes := w.metrics[MetricNameTotalBytesUncompressed]
			w.mu.Unlock()

			// this should be dynamically adjustable in the future, but for now we set it constantly
			w.SetMetric(MetricNameTargetBytesPerSecond, w.targetBytesPerSecond)

			log.Printf("Read(%d) %s rows/s %s/s (%s/s compressed), Insert(%d) %s rows/s %s/s, Total %s rows %s (%s compressed) Queries(%d) %s/s\n",
				activeReaders,
				FormatNumber(readRowsPerSecond),
				FormatBytes(readUncompressedBytesPerSecond),
				FormatBytes(readCompressedBytesPerSecond),
				activeInserters,
				FormatNumber(insertRowsPerSecond),
				FormatBytes(insertBytesPerSecond),
				FormatNumber(totalRows),
				FormatBytes(totalUncompressedBytes),
				FormatBytes(totalCompressedBytes),
				activeUsers,
				FormatNumber(userQueriesPerSecond),
			)

			if w.insertSQL != "" {
				err := w.pushMetrics(context.Background())
				if err != nil {
					fmt.Println(fmt.Errorf("failed to push metrics: %w", err))
				}
			}

			w.resetMetrics()
		case <-ctx.Done():
			return
		}
	}
}

func (w *metricsWorker) processMetrics(ctx context.Context) {
	for {
		select {
		case m := <-w.metricsQueue:
			w.applyMetricEntry(m)
		case <-ctx.Done():
			return
		}
	}
}

func (w *metricsWorker) applyMetricEntry(m MetricEntry) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if _, ok := w.metrics[m.Name]; !ok {
		w.metrics[m.Name] = 0
	}

	switch m.Mode {
	case MetricEntryModeIncrement:
		w.metrics[m.Name] += m.Value
	case MetricEntryModeDecrement:
		if m.Value >= w.metrics[m.Name] {
			w.metrics[m.Name] = 0
		} else {
			w.metrics[m.Name] -= m.Value
		}
	case MetricEntryModeSet:
		w.metrics[m.Name] = m.Value
	}
}

func (w *metricsWorker) resetMetrics() {
	w.mu.Lock()
	defer w.mu.Unlock()

	for name := range w.metrics {
		// Skip resetting these. They should probably go in their own table or something
		switch name {
		case MetricNameTargetBytesPerSecond:
		case MetricNameTotalRows:
		case MetricNameTotalBytesCompressed:
		case MetricNameTotalBytesUncompressed:
		case MetricNameActiveReaders:
		case MetricNameActiveInserters:
		case MetricNameActiveUsers:
		default:
			w.metrics[name] = 0
		}
	}
}

func (w *metricsWorker) pushMetrics(ctx context.Context) error {
	batch, err := w.conn.PrepareBatch(ctx, w.insertSQL)
	if err != nil {
		return fmt.Errorf("failed to prepare metrics batch: %w", err)
	}
	defer batch.Close()

	w.mu.Lock()
	now := time.Now()
	for name, value := range w.metrics {
		err = batch.Append(w.runID, string(name), now, value)
		if err != nil {
			w.mu.Unlock()
			return fmt.Errorf("failed to append metric (%s/%d) to batch: %w", name, value, err)
		}
	}
	w.mu.Unlock()

	err = batch.Send()
	if err != nil {
		log.Println(fmt.Errorf("failed to send metrics: %w", err))
	}

	return nil
}

func (w *metricsWorker) IncrementMetric(name MetricName, delta uint64) {
	w.metricsQueue <- MetricEntry{
		Mode:  MetricEntryModeIncrement,
		Name:  name,
		Value: delta,
	}
}

func (w *metricsWorker) DecrementMetric(name MetricName, delta uint64) {
	w.metricsQueue <- MetricEntry{
		Mode:  MetricEntryModeDecrement,
		Name:  name,
		Value: delta,
	}
}

func (w *metricsWorker) SetMetric(name MetricName, value uint64) {
	w.metricsQueue <- MetricEntry{
		Mode:  MetricEntryModeSet,
		Name:  name,
		Value: value,
	}
}

func (w *metricsWorker) GetMetric(name MetricName) uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.metrics[name]
}
