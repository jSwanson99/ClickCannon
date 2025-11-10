package main

import (
	"context"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"sync/atomic"
	"time"
)

type metricsWorker struct {
	runID     string
	conn      driver.Conn
	insertSQL string

	dataType             string
	targetBytesPerSecond int64

	ReadRowsPerSecond    atomic.Int64
	ReadBytesPerSecond   atomic.Int64
	InsertRowsPerSecond  atomic.Int64
	InsertBytesPerSecond atomic.Int64
	ActiveReaders        atomic.Int64
	ActiveInserters      atomic.Int64
}

func newMetricsWorker(runID, dataType string, targetBytesPerSecond int64, clickhouseDSN, metricsDatabase, metricsTable string) (*metricsWorker, error) {
	w := metricsWorker{
		runID:                runID,
		dataType:             dataType,
		targetBytesPerSecond: targetBytesPerSecond,
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
		fmt.Println("metrics clickhouse connected")

		err = w.conn.Exec(context.Background(), fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %q", metricsDatabase))
		if err != nil {
			return nil, fmt.Errorf("failed to create metrics database: %w", err)
		}

		metricsDDL := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %q.%q (
		    run_id String,
		    timestamp DateTime64(3),
		    data_type LowCardinality(String),
		    target_bytes_per_second UInt64,
		    read_rows UInt64,
		    read_bytes UInt64,
		    insert_rows UInt64,
		    insert_bytes UInt64,
		    total_rows UInt64,
		    total_bytes UInt64,
		    active_readers UInt8,
		    active_inserters UInt8,
		) Engine = MergeTree()
		ORDER BY (run_id, timestamp)
	`, metricsDatabase, metricsTable)

		err = w.conn.Exec(context.Background(), metricsDDL)
		if err != nil {
			return nil, fmt.Errorf("failed to create metrics table: %w", err)
		}

		w.insertSQL = fmt.Sprintf(`INSERT INTO %q.%q VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, metricsDatabase, metricsTable)
	}

	return &w, nil
}

func (w *metricsWorker) start(ctx context.Context) {
	var totalRows, totalBytes int64
	for ctx.Err() == nil {
		select {
		case <-time.After(1 * time.Second):
			now := time.Now()
			activeReaders := w.ActiveReaders.Load()
			activeInserters := w.ActiveInserters.Load()
			readRowsPerSecond := w.ReadRowsPerSecond.Load()
			readBytesPerSecond := w.ReadBytesPerSecond.Load()
			insertRowsPerSecond := w.InsertRowsPerSecond.Load()
			insertBytesPerSecond := w.InsertBytesPerSecond.Load()
			totalRows += readRowsPerSecond
			totalBytes += readBytesPerSecond

			fmt.Printf("Read(%d) %s rows/s %s/s, Insert(%d) %s rows/s %s/s, Total %s rows %s\n",
				activeReaders,
				FormatNumber(readRowsPerSecond),
				FormatBytes(readBytesPerSecond),
				activeInserters,
				FormatNumber(w.InsertRowsPerSecond.Load()),
				FormatBytes(w.InsertBytesPerSecond.Load()),
				FormatNumber(totalRows),
				FormatBytes(totalBytes),
			)

			w.ReadRowsPerSecond.Store(0)
			w.ReadBytesPerSecond.Store(0)
			w.InsertRowsPerSecond.Store(0)
			w.InsertBytesPerSecond.Store(0)

			if w.insertSQL != "" {
				err := w.conn.Exec(context.Background(), w.insertSQL,
					w.runID, now, w.dataType, w.targetBytesPerSecond,
					readRowsPerSecond, readBytesPerSecond,
					insertRowsPerSecond, insertBytesPerSecond,
					totalRows, totalBytes,
					activeReaders, activeInserters,
				)
				if err != nil {
					fmt.Println(fmt.Errorf("failed to send metrics: %w", err))
				}
			}
		case <-ctx.Done():
			return
		}
	}
}
