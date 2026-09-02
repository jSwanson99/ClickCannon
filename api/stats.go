package api

import (
	"github.com/ClickHouse/ClickCannon/internal/metrics"
)

// Stats is a cumulative snapshot of a run, read from the metrics store. Only
// populated when Config.Metrics is enabled, and best effort: the store drops
// entries rather than blocking a producer.
type Stats struct {
	// rows generated
	GeneratedRows uint64
	// rows replayed from disk
	DiskRows uint64
	// total rows inserted directly
	InsertedRows uint64
	// number of batches all inserts happened within
	InsertedBatches uint64
	// number of rows pushed via otlp
	OTelRows uint64
	// number of batches pushed via otlp
	OTelBatches uint64
	// number of exports which failed and were retried
	OTelExportsFailed uint64
	// read queries without an error
	QueriesOK uint64
	// read queries with an error
	QueriesFailed uint64
}

func statsFrom(store metrics.Store) Stats {
	return Stats{
		GeneratedRows:     store.GetMetric(metrics.GenerateRowsTotal),
		DiskRows:          store.GetMetric(metrics.DiskRowsTotal),
		InsertedRows:      store.GetMetric(metrics.InsertRowsTotal),
		InsertedBatches:   store.GetMetric(metrics.InsertBatchesTotal),
		OTelRows:          store.GetMetric(metrics.OTelRowsTotal),
		OTelBatches:       store.GetMetric(metrics.OTelBatchesTotal),
		OTelExportsFailed: store.GetMetric(metrics.OTelExportsFailedTotal),
		QueriesOK:         store.GetMetric(metrics.QueriesOkTotal),
		QueriesFailed:     store.GetMetric(metrics.QueriesFailedTotal),
	}
}
