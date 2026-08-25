package api

import (
	"sync"

	"github.com/ClickHouse/ClickCannon/internal/metrics"
)

// Stats is a cumulative snapshot of a run, for asserting from outside that the
// pipeline moved data. It is available even when Config.Metrics is disabled,
// because the Runner keeps its own counters.
//
// It deliberately carries only what an external check needs. ClickCannon's
// tuning telemetry — byte counts, worker gauges, block pool and queue depth —
// goes to ClickHouse via Config.Metrics and is charted by grafana.json.
type Stats struct {
	// Source: the denominator for detecting rows dropped before the sink.
	GeneratedRows uint64
	DiskRows      uint64

	// Write path.
	InsertedRows    uint64
	InsertedBatches uint64

	// OTLP export path. A non-zero OTelExportsFailed is the only in-process
	// evidence that the collector rejected data.
	OTelRows          uint64
	OTelBatches       uint64
	OTelExportsFailed uint64

	// Read path.
	QueriesOK     uint64
	QueriesFailed uint64
}

// memStore keeps counters in memory and optionally forwards to the
// ClickHouse-backed worker, so enabling Config.Metrics does not cost Stats.
type memStore struct {
	mu     sync.RWMutex
	values map[metrics.Name]uint64

	delegate metrics.Store
}

func newMemStore(delegate metrics.Store) *memStore {
	return &memStore{
		values:   make(map[metrics.Name]uint64, 64),
		delegate: delegate,
	}
}

func (m *memStore) IncrementMetric(name metrics.Name, delta uint64) {
	m.mu.Lock()
	m.values[name] += delta
	m.mu.Unlock()

	if m.delegate != nil {
		m.delegate.IncrementMetric(name, delta)
	}
}

func (m *memStore) IncrementMetricWithAttr(name metrics.Name, delta uint64, attrKey, attrValue string) {
	// Per-worker metrics use distinct names from their totals, so folding the
	// attribute away cannot double-count.
	m.mu.Lock()
	m.values[name] += delta
	m.mu.Unlock()

	if m.delegate != nil {
		m.delegate.IncrementMetricWithAttr(name, delta, attrKey, attrValue)
	}
}

func (m *memStore) DecrementMetric(name metrics.Name, delta uint64) {
	m.mu.Lock()
	if cur := m.values[name]; cur < delta {
		m.values[name] = 0
	} else {
		m.values[name] = cur - delta
	}
	m.mu.Unlock()

	if m.delegate != nil {
		m.delegate.DecrementMetric(name, delta)
	}
}

func (m *memStore) SetMetric(name metrics.Name, value uint64) {
	m.mu.Lock()
	m.values[name] = value
	m.mu.Unlock()

	if m.delegate != nil {
		m.delegate.SetMetric(name, value)
	}
}

func (m *memStore) GetMetric(name metrics.Name) uint64 {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.values[name]
}

func (m *memStore) AddMetricPoint(name metrics.Name, value uint64) {
	if m.delegate != nil {
		m.delegate.AddMetricPoint(name, value)
	}
}

func (m *memStore) AddMetricPointWithAttributes(name metrics.Name, value uint64, attributes map[string]string) {
	if m.delegate != nil {
		m.delegate.AddMetricPointWithAttributes(name, value, attributes)
	}
}

func (m *memStore) snapshot() Stats {
	m.mu.RLock()
	defer m.mu.RUnlock()

	get := func(n metrics.Name) uint64 { return m.values[n] }

	return Stats{
		GeneratedRows: get(metrics.GenerateRowsTotal),
		DiskRows:      get(metrics.DiskRowsTotal),

		InsertedRows:    get(metrics.InsertRowsTotal),
		InsertedBatches: get(metrics.InsertBatchesTotal),

		OTelRows:          get(metrics.OTelRowsTotal),
		OTelBatches:       get(metrics.OTelBatchesTotal),
		OTelExportsFailed: get(metrics.OTelExportsFailedTotal),

		QueriesOK:     get(metrics.QueriesOkTotal),
		QueriesFailed: get(metrics.QueriesFailedTotal),
	}
}

var _ metrics.Store = (*memStore)(nil)
