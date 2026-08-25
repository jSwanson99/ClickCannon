package api

import (
	"sync"

	"github.com/ClickHouse/ClickCannon/internal/metrics"
)

// Stats is a cumulative snapshot of a run. It is available even when
// Config.Metrics is disabled, because the Runner keeps its own counters.
type Stats struct {
	GeneratedRows   uint64
	GeneratedBlocks uint64
	DiskRows        uint64

	InsertedRows              uint64
	InsertedBatches           uint64
	InsertedBytesUncompressed uint64
	InsertedBytesCompressed   uint64

	OTelRows          uint64
	OTelBatches       uint64
	OTelBytes         uint64
	OTelExportsFailed uint64

	QueriesOK     uint64
	QueriesFailed uint64

	ActiveGenerators    uint64
	ActiveReaders       uint64
	ActiveInserters     uint64
	ActiveOTelExporters uint64
	ActiveUsers         uint64

	BlockQueueLength   int
	BlockQueueCapacity int
	BlockPoolAvailable int
	BlockPoolCapacity  int
	BlocksRetired      int64
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
		GeneratedRows:   get(metrics.GenerateRowsTotal),
		GeneratedBlocks: get(metrics.GenerateBlocksTotal),
		DiskRows:        get(metrics.DiskRowsTotal),

		InsertedRows:              get(metrics.InsertRowsTotal),
		InsertedBatches:           get(metrics.InsertBatchesTotal),
		InsertedBytesUncompressed: get(metrics.InsertBytesUncompressedTotal),
		InsertedBytesCompressed:   get(metrics.InsertBytesCompressedTotal),

		OTelRows:          get(metrics.OTelRowsTotal),
		OTelBatches:       get(metrics.OTelBatchesTotal),
		OTelBytes:         get(metrics.OTelBytesTotal),
		OTelExportsFailed: get(metrics.OTelExportsFailedTotal),

		QueriesOK:     get(metrics.QueriesOkTotal),
		QueriesFailed: get(metrics.QueriesFailedTotal),

		ActiveGenerators:    get(metrics.ActiveGenerators),
		ActiveReaders:       get(metrics.ActiveReaders),
		ActiveInserters:     get(metrics.ActiveInserters),
		ActiveOTelExporters: get(metrics.ActiveOTelExporters),
		ActiveUsers:         get(metrics.ActiveUsers),
	}
}

var _ metrics.Store = (*memStore)(nil)
