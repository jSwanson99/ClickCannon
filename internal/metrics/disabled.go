package metrics

// DisabledStore is a no-op metrics store for when metrics are disabled
type DisabledStore struct {
}

func NewDisabledStore() *DisabledStore {
	return &DisabledStore{}
}

func (d *DisabledStore) IncrementMetric(name Name, delta uint64) {
}

func (d *DisabledStore) DecrementMetric(name Name, delta uint64) {}

func (d *DisabledStore) SetMetric(name Name, value uint64) {}

func (d *DisabledStore) GetMetric(name Name) uint64 {
	return 0
}

func (d *DisabledStore) AddMetricPoint(name Name, value uint64) {}

func (d *DisabledStore) AddMetricPointWithAttributes(name Name, value uint64, attr map[string]string) {
}
