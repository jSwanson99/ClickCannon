package metrics

import "time"

type Store interface {
	IncrementMetric(name Name, delta uint64)
	// IncrementMetricWithAttr increments a metric keyed by name + a single attribute.
	// Use this for per-worker (or other per-entity) counters tracked under a distinct metric name.
	IncrementMetricWithAttr(name Name, delta uint64, attrKey, attrValue string)
	DecrementMetric(name Name, delta uint64)
	SetMetric(name Name, value uint64)
	GetMetric(name Name) uint64

	// AddMetricPoint - The other metric functions store points every 1s while
	// this function stores an individual row.
	AddMetricPoint(name Name, value uint64)
	// AddMetricPointWithAttributes - same as AddMetricPoint but with attributes map
	AddMetricPointWithAttributes(name Name, value uint64, attributes map[string]string)
}

type EntryMode int

const (
	// EntryModeIncrement Increments a metric by name
	EntryModeIncrement EntryMode = iota
	// EntryModeDecrement Decrements a metric by name
	EntryModeDecrement
	// EntryModeSet Sets a metric by name
	EntryModeSet
	// EntryModePoint Stores an individual data point value instead of a cumulative value
	EntryModePoint
)

type Entry struct {
	Mode       EntryMode
	Timestamp  time.Time
	Name       Name
	AttrKey    string
	AttrValue  string
	Attributes map[string]string // only used for point metrics
	Value      uint64
}
