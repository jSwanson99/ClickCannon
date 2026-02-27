package metrics

import "time"

type Store interface {
	IncrementMetric(name Name, delta uint64)
	DecrementMetric(name Name, delta uint64)
	SetMetric(name Name, value uint64)
	GetMetric(name Name) uint64

	// AddMetricPoint - The other metric functions store points every 1s while
	// this function stores an individual row.
	AddMetricPoint(name Name, meta string, value uint64)
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
	Mode      EntryMode
	Timestamp time.Time
	Name      Name
	Meta      string
	Value     uint64
}
