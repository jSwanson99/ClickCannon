package block

import (
	"time"

	"github.com/ClickHouse/ch-go/proto"
)

type SharedColumns interface {
	// Reset underlying data
	Reset()
	// Results structure for block decoding
	Results() proto.Results
	// Input structure for block inserting
	Input() proto.Input

	// FirstTimestamp Returns the first timestamp in the block
	FirstTimestamp() time.Time
	// LastTimestamp Returns the last timestamp in the block
	LastTimestamp() time.Time

	// UpdateDate for shifting the date component on old data to today
	UpdateDate()
	// ShiftTimestamp applies a replay time snapshot for shifting the timestamp
	ShiftTimestamp(snapshot ReplayTimeSnapshot)
	// UpdateTimestampNow for shifting the timestamp to be the current time
	UpdateTimestampNow()
	UpdateTimestampMinute()
}

// ShiftDateToToday shifts the time.Time to current date without affecting time component
func ShiftDateToToday(oldTime time.Time) time.Time {
	now := time.Now()
	hour, minute, sec := oldTime.Clock()
	nsec := oldTime.Nanosecond()
	loc := oldTime.Location()

	newTime := time.Date(
		now.Year(),
		now.Month(),
		now.Day(),
		hour,
		minute,
		sec,
		nsec,
		loc,
	)

	return newTime
}

// ShiftTimestampMinute shifts the time.Time to current minute without affecting seconds component
func ShiftTimestampMinute(original time.Time) time.Time {
	now := time.Now()
	newTime := time.Date(
		now.Year(),
		now.Month(),
		now.Day(),
		now.Hour(),
		now.Minute(),
		original.Second(),
		original.Nanosecond(),
		original.Location(),
	)

	return newTime
}
