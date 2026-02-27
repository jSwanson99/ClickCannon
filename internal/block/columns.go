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
	// UpdateDate for shifting the date component on old data to today
	UpdateDate()
	// FirstTimestamp Returns the first timestamp in the block
	FirstTimestamp() time.Time
	// UpdateTimestamp for shifting the timestamp to be relative to current time
	UpdateTimestamp(startTime time.Time)
	// UpdateTimestampNow for shifting the timestamp to be the current time
	UpdateTimestampNow()
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

var execStartTime = time.Now()

// ShiftTimestamp shifts the time.Time to be relative to the program start time.
// startTime is the reference point (first data point for the data set)
// oldTime is the timestamp to shift
func ShiftTimestamp(startTime, oldTime time.Time) time.Time {
	offset := oldTime.Sub(startTime)
	return execStartTime.Add(offset)
}
