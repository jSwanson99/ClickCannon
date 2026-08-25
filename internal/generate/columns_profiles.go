package generate

import (
	"time"

	"github.com/ClickHouse/ClickCannon/internal/block"

	"github.com/ClickHouse/ch-go/proto"
)

// newGenArrayUInt64 creates an Array(UInt64) column.
func newGenArrayUInt64() *proto.ColArr[uint64] {
	return &proto.ColArr[uint64]{
		Data: new(proto.ColUInt64),
	}
}

// newGenArrayInt32 creates an Array(Int32) column.
func newGenArrayInt32() *proto.ColArr[int32] {
	return &proto.ColArr[int32]{
		Data: new(proto.ColInt32),
	}
}

// newGenArrayInt64 creates an Array(Int64) column.
func newGenArrayInt64() *proto.ColArr[int64] {
	return &proto.ColArr[int64]{
		Data: new(proto.ColInt64),
	}
}

// GenProfilesColumns implements block.SharedColumns for the generate path.
// Uses proto.ColLowCardinality[string] (with working Append) instead of
// the LowCard[string] wrapper used by the disk decode path.
type GenProfilesColumns struct {
	Timestamp          proto.ColDateTime64Raw
	ProfileID          proto.ColStr
	SampleType         *proto.ColLowCardinality[string]
	SampleUnit         *proto.ColLowCardinality[string]
	ServiceName        *proto.ColLowCardinality[string]
	ResourceAttributes *proto.ColMap[string, string]
	ScopeName          proto.ColStr
	ScopeVersion       proto.ColStr
	ProfileAttributes  *proto.ColMap[string, string]
	SampleAttributes   *proto.ColMap[string, string]
	StackHash          proto.ColUInt64
	Addresses          *proto.ColArr[uint64]
	FunctionNames      *proto.ColArr[string]
	FileNames          *proto.ColArr[string]
	LineNumbers        *proto.ColArr[int32]
	MappingFileNames   *proto.ColArr[string]
	Values             *proto.ColArr[int64]
	TimestampsUnixNano *proto.ColArr[uint64]
	DurationNano       proto.ColUInt64
	Period             proto.ColInt64
	PeriodType         *proto.ColLowCardinality[string]
	PeriodUnit         *proto.ColLowCardinality[string]
	TraceID            proto.ColStr
	SpanID             proto.ColStr

	names       []string
	cols        []proto.Column
	cachedInput proto.Input
}

func NewGenProfilesColumns() *GenProfilesColumns {
	c := &GenProfilesColumns{
		Timestamp:          proto.ColDateTime64Raw{ColDateTime64: proto.ColDateTime64{Location: time.UTC, Precision: proto.PrecisionNano, PrecisionSet: true}},
		ProfileID:          proto.ColStr{},
		SampleType:         newGenLowCardinality(),
		SampleUnit:         newGenLowCardinality(),
		ServiceName:        newGenLowCardinality(),
		ResourceAttributes: newGenMap(),
		ScopeName:          proto.ColStr{},
		ScopeVersion:       proto.ColStr{},
		ProfileAttributes:  newGenMap(),
		SampleAttributes:   newGenMap(),
		StackHash:          proto.ColUInt64{},
		Addresses:          newGenArrayUInt64(),
		FunctionNames:      newGenArrayLowCardinality(),
		FileNames:          newGenArrayLowCardinality(),
		LineNumbers:        newGenArrayInt32(),
		MappingFileNames:   newGenArrayLowCardinality(),
		Values:             newGenArrayInt64(),
		TimestampsUnixNano: newGenArrayUInt64(),
		DurationNano:       proto.ColUInt64{},
		Period:             proto.ColInt64{},
		PeriodType:         newGenLowCardinality(),
		PeriodUnit:         newGenLowCardinality(),
		TraceID:            proto.ColStr{},
		SpanID:             proto.ColStr{},
	}

	c.names = []string{
		"Timestamp",
		"ProfileId",
		"SampleType",
		"SampleUnit",
		"ServiceName",
		"ResourceAttributes",
		"ScopeName",
		"ScopeVersion",
		"ProfileAttributes",
		"SampleAttributes",
		"StackHash",
		"Addresses",
		"FunctionNames",
		"FileNames",
		"LineNumbers",
		"MappingFileNames",
		"Values",
		"TimestampsUnixNano",
		"DurationNano",
		"Period",
		"PeriodType",
		"PeriodUnit",
		"TraceId",
		"SpanId",
	}

	c.cols = []proto.Column{
		&c.Timestamp,
		&c.ProfileID,
		c.SampleType,
		c.SampleUnit,
		c.ServiceName,
		c.ResourceAttributes,
		&c.ScopeName,
		&c.ScopeVersion,
		c.ProfileAttributes,
		c.SampleAttributes,
		&c.StackHash,
		c.Addresses,
		c.FunctionNames,
		c.FileNames,
		c.LineNumbers,
		c.MappingFileNames,
		c.Values,
		c.TimestampsUnixNano,
		&c.DurationNano,
		&c.Period,
		c.PeriodType,
		c.PeriodUnit,
		&c.TraceID,
		&c.SpanID,
	}

	c.cachedInput = make(proto.Input, len(c.names))
	for i := range c.names {
		c.cachedInput[i] = proto.InputColumn{Name: c.names[i], Data: c.cols[i]}
	}

	return c
}

func (c *GenProfilesColumns) Reset() {
	for _, col := range c.cols {
		col.Reset()
	}
}

func (c *GenProfilesColumns) Results() proto.Results { return nil }

func (c *GenProfilesColumns) Input() proto.Input { return c.cachedInput }

func (c *GenProfilesColumns) FirstTimestamp() time.Time {
	if len(c.Timestamp.Data) > 0 {
		return c.Timestamp.Data[0].Time(c.Timestamp.Precision)
	}
	return time.Time{}
}

func (c *GenProfilesColumns) LastTimestamp() time.Time {
	if len(c.Timestamp.Data) > 0 {
		return c.Timestamp.Data[len(c.Timestamp.Data)-1].Time(c.Timestamp.Precision)
	}
	return time.Time{}
}

func (c *GenProfilesColumns) UpdateDate()                               {}
func (c *GenProfilesColumns) ShiftTimestamp(_ block.ReplayTimeSnapshot) {}
func (c *GenProfilesColumns) UpdateTimestampNow()                       {}
func (c *GenProfilesColumns) UpdateTimestampMinute()                    {}
func (c *GenProfilesColumns) MutateIDs(_ int)                           {}
