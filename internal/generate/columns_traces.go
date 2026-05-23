package generate

import (
	"time"

	"clickcannon/internal/block"

	"github.com/ClickHouse/ch-go/proto"
)

// newGenLowCardinality creates a ColLowCardinality[string] suitable for Append operations.
func newGenLowCardinality() *proto.ColLowCardinality[string] {
	return proto.NewLowCardinality[string](new(proto.ColStr))
}

// newGenMap creates a ColMap[string, string] with LowCardinality keys.
func newGenMap() *proto.ColMap[string, string] {
	keys := newGenLowCardinality()
	values := new(proto.ColStr)
	return proto.NewMap[string, string](keys, values)
}

// newGenArrayLowCardinality creates an Array(LowCardinality(String)) column.
func newGenArrayLowCardinality() *proto.ColArr[string] {
	return &proto.ColArr[string]{
		Data: newGenLowCardinality(),
	}
}

// newGenArrayMapLowCardinality creates an Array(Map(LowCardinality(String), String)) column.
func newGenArrayMapLowCardinality() *proto.ColArr[map[string]string] {
	return &proto.ColArr[map[string]string]{
		Data: newGenMap(),
	}
}

// newGenArrayDateTime64 creates an Array(DateTime64) column.
func newGenArrayDateTime64() *proto.ColArr[proto.DateTime64] {
	inner := proto.ColDateTime64Raw{
		ColDateTime64: proto.ColDateTime64{
			Location:     time.UTC,
			Precision:    proto.PrecisionNano,
			PrecisionSet: true,
		},
	}
	return &proto.ColArr[proto.DateTime64]{
		Data: &inner,
	}
}

// newGenArrayString creates an Array(String) column.
func newGenArrayString() *proto.ColArr[string] {
	return &proto.ColArr[string]{
		Data: new(proto.ColStr),
	}
}

// GenTracesColumns implements block.SharedColumns for the generate path.
// Uses proto.ColLowCardinality[string] (with working Append) instead of
// the LowCard[string] wrapper used by the disk decode path.
type GenTracesColumns struct {
	Timestamp          proto.ColDateTime64Raw
	TraceID            proto.ColStr
	SpanID             proto.ColStr
	ParentSpanID       proto.ColStr
	TraceState         proto.ColStr
	SpanName           *proto.ColLowCardinality[string]
	SpanKind           *proto.ColLowCardinality[string]
	ServiceName        *proto.ColLowCardinality[string]
	ResourceAttributes *proto.ColMap[string, string]
	ScopeName          proto.ColStr
	ScopeVersion       proto.ColStr
	SpanAttributes     *proto.ColMap[string, string]
	Duration           proto.ColUInt64
	StatusCode         *proto.ColLowCardinality[string]
	StatusMessage      proto.ColStr
	EventsTimestamps   *proto.ColArr[proto.DateTime64]
	EventsNames        *proto.ColArr[string]
	EventsAttributes   *proto.ColArr[map[string]string]
	LinksTraceIDs      *proto.ColArr[string]
	LinksSpanIDs       *proto.ColArr[string]
	LinksTraceStates   *proto.ColArr[string]
	LinksAttributes    *proto.ColArr[map[string]string]

	names       []string
	cols        []proto.Column
	cachedInput proto.Input
}

func NewGenTracesColumns() *GenTracesColumns {
	c := &GenTracesColumns{
		Timestamp:          proto.ColDateTime64Raw{ColDateTime64: proto.ColDateTime64{Location: time.UTC, Precision: proto.PrecisionNano, PrecisionSet: true}},
		TraceID:            proto.ColStr{},
		SpanID:             proto.ColStr{},
		ParentSpanID:       proto.ColStr{},
		TraceState:         proto.ColStr{},
		SpanName:           newGenLowCardinality(),
		SpanKind:           newGenLowCardinality(),
		ServiceName:        newGenLowCardinality(),
		ResourceAttributes: newGenMap(),
		ScopeName:          proto.ColStr{},
		ScopeVersion:       proto.ColStr{},
		SpanAttributes:     newGenMap(),
		Duration:           proto.ColUInt64{},
		StatusCode:         newGenLowCardinality(),
		StatusMessage:      proto.ColStr{},
		EventsTimestamps:   newGenArrayDateTime64(),
		EventsNames:        newGenArrayLowCardinality(),
		EventsAttributes:   newGenArrayMapLowCardinality(),
		LinksTraceIDs:      newGenArrayString(),
		LinksSpanIDs:       newGenArrayString(),
		LinksTraceStates:   newGenArrayString(),
		LinksAttributes:    newGenArrayMapLowCardinality(),
	}

	c.names = []string{
		"Timestamp",
		"TraceId",
		"SpanId",
		"ParentSpanId",
		"TraceState",
		"SpanName",
		"SpanKind",
		"ServiceName",
		"ResourceAttributes",
		"ScopeName",
		"ScopeVersion",
		"SpanAttributes",
		"Duration",
		"StatusCode",
		"StatusMessage",
		"Events.Timestamp",
		"Events.Name",
		"Events.Attributes",
		"Links.TraceId",
		"Links.SpanId",
		"Links.TraceState",
		"Links.Attributes",
	}

	c.cols = []proto.Column{
		&c.Timestamp,
		&c.TraceID,
		&c.SpanID,
		&c.ParentSpanID,
		&c.TraceState,
		c.SpanName,
		c.SpanKind,
		c.ServiceName,
		c.ResourceAttributes,
		&c.ScopeName,
		&c.ScopeVersion,
		c.SpanAttributes,
		&c.Duration,
		c.StatusCode,
		&c.StatusMessage,
		c.EventsTimestamps,
		c.EventsNames,
		c.EventsAttributes,
		c.LinksTraceIDs,
		c.LinksSpanIDs,
		c.LinksTraceStates,
		c.LinksAttributes,
	}

	c.cachedInput = make(proto.Input, len(c.names))
	for i := range c.names {
		c.cachedInput[i] = proto.InputColumn{Name: c.names[i], Data: c.cols[i]}
	}

	return c
}

func (c *GenTracesColumns) Reset() {
	for _, col := range c.cols {
		col.Reset()
	}
}

func (c *GenTracesColumns) Results() proto.Results { return nil }

func (c *GenTracesColumns) Input() proto.Input { return c.cachedInput }

func (c *GenTracesColumns) FirstTimestamp() time.Time {
	if len(c.Timestamp.Data) > 0 {
		return c.Timestamp.Data[0].Time(c.Timestamp.Precision)
	}
	return time.Time{}
}

func (c *GenTracesColumns) LastTimestamp() time.Time {
	if len(c.Timestamp.Data) > 0 {
		return c.Timestamp.Data[len(c.Timestamp.Data)-1].Time(c.Timestamp.Precision)
	}
	return time.Time{}
}

func (c *GenTracesColumns) UpdateDate()                               {}
func (c *GenTracesColumns) ShiftTimestamp(_ block.ReplayTimeSnapshot) {}
func (c *GenTracesColumns) UpdateTimestampNow()                       {}
func (c *GenTracesColumns) UpdateTimestampMinute()                    {}
func (c *GenTracesColumns) MutateIDs(_ int)                           {}
