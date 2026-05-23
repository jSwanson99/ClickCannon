package generate

import (
	"time"

	"clickcannon/internal/block"

	"github.com/ClickHouse/ch-go/proto"
)

// GenLogsColumns implements block.SharedColumns for the generate path.
// Uses proto.ColLowCardinality[string] (with working Append) instead of
// the LowCard[string] wrapper used by the disk decode path.
// TimestampTime is excluded since it's derived on insert by ClickHouse.
type GenLogsColumns struct {
	Timestamp          proto.ColDateTime64Raw
	TraceID            proto.ColStr
	SpanID             proto.ColStr
	TraceFlags         proto.ColUInt8
	SeverityText       *proto.ColLowCardinality[string]
	SeverityNumber     proto.ColUInt8
	ServiceName        *proto.ColLowCardinality[string]
	Body               proto.ColStr
	ResourceSchemaUrl  *proto.ColLowCardinality[string]
	ResourceAttributes *proto.ColMap[string, string]
	ScopeSchemaUrl     *proto.ColLowCardinality[string]
	ScopeName          proto.ColStr
	ScopeVersion       *proto.ColLowCardinality[string]
	ScopeAttributes    *proto.ColMap[string, string]
	LogAttributes      *proto.ColMap[string, string]

	names       []string
	cols        []proto.Column
	cachedInput proto.Input
}

func NewGenLogsColumns() *GenLogsColumns {
	c := &GenLogsColumns{
		Timestamp:          proto.ColDateTime64Raw{ColDateTime64: proto.ColDateTime64{Location: time.UTC, Precision: proto.PrecisionNano, PrecisionSet: true}},
		TraceID:            proto.ColStr{},
		SpanID:             proto.ColStr{},
		TraceFlags:         proto.ColUInt8{},
		SeverityText:       newGenLowCardinality(),
		SeverityNumber:     proto.ColUInt8{},
		ServiceName:        newGenLowCardinality(),
		Body:               proto.ColStr{},
		ResourceSchemaUrl:  newGenLowCardinality(),
		ResourceAttributes: newGenMap(),
		ScopeSchemaUrl:     newGenLowCardinality(),
		ScopeName:          proto.ColStr{},
		ScopeVersion:       newGenLowCardinality(),
		ScopeAttributes:    newGenMap(),
		LogAttributes:      newGenMap(),
	}

	c.names = []string{
		"Timestamp",
		"TraceId",
		"SpanId",
		"TraceFlags",
		"SeverityText",
		"SeverityNumber",
		"ServiceName",
		"Body",
		"ResourceSchemaUrl",
		"ResourceAttributes",
		"ScopeSchemaUrl",
		"ScopeName",
		"ScopeVersion",
		"ScopeAttributes",
		"LogAttributes",
	}

	c.cols = []proto.Column{
		&c.Timestamp,
		&c.TraceID,
		&c.SpanID,
		&c.TraceFlags,
		c.SeverityText,
		&c.SeverityNumber,
		c.ServiceName,
		&c.Body,
		c.ResourceSchemaUrl,
		c.ResourceAttributes,
		c.ScopeSchemaUrl,
		&c.ScopeName,
		c.ScopeVersion,
		c.ScopeAttributes,
		c.LogAttributes,
	}

	c.cachedInput = make(proto.Input, len(c.names))
	for i := range c.names {
		c.cachedInput[i] = proto.InputColumn{Name: c.names[i], Data: c.cols[i]}
	}

	return c
}

func (c *GenLogsColumns) Reset() {
	for _, col := range c.cols {
		col.Reset()
	}
}

func (c *GenLogsColumns) Results() proto.Results { return nil }

func (c *GenLogsColumns) Input() proto.Input { return c.cachedInput }

func (c *GenLogsColumns) FirstTimestamp() time.Time {
	if len(c.Timestamp.Data) > 0 {
		return c.Timestamp.Data[0].Time(c.Timestamp.Precision)
	}
	return time.Time{}
}

func (c *GenLogsColumns) LastTimestamp() time.Time {
	if len(c.Timestamp.Data) > 0 {
		return c.Timestamp.Data[len(c.Timestamp.Data)-1].Time(c.Timestamp.Precision)
	}
	return time.Time{}
}

func (c *GenLogsColumns) UpdateDate()                               {}
func (c *GenLogsColumns) ShiftTimestamp(_ block.ReplayTimeSnapshot) {}
func (c *GenLogsColumns) UpdateTimestampNow()                       {}
func (c *GenLogsColumns) UpdateTimestampMinute()                    {}
func (c *GenLogsColumns) MutateIDs(_ int)                           {}
