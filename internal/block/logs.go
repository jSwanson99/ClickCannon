package block

import (
	"time"

	"github.com/ClickHouse/ch-go/proto"
)

type LogsSharedColumns struct {
	timestamp          proto.ColDateTime64Raw
	timestampTime      proto.ColDateTime
	traceID            proto.ColStr
	spanID             proto.ColStr
	traceFlags         proto.ColUInt8
	severityText       *proto.ColLowCardinality[string]
	severityNumber     proto.ColUInt8
	serviceName        *proto.ColLowCardinality[string]
	body               proto.ColStr
	resourceSchemaUrl  *proto.ColLowCardinality[string]
	resourceAttributes *proto.ColMap[string, string]
	scopeSchemaUrl     *proto.ColLowCardinality[string]
	scopeName          proto.ColStr
	scopeVersion       *proto.ColLowCardinality[string]
	scopeAttributes    *proto.ColMap[string, string]
	logAttributes      *proto.ColMap[string, string]

	Names []string
	Cols  []proto.Column
}

func NewLogsSharedColumns() *LogsSharedColumns {
	strSize := 512
	bSize := 16384

	c := LogsSharedColumns{
		timestamp:          newColDateTime64Raw(bSize),
		timestampTime:      newColDateTime(bSize),
		traceID:            newColString(strSize, bSize),
		spanID:             newColString(strSize, bSize),
		traceFlags:         make(proto.ColUInt8, 0, bSize),
		severityText:       newColLowCardinalityString(strSize, bSize),
		severityNumber:     make(proto.ColUInt8, 0, bSize),
		serviceName:        newColLowCardinalityString(strSize, bSize),
		body:               newColString(strSize, bSize),
		resourceSchemaUrl:  newColLowCardinalityString(strSize, bSize),
		resourceAttributes: newColMapLowCardinalityStringString(strSize, bSize),
		scopeSchemaUrl:     newColLowCardinalityString(strSize, bSize),
		scopeName:          newColString(strSize, bSize),
		scopeVersion:       newColLowCardinalityString(strSize, bSize),
		scopeAttributes:    newColMapLowCardinalityStringString(strSize, bSize),
		logAttributes:      newColMapLowCardinalityStringString(strSize, bSize),

		Names: nil,
		Cols:  nil,
	}

	c.Names = []string{
		"Timestamp",
		"TimestampTime",
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
	c.Cols = []proto.Column{
		&c.timestamp,
		&c.timestampTime,
		&c.traceID,
		&c.spanID,
		&c.traceFlags,
		c.severityText,
		&c.severityNumber,
		c.serviceName,
		&c.body,
		c.resourceSchemaUrl,
		c.resourceAttributes,
		c.scopeSchemaUrl,
		&c.scopeName,
		c.scopeVersion,
		c.scopeAttributes,
		c.logAttributes,
	}

	return &c
}

func (c *LogsSharedColumns) Reset() {
	for _, col := range c.Cols {
		col.Reset()
	}
}

func (c *LogsSharedColumns) Results() proto.Results {
	res := make(proto.Results, 0, len(c.Names))
	for i := range c.Names {
		col := proto.ResultColumn{
			Name: c.Names[i],
			Data: c.Cols[i],
		}

		res = append(res, col)
	}

	return res
}

func (c *LogsSharedColumns) Input() proto.Input {
	in := make(proto.Input, 0, len(c.Names))
	for i := range c.Names {
		col := proto.InputColumn{
			Name: c.Names[i],
			Data: c.Cols[i],
		}

		in = append(in, col)
	}

	return in
}

func (c *LogsSharedColumns) UpdateDate() {
	for i := range c.timestamp.Data {
		shiftedTime := ShiftDateToToday(c.timestamp.Data[i].Time(c.timestamp.Precision))
		c.timestamp.Data[i] = proto.ToDateTime64(shiftedTime, c.timestamp.Precision)
		c.timestampTime.Data[i] = proto.ToDateTime(shiftedTime)
	}
}

func (c *LogsSharedColumns) FirstTimestamp() time.Time {
	if len(c.timestamp.Data) > 0 {
		return c.timestamp.Data[0].Time(c.timestamp.Precision)
	}

	return time.Time{}
}

func (c *LogsSharedColumns) UpdateTimestamp(startTime time.Time) {
	for i := range c.timestamp.Data {
		shiftedTime := ShiftTimestamp(startTime, c.timestamp.Data[i].Time(c.timestamp.Precision))
		c.timestamp.Data[i] = proto.ToDateTime64(shiftedTime, c.timestamp.Precision)
		c.timestampTime.Data[i] = proto.ToDateTime(shiftedTime)
	}
}

func (c *LogsSharedColumns) UpdateTimestampNow() {
	for i := range c.timestamp.Data {
		shiftedTime := time.Now()
		c.timestamp.Data[i] = proto.ToDateTime64(shiftedTime, c.timestamp.Precision)
		c.timestampTime.Data[i] = proto.ToDateTime(shiftedTime)
	}
}
