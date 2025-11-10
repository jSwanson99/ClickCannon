package main

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
}

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

type TracesSharedColumns struct {
	timestamp          proto.ColDateTime64Raw
	traceID            proto.ColStr
	spanID             proto.ColStr
	parentSpanID       proto.ColStr
	traceState         proto.ColStr
	spanName           *proto.ColLowCardinality[string]
	spanKind           *proto.ColLowCardinality[string]
	serviceName        *proto.ColLowCardinality[string]
	resourceAttributes *proto.ColMap[string, string]
	scopeName          proto.ColStr
	scopeVersion       proto.ColStr
	spanAttributes     *proto.ColMap[string, string]
	duration           proto.ColUInt64
	statusCode         *proto.ColLowCardinality[string]
	statusMessage      proto.ColStr
	eventsTimestamps   *proto.ColArr[proto.DateTime64]
	eventsNames        *proto.ColArr[string]
	eventsAttributes   *proto.ColArr[map[string]string]
	linksTraceIDs      *proto.ColArr[string]
	linksSpanIDs       *proto.ColArr[string]
	linksTraceStates   *proto.ColArr[string]
	linksAttributes    *proto.ColArr[map[string]string]

	Names []string
	Cols  []proto.Column
}

func NewTracesSharedColumns() *TracesSharedColumns {
	strSize := 512
	bSize := 16384

	c := TracesSharedColumns{
		timestamp:          newColDateTime64Raw(bSize),
		traceID:            newColString(strSize, bSize),
		spanID:             newColString(strSize, bSize),
		parentSpanID:       newColString(strSize, bSize),
		traceState:         newColString(strSize, bSize),
		spanName:           newColLowCardinalityString(strSize, bSize),
		spanKind:           newColLowCardinalityString(strSize, bSize),
		serviceName:        newColLowCardinalityString(strSize, bSize),
		resourceAttributes: newColMapLowCardinalityStringString(strSize, bSize),
		scopeName:          newColString(strSize, bSize),
		scopeVersion:       newColString(strSize, bSize),
		spanAttributes:     newColMapLowCardinalityStringString(strSize, bSize),
		duration:           make(proto.ColUInt64, 0, bSize),
		statusCode:         newColLowCardinalityString(strSize, bSize),
		statusMessage:      newColString(strSize, bSize),
		eventsTimestamps:   newColArrayDateTime64Raw(bSize),
		eventsNames:        newColArrayLowCardinalityString(strSize, bSize),
		eventsAttributes:   newColArrayMapLowCardinalityStringString(strSize, bSize),
		linksTraceIDs:      newColArrayString(strSize, bSize),
		linksSpanIDs:       newColArrayString(strSize, bSize),
		linksTraceStates:   newColArrayString(strSize, bSize),
		linksAttributes:    newColArrayMapLowCardinalityStringString(strSize, bSize),

		Names: nil,
		Cols:  nil,
	}

	c.Names = []string{
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
	c.Cols = []proto.Column{
		&c.timestamp,
		&c.traceID,
		&c.spanID,
		&c.parentSpanID,
		&c.traceState,
		c.spanName,
		c.spanKind,
		c.serviceName,
		c.resourceAttributes,
		&c.scopeName,
		&c.scopeVersion,
		c.spanAttributes,
		&c.duration,
		c.statusCode,
		&c.statusMessage,
		c.eventsTimestamps,
		c.eventsNames,
		c.eventsAttributes,
		c.linksTraceIDs,
		c.linksSpanIDs,
		c.linksTraceStates,
		c.linksAttributes,
	}

	return &c
}

func (c *TracesSharedColumns) Reset() {
	for _, col := range c.Cols {
		col.Reset()
	}
}

func (c *TracesSharedColumns) Results() proto.Results {
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

func (c *TracesSharedColumns) Input() proto.Input {
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

func (c *TracesSharedColumns) UpdateDate() {
	for i := range c.timestamp.Data {
		shiftedTime := ShiftDateToToday(c.timestamp.Data[i].Time(c.timestamp.Precision))
		c.timestamp.Data[i] = proto.ToDateTime64(shiftedTime, c.timestamp.Precision)
		// TODO: Events.Timestamp column?
	}
}
