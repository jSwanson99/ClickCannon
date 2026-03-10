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
	severityText       *LowCard[string]
	severityNumber     proto.ColUInt8
	serviceName        *LowCard[string]
	body               proto.ColStr
	resourceSchemaUrl  *LowCard[string]
	resourceAttributes *proto.ColMap[string, string]
	scopeSchemaUrl     *LowCard[string]
	scopeName          proto.ColStr
	scopeVersion       *LowCard[string]
	scopeAttributes    *proto.ColMap[string, string]
	logAttributes      *proto.ColMap[string, string]

	hasTimestampTime bool

	// insertNames/insertCols are used for inserts (TimestampTime always excluded).
	insertNames []string
	insertCols  []proto.Column

	// decodeNames/decodeCols are the disk decode columns.
	// When hasTimestampTime=true they include TimestampTime; otherwise same as insertNames/insertCols.
	// Both point to the same column data, so we can safely use them for decode/insert.
	// This is also why Reset should use decodeCols since it contains all columns.
	decodeNames []string
	decodeCols  []proto.Column
}

func newLogsColumns(c *LogsSharedColumns) ([]string, []proto.Column) {
	names := []string{
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

	cols := []proto.Column{
		&c.timestamp,
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

	return names, cols
}

func newLogsColumnsWithTimestampTime(c *LogsSharedColumns) ([]string, []proto.Column) {
	names := []string{
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

	cols := []proto.Column{
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

	return names, cols
}

func NewLogsSharedColumns(hasTimestampTime bool) *LogsSharedColumns {
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

		hasTimestampTime: hasTimestampTime,
	}

	c.insertNames, c.insertCols = newLogsColumns(&c)

	if hasTimestampTime {
		c.decodeNames, c.decodeCols = newLogsColumnsWithTimestampTime(&c)
	} else {
		c.decodeNames, c.decodeCols = c.insertNames, c.insertCols
	}

	return &c
}

func (c *LogsSharedColumns) Reset() {
	for _, col := range c.decodeCols {
		col.Reset()
	}
}

func (c *LogsSharedColumns) Results() proto.Results {
	res := make(proto.Results, 0, len(c.decodeNames))
	for i := range c.decodeNames {
		res = append(res, proto.ResultColumn{Name: c.decodeNames[i], Data: c.decodeCols[i]})
	}

	return res
}

func (c *LogsSharedColumns) Input() proto.Input {
	in := make(proto.Input, 0, len(c.insertNames))
	for i := range c.insertNames {
		in = append(in, proto.InputColumn{Name: c.insertNames[i], Data: c.insertCols[i]})
	}

	return in
}

func (c *LogsSharedColumns) UpdateDate() {
	for i := range c.timestamp.Data {
		shiftedTime := ShiftDateToToday(c.timestamp.Data[i].Time(c.timestamp.Precision))
		c.timestamp.Data[i] = proto.ToDateTime64(shiftedTime, c.timestamp.Precision)

		if c.hasTimestampTime {
			c.timestampTime.Data[i] = proto.ToDateTime(shiftedTime)
		}
	}
}

func (c *LogsSharedColumns) UpdateTimestampMinute() {
	for i := range c.timestamp.Data {
		shiftedTime := ShiftTimestampMinute(c.timestamp.Data[i].Time(c.timestamp.Precision))
		c.timestamp.Data[i] = proto.ToDateTime64(shiftedTime, c.timestamp.Precision)

		if c.hasTimestampTime {
			c.timestampTime.Data[i] = proto.ToDateTime(shiftedTime)
		}
	}
}

func (c *LogsSharedColumns) FirstTimestamp() time.Time {
	if len(c.timestamp.Data) > 0 {
		return c.timestamp.Data[0].Time(c.timestamp.Precision)
	}

	return time.Time{}
}

func (c *LogsSharedColumns) LastTimestamp() time.Time {
	if len(c.timestamp.Data) > 0 {
		return c.timestamp.Data[len(c.timestamp.Data)-1].Time(c.timestamp.Precision)
	}

	return time.Time{}
}

func (c *LogsSharedColumns) ShiftTimestamp(snapshot ReplayTimeSnapshot) {
	for i := range c.timestamp.Data {
		shiftedTime := snapshot.ShiftTimestamp(c.timestamp.Data[i].Time(c.timestamp.Precision))
		c.timestamp.Data[i] = proto.ToDateTime64(shiftedTime, c.timestamp.Precision)

		if c.hasTimestampTime {
			c.timestampTime.Data[i] = proto.ToDateTime(shiftedTime)
		}
	}
}

func (c *LogsSharedColumns) MutateIDs(loopIndex int) {
	if loopIndex == 0 {
		return
	}

	shiftColStrLastByte(&c.traceID, loopIndex)
	shiftColStrLastByte(&c.spanID, loopIndex)
}

func (c *LogsSharedColumns) UpdateTimestampNow() {
	for i := range c.timestamp.Data {
		shiftedTime := time.Now()
		c.timestamp.Data[i] = proto.ToDateTime64(shiftedTime, c.timestamp.Precision)

		if c.hasTimestampTime {
			c.timestampTime.Data[i] = proto.ToDateTime(shiftedTime)
		}
	}
}
