package block

import (
	"time"

	"github.com/ClickHouse/ch-go/proto"
)

type TracesSharedColumns struct {
	timestamp          proto.ColDateTime64Raw
	traceID            proto.ColStr
	spanID             proto.ColStr
	parentSpanID       proto.ColStr
	traceState         proto.ColStr
	spanName           *LowCard[string]
	spanKind           *LowCard[string]
	serviceName        *LowCard[string]
	resourceAttributes *proto.ColMap[string, string]
	scopeName          proto.ColStr
	scopeVersion       proto.ColStr
	spanAttributes     *proto.ColMap[string, string]
	duration           proto.ColUInt64
	statusCode         *LowCard[string]
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
	// strSize := 512
	// bSize := 16384
	// Let the runtime figure it out
	strSize := 0
	bSize := 0

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

func (c *TracesSharedColumns) UpdateTimestampMinute() {
	for i := range c.timestamp.Data {
		shiftedTime := ShiftTimestampMinute(c.timestamp.Data[i].Time(c.timestamp.Precision))
		c.timestamp.Data[i] = proto.ToDateTime64(shiftedTime, c.timestamp.Precision)
		// TODO: Events.Timestamp column?
	}
}

func (c *TracesSharedColumns) FirstTimestamp() time.Time {
	if len(c.timestamp.Data) > 0 {
		return c.timestamp.Data[0].Time(c.timestamp.Precision)
	}

	return time.Time{}
}

func (c *TracesSharedColumns) LastTimestamp() time.Time {
	if len(c.timestamp.Data) > 0 {
		return c.timestamp.Data[len(c.timestamp.Data)-1].Time(c.timestamp.Precision)
	}

	return time.Time{}
}

func (c *TracesSharedColumns) ShiftTimestamp(snapshot ReplayTimeSnapshot) {
	for i := range c.timestamp.Data {
		shiftedTime := snapshot.ShiftTimestamp(c.timestamp.Data[i].Time(c.timestamp.Precision))
		c.timestamp.Data[i] = proto.ToDateTime64(shiftedTime, c.timestamp.Precision)
		// TODO: Events.Timestamp column?
	}
}

func (c *TracesSharedColumns) MutateIDs(loopIndex int) {
	if loopIndex == 0 {
		return
	}

	shiftColStrLastByte(&c.traceID, loopIndex)
	shiftColStrLastByte(&c.spanID, loopIndex)
	shiftColStrLastByte(&c.parentSpanID, loopIndex)
}

func (c *TracesSharedColumns) UpdateTimestampNow() {
	for i := range c.timestamp.Data {
		shiftedTime := time.Now()
		c.timestamp.Data[i] = proto.ToDateTime64(shiftedTime, c.timestamp.Precision)
		// TODO: Events.Timestamp column?
	}
}
