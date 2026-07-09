package block

import (
	"time"

	"github.com/ClickHouse/ch-go/proto"
)

type ProfilesSharedColumns struct {
	timestamp          proto.ColDateTime64Raw
	profileID          proto.ColStr
	sampleType         *LowCard[string]
	sampleUnit         *LowCard[string]
	serviceName        *LowCard[string]
	resourceAttributes *proto.ColMap[string, string]
	scopeName          proto.ColStr
	scopeVersion       proto.ColStr
	profileAttributes  *proto.ColMap[string, string]
	sampleAttributes   *proto.ColMap[string, string]
	stackHash          proto.ColUInt64
	addresses          *proto.ColArr[uint64]
	functionNames      *proto.ColArr[string]
	fileNames          *proto.ColArr[string]
	lineNumbers        *proto.ColArr[int32]
	mappingFileNames   *proto.ColArr[string]
	values             *proto.ColArr[int64]
	timestampsUnixNano *proto.ColArr[uint64]
	durationNano       proto.ColUInt64
	period             proto.ColInt64
	periodType         *LowCard[string]
	periodUnit         *LowCard[string]
	traceID            proto.ColStr
	spanID             proto.ColStr

	Names []string
	Cols  []proto.Column

	cachedResults proto.Results
	cachedInput   proto.Input
}

func NewProfilesSharedColumns() *ProfilesSharedColumns {
	// strSize := 512
	// bSize := 16384
	// Let the runtime figure it out
	strSize := 0
	bSize := 0

	c := ProfilesSharedColumns{
		timestamp:          newColDateTime64Raw(bSize),
		profileID:          newColString(strSize, bSize),
		sampleType:         newColLowCardinalityString(strSize, bSize),
		sampleUnit:         newColLowCardinalityString(strSize, bSize),
		serviceName:        newColLowCardinalityString(strSize, bSize),
		resourceAttributes: newColMapLowCardinalityStringString(strSize, bSize),
		scopeName:          newColString(strSize, bSize),
		scopeVersion:       newColString(strSize, bSize),
		profileAttributes:  newColMapLowCardinalityStringString(strSize, bSize),
		sampleAttributes:   newColMapLowCardinalityStringString(strSize, bSize),
		stackHash:          make(proto.ColUInt64, 0, bSize),
		addresses:          newColArrayUInt64(bSize),
		functionNames:      newColArrayLowCardinalityString(strSize, bSize),
		fileNames:          newColArrayLowCardinalityString(strSize, bSize),
		lineNumbers:        newColArrayInt32(bSize),
		mappingFileNames:   newColArrayLowCardinalityString(strSize, bSize),
		values:             newColArrayInt64(bSize),
		timestampsUnixNano: newColArrayUInt64(bSize),
		durationNano:       make(proto.ColUInt64, 0, bSize),
		period:             make(proto.ColInt64, 0, bSize),
		periodType:         newColLowCardinalityString(strSize, bSize),
		periodUnit:         newColLowCardinalityString(strSize, bSize),
		traceID:            newColString(strSize, bSize),
		spanID:             newColString(strSize, bSize),

		Names: nil,
		Cols:  nil,
	}

	c.Names = []string{
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
	c.Cols = []proto.Column{
		&c.timestamp,
		&c.profileID,
		c.sampleType,
		c.sampleUnit,
		c.serviceName,
		c.resourceAttributes,
		&c.scopeName,
		&c.scopeVersion,
		c.profileAttributes,
		c.sampleAttributes,
		&c.stackHash,
		c.addresses,
		c.functionNames,
		c.fileNames,
		c.lineNumbers,
		c.mappingFileNames,
		c.values,
		c.timestampsUnixNano,
		&c.durationNano,
		&c.period,
		c.periodType,
		c.periodUnit,
		&c.traceID,
		&c.spanID,
	}

	c.cachedResults = make(proto.Results, len(c.Names))
	for i := range c.Names {
		c.cachedResults[i] = proto.ResultColumn{Name: c.Names[i], Data: c.Cols[i]}
	}
	c.cachedInput = make(proto.Input, len(c.Names))
	for i := range c.Names {
		c.cachedInput[i] = proto.InputColumn{Name: c.Names[i], Data: c.Cols[i]}
	}

	return &c
}

func (c *ProfilesSharedColumns) Reset() {
	for _, col := range c.Cols {
		col.Reset()
	}
}

func (c *ProfilesSharedColumns) Results() proto.Results { return c.cachedResults }

func (c *ProfilesSharedColumns) Input() proto.Input { return c.cachedInput }

func (c *ProfilesSharedColumns) UpdateDate() {
	for i := range c.timestamp.Data {
		shiftedTime := ShiftDateToToday(c.timestamp.Data[i].Time(c.timestamp.Precision))
		c.timestamp.Data[i] = proto.ToDateTime64(shiftedTime, c.timestamp.Precision)
		// TODO: TimestampsUnixNano column?
	}
}

func (c *ProfilesSharedColumns) UpdateTimestampMinute() {
	for i := range c.timestamp.Data {
		shiftedTime := ShiftTimestampMinute(c.timestamp.Data[i].Time(c.timestamp.Precision))
		c.timestamp.Data[i] = proto.ToDateTime64(shiftedTime, c.timestamp.Precision)
		// TODO: TimestampsUnixNano column?
	}
}

func (c *ProfilesSharedColumns) FirstTimestamp() time.Time {
	if len(c.timestamp.Data) > 0 {
		return c.timestamp.Data[0].Time(c.timestamp.Precision)
	}

	return time.Time{}
}

func (c *ProfilesSharedColumns) LastTimestamp() time.Time {
	if len(c.timestamp.Data) > 0 {
		return c.timestamp.Data[len(c.timestamp.Data)-1].Time(c.timestamp.Precision)
	}

	return time.Time{}
}

func (c *ProfilesSharedColumns) ShiftTimestamp(snapshot ReplayTimeSnapshot) {
	for i := range c.timestamp.Data {
		shiftedTime := snapshot.ShiftTimestamp(c.timestamp.Data[i].Time(c.timestamp.Precision))
		c.timestamp.Data[i] = proto.ToDateTime64(shiftedTime, c.timestamp.Precision)
		// TODO: TimestampsUnixNano column?
	}
}

func (c *ProfilesSharedColumns) MutateIDs(loopIndex int) {
	if loopIndex == 0 {
		return
	}

	shiftColStrLastByte(&c.profileID, loopIndex)
	shiftColStrLastByte(&c.traceID, loopIndex)
	shiftColStrLastByte(&c.spanID, loopIndex)
}

func (c *ProfilesSharedColumns) UpdateTimestampNow() {
	for i := range c.timestamp.Data {
		shiftedTime := time.Now()
		c.timestamp.Data[i] = proto.ToDateTime64(shiftedTime, c.timestamp.Precision)
		// TODO: TimestampsUnixNano column?
	}
}
