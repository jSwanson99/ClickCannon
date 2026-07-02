package block

import "github.com/ClickHouse/ch-go/proto"

// RowString reads row i of the low-cardinality column as a string. The custom
// LowCard wrapper does not implement the generic Row method (it panics), so we
// resolve the key -> dictionary index manually. Only the string dictionary
// (proto.ColStr index) is supported, which is all this project uses.
func (c *LowCard[T]) RowString(i int) string {
	var idx int
	switch c.Key {
	case proto.KeyUInt8:
		if i >= len(c.Keys8) {
			return ""
		}
		idx = int(c.Keys8[i])
	case proto.KeyUInt16:
		if i >= len(c.Keys16) {
			return ""
		}
		idx = int(c.Keys16[i])
	case proto.KeyUInt32:
		if i >= len(c.Keys32) {
			return ""
		}
		idx = int(c.Keys32[i])
	case proto.KeyUInt64:
		if i >= len(c.Keys64) {
			return ""
		}
		idx = int(c.Keys64[i])
	default:
		return ""
	}
	if s, ok := c.Index.(*proto.ColStr); ok && idx < s.Rows() {
		return s.Row(idx)
	}
	return ""
}

// Rows implements block.LogsReader.
func (c *LogsSharedColumns) Rows() int { return len(c.timestamp.Data) }

// ReadLogRow implements block.LogsReader for the disk decode path.
func (c *LogsSharedColumns) ReadLogRow(i int, dst *LogRow) {
	dst.Timestamp = c.timestamp.Data[i].Time(c.timestamp.Precision)
	dst.TraceID = c.traceID.Row(i)
	dst.SpanID = c.spanID.Row(i)
	dst.TraceFlags = uint32(c.traceFlags[i])
	dst.SeverityText = c.severityText.RowString(i)
	dst.SeverityNumber = int32(c.severityNumber[i])
	dst.ServiceName = c.serviceName.RowString(i)
	dst.Body = c.body.Row(i)
	dst.ResourceSchemaURL = c.resourceSchemaUrl.RowString(i)
	dst.ScopeSchemaURL = c.scopeSchemaUrl.RowString(i)
	dst.ScopeName = c.scopeName.Row(i)
	dst.ScopeVersion = c.scopeVersion.RowString(i)
	dst.ResourceAttrs = MapRowKV(dst.ResourceAttrs[:0], c.resourceAttributes, i)
	dst.ScopeAttrs = MapRowKV(dst.ScopeAttrs[:0], c.scopeAttributes, i)
	dst.LogAttrs = MapRowKV(dst.LogAttrs[:0], c.logAttributes, i)
}

// Rows implements block.TracesReader.
func (c *TracesSharedColumns) Rows() int { return len(c.timestamp.Data) }

// ReadTraceRow implements block.TracesReader for the disk decode path.
func (c *TracesSharedColumns) ReadTraceRow(i int, dst *TraceRow) {
	dst.Timestamp = c.timestamp.Data[i].Time(c.timestamp.Precision)
	dst.TraceID = c.traceID.Row(i)
	dst.SpanID = c.spanID.Row(i)
	dst.ParentSpanID = c.parentSpanID.Row(i)
	dst.TraceState = c.traceState.Row(i)
	dst.SpanName = c.spanName.RowString(i)
	dst.SpanKind = c.spanKind.RowString(i)
	dst.ServiceName = c.serviceName.RowString(i)
	dst.ScopeName = c.scopeName.Row(i)
	dst.ScopeVersion = c.scopeVersion.Row(i)
	dst.Duration = c.duration[i]
	dst.StatusCode = c.statusCode.RowString(i)
	dst.StatusMessage = c.statusMessage.Row(i)
	dst.ResourceAttrs = MapRowKV(dst.ResourceAttrs[:0], c.resourceAttributes, i)
	dst.SpanAttrs = MapRowKV(dst.SpanAttrs[:0], c.spanAttributes, i)
	dst.Events = ReadEvents(dst.Events[:0], c.eventsTimestamps, c.eventsNames, c.eventsAttributes, i)
	dst.Links = ReadLinks(dst.Links[:0], c.linksTraceIDs, c.linksSpanIDs, c.linksTraceStates, c.linksAttributes, i)
}
