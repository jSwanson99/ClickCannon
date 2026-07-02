package generate

import "clickcannon/internal/block"

// This file implements block.LogsReader / block.TracesReader for the generate
// column types so the OTLP exporter can read generated rows. The generate path
// uses standard proto columns (working Row methods), so the shared block helpers
// handle both key-column variants transparently.

// Rows implements block.LogsReader.
func (c *GenLogsColumns) Rows() int { return len(c.Timestamp.Data) }

// ReadLogRow implements block.LogsReader for the generate path.
func (c *GenLogsColumns) ReadLogRow(i int, dst *block.LogRow) {
	dst.Timestamp = c.Timestamp.Data[i].Time(c.Timestamp.Precision)
	dst.TraceID = c.TraceID.Row(i)
	dst.SpanID = c.SpanID.Row(i)
	dst.TraceFlags = uint32(c.TraceFlags[i])
	dst.SeverityText = c.SeverityText.Row(i)
	dst.SeverityNumber = int32(c.SeverityNumber[i])
	dst.ServiceName = c.ServiceName.Row(i)
	dst.Body = c.Body.Row(i)
	dst.ResourceSchemaURL = c.ResourceSchemaUrl.Row(i)
	dst.ScopeSchemaURL = c.ScopeSchemaUrl.Row(i)
	dst.ScopeName = c.ScopeName.Row(i)
	dst.ScopeVersion = c.ScopeVersion.Row(i)
	dst.ResourceAttrs = block.MapRowKV(dst.ResourceAttrs[:0], c.ResourceAttributes, i)
	dst.ScopeAttrs = block.MapRowKV(dst.ScopeAttrs[:0], c.ScopeAttributes, i)
	dst.LogAttrs = block.MapRowKV(dst.LogAttrs[:0], c.LogAttributes, i)
}

// Rows implements block.TracesReader.
func (c *GenTracesColumns) Rows() int { return len(c.Timestamp.Data) }

// ReadTraceRow implements block.TracesReader for the generate path.
func (c *GenTracesColumns) ReadTraceRow(i int, dst *block.TraceRow) {
	dst.Timestamp = c.Timestamp.Data[i].Time(c.Timestamp.Precision)
	dst.TraceID = c.TraceID.Row(i)
	dst.SpanID = c.SpanID.Row(i)
	dst.ParentSpanID = c.ParentSpanID.Row(i)
	dst.TraceState = c.TraceState.Row(i)
	dst.SpanName = c.SpanName.Row(i)
	dst.SpanKind = c.SpanKind.Row(i)
	dst.ServiceName = c.ServiceName.Row(i)
	dst.ScopeName = c.ScopeName.Row(i)
	dst.ScopeVersion = c.ScopeVersion.Row(i)
	dst.Duration = c.Duration[i]
	dst.StatusCode = c.StatusCode.Row(i)
	dst.StatusMessage = c.StatusMessage.Row(i)
	dst.ResourceAttrs = block.MapRowKV(dst.ResourceAttrs[:0], c.ResourceAttributes, i)
	dst.SpanAttrs = block.MapRowKV(dst.SpanAttrs[:0], c.SpanAttributes, i)
	dst.Events = block.ReadEvents(dst.Events[:0], c.EventsTimestamps, c.EventsNames, c.EventsAttributes, i)
	dst.Links = block.ReadLinks(dst.Links[:0], c.LinksTraceIDs, c.LinksSpanIDs, c.LinksTraceStates, c.LinksAttributes, i)
}
