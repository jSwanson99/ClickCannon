package generate

import (
	"context"
	"time"

	"github.com/ClickHouse/ch-go/proto"
)

// spanNode is a node in the in-flight trace tree.
type spanNode struct {
	spanID       string
	parentSpanID string
	depth        int
}

// TracesFiller populates GenTracesColumns with correlated traces (root + children).
type TracesFiller struct {
	p    *Profile
	tcfg TracesConfig
}

func NewTracesFiller(p *Profile, tcfg TracesConfig) *TracesFiller {
	return &TracesFiller{p: p, tcfg: tcfg}
}

// Fill emits whole traces until at least n rows have been written.
// Returns the number of rows actually written; may be less if ctx is cancelled.
func (f *TracesFiller) Fill(ctx context.Context, rng *Rng, cols *GenTracesColumns, n int) int {
	p := f.p
	remaining := n
	now := time.Now()
	rowIdx := 0
	spanBuf := make([]spanNode, 0, f.tcfg.SpansPerTraceMax)

	for remaining > 0 {
		select {
		case <-ctx.Done():
			return n - remaining
		default:
		}

		spanCount := f.tcfg.SpansPerTraceMin
		if f.tcfg.SpansPerTraceMax > f.tcfg.SpansPerTraceMin {
			spanCount += rng.IntN(f.tcfg.SpansPerTraceMax - f.tcfg.SpansPerTraceMin + 1)
		}
		if spanCount > remaining {
			spanCount = remaining
		}

		traceID := rng.TraceID()
		spans := f.buildTree(rng, spanCount, spanBuf)

		// Per-trace shared columns.
		resourceAttrs := p.ResourceAttrs.Generate(rng)
		serviceName := p.ServiceName.Generate(rng)
		scopeName := p.ScopeName.Generate(rng)
		scopeVersion := p.ScopeVersion.Generate(rng)

		for _, span := range spans {
			ts := now.Add(time.Duration(rowIdx) * time.Nanosecond)
			cols.Timestamp.Data = append(cols.Timestamp.Data, proto.ToDateTime64(ts, proto.PrecisionNano))

			cols.TraceID.Append(traceID)
			cols.SpanID.Append(span.spanID)
			cols.ParentSpanID.Append(span.parentSpanID)
			cols.TraceState.Append("")
			cols.SpanName.Append(p.SpanName.Generate(rng))
			cols.SpanKind.Append(p.SpanKind.Generate(rng))
			cols.ServiceName.Append(serviceName)
			cols.ResourceAttributes.Append(resourceAttrs)
			cols.ScopeName.Append(scopeName)
			cols.ScopeVersion.Append(scopeVersion)
			cols.SpanAttributes.Append(p.SpanAttrs.Generate(rng))

			dur := f.tcfg.DurationMinUs * 1000
			if f.tcfg.DurationMaxUs > f.tcfg.DurationMinUs {
				dur += rng.Uint64N(f.tcfg.DurationMaxUs-f.tcfg.DurationMinUs) * 1000
			}
			cols.Duration = append(cols.Duration, dur)

			cols.StatusCode.Append(p.StatusCode.Generate(rng))
			cols.StatusMessage.Append(p.StatusMessage.Generate(rng))

			cols.EventsTimestamps.Append(nil)
			cols.EventsNames.Append(nil)
			cols.EventsAttributes.Append(nil)
			cols.LinksTraceIDs.Append(nil)
			cols.LinksSpanIDs.Append(nil)
			cols.LinksTraceStates.Append(nil)
			cols.LinksAttributes.Append(nil)

			rowIdx++
		}

		remaining -= spanCount
	}
	return n
}

// buildTree generates a trace tree of the requested span count, respecting MaxDepth.
// buf is reused across calls to avoid allocations; caller must not retain it.
func (f *TracesFiller) buildTree(rng *Rng, count int, buf []spanNode) []spanNode {
	buf = buf[:0]
	buf = append(buf, spanNode{spanID: rng.SpanID(), depth: 0})

	for i := 1; i < count; i++ {
		parentIdx := rng.IntN(len(buf))
		parent := buf[parentIdx]
		if parent.depth >= f.tcfg.MaxDepth-1 {
			parent = buf[0]
		}
		buf = append(buf, spanNode{
			spanID:       rng.SpanID(),
			parentSpanID: parent.spanID,
			depth:        parent.depth + 1,
		})
	}
	return buf
}
