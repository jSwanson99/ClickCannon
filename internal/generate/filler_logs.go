package generate

import (
	"context"
	"time"

	"github.com/ClickHouse/ch-go/proto"
)

// OTel severity text -> number mapping. Profile-defined SeverityText pools should
// use these exact labels so the numeric column lines up.
var severityTextToNumber = map[string]uint8{
	"TRACE":  1,
	"TRACE2": 2,
	"TRACE3": 3,
	"TRACE4": 4,
	"DEBUG":  5,
	"DEBUG2": 6,
	"DEBUG3": 7,
	"DEBUG4": 8,
	"INFO":   9,
	"INFO2":  10,
	"INFO3":  11,
	"INFO4":  12,
	"WARN":   13,
	"WARN2":  14,
	"WARN3":  15,
	"WARN4":  16,
	"ERROR":  17,
	"ERROR2": 18,
	"ERROR3": 19,
	"ERROR4": 20,
	"FATAL":  21,
	"FATAL2": 22,
	"FATAL3": 23,
	"FATAL4": 24,
}

// LogsFiller populates GenLogsColumns straight from a code-built Profile.
type LogsFiller struct {
	p *Profile
}

// NewLogsFiller wraps a profile for log generation. Profile must already have
// applyDefaults() applied (GetProfile does this).
func NewLogsFiller(p *Profile) *LogsFiller {
	return &LogsFiller{p: p}
}

// Fill writes up to n rows. Returns the number of rows actually written;
// it may be less than n if ctx is cancelled mid-block.
func (f *LogsFiller) Fill(ctx context.Context, rng *Rng, cols *GenLogsColumns, n int) int {
	p := f.p
	now := time.Now()
	for i := 0; i < n; i++ {
		if i&0xFF == 0 {
			select {
			case <-ctx.Done():
				return i
			default:
			}
		}

		ts := now.Add(time.Duration(i) * time.Nanosecond)
		cols.Timestamp.Data = append(cols.Timestamp.Data, proto.ToDateTime64(ts, proto.PrecisionNano))

		cols.TraceID.Append(rng.TraceID())
		cols.SpanID.Append(rng.SpanID())
		cols.TraceFlags = append(cols.TraceFlags, 0)

		sevText := p.SeverityText.Generate(rng)
		cols.SeverityText.Append(sevText)
		sevNum := severityTextToNumber[sevText] // zero if absent
		cols.SeverityNumber = append(cols.SeverityNumber, sevNum)

		cols.ServiceName.Append(p.ServiceName.Generate(rng))
		cols.Body.Append(p.Body.Generate(rng))
		cols.ResourceSchemaUrl.Append("")
		cols.ResourceAttributes.Append(p.ResourceAttrs.Generate(rng))
		cols.ScopeSchemaUrl.Append("")
		cols.ScopeName.Append(p.ScopeName.Generate(rng))
		cols.ScopeVersion.Append(p.ScopeVersion.Generate(rng))
		cols.ScopeAttributes.Append(p.ScopeAttrs.Generate(rng))
		cols.LogAttributes.Append(p.LogAttrs.Generate(rng))
	}
	return n
}
