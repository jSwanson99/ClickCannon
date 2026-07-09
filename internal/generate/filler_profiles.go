package generate

import (
	"context"
	"time"

	"github.com/ClickHouse/ch-go/proto"
)

// ProfilesFiller populates GenProfilesColumns straight from a code-built Profile.
type ProfilesFiller struct {
	p    *Profile
	pcfg ProfilesConfig
}

// NewProfilesFiller wraps a profile for profile generation. Profile must already
// have applyDefaults() applied (GetProfile does this).
func NewProfilesFiller(p *Profile, pcfg ProfilesConfig) *ProfilesFiller {
	return &ProfilesFiller{p: p, pcfg: pcfg}
}

// Fill writes up to n rows. Returns the number of rows actually written;
// it may be less than n if ctx is cancelled mid-block.
func (f *ProfilesFiller) Fill(ctx context.Context, rng *Rng, cols *GenProfilesColumns, n int) int {
	p := f.p
	now := time.Now()

	// Per-frame scratch buffers reused across rows to avoid allocations.
	addresses := make([]uint64, 0, f.pcfg.StackDepthMax)
	functionNames := make([]string, 0, f.pcfg.StackDepthMax)
	fileNames := make([]string, 0, f.pcfg.StackDepthMax)
	lineNumbers := make([]int32, 0, f.pcfg.StackDepthMax)
	mappingFileNames := make([]string, 0, f.pcfg.StackDepthMax)

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

		cols.ProfileID.Append(rng.HexString(32))
		cols.SampleType.Append(p.SampleType.Generate(rng))
		cols.SampleUnit.Append(p.SampleUnit.Generate(rng))
		cols.ServiceName.Append(p.ServiceName.Generate(rng))
		cols.ResourceAttributes.Append(p.ResourceAttrs.Generate(rng))
		cols.ScopeName.Append(p.ScopeName.Generate(rng))
		cols.ScopeVersion.Append(p.ScopeVersion.Generate(rng))
		cols.ProfileAttributes.Append(p.ProfileAttrs.Generate(rng))
		cols.SampleAttributes.Append(p.SampleAttrs.Generate(rng))
		cols.StackHash = append(cols.StackHash, rng.Uint64())

		depth := f.pcfg.StackDepthMin
		if f.pcfg.StackDepthMax > f.pcfg.StackDepthMin {
			depth += rng.IntN(f.pcfg.StackDepthMax - f.pcfg.StackDepthMin + 1)
		}
		addresses = addresses[:0]
		functionNames = functionNames[:0]
		fileNames = fileNames[:0]
		lineNumbers = lineNumbers[:0]
		mappingFileNames = mappingFileNames[:0]
		for d := 0; d < depth; d++ {
			addresses = append(addresses, rng.Uint64())
			functionNames = append(functionNames, p.FunctionName.Generate(rng))
			fileNames = append(fileNames, p.FileName.Generate(rng))
			lineNumbers = append(lineNumbers, int32(rng.IntN(2000)+1))
			mappingFileNames = append(mappingFileNames, p.MappingFileName.Generate(rng))
		}
		cols.Addresses.Append(addresses)
		cols.FunctionNames.Append(functionNames)
		cols.FileNames.Append(fileNames)
		cols.LineNumbers.Append(lineNumbers)
		cols.MappingFileNames.Append(mappingFileNames)

		cols.Values.Append([]int64{int64(rng.Uint64N(1000000) + 1)})
		cols.TimestampsUnixNano.Append([]uint64{uint64(ts.UnixNano())})

		dur := f.pcfg.DurationMinMs * 1000000
		if f.pcfg.DurationMaxMs > f.pcfg.DurationMinMs {
			dur += rng.Uint64N((f.pcfg.DurationMaxMs-f.pcfg.DurationMinMs)*1000000 + 1)
		}
		cols.DurationNano = append(cols.DurationNano, dur)
		cols.Period = append(cols.Period, f.pcfg.PeriodNs)
		cols.PeriodType.Append(p.PeriodType.Generate(rng))
		cols.PeriodUnit.Append(p.PeriodUnit.Generate(rng))
		cols.TraceID.Append(rng.TraceID())
		cols.SpanID.Append(rng.SpanID())
	}
	return n
}
