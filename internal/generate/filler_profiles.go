package generate

import (
	"context"
	"time"

	"github.com/ClickHouse/ch-go/proto"
)

// sampleTypeToUnit maps a profile sample type to its canonical unit. Profile-defined
// SampleType pools should use these labels so the unit column lines up.
var sampleTypeToUnit = map[string]string{
	"cpu":           "nanoseconds",
	"wall":          "nanoseconds",
	"samples":       "count",
	"alloc_space":   "bytes",
	"inuse_space":   "bytes",
	"alloc_objects": "count",
	"inuse_objects": "count",
	"goroutines":    "count",
}

// ProfilesFiller populates GenProfilesColumns with whole profiles (a shared
// ProfileId plus many unique-stack sample rows).
type ProfilesFiller struct {
	p    *Profile
	pcfg ProfilesConfig
}

// NewProfilesFiller wraps a profile for profile generation. Profile must already
// have applyDefaults() applied (GetProfile does this).
func NewProfilesFiller(p *Profile, pcfg ProfilesConfig) *ProfilesFiller {
	return &ProfilesFiller{p: p, pcfg: pcfg}
}

// Fill emits whole profiles until at least n rows have been written.
// Returns the number of rows actually written; may be less if ctx is cancelled.
func (f *ProfilesFiller) Fill(ctx context.Context, rng *Rng, cols *GenProfilesColumns, n int) int {
	p := f.p
	remaining := n
	now := time.Now()
	rowIdx := 0

	addresses := make([]uint64, 0, f.pcfg.StackDepthMax)
	functionNames := make([]string, 0, f.pcfg.StackDepthMax)
	fileNames := make([]string, 0, f.pcfg.StackDepthMax)
	lineNumbers := make([]int32, 0, f.pcfg.StackDepthMax)
	mappingFileNames := make([]string, 0, f.pcfg.StackDepthMax)
	values := make([]int64, 0, 16)
	timestamps := make([]uint64, 0, 16)

	for remaining > 0 {
		select {
		case <-ctx.Done():
			return n - remaining
		default:
		}

		sampleCount := f.pcfg.SamplesPerProfileMin
		if f.pcfg.SamplesPerProfileMax > f.pcfg.SamplesPerProfileMin {
			sampleCount += rng.IntN(f.pcfg.SamplesPerProfileMax - f.pcfg.SamplesPerProfileMin + 1)
		}
		if sampleCount < 1 {
			sampleCount = 1
		}
		if sampleCount > remaining {
			sampleCount = remaining
		}

		profileID := rng.HexString(32)
		profileTime := now.Add(time.Duration(rowIdx) * time.Nanosecond)
		serviceName := p.ServiceName.Generate(rng)
		scopeName := p.ScopeName.Generate(rng)
		scopeVersion := p.ScopeVersion.Generate(rng)
		resourceAttrs := p.ResourceAttrs.Generate(rng)
		profileAttrs := p.ProfileAttrs.Generate(rng)
		sampleType := p.SampleType.Generate(rng)
		sampleUnit := sampleTypeToUnit[sampleType]
		if sampleUnit == "" {
			sampleUnit = p.SampleUnit.Generate(rng)
		}
		periodType := p.PeriodType.Generate(rng)
		periodUnit := p.PeriodUnit.Generate(rng)

		dur := f.pcfg.DurationMinMs * 1000000
		if f.pcfg.DurationMaxMs > f.pcfg.DurationMinMs {
			dur += rng.Uint64N((f.pcfg.DurationMaxMs-f.pcfg.DurationMinMs)*1000000 + 1)
		}

		for s := 0; s < sampleCount; s++ {
			cols.Timestamp.Data = append(cols.Timestamp.Data, proto.ToDateTime64(profileTime, proto.PrecisionNano))
			cols.ProfileID.Append(profileID)
			cols.SampleType.Append(sampleType)
			cols.SampleUnit.Append(sampleUnit)
			cols.ServiceName.Append(serviceName)
			cols.ResourceAttributes.Append(resourceAttrs)
			cols.ScopeName.Append(scopeName)
			cols.ScopeVersion.Append(scopeVersion)
			cols.ProfileAttributes.Append(profileAttrs)
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

			hits := 1 + rng.IntN(16)
			values = values[:0]
			timestamps = timestamps[:0]
			for h := 0; h < hits; h++ {
				offset := time.Duration(rng.Int64N(int64(dur) + 1))
				timestamps = append(timestamps, uint64(profileTime.Add(offset).UnixNano()))
				switch sampleUnit {
				case "bytes":
					values = append(values, int64(rng.Uint64N(1048576)+1))
				case "nanoseconds":
					values = append(values, f.pcfg.PeriodNs)
				default:
					values = append(values, 1)
				}
			}
			cols.Values.Append(values)
			cols.TimestampsUnixNano.Append(timestamps)

			cols.DurationNano = append(cols.DurationNano, dur)
			cols.Period = append(cols.Period, f.pcfg.PeriodNs)
			cols.PeriodType.Append(periodType)
			cols.PeriodUnit.Append(periodUnit)

			if rng.Float64() < 0.2 {
				cols.TraceID.Append(rng.TraceID())
				cols.SpanID.Append(rng.SpanID())
			} else {
				cols.TraceID.Append("")
				cols.SpanID.Append("")
			}

			rowIdx++
		}

		remaining -= sampleCount
	}
	return n
}
