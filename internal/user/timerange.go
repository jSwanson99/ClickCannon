package user

import (
	"math"
	"math/rand/v2"
	"time"
)

type ResolvedTimeRange struct {
	Start time.Time
	End   time.Time
}

func (c *TimeRangeConfig) Resolve(anchor time.Time, rng *rand.Rand) (ResolvedTimeRange, bool) {
	if c == nil || c.Type == TimeRangeNone {
		return ResolvedTimeRange{}, false
	}

	lookback := c.sampleLookback(rng)
	return ResolvedTimeRange{
		Start: anchor.Add(-lookback),
		End:   anchor,
	}, true
}

func (c *TimeRangeConfig) sampleLookback(rng *rand.Rand) time.Duration {
	switch c.Type {
	case TimeRangeFixed:
		return c.Lookback

	case TimeRangeUniform:
		spread := c.Max - c.Min
		return c.Min + time.Duration(rng.Int64N(int64(spread)))

	case TimeRangeExponential:
		mean := float64(c.Mean)
		sample := time.Duration(-mean * math.Log(1-rng.Float64()))
		return clampDuration(sample, c.Min, c.Max)

	case TimeRangeLogNormal:
		const sigma = 0.5
		mu := math.Log(float64(c.Mean)) - (sigma*sigma)/2
		sample := time.Duration(math.Exp(mu + sigma*rng.NormFloat64()))
		return clampDuration(sample, c.Min, c.Max)

	default:
		return c.Lookback
	}
}

func clampDuration(d, min, max time.Duration) time.Duration {
	if min > 0 && d < min {
		return min
	}

	if max > 0 && d > max {
		return max
	}

	return d
}

func EffectiveTimeRange(query *QueryConfig, defaultRange *TimeRangeConfig) *TimeRangeConfig {
	if query.TimeRange != nil {
		if query.TimeRange.Type == TimeRangeNone {
			return nil
		}

		return query.TimeRange
	}

	return defaultRange
}
