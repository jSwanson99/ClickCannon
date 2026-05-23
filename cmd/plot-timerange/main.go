package main

import (
	"encoding/csv"
	"fmt"
	"hash/crc64"
	"math/rand/v2"
	"os"
	"sort"
	"strconv"
	"time"

	"clickcannon/internal/user"
)

const (
	runs = 4000000
)

// curve config
var cfg = user.TimeRangeConfig{
	Type:  user.TimeRangeLogNormal,
	Round: 1 * time.Minute,
	Mean:  100 * time.Minute,
	Min:   30 * time.Second,
	Max:   72 * time.Hour,
	Sigma: 0.6,
}

func newRand(seed string, workerID int) *rand.Rand {
	crc := crc64.Checksum([]byte(seed), crc64.MakeTable(crc64.ISO))
	return rand.New(rand.NewPCG(crc, uint64(workerID)))
}

func main() {
	rng := newRand("hyperdx", 0)
	buckets := map[time.Duration]int{}

	for range runs {
		d := cfg.SampleLookback(rng)
		d = d.Truncate(cfg.Round)
		buckets[d]++
	}

	keys := make([]time.Duration, 0, len(buckets))
	for k := range buckets {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })

	w := csv.NewWriter(os.Stdout)
	w.Write([]string{"x_minutes", "y_count"})
	for _, k := range keys {
		// pct := float64(buckets[k]) / float64(runs) * 100
		w.Write([]string{
			strconv.FormatFloat(k.Minutes(), 'f', 4, 64),
			strconv.Itoa(buckets[k]),
			// strconv.FormatFloat(pct, 'f', 3, 64),
		})
	}
	w.Flush()

	if err := w.Error(); err != nil {
		fmt.Fprintf(os.Stderr, "csv write error: %v\n", err)
		os.Exit(1)
	}
}
