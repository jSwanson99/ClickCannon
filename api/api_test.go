package api_test

import (
	"context"
	"testing"
	"time"

	"github.com/ClickHouse/ClickCannon/api"
)

func TestOTLPLoadDefaultIsSmokeRate(t *testing.T) {
	cfg := api.OTLPLoad("collector.example.com:4317", api.DataTypeLogs)

	if err := cfg.Validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}

	if cfg.OTel.URL != "collector.example.com:4317" {
		t.Fatalf("url: got %q", cfg.OTel.URL)
	}
	if cfg.Generate.RowsPerSecond != 10 {
		t.Fatalf("rows_per_second: got %d want 10", cfg.Generate.RowsPerSecond)
	}
	if cfg.Generate.Threads != 1 {
		t.Fatalf("generate threads: got %d want 1", cfg.Generate.Threads)
	}
	if cfg.Generate.RowsPerBlock != 10 {
		t.Fatalf("rows_per_block: got %d want 10", cfg.Generate.RowsPerBlock)
	}
}

// TestBlockSizeTracksRate guards the trap where RowsPerBlock and RowsPerSecond
// disagree: the limiter is charged a whole block at a time, so a block that is
// too large stalls a slow generator and one that is too small floods the sink
// with tiny requests.
func TestBlockSizeTracksRate(t *testing.T) {
	for _, tc := range []struct {
		name         string
		rps          uint64
		source, sink int
		wantBlock    int
	}{
		{"smoke", 10, 1, 1, 10},
		{"scale up", 100_000, 8, 8, 10_000},
		{"moderate", 50_000, 2, 2, 10_000},
		{"low rate many threads", 4, 8, 8, 1},
		{"unlimited", 0, 4, 4, 10_000},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := api.OTLPLoad("collector.example.com:4317", api.DataTypeLogs)
			cfg.OTel.Headers = map[string]string{"authorization": "test-key"}
			api.SetRowsPerSecond(&cfg, tc.rps)
			api.SetThreads(&cfg, tc.source, tc.sink)

			if err := cfg.Validate(); err != nil {
				t.Fatalf("validate: %v", err)
			}

			if cfg.Generate.RowsPerBlock != tc.wantBlock {
				t.Fatalf("rows_per_block: got %d want %d", cfg.Generate.RowsPerBlock, tc.wantBlock)
			}
			if cfg.OTel.BatchSize != tc.wantBlock {
				t.Fatalf("batch_size: got %d want %d", cfg.OTel.BatchSize, tc.wantBlock)
			}
			if cfg.OTel.Threads != tc.sink {
				t.Fatalf("otel threads: got %d want %d", cfg.OTel.Threads, tc.sink)
			}
		})
	}
}

// TestBlockSizeOverride checks a manual size set after SetThreads sticks.
func TestBlockSizeOverride(t *testing.T) {
	cfg := api.OTLPLoad("collector.example.com:4317", api.DataTypeLogs)
	api.SetRowsPerSecond(&cfg, 100_000)
	api.SetThreads(&cfg, 4, 4)

	cfg.Generate.RowsPerBlock = 250
	cfg.OTel.BatchSize = 500

	if err := cfg.Validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
	if cfg.Generate.RowsPerBlock != 250 {
		t.Fatalf("rows_per_block: got %d want 250", cfg.Generate.RowsPerBlock)
	}
	if cfg.OTel.BatchSize != 500 {
		t.Fatalf("batch_size: got %d want 500", cfg.OTel.BatchSize)
	}
}

// TestRunPassthroughAtSmokeRate exercises the lifecycle with no sink and no
// network, and checks the default preset really does hold ~10 rows/second.
func TestRunPassthroughAtSmokeRate(t *testing.T) {
	const runFor = 3 * time.Second

	cfg := api.OTLPLoad("unused:4317", api.DataTypeLogs)
	cfg.OTel.Enabled = false

	r, err := api.NewRunner(cfg, nil, "")
	if err != nil {
		t.Fatalf("new: %v", err)
	}

	if err := r.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), runFor)
	defer cancel()

	if err := r.Wait(ctx); err != nil {
		t.Fatalf("wait: %v", err)
	}

	// The limiter starts with a full burst of one block, so expect roughly
	// (seconds + 1) blocks of 10 rows. Bounded loosely to stay non-flaky.
	const lower, upper = 10, 60
	stats := r.Stats()
	if stats.GeneratedRows < lower || stats.GeneratedRows > upper {
		t.Fatalf("generated %d rows in %s, want between %d and %d at 10 rows/second",
			stats.GeneratedRows, runFor, lower, upper)
	}

	t.Logf("generated %d rows in %d blocks over %s", stats.GeneratedRows, stats.GeneratedBlocks, runFor)

	if err := r.Stop(); err != nil {
		t.Fatalf("second stop: %v", err)
	}
}

func TestRoundTripYAML(t *testing.T) {
	cfg := api.OTLPLoad("collector.example.com:4317", api.DataTypeTraces)

	data, err := api.ToYAML(cfg)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	got, err := api.ParseYAML(data)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	if got.App.DataType != api.DataTypeTraces {
		t.Fatalf("data type: got %q", got.App.DataType)
	}
	if got.OTel.URL != cfg.OTel.URL {
		t.Fatalf("url: got %q want %q", got.OTel.URL, cfg.OTel.URL)
	}
	if got.Generate.RowsPerSecond != cfg.Generate.RowsPerSecond {
		t.Fatalf("rows_per_second: got %d want %d", got.Generate.RowsPerSecond, cfg.Generate.RowsPerSecond)
	}
}
