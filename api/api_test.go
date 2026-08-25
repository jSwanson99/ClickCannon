package api_test

import (
	"context"
	"testing"
	"time"

	"github.com/ClickHouse/ClickCannon/api"
)

// generateOnly is a source-only config: it fills blocks and drops them, so it
// needs no sink and no network.
func generateOnly(rowsPerSecond uint64) api.Config {
	cfg := api.Config{}
	cfg.App.DataType = api.DataTypeLogs
	cfg.Generate = api.GenerateConfig{
		Enabled:             true,
		Threads:             1,
		RowsPerSecond:       rowsPerSecond,
		RowsPerBlock:        int(rowsPerSecond),
		ReuseBlocks:         true,
		BlockRetirementUses: 50,
	}

	return cfg
}

// TestRunAtFixedRate exercises the whole lifecycle and checks the rate limiter
// holds the configured rows per second.
func TestRunAtFixedRate(t *testing.T) {
	const runFor = 3 * time.Second

	r, err := api.NewRunner(generateOnly(10), nil, "")
	if err != nil {
		t.Fatalf("new runner: %v", err)
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

	t.Logf("generated %d rows over %s", stats.GeneratedRows, runFor)

	// Stop is idempotent.
	if err := r.Stop(); err != nil {
		t.Fatalf("second stop: %v", err)
	}
}

func TestStartTwiceFails(t *testing.T) {
	r, err := api.NewRunner(generateOnly(10), nil, "")
	if err != nil {
		t.Fatalf("new runner: %v", err)
	}

	if err := r.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer r.Stop()

	if err := r.Start(); err == nil {
		t.Fatal("expected an error on the second Start")
	}
}

func TestRejectsInvalidConfig(t *testing.T) {
	cfg := generateOnly(10)
	cfg.App.DataType = "metrics"

	if _, err := api.NewRunner(cfg, nil, ""); err == nil {
		t.Fatal("expected an error for an unsupported data type")
	}
}

func TestRoundTripYAML(t *testing.T) {
	cfg := generateOnly(10)
	cfg.App.DataType = api.DataTypeTraces

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
	if got.Generate.RowsPerSecond != cfg.Generate.RowsPerSecond {
		t.Fatalf("rows_per_second: got %d want %d", got.Generate.RowsPerSecond, cfg.Generate.RowsPerSecond)
	}
}
