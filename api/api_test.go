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

// TestRunLifecycle exercises the whole lifecycle. It cannot check the rate
// limiter: Stats needs Config.Metrics, which needs a ClickHouse DSN.
func TestRunLifecycle(t *testing.T) {
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

	// Metrics are disabled, so nothing is counted.
	if stats := r.Stats(); stats != (api.Stats{}) {
		t.Fatalf("expected zero stats with metrics disabled, got %+v", stats)
	}

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
