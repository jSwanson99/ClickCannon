package user

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"otelspam/internal/metrics"
	"strconv"
	"time"
)

type Worker struct {
	id  int
	log *slog.Logger

	cfg         *Config
	behavior    Behavior
	queryRunner QueryRunner
	metrics     metrics.Store
}

func NewWorker(id int, log *slog.Logger, cfg *Config, behavior Behavior, queryRunner QueryRunner, metrics metrics.Store) *Worker {
	return &Worker{
		id:          id,
		log:         log, // scheduler sets component/id attributes
		cfg:         cfg,
		behavior:    behavior,
		metrics:     metrics,
		queryRunner: queryRunner,
	}
}

func (w *Worker) Run(ctx context.Context) error {
	w.log.Info("started")
	defer w.log.Info("stopped")

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		q, err := w.behavior.NextQuery(ctx)
		if err != nil && errors.Is(err, context.Canceled) {
			continue
		} else if err != nil && errors.Is(err, sql.ErrNoRows) {
			w.log.Debug("preflight query had no rows, skipping", "err", err)
			continue
		} else if err != nil {
			return fmt.Errorf("worker %d behavior error: %w", w.id, err)
		}
		if q == nil {
			return nil
		}

		result, err := w.queryRunner.Exec(ctx, *q)
		if err != nil && errors.Is(err, context.Canceled) {
			continue
		} else if err != nil {
			w.log.Error("query failed", "name", q.Name, "err", err, "sql", q.SQL, "params", q.Params)
			w.metrics.IncrementMetric(metrics.FailedUserQueriesPerSecond, 1)
		} else {
			attr := make(map[string]string, 5)
			attr["query_name"] = result.Query.Name
			attr["behavior_name"] = w.behavior.Name()
			if perf := result.Query.Perf; perf != nil {
				attr["perf.p50"] = strconv.Itoa(int(perf.P50.Microseconds()))
				attr["perf.p90"] = strconv.Itoa(int(perf.P90.Microseconds()))
				attr["perf.p95"] = strconv.Itoa(int(perf.P95.Microseconds()))
				attr["perf.p99"] = strconv.Itoa(int(perf.P99.Microseconds()))
			}

			w.metrics.AddMetricPointWithAttributes(metrics.QueryLatencyMicros, uint64(result.Duration.Microseconds()), attr)
			w.metrics.IncrementMetric(metrics.UserQueriesPerSecond, 1)

			w.log.Debug("ran query", "name", q.Name, "query_index", result.Query.QueryIndex, "latency", result.Duration)
		}

		w.think(ctx)
	}
}

func (w *Worker) think(ctx context.Context) {
	delay := w.behavior.ThinkTime()
	select {
	case <-time.After(delay):
	case <-ctx.Done():
	}
}
