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
	id       int
	idStr string
	log      *slog.Logger

	cfg         *Config
	workflow    Workflow
	queryRunner QueryRunner
	metrics     metrics.Store
}

func NewWorker(id int, log *slog.Logger, cfg *Config, workflow Workflow, queryRunner QueryRunner, metrics metrics.Store) *Worker {
	return &Worker{
		id:          id,
		idStr:    strconv.Itoa(id),
		log:         log, // scheduler sets component/id attributes
		cfg:         cfg,
		workflow:    workflow,
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

		q, err := w.workflow.NextQuery(ctx)
		if err != nil && errors.Is(err, context.Canceled) {
			continue
		} else if err != nil && errors.Is(err, sql.ErrNoRows) {
			w.log.Debug("preflight query had no rows, skipping", "err", err)
			continue
		} else if err != nil {
			return fmt.Errorf("worker %d workflow error: %w", w.id, err)
		}
		if q == nil {
			return nil
		}

		result, err := w.queryRunner.Exec(ctx, *q)
		if err != nil && errors.Is(err, context.Canceled) {
			continue
		} else if err != nil {
			w.log.Error("query failed", "name", q.Name, "err", err, "sql", q.SQL, "params", q.Params)
			w.metrics.IncrementMetric(metrics.QueriesFailedTotal, 1)
		w.metrics.IncrementMetricWithAttr(metrics.QueriesFailedWorkerTotal, 1, "worker_id", w.idStr)
		} else {
			attr := make(map[string]string, 7)
			attr["query_name"] = result.Query.Name
			attr["workflow_name"] = w.workflow.Name()
			if result.Query.TimeRange > 0 {
				attr["time_range_seconds"] = strconv.Itoa(int(result.Query.TimeRange.Seconds()))
			}
			if perf := result.Query.Perf; perf != nil {
				if perf.P50 > 0 {
					attr["perf.p50"] = strconv.Itoa(int(perf.P50.Microseconds()))
				}
				if perf.P90 > 0 {
					attr["perf.p90"] = strconv.Itoa(int(perf.P90.Microseconds()))
				}
				if perf.P95 > 0 {
					attr["perf.p95"] = strconv.Itoa(int(perf.P95.Microseconds()))
				}
				if perf.P99 > 0 {
					attr["perf.p99"] = strconv.Itoa(int(perf.P99.Microseconds()))
				}
			}

			w.metrics.AddMetricPointWithAttributes(metrics.QueryLatencyMicros, uint64(result.Duration.Microseconds()), attr)
			w.metrics.IncrementMetric(metrics.QueriesSucceededTotal, 1)
			w.metrics.IncrementMetricWithAttr(metrics.QueriesSucceededWorkerTotal, 1, "worker_id", w.idStr)

			w.log.Debug("ran query", "name", q.Name, "query_index", result.Query.QueryIndex, "latency", result.Duration, "time_range", result.Query.TimeRange.String(), "time_range_seconds", int(result.Query.TimeRange.Seconds()))
		}

		w.think(ctx)
	}
}

func (w *Worker) think(ctx context.Context) {
	delay := w.workflow.ThinkTime()
	select {
	case <-time.After(delay):
	case <-ctx.Done():
	}
}
