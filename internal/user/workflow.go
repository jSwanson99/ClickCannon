package user

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"time"
)

type Workflow interface {
	NextQuery(ctx context.Context) (*ExecutableQuery, error)
	ThinkTime() time.Duration
	Name() string
}

func newWorkflow(log *slog.Logger, userCfg *Config, name string, cfg WorkflowBaseConfig, queryRunner QueryRunner, rng *rand.Rand, datasetStart, datasetEnd time.Time) (Workflow, error) {
	switch c := cfg.Config.(type) {
	case QueriesWorkflowConfig:
		return NewQueriesWorkflow(log, userCfg, name, &c, queryRunner, rng, datasetStart, datasetEnd), nil
	case HARWorkflowConfig:
		//return NewHARWorkflow(), nil
		return nil, fmt.Errorf("unimplemented workflow type %q", cfg.Type)
	default:
		return nil, fmt.Errorf("unknown workflow type %q", cfg.Type)
	}
}

type QueriesWorkflow struct {
	log          *slog.Logger
	name         string
	userCfg      *Config
	cfg          *QueriesWorkflowConfig
	queryRunner  QueryRunner
	rng          *rand.Rand
	index        int
	datasetStart time.Time
	datasetEnd   time.Time

	sampledTimeRange *ResolvedTimeRange
}

func NewQueriesWorkflow(log *slog.Logger, userCfg *Config, name string, cfg *QueriesWorkflowConfig, queryRunner QueryRunner, rng *rand.Rand, datasetStart, datasetEnd time.Time) *QueriesWorkflow {
	return &QueriesWorkflow{
		log:          log,
		name:         name,
		userCfg:      userCfg,
		cfg:          cfg,
		queryRunner:  queryRunner,
		rng:          rng,
		datasetStart: datasetStart,
		datasetEnd:   datasetEnd,
	}
}

func (b *QueriesWorkflow) Name() string {
	return b.name
}

func (b *QueriesWorkflow) anchor() time.Time {
	switch b.cfg.TimeAnchor {
	case TimeAnchorNow:
		return time.Now()
	case TimeAnchorDatasetEnd:
		return b.datasetEnd
	case TimeAnchorDatasetRandom:
		window := b.datasetEnd.Unix() - b.datasetStart.Unix()
		offset := b.rng.Int64N(window)
		return time.Unix(b.datasetStart.Unix()+offset, 0)
	default:
		return time.Now()
	}
}

func (b *QueriesWorkflow) NextQuery(ctx context.Context) (*ExecutableQuery, error) {
	queryIndex, q := b.nextQueryConfig()

	params := QueryParams{
		Database:  b.userCfg.Database,
		Table:     b.userCfg.Table,
		Preflight: make(map[string]string),
	}

	resolved := b.resolveTimeRange(q)
	var resolvedDuration time.Duration
	if resolved != nil {
		params.TimeStart = resolved.Start
		params.TimeEnd = resolved.End
		resolvedDuration = resolved.End.Sub(resolved.Start)
	}

	if q.PreflightQuery != nil {
		binds, err := b.queryRunner.ExecPreflight(ctx, q.PreflightQuery.Binds, q.PreflightQuery.SQL, q.PreflightQuery.Settings, params.Params())
		if err != nil {
			return nil, fmt.Errorf("preflight query for query (index=%d, name=%q) failed: %w", queryIndex, q.Name, err)
		}

		params.Preflight = binds
	}

	return &ExecutableQuery{
		QueryIndex: queryIndex,
		Name:       q.Name,
		SQL:        TryAppendFormatNull(q.SQL),
		Settings:   mergeSettings(b.cfg.DefaultSettings, q.Settings),
		Params:     params.Params(),
		TimeRange:  resolvedDuration,
		Perf:       q.Perf,
	}, nil
}

func (b *QueriesWorkflow) ThinkTime() time.Duration {
	tt := b.cfg.ThinkTime
	spread := tt.Max - tt.Min
	if spread <= 0 {
		return tt.Min
	}

	return tt.Min + time.Duration(b.rng.Int64N(int64(spread)))
}

func (b *QueriesWorkflow) resolveTimeRange(q *QueryConfig) *ResolvedTimeRange {
	// Query-level override
	if q.TimeRange != nil {
		if q.TimeRange.Type == TimeRangeNone {
			return nil
		}

		resolved, ok := q.TimeRange.Resolve(b.anchor(), b.rng)
		if !ok {
			return nil
		}

		return &resolved
	}

	// Default time range
	if b.cfg.DefaultTimeRange == nil || b.cfg.DefaultTimeRange.Type == TimeRangeNone {
		return nil
	}

	if b.cfg.TimeRangeCadence == TimeRangeCadencePerQuery || b.sampledTimeRange == nil {
		resolved, ok := b.cfg.DefaultTimeRange.Resolve(b.anchor(), b.rng)
		if !ok {
			return nil
		}

		b.sampledTimeRange = &resolved
	}

	return b.sampledTimeRange
}

func mergeSettings(defaults, overrides map[string]string) map[string]string {
	if len(defaults) == 0 {
		return overrides
	}

	merged := make(map[string]string, len(defaults)+len(overrides))
	for k, v := range defaults {
		merged[k] = v
	}

	for k, v := range overrides {
		merged[k] = v
	}

	return merged
}

func (b *QueriesWorkflow) nextQueryConfig() (int, *QueryConfig) {
	queries := b.cfg.Queries

	if b.cfg.Random {
		i := b.rng.IntN(len(queries))
		return i, &queries[i]
	}

	queryIndex := b.index % len(queries)
	loopWrapped := queryIndex == 0

	if loopWrapped && b.cfg.TimeRangeCadence == TimeRangeCadencePerLoop {
		b.sampledTimeRange = nil
	}

	q := &queries[queryIndex]
	b.index++

	return queryIndex, q
}
