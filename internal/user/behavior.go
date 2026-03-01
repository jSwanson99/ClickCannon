package user

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"time"
)

type Behavior interface {
	NextQuery(ctx context.Context) (*ExecutableQuery, error)
	ThinkTime() time.Duration
	Name() string
}

func newBehavior(log *slog.Logger, userCfg *Config, name string, cfg BehaviorBaseConfig, queryRunner QueryRunner, rng *rand.Rand, datasetStart, datasetEnd time.Time) (Behavior, error) {
	switch c := cfg.Config.(type) {
	case QueriesBehaviorConfig:
		return NewQueriesBehavior(log, userCfg, name, &c, queryRunner, rng, datasetStart, datasetEnd), nil
	case HARBehaviorConfig:
		//return NewHARBehavior(), nil
		return nil, fmt.Errorf("unimplemented behavior type %q", cfg.Type)
	default:
		return nil, fmt.Errorf("unknown behavior type %q", cfg.Type)
	}
}

type QueriesBehavior struct {
	log          *slog.Logger
	name         string
	userCfg      *Config
	cfg          *QueriesBehaviorConfig
	queryRunner  QueryRunner
	rng          *rand.Rand
	index        int
	datasetStart time.Time
	datasetEnd   time.Time
}

func NewQueriesBehavior(log *slog.Logger, userCfg *Config, name string, cfg *QueriesBehaviorConfig, queryRunner QueryRunner, rng *rand.Rand, datasetStart, datasetEnd time.Time) *QueriesBehavior {
	return &QueriesBehavior{
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

func (b *QueriesBehavior) Name() string {
	return b.name
}

func (b *QueriesBehavior) anchor() time.Time {
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

func (b *QueriesBehavior) NextQuery(ctx context.Context) (*ExecutableQuery, error) {
	queryIndex, q := b.nextQueryConfig()

	anchor := b.anchor()
	tr := EffectiveTimeRange(q, b.cfg.DefaultTimeRange)

	params := QueryParams{
		Database:  b.userCfg.Database,
		Table:     b.userCfg.Table,
		Preflight: make(map[string]string),
	}
	if tr != nil {
		resolved, ok := tr.Resolve(anchor, b.rng)
		if ok {
			params.TimeStart = resolved.Start
			params.TimeEnd = resolved.End
		}
	}

	if q.PreflightQuery != nil {
		val, err := b.queryRunner.FetchValue(ctx, q.PreflightQuery.SQL, params.Params())
		if err != nil {
			return nil, fmt.Errorf("preflight %q: %w", q.PreflightQuery.Bind, err)
		}

		params.Preflight[q.PreflightQuery.Bind] = val
	}

	return &ExecutableQuery{
		QueryIndex: queryIndex,
		Name:       q.Name,
		SQL:        TryAppendFormatNull(q.SQL),
		Params:     params.Params(),
		Perf:       q.Perf,
	}, nil
}

func (b *QueriesBehavior) ThinkTime() time.Duration {
	tt := b.cfg.ThinkTime
	spread := tt.Max - tt.Min
	if spread <= 0 {
		return tt.Min
	}

	return tt.Min + time.Duration(b.rng.Int64N(int64(spread)))
}

func (b *QueriesBehavior) nextQueryConfig() (int, *QueryConfig) {
	var queryIndex int
	queries := b.cfg.Queries
	if b.cfg.Random {
		queryIndex = b.rng.IntN(len(queries))
		return queryIndex, &queries[queryIndex]
	}

	queryIndex = b.index % len(queries)
	q := &queries[queryIndex]
	b.index++

	return queryIndex, q
}
