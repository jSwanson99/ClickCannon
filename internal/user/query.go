package user

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
)

type QueryRunner interface {
	Exec(ctx context.Context, q ExecutableQuery) (QueryResult, error)
	ExecPreflight(ctx context.Context, bindNames []string, sql string, settings map[string]string, params []any) (map[string]string, error)
}

type ExecutableQuery struct {
	QueryIndex int
	Name       string
	SQL        string
	Settings   map[string]string
	Params     []any
	Perf       *PerfConfig
}

type QueryResult struct {
	Query    ExecutableQuery
	Duration time.Duration
}

type QueryParams struct {
	Database string
	Table    string

	TimeStart time.Time
	TimeEnd   time.Time

	Preflight map[string]string
}

func (p QueryParams) Params() []any {
	params := []any{
		clickhouse.Named("database", p.Database),
		clickhouse.Named("table", p.Table),
		clickhouse.DateNamed("time_start", p.TimeStart, clickhouse.MilliSeconds),
		clickhouse.DateNamed("time_end", p.TimeEnd, clickhouse.MilliSeconds),
	}

	for k, v := range p.Preflight {
		params = append(params, clickhouse.Named(k, v))
	}

	return params
}

func TryAppendFormatNull(sql string) string {
	if strings.Contains(strings.ToUpper(sql), "SETTINGS") || strings.Contains(strings.ToUpper(sql), "FORMAT") {
		return sql
	}

	return strings.TrimRight(sql, " \t\n") + "\nFORMAT Null"
}

type ClickHouseQueryRunner struct {
	client clickhouse.Conn
}

func NewClickHouseQueryRunner(cfg *Config) (*ClickHouseQueryRunner, error) {
	qr := ClickHouseQueryRunner{}

	opt, err := clickhouse.ParseDSN(cfg.ClickHouseDSN)
	if err != nil {
		return nil, fmt.Errorf("failed to parse DSN: %w", err)
	}

	opt.MaxIdleConns = cfg.ConnectionsPerThread
	opt.MaxOpenConns = cfg.ConnectionsPerThread + 5

	client, err := clickhouse.Open(opt)
	if err != nil {
		return nil, fmt.Errorf("failed to connect: %w", err)
	}
	qr.client = client

	return &qr, nil
}

func (r *ClickHouseQueryRunner) Exec(ctx context.Context, q ExecutableQuery) (QueryResult, error) {
	ctx = clickhouse.Context(ctx, clickhouse.WithSettings(settingsToClickHouseSettings(q.Settings)))

	queryStart := time.Now()
	err := r.client.Exec(ctx, q.SQL, q.Params...)
	if errors.Is(err, context.Canceled) {
		return QueryResult{}, err
	} else if err != nil {
		return QueryResult{}, fmt.Errorf("failed to run query: %w", err)
	}

	return QueryResult{
		Query:    q,
		Duration: time.Since(queryStart),
	}, nil
}

func (r *ClickHouseQueryRunner) ExecPreflight(ctx context.Context, bindNames []string, sql string, settings map[string]string, params []any) (map[string]string, error) {
	ctx = clickhouse.Context(ctx, clickhouse.WithSettings(settingsToClickHouseSettings(settings)))
	row := r.client.QueryRow(ctx, sql, params...)
	if errors.Is(row.Err(), context.Canceled) {
		return nil, row.Err()
	} else if row.Err() != nil {
		return nil, fmt.Errorf("failed to run fetch value query: %w", row.Err())
	}

	bindValues := make([]string, len(bindNames))
	scanTargets := make([]any, len(bindValues))
	for i := range bindValues {
		scanTargets[i] = &bindValues[i]
	}

	err := row.Scan(scanTargets...)
	if err != nil {
		return nil, fmt.Errorf("failed to scan values in preflight query: %w", err)
	}

	binds := make(map[string]string, len(bindNames))
	for i, name := range bindNames {
		binds[name] = bindValues[i]
	}

	return binds, nil
}

func settingsToClickHouseSettings(settings map[string]string) clickhouse.Settings {
	chSettings := make(clickhouse.Settings, len(settings))
	if settings == nil {
		return chSettings
	}

	for k, v := range settings {
		chSettings[k] = v
	}

	return chSettings
}
