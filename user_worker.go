package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"hash/crc64"
	"log"
	"math/rand/v2"
	"text/template"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
)

func newRand(runID string, workerID int) *rand.Rand {
	crc := crc64.Checksum([]byte(runID), crc64.MakeTable(crc64.ISO))
	return rand.New(rand.NewPCG(crc, uint64(workerID)))
}

type userWorker struct {
	id int
	r  *rand.Rand

	index int

	userConfig *UserConfig
	chConfig   *ClickHouseConfig
	isLogs     bool
	database   string
	table      string

	client clickhouse.Conn

	minQueryWait time.Duration
	maxQueryWait time.Duration

	metrics MetricsStore
}

func newUserWorker(testID string, id int, config *Config, metrics MetricsStore) (*userWorker, error) {
	w := userWorker{
		id: id,
		r:  newRand(testID, id),

		userConfig: &config.User,
		chConfig:   &config.Insert.ClickHouse,
		isLogs:     config.IsLogsData(),
		database:   config.Insert.ClickHouse.Database,
		table:      config.GetInsertTable(),

		minQueryWait: config.User.MinQueryWait,
		maxQueryWait: config.User.MaxQueryWait,

		metrics: metrics,
	}

	opt := clickhouse.Options{
		Addr: []string{config.Insert.ClickHouse.Address},
		Auth: clickhouse.Auth{
			Username: config.Insert.ClickHouse.User,
			Password: config.Insert.ClickHouse.Password,
		},
		MaxIdleConns: config.User.ConnectionsPerUser,
		MaxOpenConns: config.User.ConnectionsPerUser + 5,
	}
	if config.Insert.ClickHouse.Secure {
		opt.TLS = &tls.Config{}
	}

	conn, err := clickhouse.Open(&opt)
	if err != nil {
		return nil, fmt.Errorf("failed to connect: %w", err)
	}
	w.client = conn

	return &w, nil
}

func (w *userWorker) start(ctx context.Context) {
	for ctx.Err() == nil {
		upper := w.maxQueryWait.Milliseconds() - w.minQueryWait.Milliseconds()
		rVal := w.r.Int64N(upper)
		//w.log("rand: %d total: %d", rVal, w.minQueryWait.Milliseconds()+rVal)
		time.Sleep(time.Millisecond * (time.Duration(w.minQueryWait.Milliseconds()) + time.Duration(rVal)))

		sql, query := w.getNextSQL()

		now := time.Now()
		err := w.client.Exec(ctx, sql)
		if err != nil {
			w.logErr(fmt.Errorf("failed to run query: %w", err))
			continue
		}

		meta := query.Name
		if meta == "" {
			meta = query.SQL
		}

		w.metrics.AddMetricPoint(MetricNameQueryLatencyMicros, meta, uint64(time.Since(now).Microseconds()))
		w.metrics.IncrementMetric(MetricNameUserQueriesPerSecond, 1)
		w.index++
	}
}

type SQLTemplateParams struct {
	Table          string
	TimeRangeStart string
	TimeRangeEnd   string
}

func (w *userWorker) getNextSQL() (string, *UserQueryConfig) {
	queryIndex := w.index % len(w.userConfig.Queries)
	query := w.userConfig.Queries[queryIndex]
	sqlTemplate := query.SQL

	tmpl, err := template.New("sql").Parse(sqlTemplate)
	if err != nil {
		panic(fmt.Errorf("failed to parse query template at query index %d: %w", queryIndex, err))
	}

	end := time.Now()
	start := end.Add(-query.TimeRange)
	params := SQLTemplateParams{
		Table:          fmt.Sprintf(`%q.%q`, w.database, w.table),
		TimeRangeStart: fmt.Sprintf("fromUnixTimestamp64Milli(%d)", start.UnixMilli()),
		TimeRangeEnd:   fmt.Sprintf("fromUnixTimestamp64Milli(%d)", end.UnixMilli()),
	}

	var sqlOutput bytes.Buffer
	err = tmpl.Execute(&sqlOutput, params)
	if err != nil {
		panic(fmt.Errorf("failed to execute query template at query index %d: %w", queryIndex, err))
	}

	sqlOutput.WriteByte(' ')
	sqlOutput.WriteString("Format Null")

	return sqlOutput.String(), &query
}

func (w *userWorker) stop() {
	w.log("stopping")

	if w.client != nil {
		if err := w.client.Close(); err != nil {
			w.logErr(err)
		}
	}
}

func (w *userWorker) log(fmtStr string, args ...any) {
	log.Printf("[User Worker %d] %s\n", w.id, fmt.Sprintf(fmtStr, args...))
}

func (w *userWorker) logErr(err error) {
	log.Printf("[User Worker %d | error] %s\n", w.id, err.Error())
}
