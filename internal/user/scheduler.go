package user

import (
	"clickcannon/internal/metrics"
	"context"
	"errors"
	"fmt"
	"hash/crc64"
	"log/slog"
	"math/rand/v2"
	"sync"
	"time"
)

type Scheduler struct {
	log       *slog.Logger
	workerLog *slog.Logger
	cfg       *Config

	seed string

	metrics metrics.Store
}

func NewScheduler(log *slog.Logger, seed string, cfg *Config, m metrics.Store) *Scheduler {
	return &Scheduler{
		log:       log.With("component", "user_scheduler"),
		workerLog: log,
		cfg:       cfg,
		seed:      seed,
		metrics:   m,
	}
}

func (s *Scheduler) Run(ctx context.Context) error {
	s.log.Info("started")

	if s.cfg.Duration > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, s.cfg.Duration)
		defer cancel()
	}

	rampStep := rampInterval(s.cfg.RampDuration, s.cfg.Threads)

	logStopped := func() {
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			s.log.Info("user duration elapsed")
		}

		s.log.Info("stopped")
	}

	var wg sync.WaitGroup
	for i := range s.cfg.Threads {
		if rampStep > 0 {
			select {
			case <-ctx.Done():
				wg.Wait()
				logStopped()
				return nil
			case <-time.After(rampStep):
			}
		}

		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			s.metrics.IncrementMetric(metrics.ActiveUsers, 1)
			s.runWorker(ctx, id)
			s.metrics.DecrementMetric(metrics.ActiveUsers, 1)
		}(i)
	}

	wg.Wait()
	logStopped()

	return nil
}

func (s *Scheduler) runWorker(ctx context.Context, id int) {
	const (
		baseBackoff = 500 * time.Millisecond
		maxBackoff  = 30 * time.Second
	)

	backoff := baseBackoff

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		w, err := s.newWorker(id)
		if err != nil {
			s.log.Warn("failed to init user worker, retrying", "worker_id", id, "err", err, "backoff", backoff)
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}
			backoff = min(backoff*2, maxBackoff)
			continue
		}

		backoff = baseBackoff
		err = w.Run(ctx)

		switch {
		case err == nil:
			return
		case errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded):
			return
		default:
			s.log.Warn("user worker failed, restarting", "worker_id", id, "err", err, "backoff", backoff)
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}
			backoff = min(backoff*2, maxBackoff)
		}
	}
}

func newRand(seed string, workerID int) *rand.Rand {
	crc := crc64.Checksum([]byte(seed), crc64.MakeTable(crc64.ISO))
	return rand.New(rand.NewPCG(crc, uint64(workerID)))
}

func (s *Scheduler) newWorker(id int) (*Worker, error) {
	rng := newRand(s.seed, id)
	datasetStart := time.Unix(int64(s.cfg.DatasetUnixStart), 0)
	datasetEnd := time.Unix(int64(s.cfg.DatasetUnixEnd), 0)

	workflowCfg := s.cfg.Workflows[0]

	workerLog := s.workerLog.With("component", "user_worker", "id", id, "workflow", workflowCfg.Type, "workflow_name", workflowCfg.Name)

	queryRunner, err := NewClickHouseQueryRunner(s.cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create query runner: %w", err)
	}

	workflow, err := newWorkflow(workerLog, s.cfg, workflowCfg.Name, workflowCfg, queryRunner, rng, datasetStart, datasetEnd, s.metrics)
	if err != nil {
		return nil, fmt.Errorf("failed to create workflow (type: %q, name: %q): %w", workflowCfg.Type, workflowCfg.Name, err)
	}

	return NewWorker(id, workerLog, s.cfg, workflow, queryRunner, s.metrics), nil
}

func rampInterval(ramp time.Duration, threads int) time.Duration {
	if threads <= 1 || ramp <= 0 {
		return 0
	}
	return ramp / time.Duration(threads)
}
