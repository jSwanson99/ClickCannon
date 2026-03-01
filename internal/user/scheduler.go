package user

import (
	"context"
	"errors"
	"fmt"
	"hash/crc64"
	"log/slog"
	"math/rand/v2"
	"otelspam/internal/metrics"
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

	rampStep := rampInterval(s.cfg.RampDuration, s.cfg.Threads)

	var wg sync.WaitGroup
	for i := range s.cfg.Threads {
		if rampStep > 0 {
			select {
			case <-ctx.Done():
				wg.Wait()
				s.log.Info("stopped")
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
	s.log.Info("stopped")

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
		case errors.Is(err, context.Canceled):
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

	behaviorCfg := s.cfg.Behaviors[0]

	workerLog := s.workerLog.With("component", "user_worker", "id", id, "behavior", behaviorCfg.Type, "behavior_name", behaviorCfg.Name)

	queryRunner, err := NewClickHouseQueryRunner(s.cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create query runner: %w", err)
	}

	behavior, err := newBehavior(workerLog, s.cfg, behaviorCfg.Name, behaviorCfg, queryRunner, rng, datasetStart, datasetEnd)
	if err != nil {
		return nil, fmt.Errorf("failed to create behavior (type: %q, name: %q): %w", behaviorCfg.Type, behaviorCfg.Name, err)
	}

	return NewWorker(id, workerLog, s.cfg, behavior, queryRunner, s.metrics), nil
}

func rampInterval(ramp time.Duration, threads int) time.Duration {
	if threads <= 1 || ramp <= 0 {
		return 0
	}
	return ramp / time.Duration(threads)
}
