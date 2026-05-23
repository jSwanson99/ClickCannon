package insert

import (
	"clickcannon/internal/block"
	"clickcannon/internal/metrics"
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"
)

type Scheduler struct {
	log             *slog.Logger
	workerLog       *slog.Logger
	insertCfg       *Config
	nb              *nodeBalancer
	blockCreateFunc func() block.SharedColumns
	blockPool       block.Pool
	queue           <-chan block.SharedColumns
	metrics         metrics.Store
	insertTable     string
}

func NewScheduler(
	log *slog.Logger,
	insertCfg *Config,
	insertTable string,
	blockCreateFunc func() block.SharedColumns,
	blockPool block.Pool,
	queue <-chan block.SharedColumns,
	metrics metrics.Store,
) *Scheduler {
	return &Scheduler{
		log:             log.With("component", "insert_scheduler"),
		workerLog:       log,
		insertCfg:       insertCfg,
		blockCreateFunc: blockCreateFunc,
		blockPool:       blockPool,
		queue:           queue,
		metrics:         metrics,
		insertTable:     insertTable,
	}
}

func (s *Scheduler) Run(ctx context.Context) error {
	s.log.Info("started")

	var wg sync.WaitGroup

	if s.insertCfg.BalanceNodes {
		s.nb = newNodeBalancer(s.workerLog, &s.insertCfg.ClickHouse)
		err := s.nb.FetchNodes()
		if err != nil {
			s.log.Error("failed to fetch nodes for node balancer", "err", err)
		}
	}

	for i := range s.insertCfg.Threads {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			s.metrics.IncrementMetric(metrics.ActiveInserters, 1)
			s.runWorker(ctx, id)
			s.metrics.DecrementMetric(metrics.ActiveInserters, 1)
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

		if len(s.queue) == 0 {
			select {
			case _, ok := <-s.queue:
				if !ok {
					return
				}
			default:
			}
		}

		w := newWorker(id, s.workerLog, s.insertCfg, s.nb, s.blockCreateFunc, s.blockPool, s.insertTable, s.queue, s.metrics)
		err := w.Run(ctx)

		switch {
		case err == nil:
			return
		case errors.Is(err, context.Canceled):
			return
		case errors.Is(err, errWorkerRetired):
			backoff = baseBackoff
		default:
			s.log.Warn("insert worker failed, restarting", "worker_id", id, "err", err, "backoff", backoff)
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}
			backoff = min(backoff*2, maxBackoff)
		}
	}
}
