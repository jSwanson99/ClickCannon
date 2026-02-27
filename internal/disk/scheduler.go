package disk

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"otelspam/internal/block"
	"otelspam/internal/metrics"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

type Scheduler struct {
	log         *slog.Logger
	workerLog   *slog.Logger
	diskCfg     *Config
	folderPath  string
	passthrough bool

	setFirstTimestamp bool

	blockPool   *block.StructPool[block.SharedColumns]
	insertQueue chan<- block.SharedColumns
	metrics     metrics.Store

	mu      sync.RWMutex
	workers map[int]*worker
}

func NewScheduler(log *slog.Logger, diskCfg *Config, folderPath string, blockPool *block.StructPool[block.SharedColumns], insertQueue chan<- block.SharedColumns, metrics metrics.Store, passthrough bool) *Scheduler {
	return &Scheduler{
		log:         log.With("component", "disk_scheduler"),
		workerLog:   log,
		diskCfg:     diskCfg,
		folderPath:  folderPath,
		passthrough: passthrough,
		blockPool:   blockPool,
		insertQueue: insertQueue,
		metrics:     metrics,
		workers:     make(map[int]*worker),
	}
}

func (s *Scheduler) Run(ctx context.Context) error {
	s.log.Info("started")

	dataFiles, err := getDataFiles(s.folderPath)
	if err != nil {
		return fmt.Errorf("failed to load data files: %w", err)
	}

	if len(dataFiles) == 0 {
		s.log.Warn("no data files found, exiting")
		return nil
	}

	s.log.Info("found files", "file_count", len(dataFiles))

	var wg sync.WaitGroup
	fileCh := s.generateFiles(ctx, dataFiles)
	firstTimestampReply := make(chan time.Time, 1)

	for i := range s.diskCfg.Threads {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			s.metrics.IncrementMetric(metrics.ActiveReaders, 1)
			s.runWorker(ctx, id, fileCh, firstTimestampReply)
			s.metrics.DecrementMetric(metrics.ActiveReaders, 1)
		}(i)
	}

	select {
	case <-ctx.Done():
	case firstTimestamp := <-firstTimestampReply:
		s.updateFirstTimestamp(firstTimestamp)
		s.log.Info("updated first timestamp in dataset for workers", "count", len(s.workers), "first_timestamp", firstTimestamp)
	}

	wg.Wait()
	s.log.Info("stopped")

	return nil
}

func (s *Scheduler) generateFiles(ctx context.Context, dataFiles []dataFile) <-chan dataFile {
	ch := make(chan dataFile)

	go func() {
		defer close(ch)
		i := 0
		for {
			for j, f := range dataFiles {
				select {
				case <-ctx.Done():
					return
				case ch <- f:
					if i > 0 && j == 0 {
						s.log.Info("looped files", "loop_count", i, "file_count", len(dataFiles))
					}
				}
			}

			if !s.diskCfg.Loop {
				return
			}

			i++
		}
	}()

	return ch
}

func (s *Scheduler) runWorker(ctx context.Context, workerID int, fileCh <-chan dataFile, firstTimestampReply chan<- time.Time) {
	for {
		select {
		case <-ctx.Done():
			return
		case file, ok := <-fileCh:
			if !ok {
				return
			}

			w := newWorker(workerID, s.workerLog, file.Path, file.Compressed, s.diskCfg.ShiftTimestamp, s.getNewWorkerSpeed(), s.blockPool, s.insertQueue, s.metrics, s.passthrough, firstTimestampReply)

			s.register(workerID, w)
			s.rebalanceSpeed()

			err := w.Run(ctx)

			s.unregister(workerID)
			s.rebalanceSpeed()

			if err != nil && !errors.Is(err, context.Canceled) {
				s.log.Warn("worker failed, skipping file", "worker_id", workerID, "file", file.Path, "err", err)
			}
		}
	}
}

func (s *Scheduler) register(id int, w *worker) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.workers[id] = w
}

func (s *Scheduler) unregister(id int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.workers, id)
}

func (s *Scheduler) rebalanceSpeed() {
	s.mu.RLock()
	defer s.mu.RUnlock()

	count := len(s.workers)
	if count == 0 {
		return
	}

	perWorker := (s.diskCfg.MiBytesPerSecondLimit * 1024 * 1024) / uint64(count)
	for _, w := range s.workers {
		w.UpdateSpeedLimit(perWorker)
	}
}

func (s *Scheduler) getNewWorkerSpeed() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// TODO: this is lazy. +1, compared to rebalanceSpeed()
	return (s.diskCfg.MiBytesPerSecondLimit * 1024 * 1024) / uint64(len(s.workers)+1)
}

func (s *Scheduler) updateFirstTimestamp(firstTimestamp time.Time) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	for _, w := range s.workers {
		w.SetFirstTimestamp(firstTimestamp)
	}
}

type dataFile struct {
	Path       string
	Compressed bool
}

func getDataFiles(folderPath string) ([]dataFile, error) {
	var files []dataFile

	err := filepath.Walk(folderPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if info.IsDir() {
			return nil
		}

		if strings.HasSuffix(path, ".native.zst") {
			files = append(files, dataFile{
				Path:       path,
				Compressed: true,
			})
		} else if strings.HasSuffix(path, ".native") {
			files = append(files, dataFile{
				Path:       path,
				Compressed: false,
			})
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	sort.Slice(files, func(i, j int) bool {
		return files[i].Path < files[j].Path
	})

	return files, nil
}
