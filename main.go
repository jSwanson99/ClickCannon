package main

import (
	"context"
	"errors"
	"os"
	"os/signal"
	"otelspam/internal/app"
	"otelspam/internal/block"
	"otelspam/internal/disk"
	"otelspam/internal/insert"
	"otelspam/internal/metrics"
	"sync"
	"syscall"
)

func main() {
	runID, cfg, log, closeLogFile := app.Setup()
	if closeLogFile != nil {
		defer closeLogFile()
	}

	targetBytesPerSecond := cfg.Disk.MiBytesPerSecondLimit * 1024 * 1024

	blocksToAlloc := cfg.Disk.Threads + cfg.Insert.Threads
	insertQueue := make(chan block.SharedColumns, blocksToAlloc)
	var (
		blockPool *block.StructPool[block.SharedColumns]
		err       error
	)
	if cfg.App.DataType == app.ConfigDataTypeLogs {
		blockPool, err = block.NewStructPool[block.SharedColumns](blocksToAlloc, func() (block.SharedColumns, error) {
			return block.NewLogsSharedColumns(), nil
		})
	} else if cfg.App.DataType == app.ConfigDataTypeTraces {
		blockPool, err = block.NewStructPool[block.SharedColumns](blocksToAlloc, func() (block.SharedColumns, error) {
			return block.NewTracesSharedColumns(), nil
		})
	}
	if err != nil {
		log.Error("failed to alloc blocks", "err", err)
		return
	}

	terminate := make(chan os.Signal, 1)
	signal.Notify(terminate, os.Interrupt, syscall.SIGTERM)

	metricsCtx, cancelMetrics := context.WithCancel(context.Background())
	var metricsWg sync.WaitGroup

	var metricsStore metrics.Store
	if cfg.Metrics.Enabled {
		m, metricsErr := metrics.NewWorker(log, runID, cfg.App.DataType, targetBytesPerSecond, &cfg.Metrics)
		if metricsErr != nil {
			log.Error("failed to create metrics worker", "err", metricsErr)
		}
		metricsStore = m

		metricsWg.Add(1)
		go func() {
			defer metricsWg.Done()
			mErr := m.Run(metricsCtx)
			if mErr != nil && !errors.Is(mErr, context.Canceled) {
				log.Error("metrics worker error", "err", mErr)
			}
		}()
	} else {
		metricsStore = metrics.NewDisabledStore()
	}

	var wg sync.WaitGroup
	ctx, cancelAll := context.WithCancel(context.Background())

	if cfg.Disk.Enabled {
		ds := disk.NewScheduler(log, &cfg.Disk, cfg.GetDataFolder(), blockPool, insertQueue, metricsStore, !cfg.Insert.Enabled)
		wg.Add(1)
		go func() {
			defer wg.Done()
			dsErr := ds.Run(ctx)
			if dsErr != nil && !errors.Is(dsErr, context.Canceled) {
				log.Error("disk worker scheduler error", "err", dsErr)
			}

			close(insertQueue)
		}()
	}

	if cfg.Insert.Enabled {
		is := insert.NewScheduler(log, &cfg.Insert, cfg.GetInsertTable(), blockPool, insertQueue, metricsStore)
		wg.Add(1)
		go func() {
			defer wg.Done()
			isErr := is.Run(ctx)
			if isErr != nil && !errors.Is(isErr, context.Canceled) {
				log.Error("insert worker scheduler error", "err", isErr)
			}
		}()
	}

	select {
	case <-terminate:
		log.Info("stop requested")
		cancelAll()
	}

	wg.Wait()
	cancelMetrics()
	metricsWg.Wait()

	log.Info("done")
}
