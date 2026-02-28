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

	log.Info("config",
		"disk_enabled", cfg.Disk.Enabled,
		"disk_threads", cfg.Disk.Threads,
		"mb_per_second_limit", cfg.Disk.MiBytesPerSecondLimit,
		"reuse_blocks", cfg.Disk.ReuseBlocks,
		"insert_enabled", cfg.Insert.Enabled,
		"insert_threads", cfg.Insert.Threads,
		"batch_size", cfg.Insert.BatchSize,
	)

	blocksToAlloc := (cfg.Disk.Threads + cfg.Insert.Threads) * 4
	insertQueue := make(chan block.SharedColumns, blocksToAlloc)
	var (
		blockPool block.Pool
		err       error
	)
	var blockCreateFunc func() block.SharedColumns
	if cfg.App.DataType == app.ConfigDataTypeLogs {
		blockCreateFunc = func() block.SharedColumns {
			return block.NewLogsSharedColumns()
		}
	} else if cfg.App.DataType == app.ConfigDataTypeTraces {
		blockCreateFunc = func() block.SharedColumns {
			return block.NewTracesSharedColumns()
		}
	}

	if cfg.Disk.ReuseBlocks {
		blockPool, err = block.NewStructPool[block.SharedColumns](blocksToAlloc, func() (block.SharedColumns, error) {
			return block.NewTracesSharedColumns(), nil
		})
	} else {
		blockPool = block.NewGarbageBlockPool(blockCreateFunc)
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
		m, metricsErr := metrics.NewWorker(log, runID, cfg.App.DataType, targetBytesPerSecond, &cfg.Metrics, blockPool, insertQueue)
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

	var diskWg sync.WaitGroup
	diskCtx, cancelDisk := context.WithCancel(context.Background())
	if cfg.Disk.Enabled {
		dws := disk.NewScheduler(log, &cfg.Disk, cfg.GetDataFolder(), blockPool, insertQueue, metricsStore, !cfg.Insert.Enabled)
		diskWg.Add(1)
		go func() {
			defer diskWg.Done()
			dwsErr := dws.Run(diskCtx)
			if dwsErr != nil && !errors.Is(dwsErr, context.Canceled) {
				log.Error("disk worker scheduler error", "err", dwsErr)
			}

			close(insertQueue)
		}()
	}

	var insertWg sync.WaitGroup
	insertCtx, cancelInsert := context.WithCancel(context.Background())
	if cfg.Insert.Enabled {
		iws := insert.NewScheduler(log, &cfg.Insert, cfg.GetInsertTable(), blockCreateFunc, blockPool, insertQueue, metricsStore)
		insertWg.Add(1)
		go func() {
			defer insertWg.Done()
			iwsErr := iws.Run(insertCtx)
			if iwsErr != nil && !errors.Is(iwsErr, context.Canceled) {
				log.Error("insert worker scheduler error", "err", iwsErr)
			}
		}()
	}

	select {
	case <-terminate:
		log.Info("stop requested")
	}

	cancelDisk()
	diskWg.Wait()
	cancelInsert()
	insertWg.Wait()
	cancelMetrics()
	metricsWg.Wait()

	log.Info("done")
}
