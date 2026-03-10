package main

import (
	"context"
	"errors"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"otelspam/internal/app"
	"otelspam/internal/block"
	"otelspam/internal/disk"
	"otelspam/internal/insert"
	"otelspam/internal/metrics"
	"otelspam/internal/user"
	"sync"
	"syscall"
)

func main() {
	runID, cfgFileName, cfg, log, closeLogFile := app.Setup()
	if closeLogFile != nil {
		defer closeLogFile()
	}

	if cfg.Pprof.Address != "" {
		go func() {
			log.Info("pprof listening", "address", cfg.Pprof.Address)
			if err := http.ListenAndServe(cfg.Pprof.Address, nil); err != nil {
				log.Error("pprof server error", "err", err)
			}
		}()
	}

	runName := cfgFileName
	if cfg.App.Name != "" {
		runName = cfg.App.Name
	}

	targetBytesPerSecond := cfg.Disk.MiBytesPerSecondLimit * 1024 * 1024

	log.Info("config",
		"config_file", cfgFileName,
		"run_name", runName,
		"disk_enabled", cfg.Disk.Enabled,
		"disk_threads", cfg.Disk.Threads,
		"mb_per_second_limit", cfg.Disk.MiBytesPerSecondLimit,
		"reuse_blocks", cfg.Disk.ReuseBlocks,
		"insert_enabled", cfg.Insert.Enabled,
		"insert_threads", cfg.Insert.Threads,
		"batch_size", cfg.Insert.BatchSize,
	)

	blocksToAlloc := (cfg.Disk.Threads + cfg.Insert.Threads) * 2
	insertQueue := make(chan block.SharedColumns, blocksToAlloc)
	var (
		blockPool block.Pool
		err       error
	)
	var blockCreateFunc func() block.SharedColumns
	if cfg.App.DataType == app.ConfigDataTypeLogs {
		blockCreateFunc = func() block.SharedColumns {
			return block.NewLogsSharedColumns(cfg.Disk.HasTimestampTime)
		}
	} else if cfg.App.DataType == app.ConfigDataTypeTraces {
		blockCreateFunc = func() block.SharedColumns {
			return block.NewTracesSharedColumns()
		}
	}

	if cfg.Disk.ReuseBlocks {
		blockPool, err = block.NewStructPool[block.SharedColumns](blocksToAlloc, func() (block.SharedColumns, error) {
			return blockCreateFunc(), nil
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
		m, metricsErr := metrics.NewWorker(log, runID, runName, cfg.App.DataType, targetBytesPerSecond, make(map[string]string), &cfg.Metrics, blockPool, insertQueue)
		if metricsErr != nil {
			log.Error("failed to create metrics worker", "err", metricsErr)
			return
		}
		metricsStore = m

		metricsWg.Go(func() {
			mErr := m.Run(metricsCtx)
			if mErr != nil && !errors.Is(mErr, context.Canceled) {
				log.Error("metrics worker error", "err", mErr)
			}
		})
	} else {
		metricsStore = metrics.NewDisabledStore()
	}

	var diskWg sync.WaitGroup
	diskCtx, cancelDisk := context.WithCancel(context.Background())
	if cfg.Disk.Enabled {
		dws := disk.NewScheduler(log, &cfg.Disk, cfg.GetDataFolder(), blockPool, insertQueue, metricsStore, !cfg.Insert.Enabled)
		diskWg.Go(func() {
			dwsErr := dws.Run(diskCtx)
			if dwsErr != nil && !errors.Is(dwsErr, context.Canceled) {
				log.Error("disk worker scheduler error", "err", dwsErr)
			}
			close(insertQueue)
		})
	} else {
		close(insertQueue)
	}

	var insertWg sync.WaitGroup
	insertCtx, cancelInsert := context.WithCancel(context.Background())
	if cfg.Insert.Enabled {
		if !cfg.Disk.Enabled {
			log.Warn("insert is enabled but disk is disabled, insert workers will not start")
		} else {
			iws := insert.NewScheduler(log, &cfg.Insert, cfg.GetInsertTable(), blockCreateFunc, blockPool, insertQueue, metricsStore)
			insertWg.Go(func() {
				iwsErr := iws.Run(insertCtx)
				if iwsErr != nil && !errors.Is(iwsErr, context.Canceled) {
					log.Error("insert worker scheduler error", "err", iwsErr)
				}
			})
		}
	}

	var userWg sync.WaitGroup
	userCtx, cancelUser := context.WithCancel(context.Background())
	if cfg.User.Enabled {
		uws := user.NewScheduler(log, cfg.App.Seed, &cfg.User, metricsStore)
		userWg.Go(func() {
			uwsErr := uws.Run(userCtx)
			if uwsErr != nil && !errors.Is(uwsErr, context.Canceled) {
				log.Error("user worker scheduler error", "err", uwsErr)
			}
		})
	}

	done := make(chan struct{})
	go func() {
		diskWg.Wait()
		insertWg.Wait()
		userWg.Wait()
		close(done)
	}()

	select {
	case <-terminate:
		log.Info("stop requested")
	case <-done:
	}

	cancelDisk()
	diskWg.Wait()
	cancelInsert()
	insertWg.Wait()
	cancelUser()
	userWg.Wait()
	cancelMetrics()
	metricsWg.Wait()

	log.Info("done")
}
