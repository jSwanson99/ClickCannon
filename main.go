package main

import (
	"clickcannon/internal/app"
	"clickcannon/internal/block"
	"clickcannon/internal/disk"
	"clickcannon/internal/generate"
	"clickcannon/internal/insert"
	"clickcannon/internal/metrics"
	"clickcannon/internal/user"
	"context"
	"errors"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
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
		"generate_enabled", cfg.Generate.Enabled,
		"insert_enabled", cfg.Insert.Enabled,
		"insert_threads", cfg.Insert.Threads,
		"batch_size", cfg.Insert.BatchSize,
	)

	// Determine source thread count for block pool sizing
	sourceThreads := cfg.Disk.Threads
	if cfg.Generate.Enabled {
		sourceThreads = cfg.Generate.Threads
	}

	blocksToAlloc := (sourceThreads + cfg.Insert.Threads) * 2
	insertQueue := make(chan block.SharedColumns, blocksToAlloc)
	var blockCreateFunc func() block.SharedColumns

	if cfg.Generate.Enabled {
		// Generate mode: use generate-specific column types with working Append
		if cfg.App.DataType == app.ConfigDataTypeLogs {
			blockCreateFunc = func() block.SharedColumns {
				return generate.NewGenLogsColumns()
			}
		} else if cfg.App.DataType == app.ConfigDataTypeTraces {
			blockCreateFunc = func() block.SharedColumns {
				return generate.NewGenTracesColumns()
			}
		}
	} else {
		// Disk mode: use decode-oriented column types
		if cfg.App.DataType == app.ConfigDataTypeLogs {
			blockCreateFunc = func() block.SharedColumns {
				return block.NewLogsSharedColumns(cfg.Disk.HasTimestampTime)
			}
		} else if cfg.App.DataType == app.ConfigDataTypeTraces {
			blockCreateFunc = func() block.SharedColumns {
				return block.NewTracesSharedColumns()
			}
		}
	}

	var blockPool block.Pool
	if cfg.Generate.Enabled {
		if cfg.Generate.ReuseBlocks {
			blockPool = block.NewBlockPool(blocksToAlloc, cfg.Generate.BlockRetirementUses, blockCreateFunc)
		} else {
			blockPool = block.NewGarbageBlockPool(blockCreateFunc)
		}
	} else if cfg.Disk.ReuseBlocks {
		blockPool = block.NewBlockPool(blocksToAlloc, cfg.Disk.BlockRetirementUses, blockCreateFunc)
	} else {
		blockPool = block.NewGarbageBlockPool(blockCreateFunc)
	}

	terminate := make(chan os.Signal, 1)
	signal.Notify(terminate, os.Interrupt, syscall.SIGTERM)

	metricsCtx, cancelMetrics := context.WithCancel(context.Background())
	var metricsWg sync.WaitGroup

	var metricsStore metrics.Store
	if cfg.Metrics.Enabled {
		runAttr := cfg.Metrics.Attributes
		if runAttr == nil {
			runAttr = make(map[string]string)
		}
		m, metricsErr := metrics.NewWorker(log, runID, runName, cfg.App.DataType, targetBytesPerSecond, cfg.Generate.RowsPerSecond, runAttr, &cfg.Metrics, blockPool, insertQueue)
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

	var sourceWg sync.WaitGroup
	sourceCtx, cancelSource := context.WithCancel(context.Background())
	if cfg.Generate.Enabled {
		gs := generate.NewScheduler(log, &cfg.Generate, cfg.App.Seed, cfg.App.DataType, blockPool, insertQueue, metricsStore, !cfg.Insert.Enabled)
		sourceWg.Go(func() {
			gsErr := gs.Run(sourceCtx)
			if gsErr != nil && !errors.Is(gsErr, context.Canceled) {
				log.Error("generate scheduler error", "err", gsErr)
			}
			close(insertQueue)
		})
	} else if cfg.Disk.Enabled {
		dws := disk.NewScheduler(log, &cfg.Disk, cfg.GetDataFolder(), blockPool, insertQueue, metricsStore, !cfg.Insert.Enabled)
		sourceWg.Go(func() {
			dwsErr := dws.Run(sourceCtx)
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
		if !cfg.Disk.Enabled && !cfg.Generate.Enabled {
			log.Warn("insert is enabled but no data source (disk/generate) is enabled, insert workers will not start")
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
		sourceWg.Wait()
		insertWg.Wait()
		userWg.Wait()
		close(done)
	}()

	select {
	case <-terminate:
		log.Info("stop requested")
	case <-done:
	}

	cancelSource()
	sourceWg.Wait()
	cancelInsert()
	insertWg.Wait()
	cancelUser()
	userWg.Wait()
	cancelMetrics()
	metricsWg.Wait()

	log.Info("done")
}
