package main

import (
	"context"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"syscall"

	"github.com/ClickHouse/ClickCannon/api"
	"github.com/ClickHouse/ClickCannon/internal/app"
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

	if cfg.App.Name == "" {
		cfg.App.Name = cfgFileName
	}

	log.Info("config", "config_file", cfgFileName, "run_name", cfg.App.Name)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := api.Run(ctx, *cfg, log, runID); err != nil {
		log.Error("run failed", "err", err)
		os.Exit(1)
	}

	log.Info("done")
}
