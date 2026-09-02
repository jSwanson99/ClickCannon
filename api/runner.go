package api

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"

	"github.com/ClickHouse/ClickCannon/internal/app"
	"github.com/ClickHouse/ClickCannon/internal/block"
	"github.com/ClickHouse/ClickCannon/internal/disk"
	"github.com/ClickHouse/ClickCannon/internal/generate"
	"github.com/ClickHouse/ClickCannon/internal/insert"
	"github.com/ClickHouse/ClickCannon/internal/metrics"
	otelexport "github.com/ClickHouse/ClickCannon/internal/otel"
	"github.com/ClickHouse/ClickCannon/internal/user"
)

// Runner is one ClickCannon run. It is not reusable: build a new one per run.
//
// Start is non-blocking. Stop is idempotent, drains the pipeline and returns any
// scheduler errors. A source with no natural end runs until Stop.
type Runner struct {
	cfg     *app.Config
	log     *slog.Logger
	runID   string
	runName string

	insertEnabled bool
	otelEnabled   bool

	store           metrics.Store
	blockPool       block.Pool
	blockCreateFunc func() block.SharedColumns
	insertQueue     chan block.SharedColumns

	mu      sync.Mutex
	started bool
	stopped bool

	cancelSource  context.CancelFunc
	cancelInsert  context.CancelFunc
	cancelOtel    context.CancelFunc
	cancelUser    context.CancelFunc
	cancelMetrics context.CancelFunc

	sourceWg  sync.WaitGroup
	insertWg  sync.WaitGroup
	otelWg    sync.WaitGroup
	userWg    sync.WaitGroup
	metricsWg sync.WaitGroup

	done chan struct{}

	errMu sync.Mutex
	errs  []error
}

// NewRunner validates cfg and builds the pipeline. Nothing connects until Start,
// and then only if Config.Metrics is enabled.
//
// A nil log discards ClickCannon's log output. An empty runID generates one; it
// labels the metrics rows and seeds the generator when Config.App.Seed is empty.
func NewRunner(cfg Config, log *slog.Logger, runID string) (*Runner, error) {
	if runID == "" {
		runID = app.NewRunID()
	}
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}

	if cfg.App.Seed == "" {
		cfg.App.Seed = runID
	}

	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("clickcannon: invalid config: %w", err)
	}

	// cfg is a value copy, so the Runner owns it and the caller cannot mutate a
	// run in flight.
	internal := &cfg

	runName := internal.App.Name
	if runName == "" {
		runName = "clickcannon"
	}

	insertEnabled := internal.Insert.Enabled
	otelEnabled := internal.OTel.Enabled
	if insertEnabled && otelEnabled {
		log.Warn("both insert and otel export are enabled; only one sink can consume the block queue — using otel export, insert disabled")
		insertEnabled = false
	}

	sourceThreads := internal.Disk.Threads
	if internal.Generate.Enabled {
		sourceThreads = internal.Generate.Threads
	}

	consumerThreads := 0
	if insertEnabled {
		consumerThreads = internal.Insert.Threads
	} else if otelEnabled {
		consumerThreads = internal.OTel.Threads
	}

	blocksToAlloc := (sourceThreads + consumerThreads) * 2
	blockCreateFunc, err := newBlockCreateFunc(internal)
	if err != nil {
		return nil, err
	}

	var blockPool block.Pool
	switch {
	case internal.Generate.Enabled && internal.Generate.ReuseBlocks:
		blockPool = block.NewBlockPool(blocksToAlloc, internal.Generate.BlockRetirementUses, blockCreateFunc)
	case internal.Disk.Enabled && internal.Disk.ReuseBlocks:
		blockPool = block.NewBlockPool(blocksToAlloc, internal.Disk.BlockRetirementUses, blockCreateFunc)
	default:
		blockPool = block.NewGarbageBlockPool(blockCreateFunc)
	}

	return &Runner{
		cfg:             internal,
		log:             log.With("run_id", runID),
		runID:           runID,
		runName:         runName,
		insertEnabled:   insertEnabled,
		otelEnabled:     otelEnabled,
		blockPool:       blockPool,
		blockCreateFunc: blockCreateFunc,
		insertQueue:     make(chan block.SharedColumns, blocksToAlloc),
		done:            make(chan struct{}),
	}, nil
}

// newBlockCreateFunc picks the column implementation for the source and data
// type. Generate mode needs its own types because the decode-oriented ones do
// not implement Append.
func newBlockCreateFunc(cfg *app.Config) (func() block.SharedColumns, error) {
	if cfg.Generate.Enabled {
		switch cfg.App.DataType {
		case app.ConfigDataTypeLogs:
			return func() block.SharedColumns {
				return generate.NewGenLogsColumns()
			}, nil
		case app.ConfigDataTypeTraces:
			return func() block.SharedColumns {
				return generate.NewGenTracesColumns()
			}, nil
		case app.ConfigDataTypeProfiles:
			return func() block.SharedColumns {
				return generate.NewGenProfilesColumns()
			}, nil
		}
	} else {
		switch cfg.App.DataType {
		case app.ConfigDataTypeLogs:
			return func() block.SharedColumns {
				return block.NewLogsSharedColumns(cfg.Disk.HasTimestampTime)
			}, nil
		case app.ConfigDataTypeTraces:
			return func() block.SharedColumns {
				return block.NewTracesSharedColumns()
			}, nil
		case app.ConfigDataTypeProfiles:
			return func() block.SharedColumns {
				return block.NewProfilesSharedColumns()
			}, nil
		}
	}

	return nil, fmt.Errorf("clickcannon: unsupported data type %q", cfg.App.DataType)
}

func (r *Runner) Start() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.started {
		return errors.New("clickcannon: runner already started")
	}
	r.started = true

	if err := r.startMetrics(); err != nil {
		return err
	}

	r.startSource()
	r.startInsert()
	r.startOTel()
	r.startUser()

	go func() {
		r.sourceWg.Wait()
		r.insertWg.Wait()
		r.otelWg.Wait()
		r.userWg.Wait()
		close(r.done)
	}()

	r.log.Info("started",
		"run_name", r.runName,
		"data_type", r.cfg.App.DataType,
		"seed", r.cfg.App.Seed,
		"generate_enabled", r.cfg.Generate.Enabled,
		"disk_enabled", r.cfg.Disk.Enabled,
		"insert_enabled", r.insertEnabled,
		"otel_enabled", r.otelEnabled,
		"otel_url", r.cfg.OTel.URL,
		"user_enabled", r.cfg.User.Enabled,
	)

	return nil
}

func (r *Runner) startMetrics() error {
	ctx, cancel := context.WithCancel(context.Background())
	r.cancelMetrics = cancel

	// Nothing counts when metrics are disabled, so Stats stays zero.
	if !r.cfg.Metrics.Enabled {
		r.store = metrics.NewDisabledStore()
		return nil
	}

	attrs := r.cfg.Metrics.Attributes
	if attrs == nil {
		attrs = make(map[string]string)
	}

	targetBytesPerSecond := r.cfg.Disk.MiBytesPerSecondLimit * 1024 * 1024

	w, err := metrics.NewWorker(
		r.log, r.runID, r.runName, r.cfg.App.DataType,
		targetBytesPerSecond, r.cfg.Generate.RowsPerSecond,
		attrs, &r.cfg.Metrics, r.blockPool, r.insertQueue,
	)
	if err != nil {
		cancel()
		return fmt.Errorf("clickcannon: create metrics worker: %w", err)
	}

	r.store = w

	r.metricsWg.Go(func() {
		r.record("metrics worker", w.Run(ctx))
	})

	return nil
}

func (r *Runner) startSource() {
	ctx, cancel := context.WithCancel(context.Background())
	r.cancelSource = cancel

	// Passthrough drops blocks when there is no sink, to benchmark a source
	// in isolation.
	passthrough := !(r.insertEnabled || r.otelEnabled)

	switch {
	case r.cfg.Generate.Enabled:
		s := generate.NewScheduler(r.log, &r.cfg.Generate, r.cfg.App.Seed, r.cfg.App.DataType, r.blockPool, r.insertQueue, r.store, passthrough)
		r.sourceWg.Go(func() {
			r.record("generate scheduler", s.Run(ctx))
			close(r.insertQueue)
		})
	case r.cfg.Disk.Enabled:
		s := disk.NewScheduler(r.log, &r.cfg.Disk, r.cfg.GetDataFolder(), r.blockPool, r.insertQueue, r.store, passthrough)
		r.sourceWg.Go(func() {
			r.record("disk scheduler", s.Run(ctx))
			close(r.insertQueue)
		})
	default:
		close(r.insertQueue)
	}
}

func (r *Runner) startInsert() {
	ctx, cancel := context.WithCancel(context.Background())
	r.cancelInsert = cancel

	if !r.insertEnabled {
		return
	}

	if !r.hasSource() {
		r.log.Warn("insert is enabled but no data source (disk/generate) is enabled, insert workers will not start")
		return
	}

	s := insert.NewScheduler(r.log, &r.cfg.Insert, r.cfg.GetInsertTable(), r.blockCreateFunc, r.blockPool, r.insertQueue, r.store)
	r.insertWg.Go(func() {
		r.record("insert scheduler", s.Run(ctx))
	})
}

func (r *Runner) startOTel() {
	ctx, cancel := context.WithCancel(context.Background())
	r.cancelOtel = cancel

	if !r.otelEnabled {
		return
	}

	if !r.hasSource() {
		r.log.Warn("otel export is enabled but no data source (disk/generate) is enabled, otel workers will not start")
		return
	}

	s := otelexport.NewScheduler(r.log, &r.cfg.OTel, r.cfg.App.DataType, r.blockPool, r.insertQueue, r.store)
	r.otelWg.Go(func() {
		r.record("otel scheduler", s.Run(ctx))
	})
}

func (r *Runner) startUser() {
	ctx, cancel := context.WithCancel(context.Background())
	r.cancelUser = cancel

	if !r.cfg.User.Enabled {
		return
	}

	s := user.NewScheduler(r.log, r.cfg.App.Seed, &r.cfg.User, r.store)
	r.userWg.Go(func() {
		r.record("user scheduler", s.Run(ctx))
	})
}

func (r *Runner) hasSource() bool {
	return r.cfg.Disk.Enabled || r.cfg.Generate.Enabled
}

func (r *Runner) record(msg string, err error) {
	if err == nil || errors.Is(err, context.Canceled) {
		return
	}

	r.log.Error("scheduler error", "scheduler", msg, "err", err)

	r.errMu.Lock()
	r.errs = append(r.errs, fmt.Errorf("%s: %w", msg, err))
	r.errMu.Unlock()
}

// Done is closed when every worker exits on its own. A generator without a
// duration limit never ends, so only rely on this for a finite source.
func (r *Runner) Done() <-chan struct{} { return r.done }

// Wait blocks until all workers exit or ctx is cancelled, then stops.
func (r *Runner) Wait(ctx context.Context) error {
	select {
	case <-ctx.Done():
	case <-r.done:
	}

	return r.Stop()
}

// Stop cancels the workers in order and returns the joined error of everything
// that failed. Safe to call repeatedly, and on a Runner that never started.
func (r *Runner) Stop() error {
	r.mu.Lock()
	if r.stopped || !r.started {
		r.mu.Unlock()
		return r.err()
	}
	r.stopped = true
	r.mu.Unlock()

	// Cancelling the source first lets the sinks drain the queue instead of
	// dropping in-flight blocks.
	r.cancelSource()
	r.sourceWg.Wait()
	r.cancelInsert()
	r.insertWg.Wait()
	r.cancelOtel()
	r.otelWg.Wait()
	r.cancelUser()
	r.userWg.Wait()
	r.cancelMetrics()
	r.metricsWg.Wait()

	r.log.Info("stopped")

	return r.err()
}

func (r *Runner) err() error {
	r.errMu.Lock()
	defer r.errMu.Unlock()

	return errors.Join(r.errs...)
}

// Stats is safe to call at any time, including before Start and after Stop.
// Zero unless Config.Metrics is enabled.
func (r *Runner) Stats() Stats {
	if r.store == nil {
		return Stats{}
	}

	return statsFrom(r.store)
}

// Run starts the pipeline and runs until ctx is cancelled or every worker
// finishes, then shuts down. For a fixed-duration test pass a context with a
// deadline.
func Run(ctx context.Context, cfg Config, log *slog.Logger, runID string) error {
	r, err := NewRunner(cfg, log, runID)
	if err != nil {
		return err
	}

	if err := r.Start(); err != nil {
		return err
	}

	return r.Wait(ctx)
}
