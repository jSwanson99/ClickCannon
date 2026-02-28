package block

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

// TODO: this file should really be in the disk module

// ReplayTimeKeeper keeps track of the oldest and newest timestamp within a data set across multiple files.
// This allows for more flexibility in shifting timestamps around, and even looping files with less overlap.
type ReplayTimeKeeper struct {
	log *slog.Logger

	programStart time.Time
	datasetStart time.Time
	datasetEnd   time.Time
	mu           sync.RWMutex

	datasetStartReady     chan struct{}
	latestTimestampReport chan time.Time
}

func NewReplayTimeKeeper(log *slog.Logger) *ReplayTimeKeeper {
	return &ReplayTimeKeeper{
		log:                   log.With("component", "replay_time_keeper"),
		programStart:          time.Now(),
		datasetStartReady:     make(chan struct{}),
		latestTimestampReport: make(chan time.Time, 100),
	}
}

func (k *ReplayTimeKeeper) Run(ctx context.Context) {
	k.log.Info("started", "program_start", k.programStart)

	for {
		select {
		case <-ctx.Done():
			k.log.Info("stopped",
				"dataset_start", k.datasetStart,
				"dataset_end", k.datasetEnd,
				"dataset_duration_seconds", uint64(k.datasetEnd.Sub(k.datasetStart).Seconds()),
				"program_duration_seconds", uint64(time.Since(k.programStart).Seconds()),
			)

			return
		case t := <-k.latestTimestampReport:
			k.mu.Lock()
			if t.After(k.datasetEnd) {
				k.datasetEnd = t
			}
			k.mu.Unlock()
		}
	}
}

// ReportEarliestTimestamp should only be called once by the worker with first file
func (k *ReplayTimeKeeper) ReportEarliestTimestamp(t time.Time) {
	k.mu.Lock()
	defer k.mu.Unlock()

	if !k.datasetStart.IsZero() {
		return
	}

	k.datasetStart = t
	k.log.Info("set first timestamp in dataset", "first_timestamp", t)
	close(k.datasetStartReady)
}

// ReportLatestTimestamp is called per block by all disk workers
func (k *ReplayTimeKeeper) ReportLatestTimestamp(t time.Time) {
	select {
	case k.latestTimestampReport <- t:
	default:
	}
}

func (k *ReplayTimeKeeper) Snapshot(loopIndex int) ReplayTimeSnapshot {
	<-k.datasetStartReady

	k.mu.RLock()
	defer k.mu.RUnlock()

	loopOffset := time.Duration(loopIndex) * k.datasetEnd.Sub(k.datasetStart)
	return ReplayTimeSnapshot{
		programStart: k.programStart,
		datasetStart: k.datasetStart,
		loopOffset:   loopOffset,
	}
}

type ReplayTimeSnapshot struct {
	programStart time.Time
	datasetStart time.Time
	loopOffset   time.Duration
}

func (s ReplayTimeSnapshot) ShiftTimestamp(original time.Time) time.Time {
	offset := original.Sub(s.datasetStart)
	return s.programStart.Add(s.loopOffset).Add(offset)
}
