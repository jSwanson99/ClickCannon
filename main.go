package main

import (
	"context"
	"fmt"
	"math"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/google/uuid"
)

func main() {
	config, err := loadConfig()
	if err != nil {
		fmt.Printf("failed to load config: %s\n", err)
		os.Exit(1)
	}

	otelFiles, err := getDataFiles(config.GetDataFolder())
	if err != nil {
		fmt.Printf("failed to find files for %s insert: %s\n", config.Read.DataType, err)
		os.Exit(1)
	}

	bytesPerSecond := config.Read.MiBytesPerSecondLimit * 1024 * 1024

	blockAllocMultiplier := 4
	blocksToAlloc := (config.Read.Threads + config.Insert.Threads) * blockAllocMultiplier
	blockQueue := make(chan SharedColumns, blocksToAlloc)
	var blockPool *StructPool[SharedColumns]
	if config.Read.DataType == ConfigDataTypeLogs {
		blockPool, err = NewStructPool[SharedColumns](blocksToAlloc, func() (SharedColumns, error) {
			return NewLogsSharedColumns(), nil
		})
	} else if config.Read.DataType == ConfigDataTypeTraces {
		blockPool, err = NewStructPool[SharedColumns](blocksToAlloc, func() (SharedColumns, error) {
			return NewTracesSharedColumns(), nil
		})
	}
	if err != nil {
		panic(err)
	}

	runID, err := uuid.NewUUID()
	if err != nil {
		panic(err)
	}

	fmt.Println("run ID:", runID.String())
	fmt.Println("data type:", config.Read.DataType)
	fmt.Println("speed limit:", FormatBytes(bytesPerSecond))
	mw, err := newMetricsWorker(runID.String(), config.Read.DataType, bytesPerSecond, config.Metrics.ClickHouseDSN, config.Metrics.Database, config.Metrics.Table)
	if err != nil {
		panic(err)
	}

	terminate := make(chan os.Signal, 1)
	signal.Notify(terminate, os.Interrupt, syscall.SIGTERM)

	// TODO: ...don't manage readers this way
	readerDone := make(chan struct{}, config.Read.Threads-1)
	readCtx, cancelReaders := context.WithCancel(context.Background())
	readWorkers := make([]*readWorker, 0, config.Read.Threads)
	var readWg sync.WaitGroup
	go readScheduler(readCtx, cancelReaders, &mw.ActiveReaders, otelFiles, readerDone, func(id int, f otelFile) {
		w := newReadWorker(id, f.Path, f.Compressed, bytesPerSecondPerWorker(bytesPerSecond, int64(config.Read.Threads)), blockPool, blockQueue, &mw.ReadRowsPerSecond, &mw.ReadBytesPerSecond)
		readWorkers = append(readWorkers, w)
		readWg.Go(func() {
			mw.ActiveReaders.Add(1)
			w.start(readCtx)
			mw.ActiveReaders.Add(-1)
			<-readerDone
		})
	})

	insertWorkers := make([]*insertWorker, 0, config.Insert.Threads)
	var insertWg sync.WaitGroup
	mw.ActiveInserters.Store(int64(config.Insert.Threads))
	for i := range config.Insert.Threads {
		w := newInsertWorker(i, config, blockPool, blockQueue, &mw.InsertRowsPerSecond, &mw.InsertBytesPerSecond)
		insertWorkers = append(insertWorkers, w)
		insertWg.Go(func() {
			w.start()
			mw.ActiveInserters.Add(-1)
		})
	}
	fmt.Printf("started %d insert workers\n", config.Insert.Threads)

	go speedController(readCtx, bytesPerSecond, &readWorkers, &mw.ActiveReaders)
	go mw.start(readCtx)

	select {
	case <-terminate:
		fmt.Println("exiting")
		cancelReaders()
	case <-readCtx.Done():
	}

	readWg.Wait()
	close(blockQueue)
	insertWg.Wait()

	fmt.Println("done")
}

// speedController updates the speed limit across all read threads.
// If there's 2 threads with a 1GB speed limit, each thread gets 512MB.
func speedController(ctx context.Context, bytesPerSecond int64, readWorkers *[]*readWorker, activeReaders *atomic.Int64) {
	lastLimit := float64(bytesPerSecond)
	for {
		select {
		case <-time.After(500 * time.Millisecond):
			bps := bytesPerSecondPerWorker(bytesPerSecond, activeReaders.Load())
			if math.Abs(lastLimit-bps) < 1.0 {
				continue
			}

			for _, w := range *readWorkers {
				w.UpdateSpeedLimit(bps)
			}

			lastLimit = bps
		case <-ctx.Done():
			return
		}
	}
}

func bytesPerSecondPerWorker(globalBytesPerSecond, activeReaders int64) float64 {
	return float64(globalBytesPerSecond) / float64(activeReaders)
}

type otelFile struct {
	Path       string
	Compressed bool
}

func getDataFiles(folderPath string) ([]otelFile, error) {
	var files []otelFile

	err := filepath.Walk(folderPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if info.IsDir() {
			return nil
		}

		if strings.HasSuffix(path, ".native.zst") {
			files = append(files, otelFile{
				Path:       path,
				Compressed: true,
			})
		} else if strings.HasSuffix(path, ".native") {
			files = append(files, otelFile{
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

func readScheduler(ctx context.Context, cancelReaderCtx context.CancelFunc, activeReaders *atomic.Int64, files []otelFile, readerDone chan struct{}, startReader func(id int, f otelFile)) {
	for i := 0; i < len(files); i++ {
		nextFile := files[i]
		startReader(i, nextFile)
		readerDone <- struct{}{}
	}

	// TODO: no.
	for {
		select {
		case <-time.After(1 * time.Second):
			if activeReaders.Load() == 0 {
				cancelReaderCtx()
				return
			}
		case <-ctx.Done():
			return
		}
	}
}
