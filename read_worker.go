package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sync/atomic"

	"github.com/ClickHouse/ch-go/proto"
	"github.com/klauspost/compress/zstd"
)

type readWorker struct {
	id                  int
	filePath            string
	fileCompressed      bool
	bytesPerSecondLimit float64
	passthrough         bool

	speedRd *SpeedLimitedReader

	blockPool   *StructPool[SharedColumns]
	insertQueue chan<- SharedColumns

	totalRows              *atomic.Int64
	totalBytesCompressed   *atomic.Int64
	totalBytesUncompressed *atomic.Int64
}

func newReadWorker(id int, filePath string, fileCompressed bool, bytesPerSecondLimit float64, passthrough bool, blockPool *StructPool[SharedColumns], insertQueue chan<- SharedColumns, totalRows, totalBytesCompressed, totalBytesUncompressed *atomic.Int64) *readWorker {
	w := readWorker{
		id:                  id,
		filePath:            filePath,
		fileCompressed:      fileCompressed,
		bytesPerSecondLimit: bytesPerSecondLimit,
		passthrough:         passthrough,

		blockPool:   blockPool,
		insertQueue: insertQueue,

		totalRows:              totalRows,
		totalBytesCompressed:   totalBytesCompressed,
		totalBytesUncompressed: totalBytesUncompressed,
	}

	return &w
}

func (w *readWorker) UpdateSpeedLimit(bytesPerSecond float64) {
	if w.speedRd == nil {
		return
	}

	w.speedRd.Reset(bytesPerSecond)
}

func (w *readWorker) start(ctx context.Context) {
	fmt.Printf("[reader %d] reading %s (compressed: %t)\n", w.id, w.filePath, w.fileCompressed)
	data, err := os.Open(w.filePath)
	if err != nil {
		panic(err)
	}
	defer func(data *os.File) {
		closeErr := data.Close()
		if closeErr != nil {
			panic(closeErr)
		}
	}(data)

	var dec proto.Block
	compressedSpeedRd := NewSpeedReader(data, w.totalBytesCompressed)
	bufRd := bufio.NewReader(compressedSpeedRd)
	var optReader io.Reader = bufRd
	if w.fileCompressed {
		zstdRd, err := zstd.NewReader(optReader,
			zstd.IgnoreChecksum(true),
			zstd.WithDecoderConcurrency(1), // TODO: with higher concurrency, this performs better when set to 1
			zstd.WithDecoderLowmem(true),
		)
		if err != nil {
			panic(err)
		}
		defer zstdRd.Close()
		optReader = zstdRd
	}

	w.speedRd = NewSpeedLimitedReader(optReader, w.bytesPerSecondLimit, w.totalBytesUncompressed)
	defer w.speedRd.Close()

	rd := proto.NewReader(w.speedRd)

	for ctx.Err() == nil {
		cols := w.blockPool.Acquire()
		colsRes := cols.Results()
		err = dec.DecodeRawBlock(rd, 54451, colsRes)
		if errors.Is(err, io.EOF) {
			w.blockPool.Release(cols)
			break
		} else if err != nil {
			panic(err)
		}

		cols.UpdateDate()

		w.totalRows.Add(int64(colsRes.Rows()))

		if w.passthrough {
			// Passthrough for testing max disk read speed.
			// Block is immediately released, never sent to the insert queue.
			w.blockPool.Release(cols)
		} else {
			w.insertQueue <- cols
		}
	}
}
