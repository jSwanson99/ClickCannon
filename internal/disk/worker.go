package disk

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"otelspam/internal/block"
	"otelspam/internal/metrics"
	"time"

	"github.com/ClickHouse/ch-go/proto"
	"github.com/klauspost/compress/zstd"
)

const ShiftTimestampNone = "none" // Does not shift input data timestamp, replays data as written.
const ShiftTimestampDate = "date" // Shifts only the date of the input data's timestamp to be relative to the current date.
const ShiftTimestampAll = "all"   // Shifts the input data's timestamp to be relative to the current time.

type worker struct {
	id  int
	log *slog.Logger

	filePath       string
	fileCompressed bool
	shiftTimestamp string
	passthrough    bool

	bytesPerSecondLimit uint64

	speedRd *SpeedLimitedReader

	metrics metrics.Store

	// firstTimestamp is the first timestamp in the dataset across all workers and files. Important for time shifting.
	firstTimestamp time.Time

	firstTimestampReply chan<- time.Time

	blockPool   *block.StructPool[block.SharedColumns]
	insertQueue chan<- block.SharedColumns
}

func newWorker(
	id int,
	log *slog.Logger,
	filePath string,
	fileCompressed bool,
	shiftTimestamp string,
	bytesPerSecondLimit uint64,
	blockPool *block.StructPool[block.SharedColumns],
	insertQueue chan<- block.SharedColumns,
	metrics metrics.Store,
	passthrough bool,
	firstTimestampReply chan<- time.Time,
) *worker {
	return &worker{
		id:  id,
		log: log.With("component", "disk_worker", "id", id, "file", filePath, "compressed", fileCompressed),

		filePath:       filePath,
		fileCompressed: fileCompressed,
		shiftTimestamp: shiftTimestamp,

		bytesPerSecondLimit: bytesPerSecondLimit,

		blockPool:   blockPool,
		insertQueue: insertQueue,
		metrics:     metrics,

		passthrough: passthrough,

		firstTimestampReply: firstTimestampReply,
	}
}

func (w *worker) SetFirstTimestamp(firstTimestamp time.Time) {
	w.firstTimestamp = firstTimestamp
}

func (w *worker) UpdateSpeedLimit(bytesPerSecondLimit uint64) {
	if w.speedRd == nil {
		return
	}

	w.bytesPerSecondLimit = bytesPerSecondLimit
	w.speedRd.Reset(bytesPerSecondLimit)
}

func (w *worker) Run(ctx context.Context) error {
	w.log.Info("started", "speed_limit_bytes", w.bytesPerSecondLimit)

	rd, rdClose, rdErr := w.buildReader()
	if rdErr != nil {
		return fmt.Errorf("failed to build reader: %w", rdErr)
	}
	defer rdClose()

	var dec proto.Block
	for {
		select {
		case <-ctx.Done():
			w.log.Info("stopped")
			return ctx.Err()
		default:
		}

		// Wait for first thread to find the first timestamp in the first file
		if w.id != 0 && w.firstTimestamp.IsZero() {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		err := w.decodeBlock(rd, &dec)
		if errors.Is(err, io.EOF) {
			w.log.Info("finished")
			return nil
		} else if err != nil {
			w.log.Error("read failed, stopping", "err", err)
			return nil
		}
	}
}

func (w *worker) buildReader() (*proto.Reader, func(), error) {
	data, err := os.Open(w.filePath)
	if err != nil {
		return nil, nil, fmt.Errorf("opening file: %w", err)
	}

	compressedSpeedRd := NewSpeedReader(data, func(n uint64) {
		w.metrics.IncrementMetric(metrics.ReadCompressedBytesPerSecond, n)
		w.metrics.IncrementMetric(metrics.TotalBytesCompressed, n)
	})

	var (
		optReader io.Reader = bufio.NewReader(compressedSpeedRd)
		zstdClose func()
	)

	if w.fileCompressed {
		zstdRd, err := zstd.NewReader(optReader,
			zstd.IgnoreChecksum(true),
			zstd.WithDecoderConcurrency(1),
			zstd.WithDecoderLowmem(true),
		)
		if err != nil {
			if fileErr := data.Close(); fileErr != nil {
				w.log.Error("closing file", "err", fileErr)
			}

			return nil, nil, fmt.Errorf("creating zstd reader: %w", err)
		}

		zstdClose = zstdRd.Close
		optReader = zstdRd
	}

	optReader = bufio.NewReaderSize(optReader, 32*1024)
	w.speedRd = NewSpeedLimitedReader(optReader, w.bytesPerSecondLimit, func(n uint64) {
		w.metrics.IncrementMetric(metrics.ReadUncompressedBytesPerSecond, n)
		w.metrics.IncrementMetric(metrics.TotalBytesUncompressed, n)
	})

	cleanup := func() {
		w.speedRd.Close()

		if zstdClose != nil {
			zstdClose()
		}

		if fileErr := data.Close(); fileErr != nil {
			w.log.Error("closing file", "err", fileErr)
		}
	}

	return proto.NewReader(w.speedRd), cleanup, nil
}

func (w *worker) decodeBlock(rd *proto.Reader, dec *proto.Block) error {
	cols := w.blockPool.Acquire()
	colsRes := cols.Results()
	err := dec.DecodeRawBlock(rd, 54451, colsRes)
	if errors.Is(err, io.EOF) {
		w.blockPool.Release(cols)
		return io.EOF
	} else if err != nil {
		w.blockPool.Release(cols)
		return fmt.Errorf("failed to decode block: %w", err)
	}

	if w.id == 0 && w.firstTimestamp.IsZero() {
		w.firstTimestamp = cols.FirstTimestamp()
		w.firstTimestampReply <- w.firstTimestamp
	}

	switch w.shiftTimestamp {
	case ShiftTimestampDate:
		cols.UpdateDate()
	case ShiftTimestampAll:
		cols.UpdateTimestamp(w.firstTimestamp)
	}

	w.metrics.IncrementMetric(metrics.ReadRowsPerSecond, uint64(colsRes.Rows()))
	w.metrics.IncrementMetric(metrics.TotalRows, uint64(colsRes.Rows()))

	if w.passthrough {
		// Passthrough for testing max disk read speed.
		// Block is immediately released, never sent to the insert queue.
		w.blockPool.Release(cols)
	} else {
		w.insertQueue <- cols
	}

	return nil
}
