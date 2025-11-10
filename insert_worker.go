package main

import (
	"context"
	"fmt"
	"io"
	"sync/atomic"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
)

type insertWorker struct {
	id               int
	maxRowsPerInsert int
	database         string
	table            string

	blockPool  *StructPool[SharedColumns]
	blockQueue <-chan SharedColumns

	totalRows  *atomic.Int64
	totalBytes *atomic.Int64
}

func newInsertWorker(id, maxRowsPerInsert int, database, table string, blockPool *StructPool[SharedColumns], blockQueue <-chan SharedColumns, totalRows, totalBytes *atomic.Int64) *insertWorker {
	w := insertWorker{
		id:               id,
		maxRowsPerInsert: maxRowsPerInsert,
		database:         database,
		table:            table,

		blockPool:  blockPool,
		blockQueue: blockQueue,

		totalRows:  totalRows,
		totalBytes: totalBytes,
	}

	return &w
}

func (w *insertWorker) start() {
	c, err := ch.Dial(context.Background(), ch.Options{Address: "localhost:9000"})
	if err != nil {
		panic(err)
	}
	defer func(c *ch.Client) {
		closeErr := c.Close()
		if closeErr != nil {
			panic(closeErr)
		}
	}(c)

	for {
		insertBlock := w.blockPool.Acquire()
		insertBlock.Reset()
		insertInput := insertBlock.Input()

		// Before starting query, wait for first block
		// proto.Input is not set on first block since it hasn't been swapped in yet.
		var currentInput proto.Input
		rowCount := 0

		currentBlock, ok := <-w.blockQueue
		if !ok {
			break
		}

		if err := c.Do(context.Background(), ch.Query{
			Body:  fmt.Sprintf("INSERT INTO %q.%q %s VALUES", w.database, w.table, insertInput.Columns()),
			Input: insertInput,
			OnInput: func(ctx context.Context) error {
				insertBlock.Reset()

				if currentInput != nil {
					swapInput(currentInput, insertInput)
					w.blockPool.Release(currentBlock)
					currentBlock = nil
					currentInput = nil
				}
				if rowCount > w.maxRowsPerInsert {
					// Row count may exceed maximum, but not by more than one block
					return io.EOF
				}

				if currentBlock == nil {
					select {
					case nextBlock, ok := <-w.blockQueue:
						if !ok {
							return io.EOF
						}
						currentBlock = nextBlock
					default:
						return io.EOF
					}
				}

				currentInput = currentBlock.Input()

				// This is a hack to swap block reference + underlying column pointers
				swapInput(insertInput, currentInput)

				rowCount += insertInput[0].Data.Rows()

				return nil
			},
			OnProfileEvents: func(ctx context.Context, events []ch.ProfileEvent) error {
				for _, e := range events {
					if e.Type != proto.ProfileIncrement {
						continue
					}

					// https://github.com/ClickHouse/ClickHouse/blob/master/src/Common/ProfileEvents.cpp
					switch e.Name {
					case "InsertedRows":
						w.totalRows.Add(e.Value)
					case "InsertedBytes":
						//insertBytesPerSecond.Add(e.Value)
					case "NetworkReceiveBytes":
						w.totalBytes.Add(e.Value)
					default:
						continue
					}
				}
				return nil
			},
		}); err != nil {
			panic(err)
		}

		w.blockPool.Release(insertBlock)
	}
}

// swapInput temporarily swaps the column data within a proto.Input.
// This is a hack since the ch-go client OnInput function doesn't allow using different
// blocks, but I want to save CPU and memory by simply using a placeholder block to stream data.
func swapInput(a, b proto.Input) {
	for i := range a {
		tmp := a[i]
		a[i] = b[i]
		b[i] = tmp
	}
}
