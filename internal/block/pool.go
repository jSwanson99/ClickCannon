package block

import (
	"sync"
	"sync/atomic"
)

// BlockPool is a fixed-size pool of SharedColumns blocks.
// Blocks are recycled on Release to reduce GC pressure and avoid repeated allocation.
//
// To prevent unbounded memory growth in column slices over long-running sessions,
// blocks are retired and replaced with a fresh allocation after [retireAfter] uses.
// Set retireAfter to 0 to disable retirement (blocks live for the program's lifetime).
type BlockPool struct {
	pool            chan SharedColumns
	blockCreateFunc func() SharedColumns

	retireAfter  int
	totalRetired atomic.Int64

	mu   sync.Mutex
	uses map[SharedColumns]int
}

func NewBlockPool(poolSize int, retireAfter int, newInstance func() SharedColumns) *BlockPool {
	p := &BlockPool{
		pool:            make(chan SharedColumns, poolSize),
		blockCreateFunc: newInstance,
		retireAfter:     retireAfter,
		uses:            make(map[SharedColumns]int, poolSize),
	}

	for i := 0; i < poolSize; i++ {
		cols := newInstance()
		p.pool <- cols
		p.uses[cols] = 0
	}

	return p
}

func (p *BlockPool) Stats() (int, int) {
	return len(p.pool), cap(p.pool)
}

func (p *BlockPool) TotalRetired() int64 {
	return p.totalRetired.Load()
}

func (p *BlockPool) Acquire() SharedColumns {
	return <-p.pool
}

func (p *BlockPool) Release(cols SharedColumns) {
	if p.retireAfter > 0 {
		p.mu.Lock()
		p.uses[cols]++

		if p.uses[cols] >= p.retireAfter {
			delete(p.uses, cols)
			p.mu.Unlock()

			p.totalRetired.Add(1)
			cols = p.blockCreateFunc()

			p.mu.Lock()
			p.uses[cols] = 0
		}

		p.mu.Unlock()
	}

	p.pool <- cols
}

func (p *BlockPool) Destroy() {
	for i := 0; i < cap(p.pool); i++ {
		<-p.pool
	}

	close(p.pool)
}
