package block

import "sync/atomic"

type Pool interface {
	Stats() (int, int)
	Acquire() SharedColumns
	Release(cols SharedColumns)
	// TotalRetired returns the total number of blocks retired over the program's lifetime.
	// Blocks are retired when they exceed the configured use count, triggering a fresh allocation.
	// Returns 0 for pool implementations that do not support retirement (e.g. GarbageBlockPool).
	TotalRetired() int64
}

// GarbageBlockPool does not re-use blocks from
// a pool, it just re-allocates a new one each time.
type GarbageBlockPool struct {
	blockCreateFunc func() SharedColumns
	count           atomic.Int64
}

func NewGarbageBlockPool(blockCreateFunc func() SharedColumns) *GarbageBlockPool {
	return &GarbageBlockPool{blockCreateFunc: blockCreateFunc}
}

func (p *GarbageBlockPool) Stats() (int, int) {
	return int(p.count.Load()), 0
}

func (p *GarbageBlockPool) TotalRetired() int64 {
	return 0
}

func (p *GarbageBlockPool) Acquire() SharedColumns {
	p.count.Add(1)
	return p.blockCreateFunc()
}

func (p *GarbageBlockPool) Release(_ SharedColumns) {
	p.count.Add(-1)
}
