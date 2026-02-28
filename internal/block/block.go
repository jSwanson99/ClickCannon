package block

import "sync/atomic"

type Pool interface {
	Stats() (int, int)
	Acquire() SharedColumns
	Release(cols SharedColumns)
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

func (p *GarbageBlockPool) Acquire() SharedColumns {
	p.count.Add(1)
	return p.blockCreateFunc()
}

func (p *GarbageBlockPool) Release(_ SharedColumns) {
	p.count.Add(-1)
}
