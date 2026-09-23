// Package budget provides cancellation-aware byte backpressure.
package budget

import (
	"context"
	"fmt"
	"sync"
)

type Budget struct {
	mu          sync.Mutex
	used, limit int64
	changed     chan struct{}
}

func New(limit int64) *Budget { return &Budget{limit: limit, changed: make(chan struct{})} }

// Limit returns the configured byte limit.
func (b *Budget) Limit() int64 { return b.limit }

// Oversized reports whether a record of n bytes exceeds the whole budget and
// will be admitted exclusively by Acquire.
func (b *Budget) Oversized(n int64) bool { return n > b.limit }

// Acquire blocks until n bytes fit and returns an idempotent release func.
//
// A record larger than the whole budget is admitted exclusively: it waits
// until nothing else is held, then holds the budget alone until released.
// Such a record is already in memory by the time it is accounted, so refusing
// it would only turn a memory spike into a restart loop on the same WAL.
func (b *Budget) Acquire(ctx context.Context, n int64) (func(), error) {
	if n < 0 {
		return nil, fmt.Errorf("negative record accounting size %d", n)
	}
	for {
		b.mu.Lock()
		if b.used+n <= b.limit || (n > b.limit && b.used == 0) {
			b.used += n
			b.mu.Unlock()
			var once sync.Once
			return func() {
				once.Do(func() { b.mu.Lock(); b.used -= n; close(b.changed); b.changed = make(chan struct{}); b.mu.Unlock() })
			}, nil
		}
		ch := b.changed
		b.mu.Unlock()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-ch:
		}
	}
}
