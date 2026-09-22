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
func (b *Budget) Acquire(ctx context.Context, n int64) (func(), error) {
	if n < 0 || n > b.limit {
		return nil, fmt.Errorf("record accounting size %d exceeds byte budget %d", n, b.limit)
	}
	for {
		b.mu.Lock()
		if b.used+n <= b.limit {
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
