package metrics

import (
	"context"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
)

// Counter is a lightweight counter abstraction.
type Counter struct {
	val  uint64
	desc string
}

func NewCounter(desc string) *Counter {
	return &Counter{desc: desc}
}

func (c *Counter) Inc() {
	atomic.AddUint64(&c.val, 1)
}

func (c *Counter) Add(n uint64) {
	atomic.AddUint64(&c.val, n)
}

func (c *Counter) Value() uint64 {
	return atomic.LoadUint64(&c.val)
}

// Gauge tracks an int64 value atomically (e.g., lag in ms).
type Gauge struct {
	val  int64
	desc string
}

func NewGauge(desc string) *Gauge {
	return &Gauge{desc: desc}
}

func (g *Gauge) Set(v int64) {
	atomic.StoreInt64(&g.val, v)
}

func (g *Gauge) Get() int64 {
	return atomic.LoadInt64(&g.val)
}

// Reporter periodically logs metrics values (simple placeholder until Prometheus added).
type Reporter struct {
	interval time.Duration
	counters []*Counter
	gauges   []*Gauge
	logger   *zap.Logger
}

func NewReporter(interval time.Duration, counters []*Counter, gauges []*Gauge, logger *zap.Logger) *Reporter {
	if logger == nil {
		logger = zap.NewNop()
	}
	return &Reporter{interval: interval, counters: counters, gauges: gauges, logger: logger}
}

func (r *Reporter) Start(ctx context.Context) {
	if r.interval <= 0 {
		return
	}
	t := time.NewTicker(r.interval)
	go func() {
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				for _, c := range r.counters {
					r.logger.Info("metric", zap.String("name", c.desc), zap.Uint64("value", c.Value()))
				}
				for _, g := range r.gauges {
					r.logger.Info("metric", zap.String("name", g.desc), zap.Int64("value", g.Get()))
				}
			}
		}
	}()
}
