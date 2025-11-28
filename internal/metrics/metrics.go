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

// RateCounter tracks events/second with a sliding window.
type RateCounter struct {
	count     uint64
	lastCount uint64
	lastTime  time.Time
	rate      float64
	desc      string
}

func NewRateCounter(desc string) *RateCounter {
	return &RateCounter{desc: desc, lastTime: time.Now()}
}

func (r *RateCounter) Inc() {
	atomic.AddUint64(&r.count, 1)
}

func (r *RateCounter) Add(n uint64) {
	atomic.AddUint64(&r.count, n)
}

func (r *RateCounter) Total() uint64 {
	return atomic.LoadUint64(&r.count)
}

// Rate calculates and returns the current rate (events/second).
// Call this periodically; it updates the rate based on time since last call.
func (r *RateCounter) Rate() float64 {
	now := time.Now()
	current := atomic.LoadUint64(&r.count)
	elapsed := now.Sub(r.lastTime).Seconds()

	if elapsed >= 1.0 {
		r.rate = float64(current-r.lastCount) / elapsed
		r.lastCount = current
		r.lastTime = now
	}
	return r.rate
}

func (r *RateCounter) Desc() string {
	return r.desc
}

// Histogram tracks value distributions with predefined buckets.
type Histogram struct {
	buckets []uint64 // bucket boundaries
	counts  []uint64 // count per bucket (len = len(buckets) + 1 for overflow)
	sum     uint64
	count   uint64
	desc    string
}

// NewHistogram creates a histogram with the given bucket boundaries.
// Values <= buckets[i] go into counts[i].
// Values > buckets[len-1] go into the overflow bucket.
func NewHistogram(desc string, buckets []uint64) *Histogram {
	return &Histogram{
		desc:    desc,
		buckets: buckets,
		counts:  make([]uint64, len(buckets)+1),
	}
}

func (h *Histogram) Observe(value uint64) {
	atomic.AddUint64(&h.sum, value)
	atomic.AddUint64(&h.count, 1)

	for i, boundary := range h.buckets {
		if value <= boundary {
			atomic.AddUint64(&h.counts[i], 1)
			return
		}
	}
	// Overflow bucket
	atomic.AddUint64(&h.counts[len(h.buckets)], 1)
}

func (h *Histogram) Mean() float64 {
	count := atomic.LoadUint64(&h.count)
	if count == 0 {
		return 0
	}
	return float64(atomic.LoadUint64(&h.sum)) / float64(count)
}

func (h *Histogram) Count() uint64 {
	return atomic.LoadUint64(&h.count)
}

func (h *Histogram) Sum() uint64 {
	return atomic.LoadUint64(&h.sum)
}

func (h *Histogram) Desc() string {
	return h.desc
}

// Buckets returns bucket boundaries and counts for inspection.
func (h *Histogram) Buckets() (boundaries []uint64, counts []uint64) {
	counts = make([]uint64, len(h.counts))
	for i := range h.counts {
		counts[i] = atomic.LoadUint64(&h.counts[i])
	}
	return h.buckets, counts
}
