package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const namespace = "cdc"

// PrometheusCounter wraps prometheus.Counter with the same interface as Counter.
type PrometheusCounter struct {
	counter prometheus.Counter
}

// NewPrometheusCounter creates a new Prometheus counter with the given name and help text.
func NewPrometheusCounter(subsystem, name, help string) *PrometheusCounter {
	return &PrometheusCounter{
		counter: promauto.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      name,
			Help:      help,
		}),
	}
}

func (c *PrometheusCounter) Inc() {
	c.counter.Inc()
}

func (c *PrometheusCounter) Add(n uint64) {
	c.counter.Add(float64(n))
}

// PrometheusGauge wraps prometheus.Gauge with the same interface as Gauge.
type PrometheusGauge struct {
	gauge prometheus.Gauge
}

// NewPrometheusGauge creates a new Prometheus gauge with the given name and help text.
func NewPrometheusGauge(subsystem, name, help string) *PrometheusGauge {
	return &PrometheusGauge{
		gauge: promauto.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      name,
			Help:      help,
		}),
	}
}

func (g *PrometheusGauge) Set(v int64) {
	g.gauge.Set(float64(v))
}

func (g *PrometheusGauge) Get() int64 {
	// Prometheus gauges don't have a Get method, but we can use Collect
	// For simplicity, this returns 0 - use Prometheus queries for actual values
	return 0
}

// PrometheusHistogram wraps prometheus.Histogram with the same interface as Histogram.
type PrometheusHistogram struct {
	histogram prometheus.Histogram
	sum       float64
	count     uint64
}

// NewPrometheusHistogram creates a new Prometheus histogram with the given buckets.
func NewPrometheusHistogram(subsystem, name, help string, buckets []float64) *PrometheusHistogram {
	return &PrometheusHistogram{
		histogram: promauto.NewHistogram(prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      name,
			Help:      help,
			Buckets:   buckets,
		}),
	}
}

func (h *PrometheusHistogram) Observe(value uint64) {
	h.histogram.Observe(float64(value))
}

func (h *PrometheusHistogram) Mean() float64 {
	return 0 // Use Prometheus queries for actual mean
}

func (h *PrometheusHistogram) Count() uint64 {
	return 0 // Use Prometheus queries for actual count
}

func (h *PrometheusHistogram) Sum() uint64 {
	return 0 // Use Prometheus queries for actual sum
}

// Metrics is a centralized registry of all CDC metrics.
type Metrics struct {
	// Engine metrics
	EventsTotal      *PrometheusCounter
	BatchesPublished *PrometheusCounter
	BatchLatency     *PrometheusHistogram
	TransformLatency *PrometheusHistogram

	// Publisher metrics
	JetstreamPublished  *PrometheusCounter
	JetstreamAckFailure *PrometheusCounter

	// Parser metrics
	ReplicationLag        *PrometheusGauge
	DecodeErrors          *PrometheusCounter
	TxBufferSize          *PrometheusGauge   // Current transaction buffer size (pgoutput)
	TxBufferOverflows     *PrometheusCounter // Transactions that exceeded buffer limit

	// WAL Reader metrics
	ReplicationErrors *PrometheusCounter

	// Additional useful metrics
	EventsPerSecond *PrometheusGauge
}

// NewMetrics creates a new centralized metrics registry with all CDC metrics.
func NewMetrics() *Metrics {
	return &Metrics{
		// Engine metrics
		EventsTotal: NewPrometheusCounter("engine", "events_total",
			"Total number of CDC events processed"),
		BatchesPublished: NewPrometheusCounter("engine", "batches_published_total",
			"Total number of batches published"),
		BatchLatency: NewPrometheusHistogram("engine", "batch_latency_microseconds",
			"Batch publishing latency in microseconds",
			[]float64{100, 500, 1000, 5000, 10000, 50000, 100000}),
		TransformLatency: NewPrometheusHistogram("engine", "transform_latency_nanoseconds",
			"Event transformation latency in nanoseconds",
			[]float64{100, 500, 1000, 5000, 10000, 50000}),

		// Publisher metrics
		JetstreamPublished: NewPrometheusCounter("publisher", "jetstream_published_total",
			"Total number of messages published to JetStream"),
		JetstreamAckFailure: NewPrometheusCounter("publisher", "jetstream_ack_failures_total",
			"Total number of JetStream ack failures"),

		// Parser metrics
		ReplicationLag: NewPrometheusGauge("parser", "replication_lag_milliseconds",
			"Current replication lag in milliseconds"),
		DecodeErrors: NewPrometheusCounter("parser", "decode_errors_total",
			"Total number of message decode errors"),
		TxBufferSize: NewPrometheusGauge("parser", "tx_buffer_size",
			"Current number of events buffered in transaction (pgoutput)"),
		TxBufferOverflows: NewPrometheusCounter("parser", "tx_buffer_overflows_total",
			"Total number of transactions that exceeded buffer limit and switched to streaming"),

		// WAL Reader metrics
		ReplicationErrors: NewPrometheusCounter("wal", "replication_errors_total",
			"Total number of replication errors"),

		// Throughput gauge
		EventsPerSecond: NewPrometheusGauge("engine", "events_per_second",
			"Current events processed per second"),
	}
}

// Global metrics instance
var GlobalMetrics = NewMetrics()
