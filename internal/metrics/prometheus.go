package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const namespace = "cdc"

type factory struct{ promauto.Factory }

// PrometheusCounter wraps prometheus.Counter with the same interface as Counter.
type PrometheusCounter struct {
	counter prometheus.Counter
}

// NewPrometheusCounter creates a new Prometheus counter with the given name and help text.
func (f factory) NewPrometheusCounter(subsystem, name, help string) *PrometheusCounter {
	return &PrometheusCounter{
		counter: f.NewCounter(prometheus.CounterOpts{
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
func (f factory) NewPrometheusGauge(subsystem, name, help string) *PrometheusGauge {
	return &PrometheusGauge{
		gauge: f.NewGauge(prometheus.GaugeOpts{
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
	sum       float64 //nolint:unused
	count     uint64  //nolint:unused
}

// NewPrometheusHistogram creates a new Prometheus histogram with the given buckets.
func (f factory) NewPrometheusHistogram(subsystem, name, help string, buckets []float64) *PrometheusHistogram {
	return &PrometheusHistogram{
		histogram: f.NewHistogram(prometheus.HistogramOpts{
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
	Registry *prometheus.Registry
	Pilot    *PilotMetrics
	// Engine metrics
	EventsTotal          *PrometheusCounter
	BatchesPublished     *PrometheusCounter
	BatchLatency         *PrometheusHistogram
	TransformLatency     *PrometheusHistogram
	PartialBatchFailures *PrometheusCounter // Batches with partial success (some items failed)
	EventsQuarantined    *PrometheusCounter // Events dead-lettered or skipped after a permanent publish failure

	// Publisher metrics
	JetstreamPublished  *PrometheusCounter
	JetstreamAckFailure *PrometheusCounter
	PublishRetries      *PrometheusCounter // Retry attempts for failed publishes

	// Parser metrics
	ReplicationLag    *PrometheusGauge
	DecodeErrors      *PrometheusCounter
	TxBufferSize      *PrometheusGauge   // Current transaction buffer size (pgoutput)
	TxBufferOverflows *PrometheusCounter // Transactions that exceeded buffer limit

	// WAL Reader metrics
	ReplicationErrors *PrometheusCounter

	// Additional useful metrics
	EventsPerSecond *PrometheusGauge
}

// NewMetrics creates a new centralized metrics registry with all CDC metrics.
func NewMetrics() *Metrics {
	registry := prometheus.NewRegistry()
	f := factory{promauto.With(registry)}
	return &Metrics{
		Registry: registry, Pilot: newPilot(f),
		// Engine metrics
		EventsTotal: f.NewPrometheusCounter("engine", "events_total",
			"Total number of CDC events processed"),
		BatchesPublished: f.NewPrometheusCounter("engine", "batches_published_total",
			"Total number of batches published"),
		BatchLatency: f.NewPrometheusHistogram("engine", "batch_latency_microseconds",
			"Batch publishing latency in microseconds",
			[]float64{100, 500, 1000, 5000, 10000, 50000, 100000}),
		TransformLatency: f.NewPrometheusHistogram("engine", "transform_latency_nanoseconds",
			"Event transformation latency in nanoseconds",
			[]float64{100, 500, 1000, 5000, 10000, 50000}),
		PartialBatchFailures: f.NewPrometheusCounter("engine", "partial_batch_failures_total",
			"Total number of batches with partial success (some items failed and checkpoint was not advanced)"),
		EventsQuarantined: f.NewPrometheusCounter("engine", "events_quarantined_total",
			"Total number of events dead-lettered or skipped after a permanent publish failure"),

		// Publisher metrics
		JetstreamPublished: f.NewPrometheusCounter("publisher", "jetstream_published_total",
			"Total number of messages published to JetStream"),
		JetstreamAckFailure: f.NewPrometheusCounter("publisher", "jetstream_ack_failures_total",
			"Total number of JetStream ack failures"),
		PublishRetries: f.NewPrometheusCounter("publisher", "publish_retries_total",
			"Total number of publish retry attempts due to transient failures"),

		// Parser metrics
		ReplicationLag: f.NewPrometheusGauge("parser", "replication_lag_milliseconds",
			"Current replication lag in milliseconds"),
		DecodeErrors: f.NewPrometheusCounter("parser", "decode_errors_total",
			"Total number of message decode errors"),
		TxBufferSize: f.NewPrometheusGauge("parser", "tx_buffer_size",
			"Current number of events buffered in transaction (pgoutput)"),
		TxBufferOverflows: f.NewPrometheusCounter("parser", "tx_buffer_overflows_total",
			"Total number of transactions that exceeded buffer limit and switched to streaming"),

		// WAL Reader metrics
		ReplicationErrors: f.NewPrometheusCounter("wal", "replication_errors_total",
			"Total number of replication errors"),

		// Throughput gauge
		EventsPerSecond: f.NewPrometheusGauge("engine", "events_per_second",
			"Current events processed per second"),
	}
}

// OrNew gives standalone internal components their own isolated metrics.
func OrNew(m *Metrics) *Metrics {
	if m == nil {
		return NewMetrics()
	}
	return m
}
