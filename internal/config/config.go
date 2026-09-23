package config

import (
	"fmt"
	"strings"
	"time"
)

// Config captures minimal settings for initial wiring.
type Config struct {
	RawBufferBytes, ParsedBufferBytes, MaxTxBytes, MaxSpillBytes int64
	SpillDir                                                     string
	DLQStream, DLQBucket                                         string
	DLQMaxBytes, DLQIndexMaxBytes                                int64
	DLQTimeout                                                   time.Duration
	NATSCredentialsFile, NATSTLSCA, NATSTLSCert, NATSTLSKey      string

	Database                    string
	SlotName                    string
	Plugin                      string
	DatabaseURL                 string
	BatchSize                   int
	PublishAsyncMaxPending      int
	MaxPublishRetries           int
	UnsafeUnorderedAsyncPublish bool
	BatchTimeout                time.Duration
	CheckpointFreq              time.Duration
	NATSURLs                    []string
	NATSUsername                string
	NATSPassword                string
	NATSTimeout                 time.Duration
	AllowNoopPublisher          bool
	HealthAddr                  string
	TableFilters                []string
	Publications                []string
	Debug                       bool

	// Pipeline buffer sizes for throughput optimization
	RawMessageBufferSize  int // Buffer between WAL reader and parser (default: 5000)
	ParsedEventBufferSize int // Buffer between parser and engine (default: 5000)

	// Transaction buffer limit for pgoutput parser
	// When exceeded, raw pgoutput messages are spilled to disk and replayed at COMMIT.
	// This prevents OOM during large transactions (bulk inserts, migrations).
	MaxTxBufferSize int // Maximum events to buffer per transaction (default: 100000, 0 = unlimited)

	// JetStream stream durability configuration
	StreamName      string        // JetStream stream name (default: "CDC")
	StreamSubjects  []string      // Stream subject filters (default: ["cdc.>"])
	StreamStorage   string        // "file" or "memory" (default: "file")
	StreamReplicas  int           // Number of replicas (default: 1)
	StreamMaxAge    time.Duration // Max age for messages (default: 72h)
	DuplicateWindow time.Duration // De-duplication window (default: 10m)

	// PublishFailurePolicy controls what happens when an event fails to
	// publish with a permanent (non-retryable) error such as an oversized
	// payload or invalid subject:
	//   "crash" - stop the engine (default; the process exits and replays on restart)
	//   "dlq"   - persist complete recovery data and its index, then continue
	//   "skip"  - log, count, and continue
	// Transient failures (timeouts, disconnects) always crash after retries
	// regardless of this policy, so an outage never causes data to be skipped.
	PublishFailurePolicy string

	// DLQSubjectPrefix is the subject prefix for dead-letter records when
	// PublishFailurePolicy is "dlq" (default: "cdc_dlq", outside the live stream).
	DLQSubjectPrefix string

	// EnableProfiling enables block and mutex profiling (pprof).
	// Disabled by default because SetBlockProfileRate(1) captures every blocking
	// event and adds non-trivial overhead to channel/mutex operations.
	EnableProfiling bool

	// EnablePprof exposes /debug/pprof endpoints on the health server.
	// Disabled by default to avoid exposing diagnostic endpoints in production.
	EnablePprof bool
}

const defaultPublishAsyncMaxPendingFloor = 256

// DefaultConfig provides safe defaults for local prototyping.
func DefaultConfig() Config {
	cfg := Config{
		Database:                    "postgres",
		SlotName:                    "better_cdc_slot",
		Plugin:                      "pgoutput",
		DatabaseURL:                 "postgres://postgres:postgres@localhost:5432/postgres",
		BatchSize:                   500,
		PublishAsyncMaxPending:      0,
		MaxPublishRetries:           3,
		UnsafeUnorderedAsyncPublish: false,
		BatchTimeout:                100 * time.Millisecond,
		CheckpointFreq:              1 * time.Second,
		NATSURLs:                    []string{"nats://localhost:4222"},
		NATSTimeout:                 5 * time.Second,
		HealthAddr:                  ":8080",
		Publications:                []string{"better_cdc_pub"},
		RawMessageBufferSize:        5000,
		ParsedEventBufferSize:       5000,
		MaxTxBufferSize:             100000, // 100k events max per transaction before streaming
		StreamName:                  "CDC",
		StreamSubjects:              []string{"cdc.>"},
		StreamStorage:               "file",
		StreamReplicas:              1,
		StreamMaxAge:                72 * time.Hour,
		DuplicateWindow:             10 * time.Minute,
		PublishFailurePolicy:        "crash",
		DLQSubjectPrefix:            "cdc_dlq",
		DLQMaxBytes:                 1 << 30, DLQIndexMaxBytes: 64 << 20, DLQTimeout: time.Minute,
		RawBufferBytes: 64 << 20, ParsedBufferBytes: 64 << 20, MaxTxBytes: 64 << 20, MaxSpillBytes: 1 << 30,
	}
	cfg.DLQStream, cfg.DLQBucket = defaultDLQNames(cfg.StreamName)
	return cfg
}

// defaultDLQNames derives the DLQ index stream and recovery bucket names from
// the live stream name. It is the single source of that naming rule.
func defaultDLQNames(streamName string) (stream, bucket string) {
	return streamName + "_DLQ", streamName + "_RECOVERY"
}

// Validate rejects configuration values that would crash or degrade the engine.
func (c Config) Validate() error {
	switch c.Plugin {
	case "", "wal2json", "pgoutput":
	default:
		return fmt.Errorf("CDC_PLUGIN must be pgoutput or wal2json")
	}
	if c.BatchSize < 0 {
		return fmt.Errorf("BATCH_SIZE must be >= 0")
	}
	if c.BatchTimeout <= 0 {
		return fmt.Errorf("BATCH_TIMEOUT must be > 0")
	}
	if c.PublishAsyncMaxPending < 0 {
		return fmt.Errorf("PUBLISH_ASYNC_MAX_PENDING must be >= 0")
	}
	if c.MaxPublishRetries < 0 {
		return fmt.Errorf("MAX_PUBLISH_RETRIES must be >= 0")
	}
	if c.CheckpointFreq <= 0 {
		return fmt.Errorf("CHECKPOINT_INTERVAL must be > 0")
	}
	if c.NATSTimeout <= 0 {
		return fmt.Errorf("NATS_TIMEOUT must be > 0")
	}
	if c.RawMessageBufferSize < 0 {
		return fmt.Errorf("RAW_MESSAGE_BUFFER_SIZE must be >= 0")
	}
	if c.ParsedEventBufferSize < 0 {
		return fmt.Errorf("PARSED_EVENT_BUFFER_SIZE must be >= 0")
	}
	if c.MaxTxBufferSize < 0 {
		return fmt.Errorf("MAX_TX_BUFFER_SIZE must be >= 0")
	}
	switch c.StreamStorage {
	case "", "file", "memory":
	default:
		return fmt.Errorf("STREAM_STORAGE must be file or memory")
	}
	if c.StreamReplicas <= 0 {
		return fmt.Errorf("STREAM_REPLICAS must be > 0")
	}
	if c.StreamMaxAge <= 0 {
		return fmt.Errorf("STREAM_MAX_AGE must be > 0")
	}
	if c.DuplicateWindow <= 0 {
		return fmt.Errorf("DUPLICATE_WINDOW must be > 0")
	}
	switch c.PublishFailurePolicy {
	case "", "crash", "dlq", "skip":
	default:
		return fmt.Errorf("PUBLISH_FAILURE_POLICY must be crash, dlq, or skip")
	}
	if c.PublishFailurePolicy == "dlq" && strings.TrimSpace(c.DLQSubjectPrefix) == "" {
		return fmt.Errorf("DLQ_SUBJECT_PREFIX must not be empty when PUBLISH_FAILURE_POLICY=dlq")
	}
	if c.RawBufferBytes <= 0 || c.ParsedBufferBytes <= 0 || c.MaxTxBytes <= 0 || c.MaxSpillBytes <= 0 {
		return fmt.Errorf("byte budgets must be positive")
	}
	if c.Database == "" || c.SlotName == "" {
		return fmt.Errorf("source database and slot must not be empty")
	}
	if (c.Plugin == "pgoutput" || c.Plugin == "") && len(c.Publications) == 0 {
		return fmt.Errorf("pgoutput requires a publication")
	}
	if (c.NATSTLSCert == "") != (c.NATSTLSKey == "") {
		return fmt.Errorf("NATS_TLS_CERT and NATS_TLS_KEY must be set together")
	}
	if c.NATSCredentialsFile != "" && (c.NATSUsername != "" || c.NATSPassword != "") {
		return fmt.Errorf("choose credentials file or username/password")
	}
	if c.PublishFailurePolicy == "dlq" {
		if c.DLQMaxBytes <= 0 || c.DLQIndexMaxBytes <= 0 || c.DLQStream == "" || c.DLQBucket == "" {
			return fmt.Errorf("DLQ storage names and positive byte limits are required")
		}
		if c.DLQTimeout <= 0 {
			return fmt.Errorf("DLQ_TIMEOUT must be > 0")
		}
		if c.DLQStream == c.StreamName {
			return fmt.Errorf("DLQ index requires a separate stream")
		}
		if strings.ContainsAny(c.DLQSubjectPrefix, "* >\t\r\n") || strings.HasPrefix(c.DLQSubjectPrefix, ".") || strings.HasSuffix(c.DLQSubjectPrefix, ".") || strings.Contains(c.DLQSubjectPrefix, "..") {
			return fmt.Errorf("invalid DLQ subject prefix")
		}
		for _, filter := range effectiveStreamSubjects(c.StreamSubjects) {
			if subjectPatternsOverlap(strings.Split(filter, "."), strings.Split(c.DLQSubjectPrefix+".>", ".")) {
				return fmt.Errorf("DLQ subjects must not overlap STREAM_SUBJECTS")
			}
		}
	}

	return nil
}

func (c Config) EffectivePublishAsyncMaxPending() int {
	if c.PublishAsyncMaxPending > 0 {
		return c.PublishAsyncMaxPending
	}
	if c.BatchSize > defaultPublishAsyncMaxPendingFloor {
		return c.BatchSize
	}
	return defaultPublishAsyncMaxPendingFloor
}

func effectiveStreamSubjects(subjects []string) []string {
	if len(subjects) == 0 {
		return []string{"cdc.>"}
	}
	return subjects
}

func subjectPatternsOverlap(a, b []string) bool {
	if len(a) == 0 || len(b) == 0 {
		return len(a) == len(b)
	}
	if a[0] == ">" || b[0] == ">" {
		return true
	}
	if a[0] != "*" && b[0] != "*" && a[0] != b[0] {
		return false
	}
	return subjectPatternsOverlap(a[1:], b[1:])
}
