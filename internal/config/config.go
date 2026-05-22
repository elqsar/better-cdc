package config

import (
	"fmt"
	"time"
)

// Config captures minimal settings for initial wiring.
type Config struct {
	Database               string
	SlotName               string
	Plugin                 string
	DatabaseURL            string
	BatchSize              int
	PublishAsyncMaxPending int
	MaxPublishRetries      int
	BatchTimeout           time.Duration
	CheckpointFreq         time.Duration
	NATSURLs               []string
	NATSUsername           string
	NATSPassword           string
	NATSTimeout            time.Duration
	AllowNoopPublisher     bool
	HealthAddr             string
	TableFilters           []string
	Publications           []string
	Debug                  bool

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
	DuplicateWindow time.Duration // De-duplication window (default: 2m)

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
	return Config{
		Database:               "postgres",
		SlotName:               "better_cdc_slot",
		Plugin:                 "wal2json",
		DatabaseURL:            "postgres://postgres:postgres@localhost:5432/postgres",
		BatchSize:              500,
		PublishAsyncMaxPending: 0,
		MaxPublishRetries:      3,
		BatchTimeout:           100 * time.Millisecond,
		CheckpointFreq:         1 * time.Second,
		NATSURLs:               []string{"nats://localhost:4222"},
		NATSTimeout:            5 * time.Second,
		HealthAddr:             ":8080",
		Publications:           []string{"better_cdc_pub"},
		RawMessageBufferSize:   5000,
		ParsedEventBufferSize:  5000,
		MaxTxBufferSize:        100000, // 100k events max per transaction before streaming
		StreamName:             "CDC",
		StreamSubjects:         []string{"cdc.>"},
		StreamStorage:          "file",
		StreamReplicas:         1,
		StreamMaxAge:           72 * time.Hour,
		DuplicateWindow:        2 * time.Minute,
	}
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
