package config

import (
	"time"
)

// Config captures minimal settings for initial wiring.
type Config struct {
	Database           string
	SlotName           string
	Plugin             string
	DatabaseURL        string
	BatchSize          int
	BatchTimeout       time.Duration
	CheckpointFreq     time.Duration
	NATSURLs           []string
	NATSUsername       string
	NATSPassword       string
	NATSTimeout        time.Duration
	AllowNoopPublisher bool
	HealthAddr         string
	TableFilters       []string
	Publications       []string
	Debug              bool

	// Pipeline buffer sizes for throughput optimization
	RawMessageBufferSize  int // Buffer between WAL reader and parser (default: 5000)
	ParsedEventBufferSize int // Buffer between parser and engine (default: 5000)

	// Transaction buffer limit for pgoutput parser
	// When exceeded, events are streamed immediately instead of buffered until COMMIT
	// This prevents OOM during large transactions (bulk inserts, migrations)
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

// DefaultConfig provides safe defaults for local prototyping.
func DefaultConfig() Config {
	return Config{
		Database:              "postgres",
		SlotName:              "better_cdc_slot",
		Plugin:                "wal2json",
		DatabaseURL:           "postgres://postgres:postgres@localhost:5432/postgres",
		BatchSize:             500,
		BatchTimeout:          100 * time.Millisecond,
		CheckpointFreq:        1 * time.Second,
		NATSURLs:              []string{"nats://localhost:4222"},
		NATSTimeout:           5 * time.Second,
		HealthAddr:            ":8080",
		Publications:          []string{"better_cdc_pub"},
		RawMessageBufferSize:  5000,
		ParsedEventBufferSize: 5000,
		MaxTxBufferSize:       100000, // 100k events max per transaction before streaming
		StreamName:            "CDC",
		StreamSubjects:        []string{"cdc.>"},
		StreamStorage:         "file",
		StreamReplicas:        1,
		StreamMaxAge:          72 * time.Hour,
		DuplicateWindow:       2 * time.Minute,
	}
}
