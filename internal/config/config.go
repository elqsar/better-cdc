package config

import (
	"time"
)

// Config captures minimal settings for initial wiring.
type Config struct {
	Database       string
	SlotName       string
	Plugin         string
	DatabaseURL    string
	BatchSize      int
	BatchTimeout   time.Duration
	CheckpointFreq time.Duration
	NATSURLs       []string
	NATSUsername   string
	NATSPassword   string
	NATSTimeout    time.Duration
	HealthAddr     string
	TableFilters   []string
	Publications   []string
	Debug          bool

	// Pipeline buffer sizes for throughput optimization
	RawMessageBufferSize  int // Buffer between WAL reader and parser (default: 5000)
	ParsedEventBufferSize int // Buffer between parser and engine (default: 5000)

	// Transaction buffer limit for pgoutput parser
	// When exceeded, events are streamed immediately instead of buffered until COMMIT
	// This prevents OOM during large transactions (bulk inserts, migrations)
	MaxTxBufferSize int // Maximum events to buffer per transaction (default: 100000, 0 = unlimited)
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
	}
}
