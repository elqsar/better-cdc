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
	RedisURL       string
	CheckpointKey  string
	CheckpointTTL  time.Duration
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
		RedisURL:              "redis://localhost:6379",
		CheckpointKey:         "better-cdc:checkpoint",
		CheckpointTTL:         24 * time.Hour,
		NATSURLs:              []string{"nats://localhost:4222"},
		NATSTimeout:           5 * time.Second,
		HealthAddr:            ":8080",
		Publications:          []string{"better_cdc_pub"},
		RawMessageBufferSize:  5000,
		ParsedEventBufferSize: 5000,
	}
}
