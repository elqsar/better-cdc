package config

import (
	"os"
	"strconv"
	"strings"
	"time"
)

// Load reads configuration from environment variables, falling back to defaults.
func Load() Config {
	cfg := DefaultConfig()

	if v := os.Getenv("AWS_RDS_DATABASE"); v != "" {
		cfg.Database = v
	}
	if v := os.Getenv("CDC_SLOT_NAME"); v != "" {
		cfg.SlotName = v
	}
	if v := os.Getenv("CDC_PLUGIN"); v != "" {
		cfg.Plugin = v
	}
	if v := os.Getenv("DATABASE_URL"); v != "" {
		cfg.DatabaseURL = v
	}
	if v := os.Getenv("BATCH_SIZE"); v != "" {
		if i, err := strconv.Atoi(v); err == nil {
			cfg.BatchSize = i
		}
	}
	if v := os.Getenv("BATCH_TIMEOUT"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			cfg.BatchTimeout = d
		}
	}
	if v := os.Getenv("CHECKPOINT_INTERVAL"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			cfg.CheckpointFreq = d
		}
	}
	if v := os.Getenv("NATS_URL"); v != "" {
		cfg.NATSURLs = strings.Split(v, ",")
	}
	if v := os.Getenv("NATS_USERNAME"); v != "" {
		cfg.NATSUsername = v
	}
	if v := os.Getenv("NATS_PASSWORD"); v != "" {
		cfg.NATSPassword = v
	}
	if v := os.Getenv("NATS_TIMEOUT"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			cfg.NATSTimeout = d
		}
	}
	if v := os.Getenv("HEALTH_ADDR"); v != "" {
		cfg.HealthAddr = v
	}
	if v := os.Getenv("TABLE_FILTERS"); v != "" {
		parts := strings.Split(v, ",")
		out := make([]string, 0, len(parts))
		for _, p := range parts {
			if trimmed := strings.TrimSpace(p); trimmed != "" {
				out = append(out, trimmed)
			}
		}
		cfg.TableFilters = out
	}
	if v := os.Getenv("CDC_PUBLICATIONS"); v != "" {
		parts := strings.Split(v, ",")
		out := make([]string, 0, len(parts))
		for _, p := range parts {
			if trimmed := strings.TrimSpace(p); trimmed != "" {
				out = append(out, trimmed)
			}
		}
		if len(out) > 0 {
			cfg.Publications = out
		}
	}
	if v := strings.ToLower(os.Getenv("DEBUG")); v == "1" || v == "true" || v == "yes" {
		cfg.Debug = true
	}
	if v := os.Getenv("RAW_MESSAGE_BUFFER_SIZE"); v != "" {
		if i, err := strconv.Atoi(v); err == nil && i >= 0 {
			cfg.RawMessageBufferSize = i
		}
	}
	if v := os.Getenv("PARSED_EVENT_BUFFER_SIZE"); v != "" {
		if i, err := strconv.Atoi(v); err == nil && i >= 0 {
			cfg.ParsedEventBufferSize = i
		}
	}
	if v := os.Getenv("MAX_TX_BUFFER_SIZE"); v != "" {
		if i, err := strconv.Atoi(v); err == nil && i >= 0 {
			cfg.MaxTxBufferSize = i
		}
	}

	return cfg
}
