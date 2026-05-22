package config

import (
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

// Load reads configuration from environment variables, falling back to defaults.
func Load() (Config, error) {
	cfg := DefaultConfig()

	if v := os.Getenv("CDC_SLOT_NAME"); v != "" {
		cfg.SlotName = v
	}
	if v := os.Getenv("CDC_PLUGIN"); v != "" {
		cfg.Plugin = v
	}
	if v := os.Getenv("DATABASE_URL"); v != "" {
		cfg.DatabaseURL = v
	}
	if v := os.Getenv("CDC_DATABASE_NAME"); v != "" {
		cfg.Database = v
	} else if v := os.Getenv("AWS_RDS_DATABASE"); v != "" {
		cfg.Database = v
	} else if derived := databaseNameFromURL(cfg.DatabaseURL); derived != "" {
		cfg.Database = derived
	}
	if v := os.Getenv("BATCH_SIZE"); v != "" {
		i, err := parseEnvInt("BATCH_SIZE", v)
		if err != nil {
			return cfg, err
		}
		cfg.BatchSize = i
	}
	if v := os.Getenv("PUBLISH_ASYNC_MAX_PENDING"); v != "" {
		i, err := parseEnvInt("PUBLISH_ASYNC_MAX_PENDING", v)
		if err != nil {
			return cfg, err
		}
		cfg.PublishAsyncMaxPending = i
	}
	if v := os.Getenv("MAX_PUBLISH_RETRIES"); v != "" {
		i, err := parseEnvInt("MAX_PUBLISH_RETRIES", v)
		if err != nil {
			return cfg, err
		}
		cfg.MaxPublishRetries = i
	}
	if v := os.Getenv("BATCH_TIMEOUT"); v != "" {
		d, err := parseEnvDuration("BATCH_TIMEOUT", v)
		if err != nil {
			return cfg, err
		}
		cfg.BatchTimeout = d
	}
	if v := os.Getenv("CHECKPOINT_INTERVAL"); v != "" {
		d, err := parseEnvDuration("CHECKPOINT_INTERVAL", v)
		if err != nil {
			return cfg, err
		}
		cfg.CheckpointFreq = d
	}
	if v := os.Getenv("NATS_URL"); v != "" {
		cfg.NATSURLs = splitAndTrimCSV(v)
	}
	if v := os.Getenv("NATS_USERNAME"); v != "" {
		cfg.NATSUsername = v
	}
	if v := os.Getenv("NATS_PASSWORD"); v != "" {
		cfg.NATSPassword = v
	}
	if v := os.Getenv("NATS_TIMEOUT"); v != "" {
		d, err := parseEnvDuration("NATS_TIMEOUT", v)
		if err != nil {
			return cfg, err
		}
		cfg.NATSTimeout = d
	}
	if v := os.Getenv("ALLOW_NOOP_PUBLISHER"); v != "" {
		enabled, err := parseEnvBool("ALLOW_NOOP_PUBLISHER", v)
		if err != nil {
			return cfg, err
		}
		cfg.AllowNoopPublisher = enabled
	}
	if v := os.Getenv("HEALTH_ADDR"); v != "" {
		cfg.HealthAddr = v
	}
	if v := os.Getenv("TABLE_FILTERS"); v != "" {
		cfg.TableFilters = splitAndTrimCSV(v)
	}
	if v := os.Getenv("CDC_PUBLICATIONS"); v != "" {
		if out := splitAndTrimCSV(v); len(out) > 0 {
			cfg.Publications = out
		}
	}
	if v := os.Getenv("DEBUG"); v != "" {
		enabled, err := parseEnvBool("DEBUG", v)
		if err != nil {
			return cfg, err
		}
		cfg.Debug = enabled
	}
	if v := os.Getenv("RAW_MESSAGE_BUFFER_SIZE"); v != "" {
		i, err := parseEnvInt("RAW_MESSAGE_BUFFER_SIZE", v)
		if err != nil {
			return cfg, err
		}
		cfg.RawMessageBufferSize = i
	}
	if v := os.Getenv("PARSED_EVENT_BUFFER_SIZE"); v != "" {
		i, err := parseEnvInt("PARSED_EVENT_BUFFER_SIZE", v)
		if err != nil {
			return cfg, err
		}
		cfg.ParsedEventBufferSize = i
	}
	if v := os.Getenv("MAX_TX_BUFFER_SIZE"); v != "" {
		i, err := parseEnvInt("MAX_TX_BUFFER_SIZE", v)
		if err != nil {
			return cfg, err
		}
		cfg.MaxTxBufferSize = i
	}
	if v := os.Getenv("STREAM_NAME"); v != "" {
		cfg.StreamName = v
	}
	if v := os.Getenv("STREAM_SUBJECTS"); v != "" {
		if out := splitAndTrimCSV(v); len(out) > 0 {
			cfg.StreamSubjects = out
		}
	}
	if v := os.Getenv("STREAM_STORAGE"); v != "" {
		cfg.StreamStorage = strings.ToLower(v)
	}
	if v := os.Getenv("STREAM_REPLICAS"); v != "" {
		i, err := parseEnvInt("STREAM_REPLICAS", v)
		if err != nil {
			return cfg, err
		}
		cfg.StreamReplicas = i
	}
	if v := os.Getenv("STREAM_MAX_AGE"); v != "" {
		d, err := parseEnvDuration("STREAM_MAX_AGE", v)
		if err != nil {
			return cfg, err
		}
		cfg.StreamMaxAge = d
	}
	if v := os.Getenv("DUPLICATE_WINDOW"); v != "" {
		d, err := parseEnvDuration("DUPLICATE_WINDOW", v)
		if err != nil {
			return cfg, err
		}
		cfg.DuplicateWindow = d
	}
	if v := os.Getenv("ENABLE_PROFILING"); v != "" {
		enabled, err := parseEnvBool("ENABLE_PROFILING", v)
		if err != nil {
			return cfg, err
		}
		cfg.EnableProfiling = enabled
	}
	if v := os.Getenv("ENABLE_PPROF"); v != "" {
		enabled, err := parseEnvBool("ENABLE_PPROF", v)
		if err != nil {
			return cfg, err
		}
		cfg.EnablePprof = enabled
	}

	return cfg, nil
}

func splitAndTrimCSV(v string) []string {
	parts := strings.Split(v, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if trimmed := strings.TrimSpace(p); trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}

func databaseNameFromURL(raw string) string {
	if raw == "" {
		return ""
	}
	parsed, err := url.Parse(raw)
	if err != nil {
		return ""
	}
	name := strings.Trim(parsed.Path, "/")
	if name == "" {
		return ""
	}
	return name
}

func parseEnvInt(name, value string) (int, error) {
	out, err := strconv.Atoi(value)
	if err != nil {
		return 0, fmt.Errorf("%s must be an integer: %w", name, err)
	}
	return out, nil
}

func parseEnvDuration(name, value string) (time.Duration, error) {
	out, err := time.ParseDuration(value)
	if err != nil {
		return 0, fmt.Errorf("%s must be a duration: %w", name, err)
	}
	return out, nil
}

func parseEnvBool(name, value string) (bool, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "1", "true", "yes":
		return true, nil
	case "0", "false", "no":
		return false, nil
	default:
		return false, fmt.Errorf("%s must be a boolean (true/false, yes/no, or 1/0)", name)
	}
}
