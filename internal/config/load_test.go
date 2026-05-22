package config

import "testing"

func loadConfig(t *testing.T) Config {
	t.Helper()
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load returned error: %v", err)
	}
	return cfg
}

func TestLoad_DerivesDatabaseNameFromDatabaseURL(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://user:pass@localhost:5432/appdb?sslmode=disable")

	cfg := loadConfig(t)

	if cfg.Database != "appdb" {
		t.Fatalf("expected database name %q, got %q", "appdb", cfg.Database)
	}
}

func TestLoad_CDCDatabaseNameOverridesDerivedDatabase(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://user:pass@localhost:5432/appdb")
	t.Setenv("CDC_DATABASE_NAME", "events")

	cfg := loadConfig(t)

	if cfg.Database != "events" {
		t.Fatalf("expected override database name %q, got %q", "events", cfg.Database)
	}
}

func TestLoad_AWSRDSDatabaseStillOverridesDerivedDatabase(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://user:pass@localhost:5432/appdb")
	t.Setenv("AWS_RDS_DATABASE", "legacy")

	cfg := loadConfig(t)

	if cfg.Database != "legacy" {
		t.Fatalf("expected legacy override database name %q, got %q", "legacy", cfg.Database)
	}
}

func TestLoad_KeepsDefaultDatabaseWhenURLHasNoPath(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://user:pass@localhost:5432")

	cfg := loadConfig(t)

	if cfg.Database != "postgres" {
		t.Fatalf("expected default database name %q, got %q", "postgres", cfg.Database)
	}
}

func TestLoad_AllowNoopPublisher(t *testing.T) {
	t.Setenv("ALLOW_NOOP_PUBLISHER", "true")

	cfg := loadConfig(t)

	if !cfg.AllowNoopPublisher {
		t.Fatal("expected AllowNoopPublisher to be true")
	}
}

func TestLoad_EnablePprof(t *testing.T) {
	t.Setenv("ENABLE_PPROF", "true")

	cfg := loadConfig(t)

	if !cfg.EnablePprof {
		t.Fatal("expected EnablePprof to be true")
	}
}

func TestLoad_PublishAsyncMaxPendingOverride(t *testing.T) {
	t.Setenv("PUBLISH_ASYNC_MAX_PENDING", "1024")

	cfg := loadConfig(t)

	if cfg.PublishAsyncMaxPending != 1024 {
		t.Fatalf("expected PublishAsyncMaxPending %d, got %d", 1024, cfg.PublishAsyncMaxPending)
	}
}

func TestConfigEffectivePublishAsyncMaxPending_UsesBatchSizeWhenLarger(t *testing.T) {
	cfg := DefaultConfig()
	cfg.BatchSize = 500
	cfg.PublishAsyncMaxPending = 0

	if got := cfg.EffectivePublishAsyncMaxPending(); got != 500 {
		t.Fatalf("expected effective pending %d, got %d", 500, got)
	}
}

func TestConfigEffectivePublishAsyncMaxPending_UsesFloorWhenBatchSizeIsZero(t *testing.T) {
	cfg := DefaultConfig()
	cfg.BatchSize = 0
	cfg.PublishAsyncMaxPending = 0

	if got := cfg.EffectivePublishAsyncMaxPending(); got != 256 {
		t.Fatalf("expected effective pending %d, got %d", 256, got)
	}
}

func TestConfigEffectivePublishAsyncMaxPending_UsesExplicitOverride(t *testing.T) {
	cfg := DefaultConfig()
	cfg.BatchSize = 500
	cfg.PublishAsyncMaxPending = 64

	if got := cfg.EffectivePublishAsyncMaxPending(); got != 64 {
		t.Fatalf("expected explicit pending %d, got %d", 64, got)
	}
}

func TestConfigValidate_RejectsNegativeBatchSize(t *testing.T) {
	cfg := DefaultConfig()
	cfg.BatchSize = -1

	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected validation error for negative batch size")
	}
}

func TestConfigValidate_AllowsZeroBatchSize(t *testing.T) {
	cfg := DefaultConfig()
	cfg.BatchSize = 0

	if err := cfg.Validate(); err != nil {
		t.Fatalf("expected zero batch size to be valid, got %v", err)
	}
}

func TestConfigValidate_RejectsNegativePublishAsyncMaxPending(t *testing.T) {
	cfg := DefaultConfig()
	cfg.PublishAsyncMaxPending = -1

	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected validation error for negative publish async max pending")
	}
}

func TestLoad_RejectsInvalidInteger(t *testing.T) {
	t.Setenv("BATCH_SIZE", "many")

	if _, err := Load(); err == nil {
		t.Fatal("expected invalid integer error")
	}
}

func TestLoad_RejectsInvalidDuration(t *testing.T) {
	t.Setenv("BATCH_TIMEOUT", "100")

	if _, err := Load(); err == nil {
		t.Fatal("expected invalid duration error")
	}
}

func TestLoad_RejectsInvalidBool(t *testing.T) {
	t.Setenv("DEBUG", "sure")

	if _, err := Load(); err == nil {
		t.Fatal("expected invalid bool error")
	}
}

func TestConfigValidate_RejectsInvalidPlugin(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Plugin = "decoderbufs"

	if err := cfg.Validate(); err == nil {
		t.Fatal("expected validation error for invalid plugin")
	}
}

func TestConfigValidate_RejectsNonPositiveBatchTimeout(t *testing.T) {
	cfg := DefaultConfig()
	cfg.BatchTimeout = 0

	if err := cfg.Validate(); err == nil {
		t.Fatal("expected validation error for non-positive batch timeout")
	}
}

func TestConfigValidate_RejectsNegativeBufferSize(t *testing.T) {
	cfg := DefaultConfig()
	cfg.RawMessageBufferSize = -1

	if err := cfg.Validate(); err == nil {
		t.Fatal("expected validation error for negative raw buffer size")
	}
}

func TestConfigValidate_RejectsInvalidStreamStorage(t *testing.T) {
	cfg := DefaultConfig()
	cfg.StreamStorage = "disk"

	if err := cfg.Validate(); err == nil {
		t.Fatal("expected validation error for invalid stream storage")
	}
}

func TestConfigValidate_RejectsNonPositiveStreamReplicas(t *testing.T) {
	cfg := DefaultConfig()
	cfg.StreamReplicas = 0

	if err := cfg.Validate(); err == nil {
		t.Fatal("expected validation error for non-positive stream replicas")
	}
}
