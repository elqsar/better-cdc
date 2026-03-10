package config

import "testing"

func TestLoad_DerivesDatabaseNameFromDatabaseURL(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://user:pass@localhost:5432/appdb?sslmode=disable")

	cfg := Load()

	if cfg.Database != "appdb" {
		t.Fatalf("expected database name %q, got %q", "appdb", cfg.Database)
	}
}

func TestLoad_CDCDatabaseNameOverridesDerivedDatabase(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://user:pass@localhost:5432/appdb")
	t.Setenv("CDC_DATABASE_NAME", "events")

	cfg := Load()

	if cfg.Database != "events" {
		t.Fatalf("expected override database name %q, got %q", "events", cfg.Database)
	}
}

func TestLoad_AWSRDSDatabaseStillOverridesDerivedDatabase(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://user:pass@localhost:5432/appdb")
	t.Setenv("AWS_RDS_DATABASE", "legacy")

	cfg := Load()

	if cfg.Database != "legacy" {
		t.Fatalf("expected legacy override database name %q, got %q", "legacy", cfg.Database)
	}
}

func TestLoad_KeepsDefaultDatabaseWhenURLHasNoPath(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://user:pass@localhost:5432")

	cfg := Load()

	if cfg.Database != "postgres" {
		t.Fatalf("expected default database name %q, got %q", "postgres", cfg.Database)
	}
}

func TestLoad_AllowNoopPublisher(t *testing.T) {
	t.Setenv("ALLOW_NOOP_PUBLISHER", "true")

	cfg := Load()

	if !cfg.AllowNoopPublisher {
		t.Fatal("expected AllowNoopPublisher to be true")
	}
}

func TestLoad_EnablePprof(t *testing.T) {
	t.Setenv("ENABLE_PPROF", "true")

	cfg := Load()

	if !cfg.EnablePprof {
		t.Fatal("expected EnablePprof to be true")
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
