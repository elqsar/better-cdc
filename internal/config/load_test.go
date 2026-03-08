package config

import "testing"

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
