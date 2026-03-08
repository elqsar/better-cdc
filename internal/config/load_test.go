package config

import "testing"

func TestLoad_AllowNoopPublisher(t *testing.T) {
	t.Setenv("ALLOW_NOOP_PUBLISHER", "true")

	cfg := Load()

	if !cfg.AllowNoopPublisher {
		t.Fatal("expected AllowNoopPublisher to be true")
	}
}
