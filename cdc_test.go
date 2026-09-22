package cdc_test

import (
	"context"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	cdc "github.com/elqsar/better-cdc"
)

func TestConstructionAndSingleUse(t *testing.T) {
	cfg := cdc.DefaultConfig()
	if _, err := cdc.New(cfg); err == nil {
		t.Fatal("source identity must be explicit")
	}
	cfg.Source.SourceID = "test"
	cfg.Source.DatabaseURL = "postgres://localhost:1/unreachable"
	cfg.Pipeline.SpillDir = t.TempDir() + "/not-created"
	first, err := cdc.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(cfg.Pipeline.SpillDir); !os.IsNotExist(err) {
		t.Fatalf("New performed filesystem I/O: %v", err)
	}
	second, err := cdc.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := first.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if err := first.Run(ctx); err == nil {
		t.Fatal("runner reused")
	}
	if first.Ready(context.Background()) == nil || second.Ready(context.Background()) == nil {
		t.Fatal("idle producer ready")
	}
	for _, r := range []*cdc.Runner{first, second} {
		response := httptest.NewRecorder()
		r.MetricsHandler().ServeHTTP(response, httptest.NewRequest("GET", "/metrics", nil))
		if response.Code != 200 || !strings.Contains(response.Body.String(), "cdc_engine_events_total 0") {
			t.Fatalf("metrics: %s", response.Body.String())
		}
	}
}
