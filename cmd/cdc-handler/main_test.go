package main

import (
	"context"
	"testing"
	"time"

	"better-cdc/internal/config"
	"better-cdc/internal/publisher"

	"go.uber.org/zap"
)

func TestBuildPublisher_RequiresNATSByDefault(t *testing.T) {
	_, err := buildPublisher(config.Config{}, zap.NewNop())
	if err == nil {
		t.Fatal("expected error when no NATS URLs are configured")
	}
}

func TestBuildPublisher_AllowsExplicitNoopPublisher(t *testing.T) {
	pub, err := buildPublisher(config.Config{
		AllowNoopPublisher: true,
	}, zap.NewNop())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, ok := pub.(*publisher.NoopPublisher); !ok {
		t.Fatalf("expected noop publisher, got %T", pub)
	}
}

func TestBuildPublisher_NoopPublisherIsNotReady(t *testing.T) {
	pub, err := buildPublisher(config.Config{
		AllowNoopPublisher: true,
	}, zap.NewNop())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	ready, ok := pub.(interface {
		Ready(context.Context) error
	})
	if !ok {
		t.Fatalf("expected readiness-capable publisher, got %T", pub)
	}
	if err := ready.Ready(context.Background()); err == nil {
		t.Fatal("expected noop publisher to report not ready")
	}
}

func TestBuildPublisher_TrimsConfiguredNATSURLs(t *testing.T) {
	pub, err := buildPublisher(config.Config{
		NATSURLs:    []string{" nats://localhost:4222 ", "", "  nats://localhost:4223"},
		NATSTimeout: 5 * time.Second,
	}, zap.NewNop())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, ok := pub.(*publisher.JetStreamPublisher); !ok {
		t.Fatalf("expected jetstream publisher, got %T", pub)
	}
}
