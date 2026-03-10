package publisher

import (
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"go.uber.org/zap"
)

func TestExpectedStreamConfig_AppliesDefaults(t *testing.T) {
	p := NewJetStreamPublisher(JetStreamOptions{}, zap.NewNop())

	cfg := p.expectedStreamConfig()

	if cfg.Name != "CDC" {
		t.Fatalf("expected default stream name CDC, got %q", cfg.Name)
	}
	if len(cfg.Subjects) != 1 || cfg.Subjects[0] != "cdc.>" {
		t.Fatalf("unexpected default subjects: %v", cfg.Subjects)
	}
	if cfg.Storage != nats.FileStorage {
		t.Fatalf("expected file storage, got %v", cfg.Storage)
	}
	if cfg.Replicas != 1 {
		t.Fatalf("expected 1 replica, got %d", cfg.Replicas)
	}
	if cfg.MaxAge != 72*time.Hour {
		t.Fatalf("expected 72h max age, got %v", cfg.MaxAge)
	}
	if cfg.Duplicates != 2*time.Minute {
		t.Fatalf("expected 2m duplicate window, got %v", cfg.Duplicates)
	}
}

func TestValidateStreamConfig_AcceptsEquivalentSubjectsOrder(t *testing.T) {
	actual := &nats.StreamConfig{
		Name:       "CDC",
		Subjects:   []string{"b.>", "a.>"},
		Retention:  nats.LimitsPolicy,
		Storage:    nats.FileStorage,
		Replicas:   3,
		MaxAge:     24 * time.Hour,
		Duplicates: 5 * time.Minute,
	}
	expected := &nats.StreamConfig{
		Name:       "CDC",
		Subjects:   []string{"a.>", "b.>"},
		Retention:  nats.LimitsPolicy,
		Storage:    nats.FileStorage,
		Replicas:   3,
		MaxAge:     24 * time.Hour,
		Duplicates: 5 * time.Minute,
	}

	if err := validateStreamConfig(actual, expected); err != nil {
		t.Fatalf("expected stream configs to match, got %v", err)
	}
}

func TestValidateStreamConfig_RejectsMismatchedReplicas(t *testing.T) {
	actual := &nats.StreamConfig{
		Name:       "CDC",
		Subjects:   []string{"cdc.>"},
		Retention:  nats.LimitsPolicy,
		Storage:    nats.FileStorage,
		Replicas:   1,
		MaxAge:     72 * time.Hour,
		Duplicates: 2 * time.Minute,
	}
	expected := &nats.StreamConfig{
		Name:       "CDC",
		Subjects:   []string{"cdc.>"},
		Retention:  nats.LimitsPolicy,
		Storage:    nats.FileStorage,
		Replicas:   3,
		MaxAge:     72 * time.Hour,
		Duplicates: 2 * time.Minute,
	}

	err := validateStreamConfig(actual, expected)
	if err == nil {
		t.Fatal("expected mismatch error")
	}
}
