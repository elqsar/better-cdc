package publisher

import (
	"context"
	"testing"
)

func TestNoopPublisherReady_ReturnsDegradedModeError(t *testing.T) {
	pub := NewNoopPublisher()

	err := pub.Ready(context.Background())
	if err == nil {
		t.Fatal("expected noop publisher readiness error")
	}
	if err.Error() != noopPublisherReadyError {
		t.Fatalf("expected readiness error %q, got %q", noopPublisherReadyError, err.Error())
	}
}
