package publisher

import (
	"context"
	"fmt"
	"testing"
	"time"

	"better-cdc/internal/model"

	"go.uber.org/zap"
)

func newTestPublisher() *JetStreamPublisher {
	return NewJetStreamPublisher(JetStreamOptions{}, zap.NewNop())
}

func makeItems(n int) []PublishItem {
	items := make([]PublishItem, n)
	for i := range items {
		items[i] = PublishItem{
			Subject:  "cdc.test",
			EventID:  "evt-" + string(rune('0'+i)),
			Position: model.WALPosition{LSN: "0/" + string(rune('0'+i))},
		}
	}
	return items
}

func TestWaitForAcks_AllAckedWithinTimeout(t *testing.T) {
	p := newTestPublisher()
	items := makeItems(3)

	pending := make([]*PendingAck, 3)
	for i := range pending {
		pending[i] = NewPendingAck(items[i].Subject, items[i].EventID, 1)
		pending[i].SetAcked(true)
		pending[i].Close()
	}

	result, err := p.WaitForAcks(context.Background(), pending, items, 5*time.Second)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Succeeded != 3 {
		t.Errorf("expected 3 succeeded, got %d", result.Succeeded)
	}
	if result.Failed != 0 {
		t.Errorf("expected 0 failed, got %d", result.Failed)
	}
	if !result.IsComplete() {
		t.Error("expected IsComplete() to be true")
	}
}

func TestWaitForAcks_PartialAckThenTimeout(t *testing.T) {
	p := newTestPublisher()
	items := makeItems(3)

	pending := make([]*PendingAck, 3)
	// Item 0: acked immediately
	pending[0] = NewPendingAck(items[0].Subject, items[0].EventID, 1)
	pending[0].SetAcked(true)
	pending[0].Close()

	// Item 1: acked immediately
	pending[1] = NewPendingAck(items[1].Subject, items[1].EventID, 1)
	pending[1].SetAcked(true)
	pending[1].Close()

	// Item 2: never completes (will timeout)
	pending[2] = NewPendingAck(items[2].Subject, items[2].EventID, 1)
	// don't close - simulates stuck ack

	result, err := p.WaitForAcks(context.Background(), pending, items, 50*time.Millisecond)
	if err == nil {
		t.Fatal("expected timeout error")
	}

	// The error message should reflect the accurate count (2 resolved, not 0)
	if result.Succeeded != 2 {
		t.Errorf("expected 2 succeeded, got %d", result.Succeeded)
	}
	if result.Failed != 1 {
		t.Errorf("expected 1 failed, got %d", result.Failed)
	}
}

func TestWaitForAcks_TimeoutNoneAcked(t *testing.T) {
	p := newTestPublisher()
	items := makeItems(2)

	pending := make([]*PendingAck, 2)
	for i := range pending {
		pending[i] = NewPendingAck(items[i].Subject, items[i].EventID, 1)
		// don't close - nothing resolves
	}

	result, err := p.WaitForAcks(context.Background(), pending, items, 50*time.Millisecond)
	if err == nil {
		t.Fatal("expected timeout error")
	}
	if result.Succeeded != 0 {
		t.Errorf("expected 0 succeeded, got %d", result.Succeeded)
	}
	if result.Failed != 2 {
		t.Errorf("expected 2 failed, got %d", result.Failed)
	}
}

func TestWaitForAcks_ContextCancellation(t *testing.T) {
	p := newTestPublisher()
	items := makeItems(2)

	pending := make([]*PendingAck, 2)
	// Item 0: acked
	pending[0] = NewPendingAck(items[0].Subject, items[0].EventID, 1)
	pending[0].SetAcked(true)
	pending[0].Close()

	// Item 1: never completes
	pending[1] = NewPendingAck(items[1].Subject, items[1].EventID, 1)

	ctx, cancel := context.WithCancel(context.Background())
	// Cancel after a short delay
	go func() {
		time.Sleep(30 * time.Millisecond)
		cancel()
	}()

	result, err := p.WaitForAcks(ctx, pending, items, 10*time.Second)
	if err == nil {
		t.Fatal("expected context cancellation error")
	}
	if result.Succeeded != 1 {
		t.Errorf("expected 1 succeeded, got %d", result.Succeeded)
	}
	if result.Failed != 1 {
		t.Errorf("expected 1 failed, got %d", result.Failed)
	}
}

func TestWaitForAcks_AllFailedWithErrors(t *testing.T) {
	p := newTestPublisher()
	items := makeItems(2)

	pending := make([]*PendingAck, 2)
	for i := range pending {
		pending[i] = NewPendingAck(items[i].Subject, items[i].EventID, 1)
		pending[i].SetError(fmt.Errorf("ack timeout"))
		pending[i].Close()
	}

	result, err := p.WaitForAcks(context.Background(), pending, items, 5*time.Second)
	if err == nil {
		t.Fatal("expected error")
	}
	if result.Succeeded != 0 {
		t.Errorf("expected 0 succeeded, got %d", result.Succeeded)
	}
	if result.Failed != 2 {
		t.Errorf("expected 2 failed, got %d", result.Failed)
	}
	if len(result.FailedItems) != 2 {
		t.Errorf("expected 2 failed items, got %d", len(result.FailedItems))
	}
}

func TestWaitForAcks_EmptyBatch(t *testing.T) {
	p := newTestPublisher()

	result, err := p.WaitForAcks(context.Background(), nil, nil, 5*time.Second)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Total != 0 {
		t.Errorf("expected 0 total, got %d", result.Total)
	}
}
