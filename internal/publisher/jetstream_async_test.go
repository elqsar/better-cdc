package publisher

import (
	"context"
	errorspkg "errors"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

type stubPubAckFuture struct {
	okCh  chan *nats.PubAck
	errCh chan error
}

func newStubPubAckFuture() *stubPubAckFuture {
	return &stubPubAckFuture{
		okCh:  make(chan *nats.PubAck, 1),
		errCh: make(chan error, 1),
	}
}

func (s *stubPubAckFuture) Ok() <-chan *nats.PubAck { return s.okCh }

func (s *stubPubAckFuture) Err() <-chan error { return s.errCh }

func (s *stubPubAckFuture) Msg() *nats.Msg { return nil }

func TestPendingAckComplete_FirstCompletionWins(t *testing.T) {
	pend := NewPendingAck("cdc.test", "evt-1", 1)
	firstErr := errorspkg.New("first failure")

	pend.complete(false, firstErr)
	pend.complete(true, nil)

	if err := pend.Wait(context.Background()); !errorspkg.Is(err, firstErr) {
		t.Fatalf("expected first completion error, got %v", err)
	}
	if pend.IsAcked() {
		t.Fatal("expected acked state from first completion to win")
	}
}

func TestWaitForAcks_ContextCancelDoesNotPoisonPendingAck(t *testing.T) {
	p := NewJetStreamPublisher(JetStreamOptions{PublishTimeout: time.Second}, nil)
	pend := NewPendingAck("cdc.test", "evt-1", 1)
	future := newStubPubAckFuture()
	items := []PublishItem{{Subject: "cdc.test", EventID: "evt-1"}}

	go p.awaitPendingAck(pend, future, "cdc.test")

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()

	result, err := p.WaitForAcks(ctx, []*PendingAck{pend}, items, 5*time.Second)
	if !errorspkg.Is(err, context.Canceled) {
		t.Fatalf("expected context cancellation, got %v", err)
	}
	if result.Succeeded != 0 || result.Failed != 1 {
		t.Fatalf("unexpected result before ack arrives: %+v", result)
	}
	if pend.IsAcked() {
		t.Fatal("pending ack should not be marked acked before ack arrives")
	}
	if got := pend.GetErr(); got != nil {
		t.Fatalf("pending ack should still be unresolved, got error %v", got)
	}

	future.okCh <- &nats.PubAck{Stream: "CDC", Sequence: 42}

	if err := pend.Wait(context.Background()); err != nil {
		t.Fatalf("expected pending ack to succeed after late ack, got %v", err)
	}
	if !pend.IsAcked() {
		t.Fatal("expected pending ack to be marked acked after late ack")
	}
}

func TestAwaitPendingAck_PropagatesAsyncError(t *testing.T) {
	p := NewJetStreamPublisher(JetStreamOptions{PublishTimeout: time.Second}, nil)
	pend := NewPendingAck("cdc.test", "evt-1", 1)
	future := newStubPubAckFuture()
	wantErr := errorspkg.New("async publish failed")

	go p.awaitPendingAck(pend, future, "cdc.test")
	future.errCh <- wantErr

	if err := pend.Wait(context.Background()); !errorspkg.Is(err, wantErr) {
		t.Fatalf("expected async error %v, got %v", wantErr, err)
	}
	if pend.IsAcked() {
		t.Fatal("expected async error path to leave acked=false")
	}
}
