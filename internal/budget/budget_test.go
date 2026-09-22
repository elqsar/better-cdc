package budget

import (
	"context"
	"testing"
	"time"
)

func TestBudgetBackpressureCancellationAndRelease(t *testing.T) {
	b := New(10)
	release, err := b.Acquire(context.Background(), 10)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if _, err = b.Acquire(ctx, 1); err == nil {
		t.Fatal("failed to apply backpressure")
	}
	release()
	release()
	r, err := b.Acquire(context.Background(), 10)
	if err != nil {
		t.Fatal(err)
	}
	r()
	if _, err = b.Acquire(context.Background(), 11); err == nil {
		t.Fatal("oversized record accepted")
	}
}
