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
	if _, err = b.Acquire(context.Background(), -1); err == nil {
		t.Fatal("negative size accepted")
	}
}

func TestBudgetAdmitsOversizedRecordExclusively(t *testing.T) {
	b := New(10)
	if !b.Oversized(11) || b.Oversized(10) {
		t.Fatal("Oversized misreports the limit")
	}
	small, err := b.Acquire(context.Background(), 1)
	if err != nil {
		t.Fatal(err)
	}

	// The oversized record waits while anything else is held.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if _, err = b.Acquire(ctx, 25); err == nil {
		t.Fatal("oversized record admitted alongside another reservation")
	}

	small()
	big, err := b.Acquire(context.Background(), 25)
	if err != nil {
		t.Fatalf("oversized record not admitted on an empty budget: %v", err)
	}

	// While the oversized record is held, nothing else fits.
	ctx2, cancel2 := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel2()
	if _, err = b.Acquire(ctx2, 1); err == nil {
		t.Fatal("reservation admitted while an oversized record is held")
	}

	big()
	r, err := b.Acquire(context.Background(), 10)
	if err != nil {
		t.Fatalf("budget not restored after oversized release: %v", err)
	}
	r()
}
