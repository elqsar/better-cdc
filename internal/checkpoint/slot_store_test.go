package checkpoint

import (
	"errors"
	"testing"
)

func TestPositionFromSlotRow_ReturnsSlotNotFound(t *testing.T) {
	_, err := positionFromSlotRow(false, "", "missing_slot")
	if err == nil {
		t.Fatal("expected error for missing slot")
	}
	if !errors.Is(err, ErrSlotNotFound) {
		t.Fatalf("expected ErrSlotNotFound, got %v", err)
	}
}

func TestPositionFromSlotRow_AllowsEmptyLSNForExistingSlot(t *testing.T) {
	pos, err := positionFromSlotRow(true, "", "existing_slot")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if pos.LSN != "" {
		t.Fatalf("expected empty LSN, got %q", pos.LSN)
	}
}

func TestPositionFromSlotRow_ReturnsExistingLSN(t *testing.T) {
	pos, err := positionFromSlotRow(true, "0/123", "existing_slot")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if pos.LSN != "0/123" {
		t.Fatalf("expected LSN 0/123, got %q", pos.LSN)
	}
}
