package checkpoint

import (
	"context"
	"testing"
	"time"

	"better-cdc/internal/model"

	"go.uber.org/zap"
)

type mockStore struct {
	saved []model.WALPosition
}

func (m *mockStore) Save(_ context.Context, pos model.WALPosition) error {
	m.saved = append(m.saved, pos)
	return nil
}

func (m *mockStore) Load(context.Context) (model.WALPosition, error) {
	return model.WALPosition{}, nil
}

func TestManagerFlushPendingPersistsLatestAckedPosition(t *testing.T) {
	store := &mockStore{}
	mgr := NewManager(store, time.Hour, zap.NewNop())

	now := time.Now()
	if err := mgr.MaybeFlush(context.Background(), model.WALPosition{LSN: "0/10"}, true, now); err != nil {
		t.Fatalf("MaybeFlush: %v", err)
	}
	if len(store.saved) != 1 {
		t.Fatalf("expected initial flush, got %d saves", len(store.saved))
	}

	if err := mgr.MaybeFlush(context.Background(), model.WALPosition{LSN: "0/20"}, true, now.Add(10*time.Second)); err != nil {
		t.Fatalf("MaybeFlush: %v", err)
	}
	if len(store.saved) != 1 {
		t.Fatalf("expected no interval flush, got %d saves", len(store.saved))
	}

	if err := mgr.FlushPending(context.Background(), now.Add(11*time.Second)); err != nil {
		t.Fatalf("FlushPending: %v", err)
	}
	if len(store.saved) != 2 {
		t.Fatalf("expected forced flush, got %d saves", len(store.saved))
	}
	if got := store.saved[1].LSN; got != "0/20" {
		t.Fatalf("expected latest LSN 0/20, got %s", got)
	}
}
