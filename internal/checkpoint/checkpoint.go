package checkpoint

import (
	"context"
	"time"

	"better-cdc/internal/model"
	"go.uber.org/zap"
)

// Store persists WAL positions to support restart recovery.
type Store interface {
	Save(ctx context.Context, pos model.WALPosition) error
	Load(ctx context.Context) (model.WALPosition, error)
}

// MemoryStore is a simple in-memory checkpoint store useful for initial wiring.
type MemoryStore struct {
	last model.WALPosition
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{}
}

func (s *MemoryStore) Save(ctx context.Context, pos model.WALPosition) error {
	_ = ctx
	s.last = pos
	return nil
}

func (s *MemoryStore) Load(ctx context.Context) (model.WALPosition, error) {
	_ = ctx
	return s.last, nil
}

// Manager coordinates periodic checkpointing tied to JetStream ack success.
type Manager struct {
	store     Store
	interval  time.Duration
	lastFlush model.WALPosition
	lastTime  time.Time
	logger    *zap.Logger
}

func NewManager(store Store, interval time.Duration, logger *zap.Logger) *Manager {
	if logger == nil {
		logger = zap.NewNop()
	}
	return &Manager{store: store, interval: interval, logger: logger}
}

func (m *Manager) MaybeFlush(ctx context.Context, pos model.WALPosition, acked bool, now time.Time) error {
	if pos.LSN == "" {
		return nil
	}
	if !acked {
		return nil
	}
	if m.lastFlush.LSN == "" || now.Sub(m.lastTime) >= m.interval {
		m.lastFlush = pos
		m.lastTime = now
		m.logger.Debug("saving checkpoint", zap.String("lsn", pos.LSN))
		return m.store.Save(ctx, pos)
	}
	return nil
}
