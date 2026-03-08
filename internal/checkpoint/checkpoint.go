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

// Manager coordinates periodic checkpointing tied to JetStream ack success.
type Manager struct {
	store     Store
	interval  time.Duration
	lastAcked model.WALPosition
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

// Init seeds the manager with an already-durable checkpoint (e.g. loaded from Store on startup).
func (m *Manager) Init(pos model.WALPosition, now time.Time) {
	if pos.LSN == "" {
		return
	}
	m.lastAcked = pos
	m.lastFlush = pos
	m.lastTime = now
}

func (m *Manager) LastFlushed() model.WALPosition {
	return m.lastFlush
}

func (m *Manager) MaybeFlush(ctx context.Context, pos model.WALPosition, acked bool, now time.Time) error {
	if pos.LSN == "" {
		return nil
	}
	if !acked {
		return nil
	}
	m.lastAcked = pos
	if m.lastFlush.LSN == "" || now.Sub(m.lastTime) >= m.interval {
		return m.flush(ctx, pos, now)
	}
	return nil
}

// FlushPending persists the most recent durable position regardless of interval.
func (m *Manager) FlushPending(ctx context.Context, now time.Time) error {
	if m.lastAcked.LSN == "" || m.lastAcked.LSN == m.lastFlush.LSN {
		return nil
	}
	return m.flush(ctx, m.lastAcked, now)
}

func (m *Manager) flush(ctx context.Context, pos model.WALPosition, now time.Time) error {
	m.logger.Debug("saving checkpoint", zap.String("lsn", pos.LSN))
	if err := m.store.Save(ctx, pos); err != nil {
		return err
	}
	m.lastFlush = pos
	m.lastTime = now
	return nil
}
