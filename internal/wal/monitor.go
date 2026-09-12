package wal

import (
	"context"
	"fmt"
	"sync"
	"time"

	"better-cdc/internal/metrics"
	"github.com/jackc/pgx/v5"
)

// Preflight validates existing resources. It never creates, advances or drops a slot.
func Preflight(ctx context.Context, cfg SlotConfig) error {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close(ctx) }()
	var plugin, database, slotType, status string
	err = conn.QueryRow(ctx, "SELECT plugin, database, slot_type, coalesce(wal_status,'') FROM pg_replication_slots WHERE slot_name=$1", cfg.SlotName).Scan(&plugin, &database, &slotType, &status)
	if err != nil {
		return fmt.Errorf("read existing slot %q: %w (provision the slot explicitly)", cfg.SlotName, err)
	}
	expected := cfg.Plugin
	if expected == "" {
		expected = "pgoutput"
	}
	if plugin != expected {
		return fmt.Errorf("slot %q uses %s, configured decoder is %s; keep its decoder or provision a new slot with an explicit bootstrap/recovery plan", cfg.SlotName, plugin, expected)
	}
	var currentDB string
	if err = conn.QueryRow(ctx, "SELECT current_database()").Scan(&currentDB); err != nil {
		return err
	}
	if database != currentDB || slotType != "logical" || status == "lost" {
		return fmt.Errorf("slot database/type/WAL state is not usable; explicit operator recovery required")
	}
	if expected == "pgoutput" {
		for _, pub := range cfg.Publications {
			var exists bool
			if err = conn.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM pg_publication WHERE pubname=$1)", pub).Scan(&exists); err != nil {
				return err
			}
			if !exists {
				return fmt.Errorf("publication %q does not exist", pub)
			}
		}
	}
	return nil
}

type Monitor struct {
	url, slot string
	reader    *PGReader
	mu        sync.RWMutex
	err       error
	checked   time.Time
}

func NewMonitor(url, slot string, r *PGReader) *Monitor {
	return &Monitor{url: url, slot: slot, reader: r, err: fmt.Errorf("slot monitor starting")}
}
func (m *Monitor) Ready(context.Context) error {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if time.Since(m.checked) > 30*time.Second {
		return fmt.Errorf("slot monitor stale")
	}
	return m.err
}
func (m *Monitor) Run(ctx context.Context) {
	timer := time.NewTicker(5 * time.Second)
	defer timer.Stop()
	var conn *pgx.Conn
	defer func() {
		if conn != nil {
			closeCtx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			_ = conn.Close(closeCtx)
		}
	}()
	for {
		pollCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		var err error
		if conn == nil {
			conn, err = pgx.Connect(pollCtx, m.url)
		}
		if err == nil {
			var retained, safe int64
			var active bool
			var status string
			err = conn.QueryRow(pollCtx, `SELECT coalesce(pg_wal_lsn_diff(pg_current_wal_lsn(),restart_lsn),0)::bigint, coalesce(safe_wal_size,-1), active, coalesce(wal_status,'') FROM pg_replication_slots WHERE slot_name=$1`, m.slot).Scan(&retained, &safe, &active, &status)
			if err == nil {
				metrics.Pilot.RetainedWAL.Set(retained)
				metrics.Pilot.SafeWAL.Set(safe)
				metrics.Pilot.SlotActive.Set(boolInt(active))
				metrics.Pilot.SlotLost.Set(boolInt(status == "lost" || status == "unreserved"))
				if status == "lost" {
					err = fmt.Errorf("slot lost required WAL; operator recovery required")
				}
			}
		}
		if err != nil && conn != nil {
			_ = conn.Close(pollCtx)
			conn = nil
		}
		cancel()
		received, acked, lastReceive, lastAck := m.reader.Progress()
		metrics.Pilot.ReceivedLSN.Set(int64(received))
		metrics.Pilot.AckedLSN.Set(int64(acked))
		metrics.Pilot.LastReceive.Set(lastReceive)
		metrics.Pilot.LastAck.Set(lastAck)
		m.mu.Lock()
		m.err = err
		m.checked = time.Now()
		m.mu.Unlock()
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}
	}
}
func boolInt(v bool) int64 {
	if v {
		return 1
	}
	return 0
}
