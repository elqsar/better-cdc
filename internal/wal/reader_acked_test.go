package wal

import (
	"context"
	"testing"

	"better-cdc/internal/model"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"go.uber.org/zap"
)

func TestPGReader_SetAckedPosition_Monotonic(t *testing.T) {
	r := NewPGReader(SlotConfig{}, 0, zap.NewNop())

	if err := r.SetAckedPosition(model.WALPosition{LSN: "0/10"}); err != nil {
		t.Fatalf("SetAckedPosition: %v", err)
	}
	if err := r.SetAckedPosition(model.WALPosition{LSN: "0/20"}); err != nil {
		t.Fatalf("SetAckedPosition: %v", err)
	}
	if err := r.SetAckedPosition(model.WALPosition{LSN: "0/15"}); err != nil {
		t.Fatalf("SetAckedPosition: %v", err)
	}

	want, _ := pglogrepl.ParseLSN("0/20")
	if got := r.currentAckedLSN(); got != want {
		t.Fatalf("acked LSN mismatch: got %s want %s", got, want)
	}
}

func TestPGReader_sendStandbyStatus_UsesAckedLSN(t *testing.T) {
	r := NewPGReader(SlotConfig{}, 0, zap.NewNop())
	acked, _ := pglogrepl.ParseLSN("0/42")
	r.setAckedLSN(acked)

	var (
		called int
		got    pglogrepl.StandbyStatusUpdate
	)

	orig := sendStandbyStatusUpdate
	sendStandbyStatusUpdate = func(ctx context.Context, conn *pgconn.PgConn, s pglogrepl.StandbyStatusUpdate) error {
		called++
		got = s
		return nil
	}
	defer func() { sendStandbyStatusUpdate = orig }()

	if err := r.sendStandbyStatus(context.Background(), false); err != nil {
		t.Fatalf("sendStandbyStatus: %v", err)
	}
	if called != 1 {
		t.Fatalf("expected 1 call, got %d", called)
	}
	if got.WALWritePosition != acked || got.WALFlushPosition != acked || got.WALApplyPosition != acked {
		t.Fatalf("status update LSN mismatch: got write=%s flush=%s apply=%s want %s", got.WALWritePosition, got.WALFlushPosition, got.WALApplyPosition, acked)
	}
	if got.ReplyRequested {
		t.Fatalf("expected ReplyRequested=false")
	}
}

func TestPGReader_sendStandbyStatus_ReplyRequestedSendsEvenWhenZero(t *testing.T) {
	r := NewPGReader(SlotConfig{}, 0, zap.NewNop())

	var (
		called int
		got    pglogrepl.StandbyStatusUpdate
	)

	orig := sendStandbyStatusUpdate
	sendStandbyStatusUpdate = func(ctx context.Context, conn *pgconn.PgConn, s pglogrepl.StandbyStatusUpdate) error {
		called++
		got = s
		return nil
	}
	defer func() { sendStandbyStatusUpdate = orig }()

	if err := r.sendStandbyStatus(context.Background(), false); err != nil {
		t.Fatalf("sendStandbyStatus: %v", err)
	}
	if called != 0 {
		t.Fatalf("expected 0 calls, got %d", called)
	}

	if err := r.sendStandbyStatus(context.Background(), true); err != nil {
		t.Fatalf("sendStandbyStatus: %v", err)
	}
	if called != 1 {
		t.Fatalf("expected 1 call, got %d", called)
	}
	if got.WALWritePosition != 0 || got.WALFlushPosition != 0 || got.WALApplyPosition != 0 {
		t.Fatalf("expected zero LSNs, got write=%s flush=%s apply=%s", got.WALWritePosition, got.WALFlushPosition, got.WALApplyPosition)
	}
	if !got.ReplyRequested {
		t.Fatalf("expected ReplyRequested=true")
	}
}

func TestPGReader_Stop_SendsFinalStandbyStatusWithCanceledContext(t *testing.T) {
	r := NewPGReader(SlotConfig{}, 0, zap.NewNop())
	r.conn = &pgconn.PgConn{}
	acked, _ := pglogrepl.ParseLSN("0/42")
	r.setAckedLSN(acked)

	var (
		called int
		got    pglogrepl.StandbyStatusUpdate
	)

	origSend := sendStandbyStatusUpdate
	sendStandbyStatusUpdate = func(ctx context.Context, conn *pgconn.PgConn, s pglogrepl.StandbyStatusUpdate) error {
		called++
		got = s
		return nil
	}
	defer func() { sendStandbyStatusUpdate = origSend }()

	origClose := closeReplicationConn
	closeReplicationConn = func(ctx context.Context, conn *pgconn.PgConn) error { return nil }
	defer func() { closeReplicationConn = origClose }()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := r.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if called != 1 {
		t.Fatalf("expected 1 standby status update, got %d", called)
	}
	if got.WALFlushPosition != acked {
		t.Fatalf("expected flush LSN %s, got %s", acked, got.WALFlushPosition)
	}
	if !got.ReplyRequested {
		t.Fatal("expected final standby status to request reply")
	}
}
