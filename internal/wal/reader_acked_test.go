package wal

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"better-cdc/internal/model"
	"better-cdc/internal/parser"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
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

func TestPGReader_Stop_JoinsReplicationLoopBeforeClosingConn(t *testing.T) {
	restoreReceive, restoreSend, restoreTimeoutCheck, restoreTimeout := stubReplicationTimeoutHooks(t, time.Second)
	defer restoreReceive()
	defer restoreSend()
	defer restoreTimeoutCheck()
	defer restoreTimeout()

	origStart := startReplication
	startReplication = func(ctx context.Context, conn *pgconn.PgConn, slotName string, startLSN pglogrepl.LSN, opts pglogrepl.StartReplicationOptions) error {
		return nil
	}
	defer func() { startReplication = origStart }()

	var closeCalls atomic.Int32
	origClose := closeReplicationConn
	closeReplicationConn = func(ctx context.Context, conn *pgconn.PgConn) error {
		closeCalls.Add(1)
		return nil
	}
	defer func() { closeReplicationConn = origClose }()

	var finalStatus atomic.Int32
	sendStandbyStatusUpdate = func(ctx context.Context, conn *pgconn.PgConn, s pglogrepl.StandbyStatusUpdate) error {
		if s.ReplyRequested {
			finalStatus.Add(1)
		}
		return nil
	}

	receiveReplicationMessage = func(ctx context.Context, conn *pgconn.PgConn) (pgproto3.BackendMessage, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}

	r := NewPGReader(SlotConfig{Plugin: "pgoutput"}, 0, zap.NewNop())
	r.conn = &pgconn.PgConn{}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stream, err := r.ReadWAL(ctx, model.WALPosition{LSN: "0/42"})
	if err != nil {
		t.Fatalf("ReadWAL: %v", err)
	}

	stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer stopCancel()
	if err := r.Stop(stopCtx); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	if _, ok := <-stream; ok {
		t.Fatal("expected stream to be closed after Stop")
	}
	if got := closeCalls.Load(); got != 1 {
		t.Fatalf("expected connection closed exactly once, got %d", got)
	}
	if got := finalStatus.Load(); got != 1 {
		t.Fatalf("expected 1 final standby status update, got %d", got)
	}
	if r.conn != nil {
		t.Fatal("expected conn to be nil after Stop")
	}

	// A second Stop must return immediately without touching the closed conn.
	if err := r.Stop(stopCtx); err != nil {
		t.Fatalf("second Stop: %v", err)
	}
	if got := closeCalls.Load(); got != 1 {
		t.Fatalf("expected no additional close on second Stop, got %d", got)
	}
}

func TestPGReader_LoopPGOutput_SendsStandbyStatusOnReceiveTimeout(t *testing.T) {
	restoreReceive, restoreSend, restoreTimeoutCheck, restoreTimeout := stubReplicationTimeoutHooks(t, 5*time.Millisecond)
	defer restoreReceive()
	defer restoreSend()
	defer restoreTimeoutCheck()
	defer restoreTimeout()

	r := NewPGReader(SlotConfig{}, 0, zap.NewNop())
	r.conn = &pgconn.PgConn{}
	acked, _ := pglogrepl.ParseLSN("0/42")
	r.setAckedLSN(acked)

	var got []pglogrepl.StandbyStatusUpdate
	sendStandbyStatusUpdate = func(ctx context.Context, conn *pgconn.PgConn, s pglogrepl.StandbyStatusUpdate) error {
		got = append(got, s)
		return nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	calls := 0
	receiveReplicationMessage = func(ctx context.Context, conn *pgconn.PgConn) (pgproto3.BackendMessage, error) {
		calls++
		switch calls {
		case 1:
			<-ctx.Done()
			return nil, errors.New("timeout")
		default:
			cancel()
			return nil, context.Canceled
		}
	}

	lastLSN, err := r.loopPGOutput(ctx, 0, make(chan *parser.RawMessage))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context canceled, got %v", err)
	}
	if lastLSN != acked {
		t.Fatalf("lastLSN = %s, want %s", lastLSN, acked)
	}
	if len(got) != 1 {
		t.Fatalf("expected 1 standby status update, got %d", len(got))
	}
	if got[0].WALFlushPosition != acked {
		t.Fatalf("expected flush LSN %s, got %s", acked, got[0].WALFlushPosition)
	}
	if got[0].ReplyRequested {
		t.Fatal("expected idle heartbeat to keep ReplyRequested=false")
	}
}

func TestPGReader_LoopPGOutput_DoesNotSendStandbyStatusOnReceiveTimeoutWhenAckIsZero(t *testing.T) {
	restoreReceive, restoreSend, restoreTimeoutCheck, restoreTimeout := stubReplicationTimeoutHooks(t, 5*time.Millisecond)
	defer restoreReceive()
	defer restoreSend()
	defer restoreTimeoutCheck()
	defer restoreTimeout()

	r := NewPGReader(SlotConfig{}, 0, zap.NewNop())
	r.conn = &pgconn.PgConn{}

	var called int
	sendStandbyStatusUpdate = func(ctx context.Context, conn *pgconn.PgConn, s pglogrepl.StandbyStatusUpdate) error {
		called++
		return nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	calls := 0
	receiveReplicationMessage = func(ctx context.Context, conn *pgconn.PgConn) (pgproto3.BackendMessage, error) {
		calls++
		switch calls {
		case 1:
			<-ctx.Done()
			return nil, errors.New("timeout")
		default:
			cancel()
			return nil, context.Canceled
		}
	}

	lastLSN, err := r.loopPGOutput(ctx, 0, make(chan *parser.RawMessage))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context canceled, got %v", err)
	}
	if lastLSN != 0 {
		t.Fatalf("lastLSN = %s, want 0/0", lastLSN)
	}
	if called != 0 {
		t.Fatalf("expected 0 standby status updates, got %d", called)
	}
}

func TestPGReader_LoopPGOutput_LogsAndContinuesWhenIdleStandbyStatusFails(t *testing.T) {
	restoreReceive, restoreSend, restoreTimeoutCheck, restoreTimeout := stubReplicationTimeoutHooks(t, 5*time.Millisecond)
	defer restoreReceive()
	defer restoreSend()
	defer restoreTimeoutCheck()
	defer restoreTimeout()

	core, logs := observer.New(zap.WarnLevel)
	r := NewPGReader(SlotConfig{}, 0, zap.New(core))
	r.conn = &pgconn.PgConn{}
	acked, _ := pglogrepl.ParseLSN("0/42")
	r.setAckedLSN(acked)

	wantErr := errors.New("send failed")
	sendStandbyStatusUpdate = func(ctx context.Context, conn *pgconn.PgConn, s pglogrepl.StandbyStatusUpdate) error {
		return wantErr
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	calls := 0
	receiveReplicationMessage = func(ctx context.Context, conn *pgconn.PgConn) (pgproto3.BackendMessage, error) {
		calls++
		switch calls {
		case 1:
			<-ctx.Done()
			return nil, errors.New("timeout")
		default:
			cancel()
			return nil, context.Canceled
		}
	}

	lastLSN, err := r.loopPGOutput(ctx, 0, make(chan *parser.RawMessage))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context canceled, got %v", err)
	}
	if lastLSN != acked {
		t.Fatalf("lastLSN = %s, want %s", lastLSN, acked)
	}
	if calls != 2 {
		t.Fatalf("expected loop to continue after timeout send failure, got %d receive calls", calls)
	}
	if r.errs.Value() != 1 {
		t.Fatalf("expected replication error counter to be 1, got %d", r.errs.Value())
	}
	if logs.Len() != 1 {
		t.Fatalf("expected 1 warning log, got %d", logs.Len())
	}
	if logs.All()[0].Message != "send standby status failed" {
		t.Fatalf("unexpected warning message %q", logs.All()[0].Message)
	}
}

func stubReplicationTimeoutHooks(t *testing.T, timeout time.Duration) (restoreReceive func(), restoreSend func(), restoreTimeoutCheck func(), restoreTimeout func()) {
	t.Helper()

	origReceive := receiveReplicationMessage
	origSend := sendStandbyStatusUpdate
	origTimeoutCheck := isReplicationReceiveTimeout
	origTimeout := time.Duration(replicationStandbyTimeoutNanos.Load())
	replicationStandbyTimeoutNanos.Store(int64(timeout))
	isReplicationReceiveTimeout = func(err error) bool {
		return err != nil && err.Error() == "timeout"
	}

	return func() {
			receiveReplicationMessage = origReceive
		},
		func() {
			sendStandbyStatusUpdate = origSend
		},
		func() {
			isReplicationReceiveTimeout = origTimeoutCheck
		},
		func() {
			replicationStandbyTimeoutNanos.Store(int64(origTimeout))
		}
}
