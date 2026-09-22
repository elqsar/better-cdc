package wal

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/elqsar/better-cdc/internal/model"
	"github.com/elqsar/better-cdc/internal/parser"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestFeedbackDuringBackpressure(t *testing.T) {
	original := sendStandbyStatusUpdate
	defer func() { sendStandbyStatusUpdate = original }()
	for _, mode := range []string{"budget", "channel"} {
		t.Run(mode, func(t *testing.T) {
			r := NewPGReader(SlotConfig{MaxBufferBytes: 128}, 0, nil)
			if err := r.SetAckedPosition(model.WALPosition{LSN: "0/42"}); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			calls := 0
			sendStandbyStatusUpdate = func(_ context.Context, _ *pgconn.PgConn, status pglogrepl.StandbyStatusUpdate) error {
				calls++
				if status.WALFlushPosition.String() != "0/42" || status.WALWritePosition != status.WALFlushPosition || status.WALApplyPosition != status.WALFlushPosition {
					t.Fatalf("unsafe feedback: %+v", status)
				}
				if calls == 3 {
					cancel()
				}
				return nil
			}
			deadline := time.Now().Add(time.Millisecond)
			var err error
			if mode == "budget" {
				release, e := r.rawBudget.Acquire(ctx, 128)
				if e != nil {
					t.Fatal(e)
				}
				defer release()
				_, err = r.acquireRaw(ctx, 1, time.Millisecond, &deadline)
			} else {
				err = r.deliverRaw(ctx, make(chan *parser.RawMessage), &parser.RawMessage{}, time.Millisecond, &deadline)
			}
			if !errors.Is(err, context.Canceled) || calls != 3 {
				t.Fatalf("calls=%d err=%v", calls, err)
			}
		})
	}
}

func TestBackpressureFeedbackFailureReconnects(t *testing.T) {
	original := sendStandbyStatusUpdate
	defer func() { sendStandbyStatusUpdate = original }()
	failure := errors.New("transport closed")
	sendStandbyStatusUpdate = func(context.Context, *pgconn.PgConn, pglogrepl.StandbyStatusUpdate) error { return failure }
	r := NewPGReader(SlotConfig{}, 0, nil)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	deadline := time.Now().Add(time.Millisecond)
	err := r.deliverRaw(ctx, make(chan *parser.RawMessage), &parser.RawMessage{}, time.Millisecond, &deadline)
	if !errors.Is(err, failure) || isFatalReplicationError(err) {
		t.Fatalf("must reconnect: %v", err)
	}
}
