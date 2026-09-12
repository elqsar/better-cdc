package wal

import (
	"better-cdc/internal/model"
	"better-cdc/internal/parser"
	"context"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"testing"
)

func TestMalformedFrameCannotBeSkipped(t *testing.T) {
	original := receiveReplicationMessage
	defer func() { receiveReplicationMessage = original }()
	for _, data := range [][]byte{nil, {'w', 1}, {'k', 1}, {'?'}} {
		receiveReplicationMessage = func(context.Context, *pgconn.PgConn) (pgproto3.BackendMessage, error) {
			return &pgproto3.CopyData{Data: data}, nil
		}
		r := NewPGReader(SlotConfig{}, 1, nil)
		_ = r.SetAckedPosition(model.WALPosition{LSN: "0/42"})
		pos, err := r.loopPGOutput(context.Background(), 0, make(chan *parser.RawMessage, 1))
		if !isFatalReplicationError(err) || pos.String() != "0/42" {
			t.Fatalf("malformed frame advanced or ignored: %s %v", pos, err)
		}
	}
}
func TestStreamingSQLStateClassification(t *testing.T) {
	original := receiveReplicationMessage
	defer func() { receiveReplicationMessage = original }()
	for _, tc := range []struct {
		code  string
		fatal bool
	}{{"57P01", false}, {"28000", true}, {"42501", true}, {"42704", true}} {
		receiveReplicationMessage = func(context.Context, *pgconn.PgConn) (pgproto3.BackendMessage, error) {
			return &pgproto3.ErrorResponse{Code: tc.code, Message: "test"}, nil
		}
		r := NewPGReader(SlotConfig{}, 0, nil)
		_, err := r.loopPGOutput(context.Background(), 0, make(chan *parser.RawMessage))
		if isFatalReplicationError(err) != tc.fatal {
			t.Fatalf("SQLSTATE %s: %v", tc.code, err)
		}
	}
}

func TestReplicationCompletionReconnectsFromAckedCommit(t *testing.T) {
	original := receiveReplicationMessage
	defer func() { receiveReplicationMessage = original }()
	receiveReplicationMessage = func(context.Context, *pgconn.PgConn) (pgproto3.BackendMessage, error) {
		return &pgproto3.CommandComplete{}, nil
	}
	r := NewPGReader(SlotConfig{}, 0, nil)
	if err := r.SetAckedPosition(model.WALPosition{LSN: "0/42"}); err != nil {
		t.Fatal(err)
	}
	pos, err := r.loopPGOutput(context.Background(), 0, make(chan *parser.RawMessage))
	if err == nil || isFatalReplicationError(err) || pos.String() != "0/42" {
		t.Fatalf("completion must reconnect at acknowledged commit: %s %v", pos, err)
	}
}
