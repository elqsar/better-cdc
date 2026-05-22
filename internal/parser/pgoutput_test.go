package parser

import (
	"context"
	"encoding/binary"
	"errors"
	"strings"
	"testing"
	"time"

	"better-cdc/internal/model"

	"github.com/jackc/pglogrepl"
	"go.uber.org/zap"
)

func TestPGOutputParser_CommitIncludesPosition(t *testing.T) {
	p := NewPGOutputParser(PGOutputConfig{
		Logger:     zap.NewNop(),
		BufferSize: 1,
	})
	p.tx = &txBuffer{xid: 42}

	commitLSN := pglogrepl.LSN(0x16B3748)
	commitTime := time.Now().UTC()
	out := make(chan *model.WALEvent, 1)

	if err := p.handlePGOutputMessage(context.Background(), nil, &pglogrepl.CommitMessage{
		CommitLSN:  commitLSN,
		CommitTime: commitTime,
	}, out); err != nil {
		t.Fatalf("handlePGOutputMessage returned error: %v", err)
	}

	select {
	case evt := <-out:
		if evt == nil {
			t.Fatal("expected commit event, got nil")
		}
		if !evt.Commit {
			t.Fatalf("expected commit event, got %+v", evt)
		}
		if evt.Position.LSN != commitLSN.String() {
			t.Fatalf("unexpected commit position: got %q want %q", evt.Position.LSN, commitLSN.String())
		}
		if evt.LSN != commitLSN.String() {
			t.Fatalf("unexpected commit LSN: got %q want %q", evt.LSN, commitLSN.String())
		}
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for commit event")
	}
}

func TestPGOutputParser_CommitUsesTransactionEndLSNForCheckpointPosition(t *testing.T) {
	p := NewPGOutputParser(PGOutputConfig{
		Logger:     zap.NewNop(),
		BufferSize: 1,
	})
	p.tx = &txBuffer{xid: 42}

	commitLSN := pglogrepl.LSN(0x16B3748)
	endLSN := pglogrepl.LSN(0x16B3750)
	out := make(chan *model.WALEvent, 1)

	if err := p.handlePGOutputMessage(context.Background(), nil, &pglogrepl.CommitMessage{
		CommitLSN:         commitLSN,
		TransactionEndLSN: endLSN,
	}, out); err != nil {
		t.Fatalf("handlePGOutputMessage returned error: %v", err)
	}

	select {
	case evt := <-out:
		if evt == nil {
			t.Fatal("expected commit event, got nil")
		}
		if evt.Position.LSN != endLSN.String() {
			t.Fatalf("unexpected checkpoint position: got %q want %q", evt.Position.LSN, endLSN.String())
		}
		if evt.LSN != commitLSN.String() {
			t.Fatalf("unexpected commit lsn: got %q want %q", evt.LSN, commitLSN.String())
		}
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for commit event")
	}
}

func TestPGOutputParser_ParseFatalOnParseError(t *testing.T) {
	p := NewPGOutputParser(PGOutputConfig{
		Logger:     zap.NewNop(),
		BufferSize: 1,
	})

	in := make(chan *RawMessage, 1)
	in <- &RawMessage{
		Plugin:   PluginPGOutput,
		WALStart: pglogrepl.LSN(0x16B3748),
		Data:     []byte{0x00},
	}
	close(in)

	out, err := p.Parse(context.Background(), in)
	if err != nil {
		t.Fatalf("Parse returned error: %v", err)
	}

	for evt := range out {
		t.Fatalf("unexpected event emitted on parse failure: %+v", evt)
	}

	parseErr := p.Err()
	if parseErr == nil {
		t.Fatal("expected fatal parser error, got nil")
	}
	if !strings.Contains(parseErr.Error(), "parse pgoutput message failed") {
		t.Fatalf("unexpected parser error: %v", parseErr)
	}
}

func TestPGOutputParser_TruncateEmitsDDLEventPerRelationOnCommit(t *testing.T) {
	p := NewPGOutputParser(PGOutputConfig{
		Logger:     zap.NewNop(),
		BufferSize: 4,
	})
	p.relations[1] = relationInfo{ID: 1, Schema: "public", Table: "orders"}
	p.relations[2] = relationInfo{ID: 2, Schema: "public", Table: "accounts"}

	out := make(chan *model.WALEvent, 4)
	beginLSN := pglogrepl.LSN(0x16B3700)
	commitLSN := pglogrepl.LSN(0x16B3748)
	commitTime := time.Now().UTC()

	if err := p.handlePGOutputMessage(context.Background(), nil, &pglogrepl.BeginMessage{
		FinalLSN: beginLSN,
		Xid:      42,
	}, out); err != nil {
		t.Fatalf("begin returned error: %v", err)
	}
	if err := p.handlePGOutputMessage(context.Background(), nil, &pglogrepl.TruncateMessage{
		RelationNum: 2,
		RelationIDs: []uint32{1, 2},
	}, out); err != nil {
		t.Fatalf("truncate returned error: %v", err)
	}
	if err := p.handlePGOutputMessage(context.Background(), nil, &pglogrepl.CommitMessage{
		CommitLSN:  commitLSN,
		CommitTime: commitTime,
	}, out); err != nil {
		t.Fatalf("commit returned error: %v", err)
	}

	begin := <-out
	firstDDL := <-out
	secondDDL := <-out
	commit := <-out

	if !begin.Begin {
		t.Fatalf("expected begin event, got %+v", begin)
	}

	ddlEvents := []*model.WALEvent{firstDDL, secondDDL}
	wantTables := []string{"orders", "accounts"}
	for i, evt := range ddlEvents {
		if evt.Operation != model.OperationDDL {
			t.Fatalf("ddl event %d operation = %q, want %q", i, evt.Operation, model.OperationDDL)
		}
		if evt.Schema != "public" {
			t.Fatalf("ddl event %d schema = %q, want public", i, evt.Schema)
		}
		if evt.Table != wantTables[i] {
			t.Fatalf("ddl event %d table = %q, want %q", i, evt.Table, wantTables[i])
		}
		if evt.LSN != commitLSN.String() {
			t.Fatalf("ddl event %d lsn = %q, want %q", i, evt.LSN, commitLSN.String())
		}
		if evt.Position.LSN != commitLSN.String() {
			t.Fatalf("ddl event %d position = %q, want %q", i, evt.Position.LSN, commitLSN.String())
		}
		if evt.TxID != 42 {
			t.Fatalf("ddl event %d txid = %d, want 42", i, evt.TxID)
		}
		if !evt.CommitTime.Equal(commitTime) {
			t.Fatalf("ddl event %d commit time = %v, want %v", i, evt.CommitTime, commitTime)
		}
	}

	if !commit.Commit {
		t.Fatalf("expected commit event, got %+v", commit)
	}
}

func TestPGOutputParser_TruncateRespectsTableFilter(t *testing.T) {
	p := NewPGOutputParser(PGOutputConfig{
		Logger:      zap.NewNop(),
		BufferSize:  4,
		TableFilter: map[string]struct{}{"public.orders": {}},
	})
	p.relations[1] = relationInfo{ID: 1, Schema: "public", Table: "orders"}
	p.relations[2] = relationInfo{ID: 2, Schema: "public", Table: "accounts"}

	out := make(chan *model.WALEvent, 3)

	if err := p.handlePGOutputMessage(context.Background(), nil, &pglogrepl.BeginMessage{
		FinalLSN: pglogrepl.LSN(0x16B3700),
		Xid:      7,
	}, out); err != nil {
		t.Fatalf("begin returned error: %v", err)
	}
	if err := p.handlePGOutputMessage(context.Background(), nil, &pglogrepl.TruncateMessage{
		RelationNum: 2,
		RelationIDs: []uint32{1, 2},
	}, out); err != nil {
		t.Fatalf("truncate returned error: %v", err)
	}
	if err := p.handlePGOutputMessage(context.Background(), nil, &pglogrepl.CommitMessage{
		CommitLSN: pglogrepl.LSN(0x16B3748),
	}, out); err != nil {
		t.Fatalf("commit returned error: %v", err)
	}

	<-out // begin
	ddl := <-out
	commit := <-out

	if ddl.Table != "orders" {
		t.Fatalf("unexpected ddl table %q", ddl.Table)
	}
	if ddl.Operation != model.OperationDDL {
		t.Fatalf("unexpected ddl op %q", ddl.Operation)
	}
	if !commit.Commit {
		t.Fatalf("expected commit event, got %+v", commit)
	}

	select {
	case evt := <-out:
		t.Fatalf("unexpected extra event: %+v", evt)
	default:
	}
}

func TestPGOutputParser_OverflowSpillsUntilCommitAndPreservesMetadata(t *testing.T) {
	p := NewPGOutputParser(PGOutputConfig{
		Logger:          zap.NewNop(),
		BufferSize:      8,
		MaxTxBufferSize: 1,
	})
	p.relations[1] = relationInfo{
		ID:          1,
		Schema:      "public",
		Table:       "orders",
		Columns:     []string{"id"},
		ColumnTypes: []uint32{25},
	}

	out := make(chan *model.WALEvent, 8)
	beginLSN := pglogrepl.LSN(0x16B3700)
	commitLSN := pglogrepl.LSN(0x16B3748)
	endLSN := pglogrepl.LSN(0x16B3750)
	commitTime := time.Now().UTC()

	if err := p.handlePGOutputMessage(context.Background(), nil, &pglogrepl.BeginMessage{
		FinalLSN: beginLSN,
		Xid:      42,
	}, out); err != nil {
		t.Fatalf("begin returned error: %v", err)
	}

	firstInsert := &pglogrepl.InsertMessage{
		RelationID: 1,
		Tuple: &pglogrepl.TupleData{
			Columns: []*pglogrepl.TupleDataColumn{
				{DataType: 't', Data: []byte("one")},
			},
		},
	}
	if err := p.handlePGOutputMessage(context.Background(), encodeInsertMessage(1, "one"), firstInsert, out); err != nil {
		t.Fatalf("first insert returned error: %v", err)
	}

	secondInsert := &pglogrepl.InsertMessage{
		RelationID: 1,
		Tuple: &pglogrepl.TupleData{
			Columns: []*pglogrepl.TupleDataColumn{
				{DataType: 't', Data: []byte("two")},
			},
		},
	}
	if err := p.handlePGOutputMessage(context.Background(), encodeInsertMessage(1, "two"), secondInsert, out); err != nil {
		t.Fatalf("second insert returned error: %v", err)
	}

	if got := len(out); got != 1 {
		t.Fatalf("expected only begin event before commit, got %d queued events", got)
	}

	if err := p.handlePGOutputMessage(context.Background(), nil, &pglogrepl.CommitMessage{
		CommitLSN:         commitLSN,
		TransactionEndLSN: endLSN,
		CommitTime:        commitTime,
	}, out); err != nil {
		t.Fatalf("commit returned error: %v", err)
	}

	begin := <-out
	first := <-out
	second := <-out
	commit := <-out

	if !begin.Begin {
		t.Fatalf("expected begin event, got %+v", begin)
	}

	assertSpilledInsert := func(name string, evt *model.WALEvent, wantValue string) {
		t.Helper()
		if evt.Operation != model.OperationInsert {
			t.Fatalf("%s operation = %q, want %q", name, evt.Operation, model.OperationInsert)
		}
		if evt.NewValues["id"] != wantValue {
			t.Fatalf("%s new value = %#v, want %q", name, evt.NewValues["id"], wantValue)
		}
		if evt.LSN != commitLSN.String() {
			t.Fatalf("%s lsn = %q, want %q", name, evt.LSN, commitLSN.String())
		}
		if evt.Position.LSN != endLSN.String() {
			t.Fatalf("%s position = %q, want %q", name, evt.Position.LSN, endLSN.String())
		}
		if !evt.CommitTime.Equal(commitTime) {
			t.Fatalf("%s commit time = %v, want %v", name, evt.CommitTime, commitTime)
		}
		if !evt.Timestamp.Equal(commitTime) {
			t.Fatalf("%s timestamp = %v, want %v", name, evt.Timestamp, commitTime)
		}
		if evt.TxID != 42 {
			t.Fatalf("%s txid = %d, want 42", name, evt.TxID)
		}
	}

	assertSpilledInsert("first", first, "one")
	assertSpilledInsert("second", second, "two")

	if !commit.Commit {
		t.Fatalf("expected commit event, got %+v", commit)
	}
	if commit.Position.LSN != endLSN.String() {
		t.Fatalf("commit position = %q, want %q", commit.Position.LSN, endLSN.String())
	}
	if p.tx != nil {
		t.Fatalf("expected transaction state to be cleared after commit, got %+v", p.tx)
	}
}

func TestPGOutputParser_CleanupDoesNotReleaseEmittedTransactionEvents(t *testing.T) {
	p := NewPGOutputParser(PGOutputConfig{
		Logger:     zap.NewNop(),
		BufferSize: 1,
	})

	first := model.AcquireWALEvent()
	first.Schema = "public"
	first.Table = "orders"
	first.Operation = model.OperationInsert
	second := model.AcquireWALEvent()
	second.Schema = "public"
	second.Table = "accounts"
	second.Operation = model.OperationInsert

	p.tx = &txBuffer{
		xid:    42,
		events: []*model.WALEvent{first, second},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	out := make(chan *model.WALEvent, 1)
	errCh := make(chan error, 1)
	go func() {
		errCh <- p.handlePGOutputMessage(ctx, nil, &pglogrepl.CommitMessage{
			CommitLSN: pglogrepl.LSN(0x16B3748),
		}, out)
	}()

	deadline := time.After(1 * time.Second)
	for len(out) == 0 {
		select {
		case <-deadline:
			t.Fatal("timed out waiting for first event to be emitted")
		default:
			time.Sleep(time.Millisecond)
		}
	}

	cancel()
	select {
	case err := <-errCh:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled, got %v", err)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for commit handling to stop")
	}

	p.cleanupTx()

	emitted := <-out
	if emitted != first {
		t.Fatalf("expected first event to be emitted, got %+v", emitted)
	}
	if emitted.Table != "orders" || emitted.Schema != "public" || emitted.Operation != model.OperationInsert {
		t.Fatalf("emitted event was reset during cleanup: %+v", emitted)
	}
	if second.Table != "" || second.Schema != "" || second.Operation != "" {
		t.Fatalf("expected unsent event to be released during cleanup, got %+v", second)
	}

	model.ReleaseWALEvent(emitted)
}

func encodeInsertMessage(relID uint32, values ...string) []byte {
	msg := make([]byte, 0, 1+4+1+2+len(values)*8)
	msg = append(msg, byte(pglogrepl.MessageTypeInsert))

	var rel [4]byte
	binary.BigEndian.PutUint32(rel[:], relID)
	msg = append(msg, rel[:]...)
	msg = append(msg, 'N')

	var cols [2]byte
	binary.BigEndian.PutUint16(cols[:], uint16(len(values)))
	msg = append(msg, cols[:]...)

	for _, value := range values {
		msg = append(msg, 't')
		var size [4]byte
		binary.BigEndian.PutUint32(size[:], uint32(len(value)))
		msg = append(msg, size[:]...)
		msg = append(msg, value...)
	}

	return msg
}
