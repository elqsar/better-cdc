package parser

import (
	"context"
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

	if err := p.handlePGOutputMessage(context.Background(), &pglogrepl.CommitMessage{
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

	if err := p.handlePGOutputMessage(context.Background(), &pglogrepl.CommitMessage{
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

	if err := p.handlePGOutputMessage(context.Background(), &pglogrepl.BeginMessage{
		FinalLSN: beginLSN,
		Xid:      42,
	}, out); err != nil {
		t.Fatalf("begin returned error: %v", err)
	}
	if err := p.handlePGOutputMessage(context.Background(), &pglogrepl.TruncateMessage{
		RelationNum: 2,
		RelationIDs: []uint32{1, 2},
	}, out); err != nil {
		t.Fatalf("truncate returned error: %v", err)
	}
	if err := p.handlePGOutputMessage(context.Background(), &pglogrepl.CommitMessage{
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

	if err := p.handlePGOutputMessage(context.Background(), &pglogrepl.BeginMessage{
		FinalLSN: pglogrepl.LSN(0x16B3700),
		Xid:      7,
	}, out); err != nil {
		t.Fatalf("begin returned error: %v", err)
	}
	if err := p.handlePGOutputMessage(context.Background(), &pglogrepl.TruncateMessage{
		RelationNum: 2,
		RelationIDs: []uint32{1, 2},
	}, out); err != nil {
		t.Fatalf("truncate returned error: %v", err)
	}
	if err := p.handlePGOutputMessage(context.Background(), &pglogrepl.CommitMessage{
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
