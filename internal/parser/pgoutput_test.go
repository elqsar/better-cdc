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
