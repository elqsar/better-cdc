package parser

import (
	"context"
	"strings"
	"testing"

	"github.com/jackc/pglogrepl"
	"go.uber.org/zap"
)

func TestWal2JSONParser_ParseFatalOnDecodeError(t *testing.T) {
	p := NewWal2JSONParser(Wal2JSONConfig{
		Logger:     zap.NewNop(),
		BufferSize: 1,
	})

	in := make(chan *RawMessage, 1)
	in <- &RawMessage{
		Plugin:   PluginWal2JSON,
		WALStart: pglogrepl.LSN(0x16B3748),
		Data:     []byte("{not-json"),
	}
	close(in)

	out, err := p.Parse(context.Background(), in)
	if err != nil {
		t.Fatalf("Parse returned error: %v", err)
	}

	for evt := range out {
		t.Fatalf("unexpected event emitted on decode failure: %+v", evt)
	}

	parseErr := p.Err()
	if parseErr == nil {
		t.Fatal("expected fatal parser error, got nil")
	}
	if !strings.Contains(parseErr.Error(), "decode wal2json failed") {
		t.Fatalf("unexpected parser error: %v", parseErr)
	}
}
