package parser

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

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

func TestPgTime_UnmarshalJSON(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr bool
		isZero  bool
	}{
		{name: "bare null", input: `null`, isZero: true},
		{name: "quoted null", input: `"null"`, isZero: true},
		{name: "empty string", input: `""`, isZero: true},
		{name: "rfc3339", input: `"2024-01-15T10:30:00Z"`, isZero: false},
		{name: "pg format", input: `"2024-01-15 10:30:00.123456-05:00"`, isZero: false},
		{name: "pg format short tz", input: `"2024-01-15 10:30:00-05"`, isZero: false},
		{name: "invalid", input: `"not-a-date"`, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var v pgTime
			err := json.Unmarshal([]byte(tt.input), &v)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tt.isZero && !v.Time.IsZero() {
				t.Fatalf("expected zero time, got %v", v.Time)
			}
			if !tt.isZero && v.Time.IsZero() {
				t.Fatal("expected non-zero time, got zero")
			}
		})
	}
}

func TestPgTime_InStruct(t *testing.T) {
	// Verify pgTime works correctly embedded in the wal2json message struct
	// when the timestamp field is JSON null.
	raw := `{"action":"B","xid":123,"timestamp":null}`
	var msg wal2JSONMessageV2
	if err := json.Unmarshal([]byte(raw), &msg); err != nil {
		t.Fatalf("unmarshal with null timestamp: %v", err)
	}
	if !msg.Timestamp.Time.IsZero() {
		t.Fatalf("expected zero time for null timestamp, got %v", msg.Timestamp.Time)
	}

	// And with a real timestamp
	raw = `{"action":"I","xid":456,"timestamp":"2024-01-15 10:30:00.123456-05:00","schema":"public","table":"users"}`
	if err := json.Unmarshal([]byte(raw), &msg); err != nil {
		t.Fatalf("unmarshal with real timestamp: %v", err)
	}
	if msg.Timestamp.Time.IsZero() {
		t.Fatal("expected non-zero time, got zero")
	}
	expected := time.Date(2024, 1, 15, 15, 30, 0, 123456000, time.UTC)
	if !msg.Timestamp.Time.Equal(expected) {
		t.Fatalf("expected %v, got %v", expected, msg.Timestamp.Time)
	}
}
