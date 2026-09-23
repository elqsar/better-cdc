package parser

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"better-cdc/internal/model"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgtype"
)

func TestExactNumericRoundTrips(t *testing.T) {
	p := NewPGOutputParser(PGOutputConfig{})
	for _, tc := range []struct {
		oid   uint32
		value string
	}{
		{pgtype.Int8OID, "9007199254740993"},
		{pgtype.NumericOID, "12345678901234567890.12345"},
		{pgtype.JSONOID, `{"nested":[9007199254740993,12345678901234567890.12345]}`},
		{pgtype.JSONBOID, `{"nested":[9007199254740993,12345678901234567890.12345]}`},
	} {
		value, err := p.decodeColumn(tc.oid, []byte(tc.value))
		if err != nil {
			t.Fatal(err)
		}
		encoded, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		if string(encoded) != tc.value {
			t.Fatalf("oid %d changed %s to %s", tc.oid, tc.value, encoded)
		}
	}
	raw := []byte(`{"action":"I","xid":7,"schema":"public","table":"t","columns":[{"name":"i","value":9007199254740993},{"name":"n","value":12345678901234567890.12345},{"name":"j","value":{"n":9007199254740993}}]}`)
	events, err := decodeWal2JSON(42, raw, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer model.ReleaseWALEvent(events[0])
	encoded, err := json.Marshal(events[0].NewValues)
	if err != nil {
		t.Fatal(err)
	}
	for _, number := range []string{"9007199254740993", "12345678901234567890.12345"} {
		if !strings.Contains(string(encoded), number) {
			t.Fatalf("missing exact number %s in %s", number, encoded)
		}
	}
	restored, err := RestoreChange(events[0].Recovery)
	if err != nil {
		t.Fatal(err)
	}
	defer model.ReleaseWALEvent(restored)
	again, _ := json.Marshal(restored.NewValues)
	if string(again) != string(encoded) {
		t.Fatal("recovery changed values")
	}
}
func TestInvalidInputFailsClosed(t *testing.T) {
	p := NewPGOutputParser(PGOutputConfig{})
	p.tx = &txBuffer{xid: 1}
	if _, err := p.buildEventsForReplay(&pglogrepl.InsertMessage{RelationID: 99, Tuple: &pglogrepl.TupleData{}}); err == nil {
		t.Fatal("unknown relation ignored")
	}
	if _, err := p.decodeColumn(pgtype.Int8OID, []byte("not a number")); err == nil {
		t.Fatal("invalid known type silently converted")
	}
	if _, err := p.populateTupleColumnMap(map[string]any{}, relationInfo{Columns: []string{"a", "b"}}, []*pglogrepl.TupleDataColumn{{DataType: 'n'}}); err == nil {
		t.Fatal("tuple mismatch ignored")
	}
	if _, err := decodeWal2JSON(42, []byte(`{"action":"?"}`), nil); err == nil {
		t.Fatal("unknown action ignored")
	}
	for _, raw := range [][]byte{nil, []byte("I"), []byte("R")} {
		if _, err := parseLogical(raw); err == nil {
			t.Fatalf("accepted malformed %q", raw)
		}
	}
}
func TestSpillPreservesRelationRevision(t *testing.T) {
	p := NewPGOutputParser(PGOutputConfig{MaxTxBufferSize: 1, SpillDir: t.TempDir()})
	p.tx = &txBuffer{xid: 1}
	p.relations[1] = relationInfo{ID: 1, Schema: "public", Table: "t", Columns: []string{"old_name"}, ColumnTypes: []uint32{pgtype.TextOID}}
	out := make(chan *model.WALEvent, 8)
	for i, value := range []string{"before", "after"} {
		if i == 1 {
			rel := p.relations[1]
			rel.Columns = []string{"new_name"}
			p.relations[1] = rel
		}
		raw := encodeInsertMessage(1, value)
		logical, err := parseLogical(raw)
		if err != nil {
			t.Fatal(err)
		}
		if err = p.handlePGOutputMessage(context.Background(), raw, logical, out); err != nil {
			t.Fatal(err)
		}
	}
	if p.tx.spill == nil {
		t.Fatal("expected spill")
	}
	if err := p.handlePGOutputMessage(context.Background(), nil, &pglogrepl.CommitMessage{CommitLSN: 42, TransactionEndLSN: 43}, out); err != nil {
		t.Fatal(err)
	}
	first, second := <-out, <-out
	defer model.ReleaseWALEvent(first)
	defer model.ReleaseWALEvent(second)
	defer model.ReleaseWALEvent(<-out)
	if first.NewValues["old_name"] != "before" || second.NewValues["new_name"] != "after" {
		t.Fatalf("schema revision lost: %v %v", first.NewValues, second.NewValues)
	}
}
func TestSpillCapacityAndCorruptLength(t *testing.T) {
	s, err := newTxSpill(t.TempDir(), 16)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.CloseAndRemove() }()
	if err = s.Write(make([]byte, 9)); err == nil {
		t.Fatal("accepted overflow")
	}
	if _, err = s.file.Write([]byte{255, 255, 255, 255, 255, 255, 255, 255}); err != nil {
		t.Fatal(err)
	}
	if err = s.Replay(func([]byte) error { return nil }); err == nil {
		t.Fatal("accepted corrupt length")
	}
}
func TestParserResetReleasesIncompleteSpill(t *testing.T) {
	p := NewPGOutputParser(PGOutputConfig{})
	p.tx = &txBuffer{xid: 9}
	p.relations[1] = relationInfo{ID: 1}
	in := make(chan *RawMessage, 1)
	in <- &RawMessage{Reset: true}
	close(in)
	out, err := p.Parse(context.Background(), in)
	if err != nil {
		t.Fatal(err)
	}
	for range out {
	}
	if p.tx != nil || len(p.relations) != 0 || p.Err() != nil {
		t.Fatal("session state not reset")
	}
}

func TestKeyOnlyBeforeDoesNotInventNulls(t *testing.T) {
	p := NewPGOutputParser(PGOutputConfig{})
	p.tx = &txBuffer{xid: 1}
	p.relations[1] = relationInfo{ID: 1, Schema: "public", Table: "t", Columns: []string{"id", "value"}, ColumnTypes: []uint32{pgtype.TextOID, pgtype.TextOID}, KeyColumns: []bool{true, false}}
	events, err := p.buildEventsForReplay(&pglogrepl.DeleteMessage{RelationID: 1, OldTupleType: 'K', OldTuple: &pglogrepl.TupleData{Columns: []*pglogrepl.TupleDataColumn{{DataType: 't', Data: []byte("key")}, {DataType: 'n'}}}})
	if err != nil {
		t.Fatal(err)
	}
	defer model.ReleaseWALEvent(events[0])
	if _, exists := events[0].OldValues["value"]; exists {
		t.Fatal("unknown old value represented as SQL NULL")
	}
	if len(events[0].UnavailableBefore) != 1 || events[0].UnavailableBefore[0] != "value" {
		t.Fatal("missing unavailable column metadata")
	}
}
