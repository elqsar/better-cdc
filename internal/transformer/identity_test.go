package transformer

import (
	"context"
	"strings"
	"testing"

	"github.com/elqsar/better-cdc/internal/model"
)

func TestNamespacedIdentity(t *testing.T) {
	evt := newWALEvent(model.OperationInsert, nil, map[string]interface{}{"id": 7})
	identity := model.Identity{SourceID: "cluster-a", Slot: "slot", Decoder: "pgoutput"}
	original := EventIDFor(identity, evt)
	if !strings.HasPrefix(original, "v2:") || len(original) != 67 {
		t.Fatalf("invalid ID: %s", original)
	}
	for _, changed := range []model.Identity{
		{SourceID: "cluster-b", Slot: "slot", Decoder: "pgoutput"},
		{SourceID: "cluster-a", Slot: "other", Decoder: "pgoutput"},
		{SourceID: "cluster-a", Slot: "slot", Decoder: "wal2json"},
	} {
		if original == EventIDFor(changed, evt) {
			t.Fatal("different namespace collided")
		}
	}
	evt.NewValues["id"] = 8
	if original != EventIDFor(identity, evt) {
		t.Fatal("row values changed ID")
	}
	tr := NewSimpleTransformer("db", identity)
	normalized, err := tr.Transform(context.Background(), evt)
	if err != nil {
		t.Fatal(err)
	}
	defer model.ReleaseCDCEvent(normalized)
	evt.Identity = identity
	if normalized.EventID != EventID(evt) || normalized.SchemaVersion != 2 || normalized.SourceID != "cluster-a" {
		t.Fatal("transform and failure identity differ")
	}
	// Delimiter-containing identifiers must not collapse into one encoding.
	evt.Schema, evt.Table = "a.b", "c"
	first := EventIDFor(identity, evt)
	evt.Schema, evt.Table = "a", "b.c"
	if first == EventIDFor(identity, evt) {
		t.Fatal("ambiguous identifiers collided")
	}
}

func TestLegacyRecoveryIdentity(t *testing.T) {
	evt := newWALEvent(model.OperationInsert, nil, nil)
	got, err := NewSimpleTransformer("db").Transform(context.Background(), evt)
	if err != nil {
		t.Fatal(err)
	}
	defer model.ReleaseCDCEvent(got)
	if got.EventID != "0/16A1B8:42:INSERT:public.users:0" || got.SchemaVersion != 1 || got.SourceID != "" {
		t.Fatalf("legacy recovery changed: %+v", got)
	}
}
