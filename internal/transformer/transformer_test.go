package transformer

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	"better-cdc/internal/model"
)

func newWALEvent(op model.OperationType, old, newVals map[string]interface{}) *model.WALEvent {
	return &model.WALEvent{
		Position:      model.WALPosition{LSN: "0/16A1B8"},
		Timestamp:     time.Date(2026, 4, 16, 12, 0, 0, 0, time.UTC),
		CommitTime:    time.Date(2026, 4, 16, 12, 0, 1, 0, time.UTC),
		Operation:     op,
		Schema:        "public",
		Table:         "users",
		LSN:           "0/16A1B8",
		TxID:          42,
		TransactionID: "tx-42",
		OldValues:     old,
		NewValues:     newVals,
	}
}

func TestTransform_NilEvent(t *testing.T) {
	tr := NewSimpleTransformer("test-src")

	cdcEvt, err := tr.Transform(context.Background(), nil)

	if err == nil {
		t.Fatal("expected error for nil event, got nil")
	}
	if cdcEvt != nil {
		t.Fatalf("expected nil CDCEvent, got %+v", cdcEvt)
	}
}

func TestTransform_InsertMapsAllFields(t *testing.T) {
	tr := NewSimpleTransformer("test-src")
	newVals := map[string]interface{}{"id": 7, "name": "alice"}
	evt := newWALEvent(model.OperationInsert, nil, newVals)

	cdcEvt, err := tr.Transform(context.Background(), evt)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer model.ReleaseCDCEvent(cdcEvt)

	if cdcEvt.EventType != "cdc.insert" {
		t.Errorf("EventType = %q, want cdc.insert", cdcEvt.EventType)
	}
	if cdcEvt.Source != "test-src" {
		t.Errorf("Source = %q, want test-src", cdcEvt.Source)
	}
	if !cdcEvt.Timestamp.Equal(evt.Timestamp) {
		t.Errorf("Timestamp = %v, want %v", cdcEvt.Timestamp, evt.Timestamp)
	}
	if !cdcEvt.CommitTime.Equal(evt.CommitTime) {
		t.Errorf("CommitTime = %v, want %v", cdcEvt.CommitTime, evt.CommitTime)
	}
	if cdcEvt.LSN != evt.LSN {
		t.Errorf("LSN = %q, want %q", cdcEvt.LSN, evt.LSN)
	}
	if cdcEvt.TxID != evt.TxID {
		t.Errorf("TxID = %d, want %d", cdcEvt.TxID, evt.TxID)
	}
	if cdcEvt.Schema != evt.Schema {
		t.Errorf("Schema = %q, want %q", cdcEvt.Schema, evt.Schema)
	}
	if cdcEvt.Table != evt.Table {
		t.Errorf("Table = %q, want %q", cdcEvt.Table, evt.Table)
	}
	if cdcEvt.Operation != string(model.OperationInsert) {
		t.Errorf("Operation = %q, want %q", cdcEvt.Operation, model.OperationInsert)
	}
	if len(cdcEvt.Before) != 0 {
		t.Errorf("Before = %v, want empty for insert", cdcEvt.Before)
	}
	if !reflect.DeepEqual(cdcEvt.After, newVals) {
		t.Errorf("After = %v, want %v", cdcEvt.After, newVals)
	}
	if got := cdcEvt.Metadata["txid"]; got != evt.TransactionID {
		t.Errorf("Metadata[txid] = %v, want %q", got, evt.TransactionID)
	}
	if cdcEvt.EventID == "" {
		t.Error("EventID is empty")
	}
}

func TestTransform_UpdatePopulatesBeforeAndAfter(t *testing.T) {
	tr := NewSimpleTransformer("test-src")
	oldVals := map[string]interface{}{"id": 7, "name": "alice"}
	newVals := map[string]interface{}{"id": 7, "name": "bob"}
	evt := newWALEvent(model.OperationUpdate, oldVals, newVals)

	cdcEvt, err := tr.Transform(context.Background(), evt)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer model.ReleaseCDCEvent(cdcEvt)

	if cdcEvt.EventType != "cdc.update" {
		t.Errorf("EventType = %q, want cdc.update", cdcEvt.EventType)
	}
	if !reflect.DeepEqual(cdcEvt.Before, oldVals) {
		t.Errorf("Before = %v, want %v", cdcEvt.Before, oldVals)
	}
	if !reflect.DeepEqual(cdcEvt.After, newVals) {
		t.Errorf("After = %v, want %v", cdcEvt.After, newVals)
	}
}

func TestTransform_DeleteKeepsOldValuesOnly(t *testing.T) {
	tr := NewSimpleTransformer("test-src")
	oldVals := map[string]interface{}{"id": 7}
	evt := newWALEvent(model.OperationDelete, oldVals, nil)

	cdcEvt, err := tr.Transform(context.Background(), evt)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer model.ReleaseCDCEvent(cdcEvt)

	if cdcEvt.EventType != "cdc.delete" {
		t.Errorf("EventType = %q, want cdc.delete", cdcEvt.EventType)
	}
	if !reflect.DeepEqual(cdcEvt.Before, oldVals) {
		t.Errorf("Before = %v, want %v", cdcEvt.Before, oldVals)
	}
	if len(cdcEvt.After) != 0 {
		t.Errorf("After = %v, want empty for delete", cdcEvt.After)
	}
}

func TestTransform_SourceIsConstructorArg(t *testing.T) {
	tr := NewSimpleTransformer("my-src")
	evt := newWALEvent(model.OperationInsert, nil, map[string]interface{}{"id": 1})

	cdcEvt, err := tr.Transform(context.Background(), evt)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer model.ReleaseCDCEvent(cdcEvt)

	if cdcEvt.Source != "my-src" {
		t.Errorf("Source = %q, want my-src", cdcEvt.Source)
	}
}

func TestEventType(t *testing.T) {
	cases := []struct {
		op   model.OperationType
		want string
	}{
		{model.OperationInsert, "cdc.insert"},
		{model.OperationUpdate, "cdc.update"},
		{model.OperationDelete, "cdc.delete"},
		{model.OperationDDL, "cdc.ddl"},
		{model.OperationType("BOGUS"), "cdc.unknown"},
	}

	for _, tc := range cases {
		t.Run(string(tc.op), func(t *testing.T) {
			if got := eventType(tc.op); got != tc.want {
				t.Errorf("eventType(%q) = %q, want %q", tc.op, got, tc.want)
			}
		})
	}
}

func TestBuildEventID_Format(t *testing.T) {
	evt := newWALEvent(model.OperationInsert, nil, map[string]interface{}{"id": 7})

	got := buildEventID(evt)

	want := "0/16A1B8:42:public.users:id=7"
	if got != want {
		t.Errorf("buildEventID = %q, want %q", got, want)
	}
}

func TestBuildEventID_PrefersOldValues(t *testing.T) {
	evt := newWALEvent(
		model.OperationUpdate,
		map[string]interface{}{"id": 1},
		map[string]interface{}{"id": 2},
	)

	got := buildEventID(evt)

	if !strings.HasSuffix(got, ":id=1") {
		t.Errorf("buildEventID = %q, want suffix :id=1 (OldValues should win)", got)
	}
}

func TestBuildEventID_NoPKFallback(t *testing.T) {
	evt := newWALEvent(model.OperationInsert, nil, nil)

	got := buildEventID(evt)

	want := "0/16A1B8:42:public.users:nopk"
	if got != want {
		t.Errorf("buildEventID = %q, want %q", got, want)
	}
}

func TestBuildEventID_KeysSortedAlphabetically(t *testing.T) {
	evt := newWALEvent(model.OperationInsert, nil, map[string]interface{}{
		"b": 2,
		"a": 1,
		"c": 3,
	})

	got := buildEventID(evt)

	if !strings.HasSuffix(got, ":a=1,b=2,c=3") {
		t.Errorf("buildEventID = %q, want suffix :a=1,b=2,c=3", got)
	}
}

func TestBuildEventID_StableAcrossCalls(t *testing.T) {
	evt1 := newWALEvent(model.OperationInsert, nil, map[string]interface{}{"id": 7})
	evt2 := newWALEvent(model.OperationInsert, nil, map[string]interface{}{"id": 7})

	id1 := buildEventID(evt1)
	id2 := buildEventID(evt2)

	if id1 != id2 {
		t.Errorf("buildEventID not stable: %q vs %q", id1, id2)
	}
}

func TestWriteValue(t *testing.T) {
	cases := []struct {
		name   string
		value  interface{}
		wantPK string
	}{
		{"string", "hello", "k=hello"},
		{"int", int(7), "k=7"},
		{"int64_negative", int64(-9000), "k=-9000"},
		{"uint64_max", uint64(18446744073709551615), "k=18446744073709551615"},
		{"float64", float64(1.5), "k=1.5"},
		{"bool_true", true, "k=true"},
		{"bool_false", false, "k=false"},
		{"nil", nil, "k=null"},
		{"slice_fallback", []int{1, 2}, "k=[1 2]"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			evt := newWALEvent(model.OperationInsert, nil, map[string]interface{}{"k": tc.value})

			got := buildEventID(evt)

			if !strings.HasSuffix(got, ":"+tc.wantPK) {
				t.Errorf("buildEventID = %q, want suffix :%s", got, tc.wantPK)
			}
		})
	}
}
