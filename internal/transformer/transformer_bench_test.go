package transformer

import (
	"context"
	"testing"
	"time"

	"better-cdc/internal/model"
)

// BenchmarkTransform benchmarks the full Transform operation
func BenchmarkTransform(b *testing.B) {
	t := NewSimpleTransformer("test-db")
	ctx := context.Background()

	evt := &model.WALEvent{
		Position:  model.WALPosition{LSN: "0/16B3748"},
		Timestamp: time.Now(),
		Operation: model.OperationInsert,
		Begin:     false,
		Commit:    false,
		Schema:    "public",
		Table:     "users",
		OldValues: nil,
		NewValues: map[string]interface{}{
			"id":    1,
			"name":  "test",
			"email": "test@example.com",
		},
		TransactionID: "12345",
		CommitTime:    time.Now(),
		LSN:           "0/16B3748",
		TxID:          12345,
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := t.Transform(ctx, evt)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkTransformUpdate benchmarks Transform with both old and new values
func BenchmarkTransformUpdate(b *testing.B) {
	t := NewSimpleTransformer("test-db")
	ctx := context.Background()

	evt := &model.WALEvent{
		Position:  model.WALPosition{LSN: "0/16B3748"},
		Timestamp: time.Now(),
		Operation: model.OperationUpdate,
		Begin:     false,
		Commit:    false,
		Schema:    "public",
		Table:     "users",
		OldValues: map[string]interface{}{
			"id":    1,
			"name":  "old_name",
			"email": "old@example.com",
		},
		NewValues: map[string]interface{}{
			"id":    1,
			"name":  "new_name",
			"email": "new@example.com",
		},
		TransactionID: "12345",
		CommitTime:    time.Now(),
		LSN:           "0/16B3748",
		TxID:          12345,
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := t.Transform(ctx, evt)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkTransformLargePayload benchmarks Transform with large payloads
func BenchmarkTransformLargePayload(b *testing.B) {
	t := NewSimpleTransformer("test-db")
	ctx := context.Background()

	// Create large payload with 50 columns
	values := make(map[string]interface{}, 50)
	for i := 0; i < 50; i++ {
		values["column_"+string(rune('a'+i%26))+string(rune('0'+i/26))] = "some value representing realistic data"
	}

	evt := &model.WALEvent{
		Position:      model.WALPosition{LSN: "0/16B3748"},
		Timestamp:     time.Now(),
		Operation:     model.OperationUpdate,
		Begin:         false,
		Commit:        false,
		Schema:        "public",
		Table:         "large_table",
		OldValues:     values,
		NewValues:     values,
		TransactionID: "12345",
		CommitTime:    time.Now(),
		LSN:           "0/16B3748",
		TxID:          12345,
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := t.Transform(ctx, evt)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkPrimaryKeyFragment benchmarks primary key fragment generation in isolation
func BenchmarkPrimaryKeyFragment(b *testing.B) {
	evt := &model.WALEvent{
		OldValues: map[string]interface{}{
			"id":        1,
			"tenant_id": "abc-123",
			"version":   5,
		},
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = primaryKeyFragment(evt)
	}
}

// BenchmarkPrimaryKeyFragmentSingleKey benchmarks with a single primary key
func BenchmarkPrimaryKeyFragmentSingleKey(b *testing.B) {
	evt := &model.WALEvent{
		OldValues: map[string]interface{}{
			"id": 1,
		},
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = primaryKeyFragment(evt)
	}
}

// BenchmarkLowerOp benchmarks operation type conversion
func BenchmarkLowerOp(b *testing.B) {
	ops := []model.OperationType{
		model.OperationInsert,
		model.OperationUpdate,
		model.OperationDelete,
		model.OperationDDL,
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = lowerOp(ops[i%4])
	}
}
