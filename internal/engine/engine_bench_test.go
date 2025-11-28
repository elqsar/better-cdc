package engine

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"better-cdc/internal/model"
)

// BenchmarkCDCEventMarshal benchmarks JSON marshaling of CDCEvent (hot path)
func BenchmarkCDCEventMarshal(b *testing.B) {
	evt := &model.CDCEvent{
		EventID:    "0/16B3748:12345:public.users:id=1,email=test@example.com",
		EventType:  "cdc.insert",
		Source:     "test-db",
		Timestamp:  time.Now(),
		CommitTime: time.Now(),
		LSN:        "0/16B3748",
		TxID:       12345,
		Schema:     "public",
		Table:      "users",
		Operation:  "INSERT",
		Before:     nil,
		After: map[string]interface{}{
			"id":         1,
			"email":      "test@example.com",
			"name":       "Test User",
			"created_at": time.Now().Format(time.RFC3339),
			"updated_at": time.Now().Format(time.RFC3339),
			"is_active":  true,
			"balance":    123.45,
		},
		Metadata: map[string]interface{}{"txid": "12345"},
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := json.Marshal(evt)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkCDCEventMarshalLarge benchmarks JSON marshaling with larger payloads
func BenchmarkCDCEventMarshalLarge(b *testing.B) {
	after := make(map[string]interface{}, 50)
	for i := 0; i < 50; i++ {
		after["field_"+string(rune('a'+i%26))+string(rune('0'+i/26))] = "some value that represents realistic data in a column"
	}

	evt := &model.CDCEvent{
		EventID:    "0/16B3748:12345:public.large_table:id=1",
		EventType:  "cdc.update",
		Source:     "test-db",
		Timestamp:  time.Now(),
		CommitTime: time.Now(),
		LSN:        "0/16B3748",
		TxID:       12345,
		Schema:     "public",
		Table:      "large_table",
		Operation:  "UPDATE",
		Before:     after,
		After:      after,
		Metadata:   map[string]interface{}{"txid": "12345"},
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := json.Marshal(evt)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// generateTestEvents creates realistic WALEvents for benchmarking
func generateTestEvents(n int) []*model.WALEvent {
	events := make([]*model.WALEvent, n)
	baseTime := time.Now()

	for i := 0; i < n; i++ {
		events[i] = &model.WALEvent{
			Position:  model.WALPosition{LSN: "0/16B3748"},
			Timestamp: baseTime,
			Operation: model.OperationInsert,
			Begin:     false,
			Commit:    false,
			Schema:    "public",
			Table:     "users",
			OldValues: nil,
			NewValues: map[string]interface{}{
				"id":         i,
				"email":      "user@example.com",
				"name":       "Test User",
				"created_at": baseTime.Format(time.RFC3339),
			},
			TransactionID: "12345",
			CommitTime:    baseTime,
			LSN:           "0/16B3748",
			TxID:          12345,
		}
	}
	return events
}

// MockBatchPublisher implements BatchPublisher for isolated benchmarking
type MockBatchPublisher struct {
	publishedCount int
}

func (m *MockBatchPublisher) Connect() error { return nil }
func (m *MockBatchPublisher) Close() error   { return nil }
func (m *MockBatchPublisher) Publish(ctx context.Context, subject string, data []byte) error {
	return nil
}
func (m *MockBatchPublisher) PublishWithRetries(ctx context.Context, subject string, data []byte, maxRetries int) error {
	return nil
}

func (m *MockBatchPublisher) PublishBatchAsync(ctx context.Context, items []PublishItemForTest) ([]*PendingAckForTest, error) {
	pending := make([]*PendingAckForTest, len(items))
	for i := range items {
		pending[i] = &PendingAckForTest{Acked: true, done: make(chan struct{})}
		close(pending[i].done)
	}
	m.publishedCount += len(items)
	return pending, nil
}

func (m *MockBatchPublisher) WaitForAcks(ctx context.Context, pending []*PendingAckForTest, timeout time.Duration) (int, error) {
	return len(pending), nil
}

// Test types to avoid import cycle
type PublishItemForTest struct {
	Subject string
	Data    []byte
	EventID string
	TxID    uint64
}

type PendingAckForTest struct {
	Acked bool
	done  chan struct{}
}
