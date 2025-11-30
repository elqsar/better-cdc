package publisher

import (
	"context"
	"testing"
	"time"

	"better-cdc/internal/model"
)

// BenchmarkSubjectForEvent benchmarks subject string construction
func BenchmarkSubjectForEvent(b *testing.B) {
	evt := &model.CDCEvent{
		Schema: "public",
		Table:  "users",
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := SubjectForEvent("mydb", evt)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkSubjectForEventLongNames benchmarks with longer schema/table names
func BenchmarkSubjectForEventLongNames(b *testing.B) {
	evt := &model.CDCEvent{
		Schema: "application_analytics_schema",
		Table:  "user_behavior_tracking_events",
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := SubjectForEvent("production_analytics_database", evt)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkPublishItemCreation benchmarks creating PublishItem structs
func BenchmarkPublishItemCreation(b *testing.B) {
	data := []byte(`{"event_id":"test","event_type":"cdc.insert"}`)
	position := model.WALPosition{LSN: "0/16B3748"}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = PublishItem{
			Subject:  "cdc.mydb.public.users",
			Data:     data,
			EventID:  "0/16B3748:12345:public.users:id=1",
			TxID:     12345,
			Position: position,
		}
	}
}

// BenchmarkPendingAckCreation benchmarks creating PendingAck structs
func BenchmarkPendingAckCreation(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		pa := &PendingAck{
			Subject: "cdc.mydb.public.users",
			EventID: "0/16B3748:12345:public.users:id=1",
			TxID:    12345,
			done:    make(chan struct{}),
		}
		close(pa.done)
	}
}

// BenchmarkPublishItemSliceAlloc benchmarks allocating slices of PublishItems
func BenchmarkPublishItemSliceAlloc(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		items := make([]PublishItem, 0, 500) // default batch size
		_ = items
	}
}

// BenchmarkPendingAckSliceAlloc benchmarks allocating slices of PendingAcks
func BenchmarkPendingAckSliceAlloc(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		pending := make([]*PendingAck, 0, 500) // default batch size
		_ = pending
	}
}

// BenchmarkNoopPublish benchmarks the noop publisher path
func BenchmarkNoopPublish(b *testing.B) {
	p := NewNoopPublisher()
	data := []byte(`{"event_id":"test"}`)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		err := p.Publish(context.TODO(), "cdc.mydb.public.users", data)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkBackoff benchmarks the backoff calculation
func BenchmarkBackoff(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = backoff(i % 5)
	}
}

// BenchmarkPendingAckWaitResolved benchmarks waiting on already-resolved acks
func BenchmarkPendingAckWaitResolved(b *testing.B) {
	// Pre-create resolved pending acks
	pending := make([]*PendingAck, 100)
	for i := range pending {
		pa := &PendingAck{
			Acked: true,
			done:  make(chan struct{}),
		}
		close(pa.done)
		pending[i] = pa
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// Simulate checking all acks
		count := 0
		for _, pa := range pending {
			select {
			case <-pa.done:
				if pa.Acked {
					count++
				}
			default:
			}
		}
		_ = count
	}
}

// BenchmarkCreatePublishItems benchmarks creating a batch of publish items
func BenchmarkCreatePublishItems(b *testing.B) {
	data := []byte(`{"event_id":"0/16B3748:12345:public.users:id=1","event_type":"cdc.insert","source":"mydb","schema":"public","table":"users"}`)
	position := model.WALPosition{LSN: "0/16B3748"}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		items := make([]PublishItem, 500)
		for j := 0; j < 500; j++ {
			items[j] = PublishItem{
				Subject:  "cdc.mydb.public.users",
				Data:     data,
				EventID:  "0/16B3748:12345:public.users:id=1",
				TxID:     12345,
				Position: position,
			}
		}
		_ = items
	}
}

// BenchmarkTimeAfter benchmarks time.After allocation (used in ack waiting)
func BenchmarkTimeAfter(b *testing.B) {
	timeout := 5 * time.Second

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		timer := time.NewTimer(timeout)
		timer.Stop()
	}
}
