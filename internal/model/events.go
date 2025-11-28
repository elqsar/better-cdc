package model

import (
	"sync"
	"time"
)

// WALPosition represents a logical replication position.
type WALPosition struct {
	LSN string
}

// OperationType represents a WAL operation.
type OperationType string

const (
	OperationInsert OperationType = "INSERT"
	OperationUpdate OperationType = "UPDATE"
	OperationDelete OperationType = "DELETE"
	OperationDDL    OperationType = "DDL"
)

// WALEvent is the raw event parsed from logical decoding output.
type WALEvent struct {
	Position      WALPosition
	Timestamp     time.Time
	Operation     OperationType
	Begin         bool
	Commit        bool
	Schema        string
	Table         string
	OldValues     map[string]interface{}
	NewValues     map[string]interface{}
	TransactionID string
	CommitTime    time.Time
	LSN           string
	TxID          uint64
}

// CDCEvent is the normalized event ready for publication.
type CDCEvent struct {
	EventID    string                 `json:"event_id"`
	EventType  string                 `json:"event_type"`
	Source     string                 `json:"source"`
	Timestamp  time.Time              `json:"timestamp"`
	CommitTime time.Time              `json:"commit_time"`
	LSN        string                 `json:"lsn"`
	TxID       uint64                 `json:"txid"`
	Schema     string                 `json:"schema"`
	Table      string                 `json:"table"`
	Operation  string                 `json:"operation"`
	Before     map[string]interface{} `json:"before,omitempty"`
	After      map[string]interface{} `json:"after,omitempty"`
	Metadata   map[string]interface{} `json:"metadata,omitempty"`
}

var cdcEventPool = sync.Pool{
	New: func() interface{} {
		return &CDCEvent{
			Metadata: make(map[string]interface{}, 1),
		}
	},
}

// AcquireCDCEvent returns a CDCEvent from the pool.
func AcquireCDCEvent() *CDCEvent {
	return cdcEventPool.Get().(*CDCEvent)
}

// ReleaseCDCEvent returns a CDCEvent to the pool after resetting it.
func ReleaseCDCEvent(evt *CDCEvent) {
	if evt == nil {
		return
	}
	// Reset all fields
	evt.EventID = ""
	evt.EventType = ""
	evt.Source = ""
	evt.Timestamp = time.Time{}
	evt.CommitTime = time.Time{}
	evt.LSN = ""
	evt.TxID = 0
	evt.Schema = ""
	evt.Table = ""
	evt.Operation = ""
	evt.Before = nil
	evt.After = nil
	// Clear and reuse metadata map
	for k := range evt.Metadata {
		delete(evt.Metadata, k)
	}
	cdcEventPool.Put(evt)
}
