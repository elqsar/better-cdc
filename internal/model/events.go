package model

import (
	"encoding/json"
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
// RecoveryChange is a versioned, value-independent replay capsule. Data and
// relation metadata preserve the input even when normalized JSON cannot be built.
type RecoveryChange struct {
	Version    int             `json:"version"`
	Plugin     string          `json:"plugin"`
	Data       []byte          `json:"data"`
	Relations  json.RawMessage `json:"relations,omitempty"`
	WALStart   uint64          `json:"wal_start"`
	Index      int             `json:"index"`
	LSN        string          `json:"lsn"`
	Position   WALPosition     `json:"position"`
	TxID       uint64          `json:"txid"`
	CommitTime time.Time       `json:"commit_time"`
	SeqInTx    uint32          `json:"seq_in_tx"`
}

type Identity struct {
	SourceID string `json:"source_id"`
	Slot     string `json:"slot"`
	Decoder  string `json:"decoder"`
}

type WALEvent struct {
	Identity          Identity
	Recovery          *RecoveryChange
	UnavailableBefore []string
	UnavailableAfter  []string
	ReleaseBytes      func() `json:"-"`

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
	// SeqInTx is the event's deterministic ordinal (WAL order) within its
	// transaction. It disambiguates events that share the same commit LSN and
	// xid so the EventID stays unique per logical change while remaining stable
	// across crash-replay.
	SeqInTx uint32
}

// Default map capacity for pooled WALEvents (typical table has ~10-20 columns).
const defaultMapCapacity = 16

var walEventPool = sync.Pool{
	New: func() interface{} {
		return &WALEvent{
			OldValues: make(map[string]interface{}, defaultMapCapacity),
			NewValues: make(map[string]interface{}, defaultMapCapacity),
		}
	},
}

// AcquireWALEvent returns a WALEvent from the pool with pre-allocated maps.
func AcquireWALEvent() *WALEvent {
	return walEventPool.Get().(*WALEvent)
}

// ReleaseWALEvent returns a WALEvent to the pool after resetting it.
// Safe to call on events that were not originally acquired from the pool
// (e.g. pgoutput Begin/Commit markers allocated with &WALEvent{}).
func ReleaseWALEvent(evt *WALEvent) {
	if evt == nil {
		return
	}
	if evt.ReleaseBytes != nil {
		evt.ReleaseBytes()
		evt.ReleaseBytes = nil
	}
	evt.Identity = Identity{}
	evt.Recovery = nil
	evt.UnavailableBefore = nil
	evt.UnavailableAfter = nil
	// Reset scalar fields
	evt.Position = WALPosition{}
	evt.Timestamp = time.Time{}
	evt.Operation = ""
	evt.Begin = false
	evt.Commit = false
	evt.Schema = ""
	evt.Table = ""
	evt.TransactionID = ""
	evt.CommitTime = time.Time{}
	evt.LSN = ""
	evt.TxID = 0
	evt.SeqInTx = 0

	// Clear maps but keep the underlying storage.
	// Reinitialize nil maps so the pool invariant (non-nil maps) is preserved.
	// This handles events not originally from the pool (e.g. pgoutput Begin/Commit).
	if evt.OldValues == nil {
		evt.OldValues = make(map[string]interface{}, defaultMapCapacity)
	} else {
		for k := range evt.OldValues {
			delete(evt.OldValues, k)
		}
	}
	if evt.NewValues == nil {
		evt.NewValues = make(map[string]interface{}, defaultMapCapacity)
	} else {
		for k := range evt.NewValues {
			delete(evt.NewValues, k)
		}
	}

	walEventPool.Put(evt)
}

// CDCEvent is the normalized event ready for publication.
type CDCEvent struct {
	SourceID      string                 `json:"source_id,omitempty"`
	SchemaVersion int                    `json:"schema_version"`
	SeqInTx       uint32                 `json:"seq_in_tx"`
	EventID       string                 `json:"event_id"`
	EventType     string                 `json:"event_type"`
	Source        string                 `json:"source"`
	Timestamp     time.Time              `json:"timestamp"`
	CommitTime    time.Time              `json:"commit_time"`
	LSN           string                 `json:"lsn"`
	TxID          uint64                 `json:"txid"`
	Schema        string                 `json:"schema"`
	Table         string                 `json:"table"`
	Operation     string                 `json:"operation"`
	Before        map[string]interface{} `json:"before,omitempty"`
	After         map[string]interface{} `json:"after,omitempty"`
	Metadata      map[string]interface{} `json:"metadata,omitempty"`
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
	evt.SourceID = ""
	evt.SchemaVersion = 0
	evt.SeqInTx = 0
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
	if evt.Metadata == nil {
		evt.Metadata = make(map[string]interface{})
	}
	for k := range evt.Metadata {
		delete(evt.Metadata, k)
	}
	cdcEventPool.Put(evt)
}
