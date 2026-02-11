package checkpoint

import (
	"context"
	"fmt"

	"better-cdc/internal/model"

	"github.com/jackc/pgx/v5/pgconn"
)

// SlotStore reads the checkpoint from the PostgreSQL replication slot's
// confirmed_flush_lsn. Save is a no-op because the StandbyStatusUpdate
// heartbeat already persists the position in Postgres.
type SlotStore struct {
	databaseURL string
	slotName    string
}

func NewSlotStore(databaseURL, slotName string) *SlotStore {
	return &SlotStore{
		databaseURL: databaseURL,
		slotName:    slotName,
	}
}

func (s *SlotStore) Load(ctx context.Context) (model.WALPosition, error) {
	conn, err := pgconn.Connect(ctx, s.databaseURL)
	if err != nil {
		return model.WALPosition{}, fmt.Errorf("slot store connect: %w", err)
	}
	defer conn.Close(ctx)

	result := conn.ExecParams(
		ctx,
		"SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = $1",
		[][]byte{[]byte(s.slotName)},
		nil,  // paramOIDs
		nil,  // resultFormats (text)
		nil,  // resultFormatCodes
	)

	var lsn string
	for result.NextRow() {
		if val := result.Values()[0]; val != nil {
			lsn = string(val)
		}
	}
	if _, err := result.Close(); err != nil {
		return model.WALPosition{}, fmt.Errorf("slot store query: %w", err)
	}

	if lsn == "" {
		return model.WALPosition{}, nil
	}
	return model.WALPosition{LSN: lsn}, nil
}

func (s *SlotStore) Save(_ context.Context, _ model.WALPosition) error {
	return nil
}
