package metrics

// Pilot metrics have bounded cardinality: one instance owns one source slot.
var Pilot = struct {
	RetainedWAL, SafeWAL, SlotActive, SlotLost, ReceivedLSN, AckedLSN, LastReceive, LastAck *PrometheusGauge
	SpillBytes, TxBytes, DLQBytes, DLQRecords, DLQOldest                                    *PrometheusGauge
}{
	NewPrometheusGauge("wal", "retained_bytes", "WAL retained by the source slot"),
	NewPrometheusGauge("wal", "safe_bytes", "Bytes until slot retention limit; -1 when unlimited or unavailable"),
	NewPrometheusGauge("wal", "slot_active", "Whether PostgreSQL reports an active slot"),
	NewPrometheusGauge("wal", "slot_at_risk", "Slot is unreserved or lost"),
	NewPrometheusGauge("wal", "received_lsn", "Received WAL position as a numeric diagnostic"),
	NewPrometheusGauge("wal", "acked_lsn", "Acknowledged commit position as a numeric diagnostic"),
	NewPrometheusGauge("wal", "last_receive_seconds", "Unix time of last replication message"),
	NewPrometheusGauge("wal", "last_ack_seconds", "Unix time of last acknowledged commit progress"),
	NewPrometheusGauge("parser", "spill_bytes", "Current transaction spill bytes"),
	NewPrometheusGauge("parser", "tx_bytes", "Accounted transaction memory bytes"),
	NewPrometheusGauge("dlq", "storage_bytes", "Recovery bucket bytes retained"),
	NewPrometheusGauge("dlq", "records", "Retained quarantine index records, including redriven records"),
	NewPrometheusGauge("dlq", "oldest_seconds", "Unix time of oldest retained quarantine record"),
}

// OversizedRecords counts records larger than a whole pipeline byte budget.
// They are admitted one at a time instead of stopping capture.
var OversizedRecords = NewPrometheusCounter("pipeline", "oversized_records_total", "Records admitted exclusively because they exceed a pipeline byte budget")
