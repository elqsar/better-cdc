package metrics

// PilotMetrics belong to one producer instance.
type PilotMetrics struct {
	RetainedWAL, SafeWAL, SlotActive, SlotLost, ReceivedLSN, AckedLSN, LastReceive, LastAck *PrometheusGauge
	SpillBytes, TxBytes, DLQBytes, DLQRecords, DLQOldest                                    *PrometheusGauge
}

func newPilot(f factory) *PilotMetrics {
	return &PilotMetrics{
		f.NewPrometheusGauge("wal", "retained_bytes", "WAL retained by the source slot"),
		f.NewPrometheusGauge("wal", "safe_bytes", "Bytes until slot retention limit; -1 when unlimited or unavailable"),
		f.NewPrometheusGauge("wal", "slot_active", "Whether PostgreSQL reports an active slot"),
		f.NewPrometheusGauge("wal", "slot_at_risk", "Slot is unreserved or lost"),
		f.NewPrometheusGauge("wal", "received_lsn", "Received WAL position as a numeric diagnostic"),
		f.NewPrometheusGauge("wal", "acked_lsn", "Acknowledged commit position as a numeric diagnostic"),
		f.NewPrometheusGauge("wal", "last_receive_seconds", "Unix time of last replication message"),
		f.NewPrometheusGauge("wal", "last_ack_seconds", "Unix time of last acknowledged commit progress"),
		f.NewPrometheusGauge("parser", "spill_bytes", "Current transaction spill bytes"),
		f.NewPrometheusGauge("parser", "tx_bytes", "Accounted transaction memory bytes"),
		f.NewPrometheusGauge("dlq", "storage_bytes", "Recovery bucket bytes retained"),
		f.NewPrometheusGauge("dlq", "records", "Retained quarantine index records, including redriven records"),
		f.NewPrometheusGauge("dlq", "oldest_seconds", "Unix time of oldest retained quarantine record"),
	}

}
