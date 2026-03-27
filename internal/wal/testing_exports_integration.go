//go:build integration

package wal

import "time"

// SetStandbyTimeoutForTesting overrides the replication standby timeout for integration tests.
func SetStandbyTimeoutForTesting(timeout time.Duration) func() {
	prev := time.Duration(replicationStandbyTimeoutNanos.Load())
	replicationStandbyTimeoutNanos.Store(int64(timeout))
	return func() {
		replicationStandbyTimeoutNanos.Store(int64(prev))
	}
}
