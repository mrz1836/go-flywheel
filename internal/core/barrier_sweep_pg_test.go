//go:build integration

package core

import "testing"

// TestCrashDiscardedChildFiresItsParentsBarrierPostgres runs the sweep's barrier
// check on PostgreSQL, inside the reclaim statement's transaction.
func TestCrashDiscardedChildFiresItsParentsBarrierPostgres(t *testing.T) {
	t.Parallel()
	crashDiscardBarrierSuite(t, NewPostgresIsolatedDB(t))
}
