//go:build integration

package core

import "testing"

// TestCooperativeCancelCancelsTheWorkerWithinOneHeartbeatPostgres runs the
// cooperative cancel on PostgreSQL: the renewal that finds the claim gone, the
// state read after it, and the cancelled attempt's record.
func TestCooperativeCancelCancelsTheWorkerWithinOneHeartbeatPostgres(t *testing.T) {
	t.Parallel()
	cooperativeCancelSuite(t, postgresOpener)
}

// TestAReclaimNeverCancelsTheWorkerPostgres takes the claim each way but a cancel
// on PostgreSQL, where the sweep decides each row's state in one statement.
func TestAReclaimNeverCancelsTheWorkerPostgres(t *testing.T) {
	t.Parallel()
	reclaimNeverCancelsSuite(t, postgresOpener)
}

// TestWithoutCooperativeCancelACancelledWorkerRunsOnPostgres proves the setting
// changes nothing while off, on PostgreSQL.
func TestWithoutCooperativeCancelACancelledWorkerRunsOnPostgres(t *testing.T) {
	t.Parallel()
	withoutCooperativeCancelSuite(t, NewPostgresIsolatedDB(t))
}

// TestACancelWaitingOnARunningAttemptIsVisiblePostgres reads a cancel still
// waiting on a running attempt through the public reads on PostgreSQL.
func TestACancelWaitingOnARunningAttemptIsVisiblePostgres(t *testing.T) {
	t.Parallel()
	cancelWaitingSuite(t, postgresOpener)
}

// TestACooperativeCancelFiresItsParentsBarrierOncePostgres runs the barrier case
// on PostgreSQL, where the barrier check locks the parent row.
func TestACooperativeCancelFiresItsParentsBarrierOncePostgres(t *testing.T) {
	t.Parallel()
	cooperativeCancelBarrierSuite(t, NewPostgresIsolatedDB(t))
}
