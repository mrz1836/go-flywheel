//go:build integration

package core

import "testing"

// TestInsertHookStampsEveryInsertPathPostgres runs the hook's insert paths on
// PostgreSQL, whose finalize, conflict inserts, and reclaim statement are its
// own: the Client's inserts, a finalize's follow-ups and its continuation, and
// the continuation the lease sweep fires.
func TestInsertHookStampsEveryInsertPathPostgres(t *testing.T) {
	t.Parallel()
	t.Run("client", func(t *testing.T) {
		t.Parallel()
		runHookCases(t, postgresOpener, hookClientCases())
	})
	runHookCases(t, postgresOpener, []hookPathCase{
		{"follow_up", hookFollowUpCase},
		{"finalize_barrier", hookFinalizeBarrierCase},
		{"sweep_barrier", hookSweepBarrierCase},
	})
}
