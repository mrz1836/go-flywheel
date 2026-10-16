//go:build integration

package core

import (
	"testing"
	"time"

	"github.com/mrz1836/go-foundation/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestFinishFeedNeverSkipsALateCommitWithinTheSettleWindowPostgres proves the
// settle window on PostgreSQL with a real late commit: a finalize stamped before
// a finish already committed is still open in its transaction when a poll runs,
// commits after it, and is read by the next poll in finish order.
func TestFinishFeedNeverSkipsALateCommitWithinTheSettleWindowPostgres(t *testing.T) {
	t.Parallel()
	db := NewPostgresIsolatedDB(t)
	writeHistory(t, db, []histRun{
		feedRun("run-a", "alpha", feedNow.Add(-30*time.Second)),
		feedRun("run-b", "alpha", feedNow.Add(-2*time.Second)),
	})
	started := feedNow.Add(-4 * time.Second)
	jobID := models.NewID()
	seedJob(t, db, jobRow{
		ID: jobID, Kind: "alpha", Queue: defaultQueue, State: string(StateRunning), Attempt: 1, MaxAttempts: 1,
		CreatedAt: started, UpdatedAt: started, ScheduledAt: started,
	})
	seedRun(t, db, jobRunRow{
		ID: "run-x", JobID: jobID, Attempt: 1, Kind: "alpha", Queue: defaultQueue, ExecutorID: "exec",
		StartedAt: started, Outcome: string(OutcomeStarted), CreatedAt: started,
	})

	tx := db.Begin()
	require.NoError(t, tx.Error)
	committed := false
	t.Cleanup(func() {
		if !committed {
			tx.Rollback()
		}
	})
	require.NoError(t, finalizeRunRow(tx, "run-x", map[string]any{
		"finished_at": feedNow.Add(-3 * time.Second).UTC(), "outcome": string(OutcomeSuccess), "duration_ms": 1000,
	}))

	first, err := ListRunFinishes(feedCtx(feedNow), db, ListRunFinishesParams{Since: feedNow.Add(-time.Hour)})
	require.NoError(t, err)
	assert.Equal(t, []string{"run-a"}, feedRunIDs(first), "run-b waits inside the settle window; run-x is uncommitted")

	require.NoError(t, tx.Commit().Error)
	committed = true

	later, err := ListRunFinishes(feedCtx(feedNow.Add(10*time.Second)), db, ListRunFinishesParams{After: first.NextCursor})
	require.NoError(t, err)
	assert.Equal(t, []string{"run-x", "run-b"}, feedRunIDs(later), "the late commit is read, in finish order")
}
