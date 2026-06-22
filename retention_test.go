package flywheel

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// jobExists reports whether a jobs row with id is physically present (raw table,
// so a hard delete reads as absent and a soft delete as present).
func jobExists(t *testing.T, db *gorm.DB, id string) bool {
	t.Helper()
	var n int64
	require.NoError(t, db.Table("jobs").Where("id = ?", id).Count(&n).Error)
	return n > 0
}

// seedFinished seeds a terminal job finalized at finalizedAt, plus one run row.
func seedFinished(t *testing.T, db *gorm.DB, id string, state JobState, finalizedAt time.Time) {
	t.Helper()
	fin := finalizedAt
	seedJob(t, db, jobRow{
		ID: id, Kind: "k", State: string(state), FinalizedAt: &fin,
		CreatedAt: finalizedAt, UpdatedAt: finalizedAt, ScheduledAt: finalizedAt,
	})
	seedRun(t, db, jobRunRow{
		ID: "run-" + id, JobID: id, Attempt: 1, ExecutorID: "h",
		StartedAt: finalizedAt, Outcome: string(OutcomeSuccess), CreatedAt: finalizedAt,
	})
}

func TestDeleteFinishedJobsDeletesOnlyOldTerminalJobsAndRuns(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)
	old := now.Add(-30 * 24 * time.Hour)
	recent := now.Add(-time.Hour)
	cutoff := now.Add(-14 * 24 * time.Hour)

	seedFinished(t, db, "old-done", StateSucceeded, old)
	seedFinished(t, db, "old-cancelled", StateCancelled, old)
	seedFinished(t, db, "new-done", StateSucceeded, recent)

	// An old but still-active job (no finalized_at) must never be pruned.
	seedJob(t, db, jobRow{
		ID: "old-active", Kind: "k", State: string(StateRunning),
		CreatedAt: old, UpdatedAt: old, ScheduledAt: old,
	})
	seedRun(t, db, jobRunRow{
		ID: "run-old-active", JobID: "old-active", Attempt: 1, ExecutorID: "h",
		StartedAt: old, Outcome: string(OutcomeStarted), CreatedAt: old,
	})

	deleted, err := DeleteFinishedJobs(ctx, db, cutoff)
	require.NoError(t, err)
	assert.EqualValues(t, 2, deleted, "only the two old terminal jobs are deleted")

	assert.False(t, jobExists(t, db, "old-done"))
	assert.False(t, jobExists(t, db, "old-cancelled"))
	assert.True(t, jobExists(t, db, "new-done"), "a recent terminal job is kept")
	assert.True(t, jobExists(t, db, "old-active"), "an old non-terminal job is kept")

	assert.EqualValues(t, 0, runCount(t, db, "old-done"), "the old terminal job's runs are deleted")
	assert.EqualValues(t, 1, runCount(t, db, "new-done"), "the recent job's runs are kept")
	assert.EqualValues(t, 1, runCount(t, db, "old-active"), "the active job's runs are kept")
}

func TestSchedulerPruneRetentionDeletesOldTerminalJobs(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	now := time.Now().UTC().Truncate(time.Second)
	ctx := clockCtx(context.Background(), NewFixedClock(now))

	seedFinished(t, db, "old-done", StateSucceeded, now.Add(-30*24*time.Hour))
	seedFinished(t, db, "new-done", StateSucceeded, now.Add(-time.Hour))

	sched := NewSchedulerWithConfig(SchedulerConfig{
		DB: db, Client: NewClient(db), RetentionMaxAge: 14 * 24 * time.Hour,
	})
	deleted, err := sched.PruneRetention(ctx)
	require.NoError(t, err)
	assert.EqualValues(t, 1, deleted)
	assert.False(t, jobExists(t, db, "old-done"))
	assert.True(t, jobExists(t, db, "new-done"))
}

func TestSchedulerPruneRetentionDisabledIsNoOp(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	now := time.Now().UTC().Truncate(time.Second)
	ctx := clockCtx(context.Background(), NewFixedClock(now))

	seedFinished(t, db, "old-done", StateSucceeded, now.Add(-30*24*time.Hour))

	sched := NewScheduler(db, NewClient(db)) // no RetentionMaxAge => disabled
	deleted, err := sched.PruneRetention(ctx)
	require.NoError(t, err)
	assert.Zero(t, deleted, "retention disabled deletes nothing")
	assert.True(t, jobExists(t, db, "old-done"))
}

func TestSchedulerRunFiresRetentionOnCadence(t *testing.T) {
	t.Parallel()
	db := newWALFileDB(t) // file-backed WAL so the Run goroutine and the poll don't deadlock
	now := time.Now().UTC().Truncate(time.Second)

	seedFinished(t, db, "old-done", StateSucceeded, now.Add(-30*24*time.Hour))

	sched := NewSchedulerWithConfig(SchedulerConfig{
		DB: db, Client: NewClient(db),
		TickInterval:      time.Hour, // keep the periodic and lease sweeps quiet
		SweepInterval:     time.Hour,
		RetentionMaxAge:   14 * 24 * time.Hour,
		RetentionInterval: 10 * time.Millisecond,
	})
	ctx, cancel := context.WithCancel(clockCtx(context.Background(), NewFixedClock(now)))
	defer cancel()
	go func() { _ = sched.Run(ctx) }()

	require.Eventually(t, func() bool {
		return !jobExists(t, db, "old-done")
	}, 3*time.Second, 10*time.Millisecond, "the retention ticker prunes the old terminal job on cadence")
}
