package flywheel

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestFinalizePersistsThroughCancelledContext covers A1: a finalize whose ctx is
// already cancelled (as during a drain) must still persist the job state and the
// run row rather than roll them back.
func TestFinalizePersistsThroughCancelledContext(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	d := baseDriver{db: db}
	now := time.Now().UTC().Truncate(time.Second)

	raw := RawJob{ID: "job-cancel-ctx", Kind: "k", Attempt: 1, MaxAttempts: 5}
	seedJob(t, db, jobRow{
		ID: raw.ID, Kind: raw.Kind, State: string(StateRunning),
		Attempt: 1, MaxAttempts: 5, CreatedAt: now, UpdatedAt: now, ScheduledAt: now,
	})
	runID := NewID()
	require.NoError(t, d.InsertRunStub(context.Background(), runID, raw, now, ExecutorClass("local"), "h1"))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	require.NoError(t, d.Finalize(
		ctx, raw, runID, Result{Output: map[string]any{"ok": true}}, nil, now.Add(time.Second),
	))

	assert.Equal(t, string(StateSucceeded), jobState(t, db, raw.ID), "the job state persists despite the cancelled ctx")
	var outcome string
	require.NoError(t, db.Table("job_runs").Select("outcome").Where("id = ?", runID).Scan(&outcome).Error)
	assert.Equal(t, string(OutcomeSuccess), outcome, "the run row persists despite the cancelled ctx")
}

// TestFinalizeSkipsSupersededCancel covers A2(a): a job cancelled out from under
// a running attempt (operator CancelJob, or a worker cancelling its own job
// mid-run) stays cancelled when the attempt finishes — the finishing worker does
// not overwrite it — while the attempt is still audited and no follow-ups fire.
func TestFinalizeSkipsSupersededCancel(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	d := baseDriver{db: db}
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)

	raw := RawJob{ID: "job-superseded", Kind: "k", Attempt: 1, MaxAttempts: 5}
	seedJob(t, db, jobRow{
		ID: raw.ID, Kind: raw.Kind, State: string(StateRunning),
		Attempt: 1, MaxAttempts: 5, CreatedAt: now, UpdatedAt: now, ScheduledAt: now, LeasedUntil: &now,
	})
	runID := NewID()
	require.NoError(t, d.InsertRunStub(ctx, runID, raw, now, ExecutorClass("local"), "h1"))

	// The job is cancelled out from under the running attempt.
	require.NoError(t, CancelJob(ctx, db, raw.ID))

	// The worker then returns success, with a follow-up.
	result := Result{
		Output:    map[string]any{"ok": true},
		FollowUps: []FollowUp{{Kind: "child", Args: map[string]any{}}},
	}
	require.NoError(t, d.Finalize(ctx, raw, runID, result, nil, now.Add(time.Second)))

	assert.Equal(t, string(StateCancelled), jobState(t, db, raw.ID), "cancel is not overwritten by the finishing worker")
	assert.EqualValues(t, 1, runCount(t, db, raw.ID), "the attempt is still audited exactly once")

	var children int64
	require.NoError(t, db.Table("jobs").Where("kind = ?", "child").Count(&children).Error)
	assert.Zero(t, children, "a superseded finalize enqueues no follow-ups")

	var enqueued int
	require.NoError(t, db.Table("job_runs").Select("enqueued_children").Where("id = ?", runID).Scan(&enqueued).Error)
	assert.Zero(t, enqueued, "the audit row records that no children were enqueued")
}

// TestFinalizeSuccessPathEnqueuesFollowUps covers A2(b): the normal success path
// is unchanged — the state advances to succeeded and follow-ups are enqueued.
func TestFinalizeSuccessPathEnqueuesFollowUps(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	d := baseDriver{db: db}
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)

	raw := RawJob{ID: "job-success", Kind: "k", Attempt: 1, MaxAttempts: 5}
	seedJob(t, db, jobRow{
		ID: raw.ID, Kind: raw.Kind, State: string(StateRunning),
		Attempt: 1, MaxAttempts: 5, CreatedAt: now, UpdatedAt: now, ScheduledAt: now,
	})
	runID := NewID()
	require.NoError(t, d.InsertRunStub(ctx, runID, raw, now, ExecutorClass("local"), "h1"))

	result := Result{FollowUps: []FollowUp{{Kind: "child", Args: map[string]any{}}}}
	require.NoError(t, d.Finalize(ctx, raw, runID, result, nil, now.Add(time.Second)))

	assert.Equal(t, string(StateSucceeded), jobState(t, db, raw.ID))
	var children int64
	require.NoError(t, db.Table("jobs").Where("kind = ?", "child").Count(&children).Error)
	assert.EqualValues(t, 1, children, "the success path enqueues the follow-up")

	var enqueued int
	require.NoError(t, db.Table("job_runs").Select("enqueued_children").Where("id = ?", runID).Scan(&enqueued).Error)
	assert.Equal(t, 1, enqueued)
}
