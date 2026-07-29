package flywheel

import (
	"context"
	"testing"
	"time"

	"github.com/mrz1836/go-foundation/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// TestFinalizePersistsThroughCancelledContext covers A1: a finalize whose ctx is
// already cancelled (as during a drain) must still persist the job state and the
// run row rather than roll them back.
func TestFinalizePersistsThroughCancelledContext(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	d := baseDriver{db: db}
	now := time.Now().UTC().Truncate(time.Second)

	token := models.NewID()
	raw := RawJob{ID: "job-cancel-ctx", Kind: "k", Attempt: 1, MaxAttempts: 5, LeaseToken: token}
	seedJob(t, db, jobRow{
		ID: raw.ID, Kind: raw.Kind, State: string(StateRunning), LeaseToken: &token,
		Attempt: 1, MaxAttempts: 5, CreatedAt: now, UpdatedAt: now, ScheduledAt: now,
	})
	runID := models.NewID()
	require.NoError(t, d.InsertRunStub(context.Background(), runID, raw, now, ExecutorClass("local"), "h1"))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	out, finalizeErr := d.Finalize(
		ctx, raw, runID, Result{Output: map[string]any{"ok": true}}, nil, now.Add(time.Second),
	)
	require.NoError(t, finalizeErr)
	assert.False(t, out.Superseded, "the claim was held throughout")
	assert.Equal(t, StateSucceeded, out.State, "the reported state is the one persisted")

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

	token := models.NewID()
	raw := RawJob{ID: "job-superseded", Kind: "k", Attempt: 1, MaxAttempts: 5, LeaseToken: token}
	seedJob(t, db, jobRow{
		ID: raw.ID, Kind: raw.Kind, State: string(StateRunning), LeaseToken: &token,
		Attempt: 1, MaxAttempts: 5, CreatedAt: now, UpdatedAt: now, ScheduledAt: now, LeasedUntil: &now,
	})
	runID := models.NewID()
	require.NoError(t, d.InsertRunStub(ctx, runID, raw, now, ExecutorClass("local"), "h1"))

	// The job is cancelled out from under the running attempt.
	require.NoError(t, CancelJob(ctx, db, raw.ID))

	// The worker then returns success, with a follow-up.
	result := Result{
		Output:    map[string]any{"ok": true},
		FollowUps: []FollowUp{{Kind: "child", Args: map[string]any{}}},
	}
	out, finalizeErr := d.Finalize(ctx, raw, runID, result, nil, now.Add(time.Second))
	require.NoError(t, finalizeErr)
	assert.True(t, out.Superseded, "the cancel took the claim away")
	assert.Equal(t, StateCancelled, out.State, "the reported state is the one the cancel left, not the one planned")
	assert.Equal(t, OutcomeSuccess, out.RunOutcome, "the audit row still records the attempt's real outcome")
	assert.Zero(t, out.EnqueuedChildren)

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

	token := models.NewID()
	raw := RawJob{ID: "job-success", Kind: "k", Attempt: 1, MaxAttempts: 5, LeaseToken: token}
	seedJob(t, db, jobRow{
		ID: raw.ID, Kind: raw.Kind, State: string(StateRunning), LeaseToken: &token,
		Attempt: 1, MaxAttempts: 5, CreatedAt: now, UpdatedAt: now, ScheduledAt: now,
	})
	runID := models.NewID()
	require.NoError(t, d.InsertRunStub(ctx, runID, raw, now, ExecutorClass("local"), "h1"))

	result := Result{FollowUps: []FollowUp{{Kind: "child", Args: map[string]any{}}}}
	out, finalizeErr := d.Finalize(ctx, raw, runID, result, nil, now.Add(time.Second))
	require.NoError(t, finalizeErr)
	assert.False(t, out.Superseded)
	assert.Equal(t, 1, out.EnqueuedChildren, "the outcome reports the follow-up it enqueued")

	assert.Equal(t, string(StateSucceeded), jobState(t, db, raw.ID))
	var children int64
	require.NoError(t, db.Table("jobs").Where("kind = ?", "child").Count(&children).Error)
	assert.EqualValues(t, 1, children, "the success path enqueues the follow-up")

	var enqueued int
	require.NoError(t, db.Table("job_runs").Select("enqueued_children").Where("id = ?", runID).Scan(&enqueued).Error)
	assert.Equal(t, 1, enqueued)
}

// seedRunningJob seeds a running job holding token and its started run stub, and
// returns the raw job and run id — the common setup for the follow-up fan-out
// tests below.
func seedRunningJob(t *testing.T, db *gorm.DB, d baseDriver, id string, now time.Time) (RawJob, string) {
	t.Helper()
	token := models.NewID()
	raw := RawJob{ID: id, Kind: "k", Attempt: 1, MaxAttempts: 5, LeaseToken: token}
	seedJob(t, db, jobRow{
		ID: raw.ID, Kind: raw.Kind, State: string(StateRunning), LeaseToken: &token,
		Attempt: 1, MaxAttempts: 5, CreatedAt: now, UpdatedAt: now, ScheduledAt: now,
	})
	runID := models.NewID()
	require.NoError(t, d.InsertRunStub(context.Background(), runID, raw, now, ExecutorClass("local"), "h1"))
	return raw, runID
}

// TestFinalizeChunksLargeFollowUpFanOut is A5's success half: a 5000-child fan-out
// is enqueued and counted exactly. It also proves the insert is chunked — 5000
// rows × 22 columns exceed both dialects' bind-parameter caps (65535 on Postgres,
// 32766 on SQLite) in a single INSERT, so a finalize that succeeds can only have
// split them, ⌈5000/500⌉ = 10 statements at the default chunk size.
func TestFinalizeChunksLargeFollowUpFanOut(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	d := baseDriver{db: db}
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)

	raw, runID := seedRunningJob(t, db, d, "job-fanout", now)

	const n = 5000
	followUps := make([]FollowUp, n)
	for i := range followUps {
		followUps[i] = FollowUp{Kind: "child", Args: map[string]any{"n": i}}
	}
	out, finalizeErr := d.Finalize(ctx, raw, runID, Result{FollowUps: followUps}, nil, now.Add(time.Second))
	require.NoError(t, finalizeErr)
	assert.Equal(t, n, out.EnqueuedChildren, "every child is enqueued and counted exactly")

	var children int64
	require.NoError(t, db.Table("jobs").Where("kind = ?", "child").Count(&children).Error)
	assert.EqualValues(t, n, children)

	var enqueued int
	require.NoError(t, db.Table("job_runs").Select("enqueued_children").Where("id = ?", runID).Scan(&enqueued).Error)
	assert.Equal(t, n, enqueued, "the audit row records the exact fan-out")
}

// TestFinalizeFailsLoudlyOverFollowUpLimit is A5's failure half: one child past
// the configured limit fails the finalize with ErrFollowUpLimit and enqueues
// nothing — the whole transaction rolls back, so the state advance and the run
// row roll back with the fan-out.
func TestFinalizeFailsLoudlyOverFollowUpLimit(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	d := baseDriver{db: db, opts: DriverOpts{FollowUpLimit: 3}}
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)

	raw, runID := seedRunningJob(t, db, d, "job-overlimit", now)

	followUps := make([]FollowUp, d.opts.followUpLimit()+1)
	for i := range followUps {
		followUps[i] = FollowUp{Kind: "child", Args: map[string]any{"n": i}}
	}
	out, finalizeErr := d.Finalize(ctx, raw, runID, Result{FollowUps: followUps}, nil, now.Add(time.Second))
	require.ErrorIs(t, finalizeErr, ErrFollowUpLimit)
	assert.Zero(t, out.EnqueuedChildren)

	var children int64
	require.NoError(t, db.Table("jobs").Where("kind = ?", "child").Count(&children).Error)
	assert.Zero(t, children, "nothing is enqueued when the fan-out exceeds the limit")
	assert.Equal(t, string(StateRunning), jobState(t, db, raw.ID), "the state advance rolled back with the fan-out")

	var outcome string
	require.NoError(t, db.Table("job_runs").Select("outcome").Where("id = ?", runID).Scan(&outcome).Error)
	assert.Equal(t, string(OutcomeStarted), outcome, "the run row was not advanced")
}

// TestFinalizeAllowsFollowUpsAtLimit pins the boundary: a fan-out exactly at the
// limit succeeds (only a count strictly greater fails), across more than one
// chunk.
func TestFinalizeAllowsFollowUpsAtLimit(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	d := baseDriver{db: db, opts: DriverOpts{FollowUpLimit: 3, FollowUpChunkSize: 2}}
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)

	raw, runID := seedRunningJob(t, db, d, "job-atlimit", now)

	followUps := make([]FollowUp, 3) // exactly the limit → 2 chunks at size 2
	for i := range followUps {
		followUps[i] = FollowUp{Kind: "child", Args: map[string]any{"n": i}}
	}
	out, finalizeErr := d.Finalize(ctx, raw, runID, Result{FollowUps: followUps}, nil, now.Add(time.Second))
	require.NoError(t, finalizeErr)
	assert.Equal(t, 3, out.EnqueuedChildren)

	var children int64
	require.NoError(t, db.Table("jobs").Where("kind = ?", "child").Count(&children).Error)
	assert.EqualValues(t, 3, children)
}

// TestFinalizeSkipsCollidingFollowUp proves the chunked fan-out preserves the old
// per-child ErrAlreadyEnqueued swallow: a child whose unique_key already exists is
// skipped by ON CONFLICT DO NOTHING, not counted, and not fatal.
func TestFinalizeSkipsCollidingFollowUp(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	d := baseDriver{db: db}
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)

	// A child with this unique_key is already enqueued.
	_, err := Enqueue(ctx, NewClient(db), "child", []byte(`{}`), InsertOpts{UniqueKey: "dup"})
	require.NoError(t, err)

	raw, runID := seedRunningJob(t, db, d, "job-collide", now)

	result := Result{FollowUps: []FollowUp{
		{Kind: "child", Args: map[string]any{"n": 1}},
		{Kind: "child", Args: map[string]any{"n": 2}, UniqueKey: "dup"}, // collides, skipped
		{Kind: "child", Args: map[string]any{"n": 3}},
	}}
	out, finalizeErr := d.Finalize(ctx, raw, runID, result, nil, now.Add(time.Second))
	require.NoError(t, finalizeErr)
	assert.Equal(t, 2, out.EnqueuedChildren, "the colliding child is skipped, not counted, not fatal")

	var children int64
	require.NoError(t, db.Table("jobs").Where("kind = ?", "child").Count(&children).Error)
	assert.EqualValues(t, 3, children, "the pre-existing child plus the two that landed")
}
