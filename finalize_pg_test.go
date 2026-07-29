//go:build integration

package flywheel

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestFinalizeChunksLargeFollowUpFanOutPostgres is A5's success half on Postgres:
// a 5000-child fan-out is enqueued and counted exactly. 5000 rows × 22 columns
// exceed Postgres's 65535 bind-parameter ceiling in a single INSERT, so a
// finalize that succeeds can only have chunked them.
func TestFinalizeChunksLargeFollowUpFanOutPostgres(t *testing.T) {
	t.Parallel()
	db := NewPostgresIsolatedDB(t)
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

// TestFinalizeFailsLoudlyOverFollowUpLimitPostgres is A5's failure half on
// Postgres: one child past the limit fails the finalize with ErrFollowUpLimit and
// rolls the whole transaction back — nothing enqueued, state and run row unchanged.
func TestFinalizeFailsLoudlyOverFollowUpLimitPostgres(t *testing.T) {
	t.Parallel()
	db := NewPostgresIsolatedDB(t)
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

// TestFinalizeSkipsCollidingFollowUpPostgres proves the chunked fan-out preserves
// the per-child ErrAlreadyEnqueued swallow on Postgres: a child whose unique_key
// already exists is skipped by ON CONFLICT DO NOTHING, not counted, not fatal.
func TestFinalizeSkipsCollidingFollowUpPostgres(t *testing.T) {
	t.Parallel()
	db := NewPostgresIsolatedDB(t)
	d := baseDriver{db: db}
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)

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
