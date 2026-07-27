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

// seedClaimable writes n available jobs on the default queue, ready now, and
// returns their ids in insertion order. It is the fixture the fence tests share:
// every one of them needs rows a Dequeue will actually pick up, and nothing
// else about the rows matters.
//
// scheduled_at is backdated and left in the driver's own zone rather than
// normalized to UTC. SQLite compares datetimes as text, so a UTC-rendered
// timestamp sorts against a locally-rendered one by offset digits rather than by
// instant — which reads as "not yet due" and claims nothing.
func seedClaimable(t testing.TB, db *gorm.DB, n int) []string {
	t.Helper()
	now := time.Now().Add(-time.Minute)
	ids := make([]string, n)
	for i := range n {
		ids[i] = "fence-job-" + string(rune('a'+i))
		seedJob(t, db, jobRow{
			ID: ids[i], Kind: "k", Queue: "default", State: string(StateAvailable),
			MaxAttempts: 5, CreatedAt: now, UpdatedAt: now, ScheduledAt: now,
		})
	}
	return ids
}

// leaseToken reads a job's stored lease token, nil when the column is null.
func leaseToken(t testing.TB, db *gorm.DB, jobID string) *string {
	t.Helper()
	var row jobRow
	require.NoError(t, db.Select("lease_token").Where("id = ?", jobID).First(&row).Error)
	return row.LeaseToken
}

// TestFenceClaimStampsATokenOnEveryRow covers FR-05-07's per-batch half: one
// claim mints one token and stamps it on every row it took, and what the driver
// hands back matches what it wrote.
func TestFenceClaimStampsATokenOnEveryRow(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	d := NewSQLiteDriver(db)
	ctx := context.Background()

	seedClaimable(t, db, 3)

	batch, err := d.Dequeue(ctx, []string{"default"}, AnyClass, true, 3, time.Minute)
	require.NoError(t, err)
	require.Len(t, batch, 3)

	token := batch[0].LeaseToken
	assert.NotEmpty(t, token, "a claim mints a token")
	for _, raw := range batch {
		assert.Equal(t, token, raw.LeaseToken, "one token per claim call, stamped on the whole batch")
		stored := leaseToken(t, db, raw.ID)
		require.NotNil(t, stored, "the claimed row carries the token")
		assert.Equal(t, token, *stored, "the returned token is the one persisted")
	}
}

// TestFenceEachClaimMintsAFreshToken covers FR-05-07's across-claims half: the
// token distinguishes a claim from any prior or subsequent claim of the same
// job. Without this, a reclaimed job would still match its previous attempt's
// finalize and the fence would be decorative.
func TestFenceEachClaimMintsAFreshToken(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	d := NewSQLiteDriver(db)
	ctx := context.Background()

	id := seedClaimable(t, db, 1)[0]

	first, err := d.Dequeue(ctx, []string{"default"}, AnyClass, true, 1, time.Minute)
	require.NoError(t, err)
	require.Len(t, first, 1)

	// Release the job the way the sweep does, then claim it again.
	require.NoError(t, db.Model(&jobRow{}).Where("id = ?", id).Updates(map[string]any{
		"state": string(StateAvailable), "leased_until": nil, "lease_token": nil,
	}).Error)

	second, err := d.Dequeue(ctx, []string{"default"}, AnyClass, true, 1, time.Minute)
	require.NoError(t, err)
	require.Len(t, second, 1)

	assert.NotEqual(t, first[0].LeaseToken, second[0].LeaseToken,
		"a re-claim mints a token distinct from the claim it superseded")
	assert.Equal(t, first[0].ID, second[0].ID, "both claims are of the same job")
}

// TestFenceUnclaimedRowHasNoToken is the other half of the column's meaning: it
// is null for exactly as long as no claim holds the job. A token that lingered
// past a release would make "released" indistinguishable from "held" in a
// database an operator is querying by hand.
func TestFenceUnclaimedRowHasNoToken(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	d := NewSQLiteDriver(db)
	ctx := context.Background()

	ids := seedClaimable(t, db, 2)
	assert.Nil(t, leaseToken(t, db, ids[0]), "a freshly enqueued job holds no token")

	// Claim exactly one of the two.
	batch, err := d.Dequeue(ctx, []string{"default"}, AnyClass, true, 1, time.Minute)
	require.NoError(t, err)
	require.Len(t, batch, 1)

	unclaimed := ids[0]
	if batch[0].ID == unclaimed {
		unclaimed = ids[1]
	}
	assert.Nil(t, leaseToken(t, db, unclaimed), "a row this claim did not take is left untouched")
}

// TestFenceSupersededFinalizeAdvancesNothing is A4, and it is the guarantee the
// whole fence exists for.
//
// A job reclaimed by the sweep is running again under a *different* attempt.
// Guarded on state alone the original attempt's finalize still matches the row
// and advances it, so whichever attempt finishes first wins regardless of which
// one holds the lease. Guarded on the token it matches nothing: the audit row is
// written, and the job — state, scheduled_at, finalized_at, token — is left
// exactly as the new claim left it.
func TestFenceSupersededFinalizeAdvancesNothing(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	d := NewSQLiteDriver(db)
	ctx := context.Background()

	seedClaimable(t, db, 1)

	// The first attempt claims the job and commits its audit stub.
	first, err := d.Dequeue(ctx, []string{"default"}, AnyClass, true, 1, time.Millisecond)
	require.NoError(t, err)
	require.Len(t, first, 1)
	runID := models.NewID()
	startedAt := time.Now()
	require.NoError(t, d.InsertRunStub(ctx, runID, first[0], startedAt, "local", "exec-1"))

	// Its lease expires and the sweep reclaims it; a second attempt takes it.
	reclaimed, err := d.Sweep(ctx, time.Now().Add(time.Second))
	require.NoError(t, err)
	require.Equal(t, 1, reclaimed)

	second, err := d.Dequeue(ctx, []string{"default"}, AnyClass, true, 1, time.Minute)
	require.NoError(t, err)
	require.Len(t, second, 1)
	require.NotEqual(t, first[0].LeaseToken, second[0].LeaseToken)

	// Only now does the first attempt finish — successfully, with a follow-up.
	result := Result{
		Output:    map[string]any{"ok": true},
		FollowUps: []FollowUp{{Kind: "fence.child", Args: map[string]any{}}},
	}
	out, err := d.Finalize(ctx, first[0], runID, result, nil, time.Now())
	require.NoError(t, err)
	assert.True(t, out.Superseded, "the finalize reports that it persisted no state advance")
	assert.Equal(t, StateRunning, out.State, "and reports the state the superseding claim left")
	assert.Equal(t, OutcomeSuccess, out.RunOutcome, "while still reporting the attempt's real outcome")
	assert.Zero(t, out.EnqueuedChildren)

	var row jobRow
	require.NoError(t, db.Where("id = ?", first[0].ID).First(&row).Error)
	assert.Equal(t, string(StateRunning), row.State, "the superseded attempt does not advance the job's state")
	assert.Nil(t, row.FinalizedAt, "nor stamp it finalized")
	require.NotNil(t, row.LeaseToken)
	assert.Equal(t, second[0].LeaseToken, *row.LeaseToken, "nor clear the claim it no longer holds")

	var children int64
	require.NoError(t, db.Table("jobs").Where("kind = ?", "fence.child").Count(&children).Error)
	assert.Zero(t, children, "a superseded finalize enqueues no follow-ups")

	// The attempt happened, so it is audited with its real outcome — losing the
	// claim discards the outcome's effect on the job, not the record of the work.
	var outcome string
	require.NoError(t, db.Table("job_runs").Select("outcome").Where("id = ?", runID).Scan(&outcome).Error)
	assert.Equal(t, string(OutcomeSuccess), outcome, "the attempt is audited with the outcome it actually had")
}

// TestFenceSweepClearsTheToken covers FR-05-11 directly. The sweep is the one
// reclaim path with no attempt on the other side of it to clear the token, so a
// sweep that returned the job to available while leaving its token set would
// leave the previous attempt able to finalize over the next claim.
func TestFenceSweepClearsTheToken(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	d := NewSQLiteDriver(db)
	ctx := context.Background()

	seedClaimable(t, db, 1)
	batch, err := d.Dequeue(ctx, []string{"default"}, AnyClass, true, 1, time.Millisecond)
	require.NoError(t, err)
	require.Len(t, batch, 1)
	require.NotNil(t, leaseToken(t, db, batch[0].ID))

	reclaimed, err := d.Sweep(ctx, time.Now().Add(time.Second))
	require.NoError(t, err)
	assert.Equal(t, 1, reclaimed)
	assert.Nil(t, leaseToken(t, db, batch[0].ID), "a reclaimed job holds no token")
}

// TestFenceOperatorActionsClearTheToken covers the other two paths out of the
// running state. Both already null leased_until; both must null the token for
// the same reason, or the attempt still running finalizes over the operator's
// action.
func TestFenceOperatorActionsClearTheToken(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		act  func(ctx context.Context, db *gorm.DB, id string) error
	}{
		{"cancel", CancelJob},
		{"retry", RetryJob},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			db := newDB(t)
			d := NewSQLiteDriver(db)
			ctx := context.Background()

			seedClaimable(t, db, 1)
			batch, err := d.Dequeue(ctx, []string{"default"}, AnyClass, true, 1, time.Minute)
			require.NoError(t, err)
			require.Len(t, batch, 1)

			require.NoError(t, tc.act(ctx, db, batch[0].ID))
			assert.Nil(t, leaseToken(t, db, batch[0].ID), "the operator action releases the claim")

			// And the running attempt's finalize is now a true no-op.
			runID := models.NewID()
			require.NoError(t, d.InsertRunStub(ctx, runID, batch[0], time.Now(), "local", "exec-1"))
			out, err := d.Finalize(ctx, batch[0], runID, Result{}, nil, time.Now())
			require.NoError(t, err)
			assert.True(t, out.Superseded)
			assert.NotEqual(t, string(StateSucceeded), jobState(t, db, batch[0].ID),
				"the finishing worker does not overwrite the operator's action")
		})
	}
}
