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

// leasedUntil reads a job's lease expiry, nil when it holds no lease.
func leasedUntil(t testing.TB, db *gorm.DB, jobID string) *time.Time {
	t.Helper()
	var row jobRow
	require.NoError(t, db.Select("leased_until").Where("id = ?", jobID).First(&row).Error)
	return row.LeasedUntil
}

// TestRenewLeaseExtendsAHeldClaim is the happy path: the claim that holds the
// job can push its expiry out, and the reported held is true.
func TestRenewLeaseExtendsAHeldClaim(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	d := NewSQLiteDriver(db)
	ctx := context.Background()

	seedClaimable(t, db, 1)
	batch, err := d.Dequeue(ctx, []string{"default"}, AnyClass, true, 1, time.Minute)
	require.NoError(t, err)
	require.Len(t, batch, 1)

	before := leasedUntil(t, db, batch[0].ID)
	require.NotNil(t, before)

	until := before.Add(time.Hour)
	held, err := d.RenewLease(ctx, batch[0].ID, batch[0].LeaseToken, until)
	require.NoError(t, err)
	assert.True(t, held, "the claim that holds the job renews it")

	after := leasedUntil(t, db, batch[0].ID)
	require.NotNil(t, after)
	assert.True(t, after.After(*before), "leased_until moved forward")
	assert.WithinDuration(t, until, *after, time.Second, "renewal writes the absolute expiry it was given")
}

// TestRenewLeaseReportsALostClaim is the lost-claim contract at the driver
// seam: an attempt whose claim was superseded is told so, and — critically —
// does not extend the lease the *new* claim is relying on.
//
// This is the failure the heartbeat would otherwise cause rather than prevent.
// A renewal guarded on state alone would match the reclaimed row, and the
// superseded attempt would hold the new attempt's job open indefinitely.
func TestRenewLeaseReportsALostClaim(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	d := NewSQLiteDriver(db)
	ctx := context.Background()

	seedClaimable(t, db, 1)
	first, err := d.Dequeue(ctx, []string{"default"}, AnyClass, true, 1, time.Millisecond)
	require.NoError(t, err)
	require.Len(t, first, 1)

	reclaimed, err := d.Sweep(ctx, time.Now().Add(time.Second))
	require.NoError(t, err)
	require.Equal(t, 1, reclaimed)

	second, err := d.Dequeue(ctx, []string{"default"}, AnyClass, true, 1, time.Minute)
	require.NoError(t, err)
	require.Len(t, second, 1)
	newExpiry := leasedUntil(t, db, second[0].ID)
	require.NotNil(t, newExpiry)

	held, err := d.RenewLease(ctx, first[0].ID, first[0].LeaseToken, time.Now().Add(24*time.Hour))
	require.NoError(t, err, "a lost claim is an outcome, not a failure")
	assert.False(t, held, "the superseded attempt is told its claim is gone")

	still := leasedUntil(t, db, second[0].ID)
	require.NotNil(t, still)
	assert.WithinDuration(t, *newExpiry, *still, time.Second,
		"a superseded attempt cannot extend the lease the new claim holds")
}

// TestRenewLeaseRejectsANonRunningJob covers the state half of the guard. A job
// that finished, was cancelled, or was retried is no longer anyone's to renew
// even if a stale token were somehow presented.
func TestRenewLeaseRejectsANonRunningJob(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	d := NewSQLiteDriver(db)
	ctx := context.Background()
	now := time.Now()

	token := models.NewID()
	seedJob(t, db, jobRow{
		ID: "renew-terminal", Kind: "k", Queue: "default", State: string(StateSucceeded),
		LeaseToken: &token, MaxAttempts: 5, CreatedAt: now, UpdatedAt: now, ScheduledAt: now,
	})

	held, err := d.RenewLease(ctx, "renew-terminal", token, now.Add(time.Hour))
	require.NoError(t, err)
	assert.False(t, held, "a job that is not running holds no renewable claim")
}

// TestRenewLeaseUnknownJobIsNotAnError is the missing-row case, kept distinct
// from the superseded one only in that there is nothing left to supersede. Both
// mean the same thing to a caller — stop renewing — so both report held=false
// rather than an error a caller would have to classify.
func TestRenewLeaseUnknownJobIsNotAnError(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	d := NewSQLiteDriver(db)

	held, err := d.RenewLease(context.Background(), "no-such-job", "no-such-token", time.Now().Add(time.Hour))
	require.NoError(t, err)
	assert.False(t, held)
}
