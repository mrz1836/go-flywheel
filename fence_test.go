package flywheel

import (
	"context"
	"testing"
	"time"

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
