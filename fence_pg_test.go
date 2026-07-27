//go:build integration

package flywheel

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestFenceClaimStampsATokenPostgres is the dialect-parity half of the fence
// claim. The two Dequeues share no SQL — one is a single CTE with
// RETURNING, the other a SELECT-then-UPDATE — so "the claim stamps a token"
// proven on SQLite proves nothing about the statement that runs in production.
func TestFenceClaimStampsATokenPostgres(t *testing.T) {
	db := NewPostgresIsolatedDB(t)
	d := NewPostgresDriver(db)
	ctx := context.Background()

	ids := seedClaimable(t, db, 3)

	batch, err := d.Dequeue(ctx, []string{"default"}, AnyClass, true, 2, time.Minute)
	require.NoError(t, err)
	require.Len(t, batch, 2)

	token := batch[0].LeaseToken
	assert.NotEmpty(t, token, "the CTE claim mints a token")
	for _, raw := range batch {
		assert.Equal(t, token, raw.LeaseToken, "one token per claim call, stamped on the whole batch")
		stored := leaseToken(t, db, raw.ID)
		require.NotNil(t, stored, "the claimed row carries the token")
		assert.Equal(t, token, *stored,
			"the token the driver returns is the one it wrote — it is not read back from RETURNING")
	}

	// The third row was outside the LIMIT, so nothing touched it.
	claimedIDs := map[string]bool{batch[0].ID: true, batch[1].ID: true}
	for _, id := range ids {
		if claimedIDs[id] {
			continue
		}
		assert.Nil(t, leaseToken(t, db, id), "a row this claim did not take is left untouched")
	}

	// A second claim of the remaining row mints its own token.
	next, err := d.Dequeue(ctx, []string{"default"}, AnyClass, true, 1, time.Minute)
	require.NoError(t, err)
	require.Len(t, next, 1)
	assert.NotEqual(t, token, next[0].LeaseToken, "each claim call mints a fresh token")
}
