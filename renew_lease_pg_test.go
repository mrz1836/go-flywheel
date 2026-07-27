//go:build integration

package flywheel

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRenewLeasePostgres is the dialect-parity half. RenewLease has no dialect
// split — it is one guarded UPDATE both drivers inherit — so this proves the
// claim it is guarded against is the one Postgres actually wrote, not that the
// SQL differs.
func TestRenewLeasePostgres(t *testing.T) {
	db := NewPostgresIsolatedDB(t)
	d := NewPostgresDriver(db)
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
	assert.True(t, held, "the token the CTE claim minted is the one the renewal matches")

	after := leasedUntil(t, db, batch[0].ID)
	require.NotNil(t, after)
	assert.WithinDuration(t, until, *after, time.Second)

	held, err = d.RenewLease(ctx, batch[0].ID, "some-other-token", until.Add(time.Hour))
	require.NoError(t, err)
	assert.False(t, held, "a token that never held this claim renews nothing")
}
