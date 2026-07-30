package flywheel

import (
	"context"
	"testing"
	"time"

	"github.com/mrz1836/go-foundation/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// dbLimiterCtx wraps ctx with a manual clock so a DBLimiter test drives refill and
// hold expiry deterministically.
func dbLimiterCtx(clk *manualClock) context.Context {
	return models.WithClock(context.Background(), clk)
}

// --- construction ------------------------------------------------------------

// TestDBLimiterConstructorValidation proves a configuration that could never admit
// work is rejected at construction rather than surfacing as a silent zero budget.
func TestDBLimiterConstructorValidation(t *testing.T) {
	t.Parallel()
	db := newDB(t)

	_, err := NewDBLimiter(nil, DBLimiterConfig{})
	require.Error(t, err, "a nil db is rejected")

	for name, cfg := range map[string]DBLimiterConfig{
		"negative rate":              {Rate: -1},
		"rate without interval":      {Rate: 10},
		"max concurrent without ttl": {MaxConcurrent: 5},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			_, err := NewDBLimiter(db, cfg)
			require.Error(t, err)
		})
	}

	_, err = NewDBLimiter(db, DBLimiterConfig{Rate: 10, Interval: time.Second, MaxConcurrent: 5, HoldTTL: time.Minute})
	require.NoError(t, err, "a complete config constructs")
}

// TestDBLimiterShortCircuitsWhenUnbounded proves an all-off limiter grants without
// touching the database — no bucket row, no round trip.
func TestDBLimiterShortCircuitsWhenUnbounded(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	lim, err := NewDBLimiter(db, DBLimiterConfig{})
	require.NoError(t, err)

	g, err := lim.Acquire(context.Background(), "r", 7)
	require.NoError(t, err)
	assert.Equal(t, 7, g.N, "an unbounded limiter grants the whole ask")

	var buckets int64
	require.NoError(t, db.Table("limiter_buckets").Count(&buckets).Error)
	assert.Zero(t, buckets, "no bucket row is created when both caps are off")
}

// --- A9 (FR-08-11): the same budget under the SQLite write-lock --------------

// TestDBLimiterRateBudgetOnSQLite runs the rate path on a single-connection
// in-memory SQLite database — the DSN shape whose BEGIN IMMEDIATE write-lock
// serializes writers in place of FOR UPDATE — and asserts the budget is enforced
// exactly as it would be on Postgres.
func TestDBLimiterRateBudgetOnSQLite(t *testing.T) {
	t.Parallel()
	db := newSingleConnMemoryDB(t)
	clk := newManualClock(time.Unix(1_000_000, 0))
	ctx := dbLimiterCtx(clk)

	lim, err := NewDBLimiter(db, DBLimiterConfig{Rate: 3, Interval: time.Second, Burst: 3})
	require.NoError(t, err)

	g, err := lim.Acquire(ctx, "provider:x", 10)
	require.NoError(t, err)
	assert.Equal(t, 3, g.N, "the initial burst is capped at Burst")

	g, err = lim.Acquire(ctx, "provider:x", 10)
	require.NoError(t, err)
	assert.Zero(t, g.N, "a drained bucket grants nothing")
	assert.Positive(t, g.RetryAfter, "a rate-bound denial hints when to retry")

	clk.advance(time.Second) // one interval refills the whole burst
	g, err = lim.Acquire(ctx, "provider:x", 10)
	require.NoError(t, err)
	assert.Equal(t, 3, g.N, "the budget refills at Rate under the write-lock")
}

// TestDBLimiterConcurrencyBudgetOnSQLite proves the concurrency ceiling and its
// Release path work under the same write-lock.
func TestDBLimiterConcurrencyBudgetOnSQLite(t *testing.T) {
	t.Parallel()
	db := newSingleConnMemoryDB(t)
	ctx := context.Background()
	lim, err := NewDBLimiter(db, DBLimiterConfig{MaxConcurrent: 3, HoldTTL: time.Minute})
	require.NoError(t, err)

	g1, err := lim.Acquire(ctx, "r", 2)
	require.NoError(t, err)
	require.Equal(t, 2, g1.N)
	require.NotEmpty(t, g1.Token, "a concurrency grant carries a token")

	g2, err := lim.Acquire(ctx, "r", 2)
	require.NoError(t, err)
	require.Equal(t, 1, g2.N, "only one of the ceiling remained")

	g3, err := lim.Acquire(ctx, "r", 2)
	require.NoError(t, err)
	require.Zero(t, g3.N, "the ceiling is full")

	lim.Release(ctx, g1)
	g4, err := lim.Acquire(ctx, "r", 2)
	require.NoError(t, err)
	assert.Equal(t, 2, g4.N, "releasing g1 freed exactly two")
}

// --- FR-08-05: inline expiry reclaim self-heals without a sweeper ------------

// TestDBLimiterInlineReclaimSelfHeals is the crashed-holder case: a permit whose
// TTL has lapsed is reclaimed by the next Acquire itself — no sweeper — so a
// process that died holding capacity does not permanently reduce the budget.
func TestDBLimiterInlineReclaimSelfHeals(t *testing.T) {
	t.Parallel()
	db := newSingleConnMemoryDB(t)
	clk := newManualClock(time.Unix(1_000_000, 0))
	ctx := dbLimiterCtx(clk)
	lim, err := NewDBLimiter(db, DBLimiterConfig{MaxConcurrent: 2, HoldTTL: time.Second})
	require.NoError(t, err)

	// Take the whole ceiling and then abandon the grant (a crashed holder).
	g1, err := lim.Acquire(ctx, "r", 2)
	require.NoError(t, err)
	require.Equal(t, 2, g1.N)

	g2, err := lim.Acquire(ctx, "r", 2)
	require.NoError(t, err)
	require.Zero(t, g2.N, "the ceiling is full while the holder is alive")

	// Its TTL lapses. The next Acquire reclaims it inline, before counting.
	clk.advance(2 * time.Second)
	g3, err := lim.Acquire(ctx, "r", 2)
	require.NoError(t, err)
	assert.Equal(t, 2, g3.N, "the lapsed reservation is reclaimed inline, with no sweeper")

	var live int64
	require.NoError(t, db.Table("limiter_holds").Where("resource = ? AND n > 0", "r").
		Where("expires_at > ?", clk.Now(ctx)).Count(&live).Error)
	assert.EqualValues(t, 1, live, "only the fresh grant's hold survives")
}

// --- Release: atomic, idempotent, partial ------------------------------------

// TestDBLimiterReleaseIsIdempotent proves a double or over-large Release cannot
// drive the held count negative, and that a partial release frees only its share.
func TestDBLimiterReleaseIsIdempotent(t *testing.T) {
	t.Parallel()
	db := newSingleConnMemoryDB(t)
	ctx := context.Background()
	lim, err := NewDBLimiter(db, DBLimiterConfig{MaxConcurrent: 5, HoldTTL: time.Minute})
	require.NoError(t, err)

	g, err := lim.Acquire(ctx, "r", 4)
	require.NoError(t, err)
	require.Equal(t, 4, g.N)

	// Release two of the four, then release the whole grant twice more.
	lim.Release(ctx, Grant{Resource: "r", N: 2, Token: g.Token})
	lim.Release(ctx, g)
	lim.Release(ctx, g)

	// All four are back: the ceiling grants a fresh five.
	g2, err := lim.Acquire(ctx, "r", 5)
	require.NoError(t, err)
	assert.Equal(t, 5, g2.N, "every permit returned exactly once; none double-counted")
}

// --- Sweep: bounded reclamation of expired and drained holds -----------------

// TestDBLimiterSweepReapsExpiredAndDrained proves the host-driven sweeper deletes
// the holds Acquire's inline reclaim would eventually reach — an expired hold and
// a drained (n<=0) straggler — while leaving a live one alone.
func TestDBLimiterSweepReapsExpiredAndDrained(t *testing.T) {
	t.Parallel()
	db := newSingleConnMemoryDB(t)
	anchor := time.Unix(1_000_000, 0)
	ctx := dbLimiterCtx(newManualClock(anchor))
	lim, err := NewDBLimiter(db, DBLimiterConfig{MaxConcurrent: 100, HoldTTL: time.Hour})
	require.NoError(t, err)

	live, err := lim.Acquire(ctx, "r", 1)
	require.NoError(t, err)
	expired, err := lim.Acquire(ctx, "r", 1)
	require.NoError(t, err)
	drained, err := lim.Acquire(ctx, "r", 1)
	require.NoError(t, err)

	// Age one hold past its TTL and drain another to n=0 directly.
	require.NoError(t, db.Table("limiter_holds").Where("token = ?", expired.Token).
		Update("expires_at", anchor.Add(-time.Hour)).Error)
	require.NoError(t, db.Table("limiter_holds").Where("token = ?", drained.Token).
		Update("n", 0).Error)

	reclaimed, err := lim.Sweep(dbLimiterCtx(newManualClock(anchor)))
	require.NoError(t, err)
	assert.Equal(t, 2, reclaimed, "the expired and the drained holds are reaped")

	var survivors []string
	require.NoError(t, db.Table("limiter_holds").Pluck("token", &survivors).Error)
	assert.Equal(t, []string{live.Token}, survivors, "only the live hold survives")
}
