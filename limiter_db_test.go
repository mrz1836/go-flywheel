package flywheel

import (
	"context"
	"log/slog"
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

// --- the same budget under the SQLite write-lock ---------------------------

// TestDBLimiterRateBudgetOnSQLite runs the rate path on a single-connection
// in-memory SQLite database — where the one-connection pool serializes writers in
// place of the FOR UPDATE the limiter omits on SQLite — and asserts the budget is
// enforced exactly as it would be on Postgres.
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

// --- inline expiry reclaim self-heals without a sweeper --------------------

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

// --- refill: whole tokens, exact carry, top-out snap ------------------------

// TestDBLimiterRefillBranches drives refill directly across its arms: rate off,
// no elapsed, a sub-token elapse that carries the fraction, a partial refill that
// advances refilled_at by the tokens' exact cost, reaching burst on a partial
// elapse, and a top-out past burstTime.
func TestDBLimiterRefillBranches(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	anchor := time.Unix(1_000_000, 0)

	t.Run("rate off is a no-op", func(t *testing.T) {
		t.Parallel()
		lim, err := NewDBLimiter(db, DBLimiterConfig{MaxConcurrent: 1, HoldTTL: time.Minute}) // rate 0
		require.NoError(t, err)
		b := limiterBucketRow{Tokens: 2, RefilledAt: anchor}
		lim.refill(&b, anchor.Add(time.Hour))
		assert.Equal(t, 2, b.Tokens, "with rate limiting off, refill changes nothing")
		assert.Equal(t, anchor, b.RefilledAt)
	})

	t.Run("no elapsed time is a no-op", func(t *testing.T) {
		t.Parallel()
		lim, err := NewDBLimiter(db, DBLimiterConfig{Rate: 1, Interval: time.Second, Burst: 10})
		require.NoError(t, err)
		b := limiterBucketRow{Tokens: 2, RefilledAt: anchor}
		lim.refill(&b, anchor)
		assert.Equal(t, 2, b.Tokens)
		assert.Equal(t, anchor, b.RefilledAt)
	})

	t.Run("a sub-token elapse carries the fraction", func(t *testing.T) {
		t.Parallel()
		lim, err := NewDBLimiter(db, DBLimiterConfig{Rate: 1, Interval: time.Second, Burst: 10})
		require.NoError(t, err)
		b := limiterBucketRow{Tokens: 3, RefilledAt: anchor}
		lim.refill(&b, anchor.Add(100*time.Millisecond)) // < 1 token's worth
		assert.Equal(t, 3, b.Tokens, "no whole token accrued")
		assert.Equal(t, anchor, b.RefilledAt, "refilled_at is held so the fraction carries")
	})

	t.Run("a partial refill advances refilled_at by the tokens' exact cost", func(t *testing.T) {
		t.Parallel()
		lim, err := NewDBLimiter(db, DBLimiterConfig{Rate: 2, Interval: time.Second, Burst: 10})
		require.NoError(t, err)
		b := limiterBucketRow{Tokens: 0, RefilledAt: anchor}
		// 1.5s at 2/s => 3 whole tokens; refilled_at advances by exactly their cost (1.5s).
		lim.refill(&b, anchor.Add(1500*time.Millisecond))
		assert.Equal(t, 3, b.Tokens)
		assert.Equal(t, anchor.Add(1500*time.Millisecond), b.RefilledAt)
	})

	t.Run("reaching burst on a partial elapse snaps refilled_at", func(t *testing.T) {
		t.Parallel()
		lim, err := NewDBLimiter(db, DBLimiterConfig{Rate: 1, Interval: time.Second, Burst: 5})
		require.NoError(t, err)
		b := limiterBucketRow{Tokens: 4, RefilledAt: anchor}
		now := anchor.Add(time.Second) // one token accrues, reaching burst below burstTime
		lim.refill(&b, now)
		assert.Equal(t, 5, b.Tokens)
		assert.Equal(t, now, b.RefilledAt, "a full bucket has no fraction to keep")
	})

	t.Run("a top-out past burstTime fills the bucket", func(t *testing.T) {
		t.Parallel()
		lim, err := NewDBLimiter(db, DBLimiterConfig{Rate: 1, Interval: time.Second, Burst: 5})
		require.NoError(t, err)
		b := limiterBucketRow{Tokens: 0, RefilledAt: anchor}
		now := anchor.Add(time.Hour) // well past burstTime
		lim.refill(&b, now)
		assert.Equal(t, 5, b.Tokens, "the bucket fills to burst")
		assert.Equal(t, now, b.RefilledAt)
	})
}

// --- Release: no-op guards and the errorless failure log --------------------

// TestDBLimiterReleaseGuardsAndLogsFailure proves Release is a no-op for a grant
// carrying no permit, and that a genuine failure is logged rather than returned —
// Release is errorless by contract, and the stranded capacity self-heals via TTL.
func TestDBLimiterReleaseGuardsAndLogsFailure(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	lim, err := NewDBLimiter(db, DBLimiterConfig{MaxConcurrent: 2, HoldTTL: time.Minute})
	require.NoError(t, err)
	ctx := context.Background()

	assert.NotPanics(t, func() {
		lim.Release(ctx, Grant{})                 // N <= 0
		lim.Release(ctx, Grant{N: 1, Token: ""})  // no token
		lim.Release(ctx, Grant{N: 0, Token: "t"}) // N <= 0 with a token
	}, "a grant carrying no permit is a no-op")

	g, err := lim.Acquire(ctx, "r", 1)
	require.NoError(t, err)
	require.Equal(t, 1, g.N)

	h := &captureHandler{}
	lim.log = slog.New(h)
	closeDB(t, db)
	assert.NotPanics(t, func() { lim.Release(ctx, g) })
	assert.True(t, h.has("flywheel: limiter release failed; capacity self-heals via TTL"),
		"a failed release is logged, not returned")
}

// --- lockBucket: the load-for-update miss -----------------------------------

// TestDBLimiterLockBucketSurfacesMissingRow proves the bucket lock surfaces a
// failed load — here a resource with no bucket row, so the First returns
// record-not-found wrapped as a lock-bucket error.
func TestDBLimiterLockBucketSurfacesMissingRow(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	lim, err := NewDBLimiter(db, DBLimiterConfig{MaxConcurrent: 1, HoldTTL: time.Minute})
	require.NoError(t, err)

	_, err = lim.lockBucket(db, "no-such-resource")
	require.ErrorContains(t, err, "lock bucket")
}

// --- sweepBatch: empty, failed find, failed delete --------------------------

// TestDBLimiterSweepBatchBranches drives sweepBatch's three exits: an empty batch
// reaps nothing, a failed find is surfaced, and a failed delete is surfaced after
// the find selected rows.
func TestDBLimiterSweepBatchBranches(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	anchor := time.Unix(1_000_000, 0)

	t.Run("an empty batch reaps nothing", func(t *testing.T) {
		t.Parallel()
		db := newDB(t)
		lim, err := NewDBLimiter(db, DBLimiterConfig{MaxConcurrent: 1, HoldTTL: time.Minute})
		require.NoError(t, err)
		n, err := lim.sweepBatch(ctx, anchor, 100)
		require.NoError(t, err)
		assert.Zero(t, n, "no expired holds means nothing deleted")
	})

	t.Run("a failed find is surfaced", func(t *testing.T) {
		t.Parallel()
		db := newDB(t)
		lim, err := NewDBLimiter(db, DBLimiterConfig{MaxConcurrent: 1, HoldTTL: time.Minute})
		require.NoError(t, err)
		require.NoError(t, db.Migrator().DropTable(&limiterHoldRow{}), "drop the holds table so the find fails")
		_, err = lim.sweepBatch(ctx, anchor, 100)
		require.ErrorContains(t, err, "find expired holds")
	})

	t.Run("a failed delete is surfaced", func(t *testing.T) {
		t.Parallel()
		db := newSingleConnMemoryDB(t)
		lim, err := NewDBLimiter(db, DBLimiterConfig{MaxConcurrent: 5, HoldTTL: time.Minute})
		require.NoError(t, err)

		g, err := lim.Acquire(dbLimiterCtx(newManualClock(anchor)), "r", 1)
		require.NoError(t, err)
		require.NotEmpty(t, g.Token)
		// Expire the hold so the find selects it, then a read-only pragma fails the delete.
		require.NoError(t, db.Table("limiter_holds").Where("token = ?", g.Token).
			Update("expires_at", anchor.Add(-time.Hour)).Error)
		require.NoError(t, db.Exec(`PRAGMA query_only = ON`).Error)

		_, err = lim.sweepBatch(dbLimiterCtx(newManualClock(anchor)), anchor, 100)
		require.ErrorContains(t, err, "delete expired holds")
	})
}

// --- Acquire / Sweep / RunSweeper failure and lifecycle branches ------------

// TestDBLimiterAcquireSurfacesDBError proves a failed Acquire round trip is
// surfaced fail-closed: the error is returned and the grant is empty, so a
// database fault cannot silently admit work past the ceiling.
func TestDBLimiterAcquireSurfacesDBError(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	lim, err := NewDBLimiter(db, DBLimiterConfig{MaxConcurrent: 2, HoldTTL: time.Minute})
	require.NoError(t, err)
	closeDB(t, db)

	g, err := lim.Acquire(context.Background(), "r", 1)
	require.Error(t, err, "a failed acquire is surfaced")
	assert.Zero(t, g.N, "a failed acquire grants nothing (fail-closed)")
}

// TestDBLimiterSweepHonorsCancellation proves Sweep checks the context between
// batches and reports the cancellation rather than looping.
func TestDBLimiterSweepHonorsCancellation(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	lim, err := NewDBLimiter(db, DBLimiterConfig{MaxConcurrent: 1, HoldTTL: time.Minute})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = lim.Sweep(ctx)
	require.ErrorIs(t, err, context.Canceled)
}

// TestDBLimiterRunSweeperStopsOnContextCancel proves the host-driven sweeper loop
// runs on its interval and returns cleanly when its context is cancelled.
func TestDBLimiterRunSweeperStopsOnContextCancel(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	lim, err := NewDBLimiter(db, DBLimiterConfig{
		MaxConcurrent: 1, HoldTTL: time.Minute, SweepInterval: time.Millisecond,
	})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); lim.RunSweeper(ctx) }()

	time.Sleep(20 * time.Millisecond) // let a few sweep ticks fire
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("RunSweeper did not return after context cancel")
	}
}
