package flywheel

import (
	"context"
	"testing"
	"time"

	"github.com/mrz1836/go-foundation/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// tbAnchor is a stable clock anchor for the deterministic token-bucket tests.
//
//nolint:gochecknoglobals // a fixed test anchor shared by the token-bucket tests
var tbAnchor = time.Unix(1_000_000, 0)

// --- construction ------------------------------------------------------------

// TestTokenBucketConstructorRejectsInvalidConfig proves a config that could never
// admit work fails loudly at construction rather than as a silent zero budget.
func TestTokenBucketConstructorRejectsInvalidConfig(t *testing.T) {
	t.Parallel()
	for name, cfg := range map[string]TokenBucketConfig{
		"negative rate":           {Rate: -1, Interval: time.Second},
		"negative burst":          {Burst: -1},
		"negative max concurrent": {MaxConcurrent: -1},
		"negative interval":       {Interval: -1},
		"rate without interval":   {Rate: 10},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			assert.Panics(t, func() { NewTokenBucket(cfg) })
		})
	}
	assert.NotPanics(t, func() {
		NewTokenBucket(TokenBucketConfig{Rate: 10, Interval: time.Second, MaxConcurrent: 5})
	}, "a valid config constructs")
	assert.NotPanics(t, func() {
		NewTokenBucket(TokenBucketConfig{MaxConcurrent: 5})
	}, "concurrency-only (no rate) is valid")
}

// --- rate: burst, refill, and the exact retry hint ---------------------------

// TestTokenBucketInitialBurstThenDenies proves a fresh bucket starts full — it
// admits an initial burst up to Burst — then denies with a retry hint once drained.
func TestTokenBucketInitialBurstThenDenies(t *testing.T) {
	t.Parallel()
	ctx := models.WithClock(context.Background(), newManualClock(tbAnchor))
	tb := NewTokenBucket(TokenBucketConfig{Rate: 10, Interval: time.Second, Burst: 5})

	g, err := tb.Acquire(ctx, "r", 10)
	require.NoError(t, err)
	assert.Equal(t, 5, g.N, "the initial grant is capped at Burst")

	g, err = tb.Acquire(ctx, "r", 10)
	require.NoError(t, err)
	assert.Zero(t, g.N, "a drained bucket grants nothing")
	assert.Positive(t, g.RetryAfter, "a rate-bound denial hints when to retry")
}

// TestTokenBucketRefillsAtRateCappedAtBurst walks the clock and checks that
// tokens accrue at Rate and never exceed Burst.
func TestTokenBucketRefillsAtRateCappedAtBurst(t *testing.T) {
	t.Parallel()
	clk := newManualClock(tbAnchor)
	ctx := models.WithClock(context.Background(), clk)
	tb := NewTokenBucket(TokenBucketConfig{Rate: 10, Interval: time.Second, Burst: 10})

	g, err := tb.Acquire(ctx, "r", 10)
	require.NoError(t, err)
	require.Equal(t, 10, g.N, "the burst drains in one grant")

	clk.advance(500 * time.Millisecond) // 10/s * 0.5s = 5 tokens
	g, err = tb.Acquire(ctx, "r", 10)
	require.NoError(t, err)
	assert.Equal(t, 5, g.N, "half a second refills half the rate")

	clk.advance(2 * time.Second) // 20 tokens accrued, capped at Burst 10
	g, err = tb.Acquire(ctx, "r", 10)
	require.NoError(t, err)
	assert.Equal(t, 10, g.N, "refill is capped at Burst")
}

// TestTokenBucketRetryAfterIsExact proves the denial's wait is computed from the
// deficit and the refill rate, not guessed: with 0.4 of a token and 250 ms per
// token, the wait to the next whole token is 0.6 * 250 ms = 150 ms.
func TestTokenBucketRetryAfterIsExact(t *testing.T) {
	t.Parallel()
	clk := newManualClock(tbAnchor)
	ctx := models.WithClock(context.Background(), clk)
	tb := NewTokenBucket(TokenBucketConfig{Rate: 4, Interval: time.Second, Burst: 4})

	_, err := tb.Acquire(ctx, "r", 4) // drain to zero
	require.NoError(t, err)

	clk.advance(100 * time.Millisecond) // 4/s * 0.1s = 0.4 tokens
	g, err := tb.Acquire(ctx, "r", 4)
	require.NoError(t, err)
	require.Zero(t, g.N, "0.4 of a token grants nothing")
	assert.InDelta(t, float64(150*time.Millisecond), float64(g.RetryAfter), float64(time.Millisecond),
		"the wait is the exact time to the next token")
}

// TestTokenBucketRateZeroDisablesRate proves Rate 0 removes the rate ceiling,
// leaving only the concurrency cap (or nothing) to bound a grant.
func TestTokenBucketRateZeroDisablesRate(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	unbounded := NewTokenBucket(TokenBucketConfig{})
	g, err := unbounded.Acquire(ctx, "r", 1000)
	require.NoError(t, err)
	assert.Equal(t, 1000, g.N, "no rate and no concurrency cap grants everything")
	assert.Empty(t, g.Token, "an unbounded grant carries no token")

	concurrencyOnly := NewTokenBucket(TokenBucketConfig{MaxConcurrent: 3})
	g, err = concurrencyOnly.Acquire(ctx, "r", 1000)
	require.NoError(t, err)
	assert.Equal(t, 3, g.N, "Rate 0 leaves only the concurrency ceiling")
	assert.NotEmpty(t, g.Token, "a concurrency grant carries a token")
}

// --- concurrency: the ceiling, and idempotent partial release ----------------

// TestTokenBucketMaxConcurrentCapsHoldersAndReleaseIsIdempotent proves the
// concurrency ceiling bounds simultaneous holders, that Release frees capacity,
// and that a double or over-large Release cannot drive the held count negative.
func TestTokenBucketMaxConcurrentCapsHoldersAndReleaseIsIdempotent(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	tb := NewTokenBucket(TokenBucketConfig{MaxConcurrent: 5})

	g1, err := tb.Acquire(ctx, "r", 3)
	require.NoError(t, err)
	require.Equal(t, 3, g1.N)

	g2, err := tb.Acquire(ctx, "r", 3)
	require.NoError(t, err)
	require.Equal(t, 2, g2.N, "only two of the ceiling remained")

	g3, err := tb.Acquire(ctx, "r", 3)
	require.NoError(t, err)
	require.Zero(t, g3.N, "the ceiling is full")

	tb.Release(ctx, g1)
	// A second release of the same grant, and an over-large one, are no-ops.
	tb.Release(ctx, g1)
	tb.Release(ctx, Grant{Resource: "r", N: 99, Token: g2.Token})

	g4, err := tb.Acquire(ctx, "r", 3)
	require.NoError(t, err)
	assert.Equal(t, 3, g4.N, "releasing g1 freed exactly three, no more")
}

// --- the budget is keyed on the resource ------------------------------------

// TestTokenBucketKeysOnResource is the deterministic core of resource keying: exhausting
// one resource's budget leaves another's untouched, because the key is the
// resource string and nothing else.
func TestTokenBucketKeysOnResource(t *testing.T) {
	t.Parallel()
	ctx := models.WithClock(context.Background(), newManualClock(tbAnchor))
	tb := NewTokenBucket(TokenBucketConfig{Rate: 2, Interval: time.Second, Burst: 2})

	g, err := tb.Acquire(ctx, "provider:a", 2)
	require.NoError(t, err)
	require.Equal(t, 2, g.N, "resource a's burst is available")

	g, err = tb.Acquire(ctx, "provider:a", 2)
	require.NoError(t, err)
	require.Zero(t, g.N, "resource a is now exhausted")

	g, err = tb.Acquire(ctx, "provider:b", 2)
	require.NoError(t, err)
	assert.Equal(t, 2, g.N, "resource b has its own independent budget")
}

// --- holders never exceed MaxConcurrent -------------------------------------

// TestGatedRunnerHoldersNeverExceedMaxConcurrent runs an eight-slot pool under a
// real token bucket with a loose rate and MaxConcurrent 5, and asserts — by
// instrumenting the worker, not by trusting the limiter — that no more than five
// jobs are ever in flight at once, while still proving the bound is not vacuous.
func TestGatedRunnerHoldersNeverExceedMaxConcurrent(t *testing.T) {
	t.Parallel()
	const maxConcurrent = 5
	tb := NewTokenBucket(TokenBucketConfig{
		Rate: 100_000, Interval: time.Second, Burst: 50, MaxConcurrent: maxConcurrent,
	})
	w := &peakWorker{hold: func(context.Context, int) { time.Sleep(3 * time.Millisecond) }}
	d := newPoolDriver(120)
	reg := NewRegistry()
	Register(reg, w)
	r := newGatedRunner(t, RunnerConfig{Driver: d, Registry: reg, Limiter: tb, Concurrency: 8})

	stop := runInBackground(t, r)
	require.Eventually(t, func() bool { return w.done.Load() == 120 },
		10*time.Second, 5*time.Millisecond, "the backlog drains")
	stop()

	assert.LessOrEqual(t, w.peak.Load(), int32(maxConcurrent),
		"simultaneous holders never exceed MaxConcurrent")
	assert.Greater(t, w.peak.Load(), int32(1),
		"jobs did run concurrently, so the bound is not vacuous")
}

// --- topology: one class, two runners, two resources ------------------------

// TestGatedRunnersShareOneLimiterWithIndependentResources runs the documented
// topology — two runners on one executor class, each keyed on its own resource,
// sharing one limiter — and proves both drain concurrently, neither starving the
// other. It uses independent pool drivers so the assertion is about the gate, not
// about SQLite's single-writer contention. Per-resource independence itself is
// pinned deterministically in TestTokenBucketKeysOnResource, and the realistic
// multi-process shared budget is the Postgres integration test.
func TestGatedRunnersShareOneLimiterWithIndependentResources(t *testing.T) {
	t.Parallel()
	// One shared limiter, keyed per resource, with a loose rate so both drain.
	tb := NewTokenBucket(TokenBucketConfig{Rate: 100_000, Interval: time.Second, Burst: 100})

	build := func(resource string, jobs int) (*Runner, *peakWorker) {
		w := &peakWorker{}
		reg := NewRegistry()
		Register(reg, w)
		r := newGatedRunner(t, RunnerConfig{
			Driver: newPoolDriver(jobs), Registry: reg, ExecutorClass: "local",
			Limiter: tb, Resource: resource, Concurrency: 4,
		})
		return r, w
	}
	ra, wa := build("provider:a", 30)
	rb, wb := build("provider:b", 30)

	stopA := runInBackground(t, ra)
	stopB := runInBackground(t, rb)
	require.Eventually(t, func() bool { return wa.done.Load() == 30 && wb.done.Load() == 30 },
		10*time.Second, 5*time.Millisecond, "both runners drain under one shared limiter")
	stopA()
	stopB()

	// Each runner minted its own resource key: the shared limiter tracked them as
	// two independent buckets: resource keying at the runner level.
	tb.mu.Lock()
	_, hasA := tb.buckets["provider:a"]
	_, hasB := tb.buckets["provider:b"]
	tb.mu.Unlock()
	assert.True(t, hasA && hasB, "each resource is keyed to its own bucket")
}
