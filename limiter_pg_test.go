//go:build integration

package flywheel

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mrz1836/go-foundation/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newSharedDBLimiters builds n DBLimiter instances over one Postgres schema —
// independent objects sharing only the database, which is a sound stand-in for n
// OS processes: the limiter keeps no in-memory or session-scoped state, so all
// coordination is row locks that release at commit, exactly as they would across
// processes.
func newSharedDBLimiters(t *testing.T, n int, cfg DBLimiterConfig) []*DBLimiter {
	t.Helper()
	db := NewPostgresIsolatedDB(t)
	limiters := make([]*DBLimiter, n)
	for i := range limiters {
		lim, err := NewDBLimiter(db, cfg)
		require.NoError(t, err)
		limiters[i] = lim
	}
	return limiters
}

// TestDBLimiterSharedCeilingAcrossInstancesPostgres is the shared-budget check:
// three independent limiter instances contend for one MaxConcurrent ceiling, and
// the total admitted never exceeds it even under concurrent Acquire — the bucket
// row lock serializes the reclaim-count-grant across every instance.
func TestDBLimiterSharedCeilingAcrossInstancesPostgres(t *testing.T) {
	t.Parallel()
	const ceiling = 10
	limiters := newSharedDBLimiters(t, 3, DBLimiterConfig{MaxConcurrent: ceiling, HoldTTL: time.Hour})

	var granted atomic.Int64
	var wg sync.WaitGroup
	// Thirty concurrent acquirers of one permit each, spread across the three
	// instances, against a ceiling of ten.
	for i := range 30 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			g, err := limiters[i%3].Acquire(context.Background(), "provider:shared", 1)
			if err == nil {
				granted.Add(int64(g.N))
			}
		}()
	}
	wg.Wait()

	assert.EqualValues(t, ceiling, granted.Load(),
		"three instances admit exactly the shared ceiling, no more")
}

// TestDBLimiterInlineReclaimAcrossInstancesPostgres is the crash-reclaim check: a
// holder of four permits dies without releasing, and once its TTL lapses another
// instance's Acquire reclaims exactly those four inline — with no sweeper running —
// while a second, still-valid holder's reservation is left intact. The clock is
// injected, so the reclamation is asserted deterministically rather than waited on.
func TestDBLimiterInlineReclaimAcrossInstancesPostgres(t *testing.T) {
	t.Parallel()
	const ceiling = 10
	limiters := newSharedDBLimiters(t, 3, DBLimiterConfig{MaxConcurrent: ceiling, HoldTTL: 30 * time.Second})
	crashed, alive, late := limiters[0], limiters[1], limiters[2]

	anchor := time.Unix(1_000_000, 0)
	clk := newManualClock(anchor)
	ctx := models.WithClock(context.Background(), clk)

	// The doomed instance takes four permits, then "crashes" — it never releases.
	g, err := crashed.Acquire(ctx, "r", 4)
	require.NoError(t, err)
	require.Equal(t, 4, g.N)

	// A healthy instance takes the remaining six, ten seconds later, so its holds
	// outlive the crashed instance's.
	clk.advance(10 * time.Second)
	gAlive, err := alive.Acquire(ctx, "r", 6)
	require.NoError(t, err)
	require.Equal(t, 6, gAlive.N, "the ceiling is now full")

	full, err := late.Acquire(ctx, "r", 1)
	require.NoError(t, err)
	require.Zero(t, full.N, "no capacity while both holders are alive")

	// The crashed instance's TTL lapses (but the healthy one's has not). A third
	// instance's Acquire reclaims exactly the four abandoned permits inline.
	clk.advance(21 * time.Second) // anchor+31s: past the crashed TTL (30s), before the alive one (40s)
	reclaimed, err := late.Acquire(ctx, "r", 4)
	require.NoError(t, err)
	assert.Equal(t, 4, reclaimed.N, "the crashed holder's four permits are reclaimed inline, no sweeper")

	none, err := late.Acquire(ctx, "r", 1)
	require.NoError(t, err)
	assert.Zero(t, none.N, "the healthy holder's six are untouched, so the ceiling is full again")
}
