//go:build integration

package core

import (
	"context"
	"math/rand/v2"
	"sync"
	"testing"
	"time"

	"github.com/mrz1836/go-foundation/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// postgresOpener is the PostgreSQL dbOpener.
func postgresOpener(t *testing.T) *gorm.DB {
	t.Helper()
	return NewPostgresIsolatedDB(t)
}

// TestRollupSuitePostgres is the PostgreSQL half of the rollup suite.
func TestRollupSuitePostgres(t *testing.T) {
	t.Parallel()
	rollupSuite(t, postgresOpener)
}

// TestStatsHybridReadEqualsRawOraclePostgres is the PostgreSQL half of the
// oracle, where the aggregate's casts, the boolean superseded column, and the
// window function each take their PostgreSQL spelling.
func TestStatsHybridReadEqualsRawOraclePostgres(t *testing.T) {
	t.Parallel()
	oracleSuite(t, postgresOpener)
}

// TestConcurrentRollupsConvergePostgres proves a duplicate Scheduler is
// harmless: several passes racing over the same hours — each a delete then an
// upsert per hour — leave exactly the rows one pass leaves alone.
func TestConcurrentRollupsConvergePostgres(t *testing.T) {
	t.Parallel()
	rng := rand.New(rand.NewPCG(9, 9)) //nolint:gosec // deterministic test data
	runs := randomHistory(rng, 1500, rollupBase, 24*time.Hour)
	// Mint ids once, so the two databases hold identical rows — the slowest-run
	// id included.
	for i := range runs {
		runs[i].ID, runs[i].JobID = models.NewID(), models.NewID()
	}
	now := rollupBase.Add(26 * time.Hour)

	solo := NewPostgresIsolatedDB(t)
	seedHistory(t, solo, runs)
	rollupAll(t, solo, now)
	want := statsRows(t, solo)
	require.NotEmpty(t, want)

	db := NewPostgresIsolatedDB(t)
	seedHistory(t, db, runs)
	var wg sync.WaitGroup
	errs := make(chan error, 6)
	for range 6 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := rollupPass(context.Background(), db, now, rollupConfig{
				grace: defaultStatsRollupGrace, maxHours: 1000, retention: defaultStatsRetention,
			})
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	assert.Equal(t, want, statsRows(t, db), "racing passes converge on the solo pass's rows")
}

// TestRebuildRacingTheRollupConvergesPostgres runs RebuildStats over hours the
// rollup is rolling at the same moment: per-hour replace transactions make the
// overlap safe.
func TestRebuildRacingTheRollupConvergesPostgres(t *testing.T) {
	t.Parallel()
	rng := rand.New(rand.NewPCG(4, 2)) //nolint:gosec // deterministic test data
	runs := randomHistory(rng, 800, rollupBase, 12*time.Hour)
	now := rollupBase.Add(14 * time.Hour)
	db := NewPostgresIsolatedDB(t)
	seedHistory(t, db, runs)

	var wg sync.WaitGroup
	wg.Add(2)
	var rollErr, rebuildErr error
	go func() {
		defer wg.Done()
		_, rollErr = rollupPass(context.Background(), db, now, rollupConfig{
			grace: defaultStatsRollupGrace, maxHours: 1000, retention: defaultStatsRetention,
		})
	}()
	go func() {
		defer wg.Done()
		_, rebuildErr = RebuildStats(fixedClockCtx(now), db, RebuildOpts{From: rollupBase, To: now, Force: true})
	}()
	wg.Wait()
	require.NoError(t, rollErr)
	require.NoError(t, rebuildErr)

	got, err := Stats(context.Background(), db, StatsParams{From: rollupBase, To: rollupBase.Add(12 * time.Hour)})
	require.NoError(t, err)
	_, want := rawKindStats(t, db, rollupBase, rollupBase.Add(12*time.Hour), statsFilter{})
	assert.Equal(t, want, got.Total)
}

// TestNormalizeRunTimestampsIsANoOpOnPostgres pins the dialect gate: a
// timestamptz has no stored zone to re-stamp.
func TestNormalizeRunTimestampsIsANoOpOnPostgres(t *testing.T) {
	t.Parallel()
	db := NewPostgresIsolatedDB(t)
	seedHistory(t, db, []histRun{okRun("a", rollupBase, 100)})
	n, err := NormalizeRunTimestamps(context.Background(), db)
	require.NoError(t, err)
	assert.Zero(t, n)
}

// TestCoverageSuitePostgres is the PostgreSQL half of the coverage suite.
func TestCoverageSuitePostgres(t *testing.T) {
	t.Parallel()
	coverageSuite(t, postgresOpener)
}

// TestRacingWritersKeepTheCoveredRangeExactPostgres races everything that
// extends the covered range — bounded rollup passes and rebuilds of ranges above,
// inside, and below it — and asserts the range they leave has no gap: every hour
// in it holds exactly a fresh recompute of the hour.
func TestRacingWritersKeepTheCoveredRangeExactPostgres(t *testing.T) {
	t.Parallel()
	rng := rand.New(rand.NewPCG(7, 3)) //nolint:gosec // deterministic test data
	db := NewPostgresIsolatedDB(t)
	seedHistory(t, db, randomHistory(rng, 900, rollupBase, 48*time.Hour))
	now := rollupBase.Add(50 * time.Hour)

	var wg sync.WaitGroup
	errs := make(chan error, 12)
	for i := range 12 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx := fixedClockCtx(now)
			if i%2 == 0 {
				_, err := rollupPass(ctx, db, now, rollupConfig{
					grace: defaultStatsRollupGrace, maxHours: 2 + i, retention: defaultStatsRetention,
				})
				errs <- err
				return
			}
			from := hourAt(rollupBase, (i*7)%40)
			_, err := RebuildStats(ctx, db, RebuildOpts{From: from, To: from.Add(time.Duration(3+i) * time.Hour)})
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	assertCoveredHoursExact(t, db)
	got, err := Stats(context.Background(), db, StatsParams{From: rollupBase, To: now, MaxRawSpan: -1})
	require.NoError(t, err)
	_, want := rawKindStats(t, db, rollupBase, now, statsFilter{})
	assert.Equal(t, want, got.Total)
}
