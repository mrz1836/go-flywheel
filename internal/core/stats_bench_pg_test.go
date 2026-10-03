//go:build integration

package core

import (
	"context"
	"math/rand/v2"
	"testing"
	"time"
)

// BenchmarkStatsReadsPostgres measures the analytics reads against PostgreSQL
// over a 100k-run, 30-day history rolled up through all but the open hour: the
// same fixture the SQLite benchmarks use, seeded once and shared by the
// sub-benchmarks so the isolated schema outlives them all. The 1M-run numbers
// BENCHMARKS.md publishes come from `explain -query stats`, which seeds
// server-side; this is the in-repo regression guard.
func BenchmarkStatsReadsPostgres(b *testing.B) {
	db := NewPostgresIsolatedDB(b)
	rng := rand.New(rand.NewPCG(11, 12)) //nolint:gosec // deterministic fixture
	span := 30 * 24 * time.Hour
	runs := randomHistory(rng, benchRuns, rollupBase, span)
	for i := range runs {
		runs[i].Legacy = false
	}
	seedHistory(b, db, runs)
	now := rollupBase.Add(span)
	rollupAll(b, db, now)
	for _, table := range []string{"job_runs", "job_run_finishes", "job_stats_hourly"} {
		// One statement each: a multi-statement string runs as one implicit
		// transaction, and VACUUM refuses to run inside one.
		if err := db.Exec(`VACUUM (ANALYZE) ` + table).Error; err != nil {
			b.Fatal(err)
		}
	}
	ctx := context.Background()
	hour := floorHour(now.Add(-48 * time.Hour))

	b.Run("Stats24h", func(b *testing.B) {
		for b.Loop() {
			if _, err := Stats(ctx, db, StatsParams{From: now.Add(-24 * time.Hour), To: now}); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("Stats30d", func(b *testing.B) {
		for b.Loop() {
			if _, err := Stats(ctx, db, StatsParams{From: now.Add(-span), To: now}); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("StatsSeries30dHourly", func(b *testing.B) {
		for b.Loop() {
			if _, err := StatsSeries(ctx, db, SeriesParams{From: now.Add(-span), To: now}); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("RollupHour", func(b *testing.B) {
		for b.Loop() {
			if _, err := rollupHour(ctx, db, hour, now, false); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("AggregateRawHour", func(b *testing.B) {
		for b.Loop() {
			if _, err := aggregateRuns(ctx, db, hour, hour.Add(time.Hour), statsFilter{}); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("ListRunning", func(b *testing.B) {
		clock := fixedClockCtx(now)
		for b.Loop() {
			if _, err := ListRunning(clock, db, ListRunningParams{WithBaseline: true}); err != nil {
				b.Fatal(err)
			}
		}
	})
}
