package core

import (
	"context"
	"fmt"
	"math/rand/v2"
	"sync"
	"testing"
	"time"

	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// benchRuns is the SQLite benchmark fixture's size: the plan's SQLite target is
// stated at 100k runs.
const benchRuns = 100_000

// benchDB is one seeded fixture per process, so `go test -bench` pays the seed
// once rather than once per benchmark.
//
//nolint:gochecknoglobals // per-process benchmark fixture
var benchDB struct {
	once sync.Once
	db   *gorm.DB
	now  time.Time
}

// seededBenchDB returns a SQLite database holding benchRuns runs over 30 days
// across four kinds, rolled up through all but the last hour, plus running jobs
// for the live reads.
func seededBenchDB(b *testing.B) (*gorm.DB, time.Time) {
	b.Helper()
	benchDB.once.Do(func() {
		// Not newBareSQLite: that ties the handle to the first benchmark's
		// cleanup, and every later benchmark would find it closed. A process-wide
		// fixture lives as long as the process.
		db, err := gorm.Open(sqlite.Open(fmt.Sprintf("file:flywheel-bench-%d?mode=memory&cache=shared", dbSeq.Add(1))),
			&gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
		if err != nil {
			b.Fatalf("open: %v", err)
		}
		if err := Migrate(db); err != nil {
			b.Fatalf("migrate: %v", err)
		}
		rng := rand.New(rand.NewPCG(11, 12)) //nolint:gosec // deterministic fixture
		span := 30 * 24 * time.Hour
		runs := randomHistory(rng, benchRuns, rollupBase, span)
		for i := range runs {
			runs[i].Legacy = false
		}
		seedHistory(b, db, runs)
		now := rollupBase.Add(span)
		rollupAll(b, db, now)
		for i := range 50 {
			seedJob(b, db, jobRow{
				ID: "bench-running-" + string(rune('a'+i%26)) + string(rune('a'+i/26)), Kind: "alpha",
				State: string(StateRunning), ScheduledAt: now.Add(-time.Hour),
			})
		}
		benchDB.db, benchDB.now = db, now
	})
	return benchDB.db, benchDB.now
}

// BenchmarkStats24hSQLite is the plan's SQLite target: Stats over the last day
// of a 100k-run history, served from rollups plus the open hour raw.
func BenchmarkStats24hSQLite(b *testing.B) {
	db, now := seededBenchDB(b)
	ctx := context.Background()
	for b.Loop() {
		if _, err := Stats(ctx, db, StatsParams{From: now.Add(-24 * time.Hour), To: now}); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkStats30dSQLite reads the whole history: 720 rolled hours per kind.
func BenchmarkStats30dSQLite(b *testing.B) {
	db, now := seededBenchDB(b)
	ctx := context.Background()
	for b.Loop() {
		if _, err := Stats(ctx, db, StatsParams{From: now.Add(-30 * 24 * time.Hour), To: now}); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkStatsSeries30dHourlySQLite is a 720-point hourly trend chart.
func BenchmarkStatsSeries30dHourlySQLite(b *testing.B) {
	db, now := seededBenchDB(b)
	ctx := context.Background()
	for b.Loop() {
		if _, err := StatsSeries(ctx, db, SeriesParams{From: now.Add(-30 * 24 * time.Hour), To: now}); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkRollupHourSQLite recomputes and replaces one closed hour.
func BenchmarkRollupHourSQLite(b *testing.B) {
	db, now := seededBenchDB(b)
	ctx := context.Background()
	hour := floorHour(now.Add(-48 * time.Hour))
	for b.Loop() {
		if _, err := rollupHour(ctx, db, hour, now, false); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkListRunningSQLite is the "running now" panel.
func BenchmarkListRunningSQLite(b *testing.B) {
	db, now := seededBenchDB(b)
	ctx := fixedClockCtx(now)
	for b.Loop() {
		if _, err := ListRunning(ctx, db, ListRunningParams{WithBaseline: true}); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkAggregateRawHourSQLite is the live tail's unit of work: one raw hour
// through the shared aggregate.
func BenchmarkAggregateRawHourSQLite(b *testing.B) {
	db, now := seededBenchDB(b)
	ctx := context.Background()
	hour := floorHour(now.Add(-48 * time.Hour))
	for b.Loop() {
		if _, err := aggregateRuns(ctx, db, hour, hour.Add(time.Hour), statsFilter{}); err != nil {
			b.Fatal(err)
		}
	}
}
