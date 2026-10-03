package core

import (
	"context"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// oracleSpan is how much history the oracle seeds.
const oracleSpan = 72 * time.Hour

// oracleSuite is the main correctness gate for the hybrid read: for random
// windows and filters, Stats and StatsSeries served from rollups plus a raw tail
// must equal the same window aggregated entirely from raw job_runs. It runs
// twice — with the watermark mid-history, so most windows straddle it, and fully
// rolled — over seeded history with every outcome, superseded and crashed rows,
// missing waits, and legacy rows that resolve their kind through jobs.
func oracleSuite(t *testing.T, open dbOpener) {
	db := open(t)
	rng := rand.New(rand.NewPCG(42, 1)) //nolint:gosec // deterministic test data
	runs := randomHistory(rng, 3000, rollupBase, oracleSpan)
	seedHistory(t, db, runs)
	end := rollupBase.Add(oracleSpan)

	t.Run("watermark mid-history", func(t *testing.T) {
		res := rollupAll(t, db, rollupBase.Add(40*time.Hour))
		require.True(t, res.Watermark.After(rollupBase) && res.Watermark.Before(end))
		checkOracle(t, db, rng, 40)
	})
	t.Run("fully rolled", func(t *testing.T) {
		rollupAll(t, db, end.Add(2*time.Hour))
		checkOracle(t, db, rng, 40)
	})
}

// oracleFilters are the filter shapes each window is checked under.
//
//nolint:gochecknoglobals // shared fixture
var oracleFilters = []statsFilter{
	{}, {Kind: "alpha"}, {Queue: "bulk"}, {Kind: "gamma", Queue: "default"}, {Kind: "no-such-kind"},
}

// checkOracle compares n random windows (unaligned edges, reaching past both
// ends of the history) and their hourly and daily series.
func checkOracle(t *testing.T, db *gorm.DB, rng *rand.Rand, n int) {
	t.Helper()
	ctx := context.Background()
	spanMin := int64(oracleSpan / time.Minute)
	for i := range n {
		from := rollupBase.Add(time.Duration(rng.Int64N(spanMin+240)-120) * time.Minute)
		to := from.Add(time.Duration(1+rng.Int64N(spanMin)) * time.Minute)
		f := oracleFilters[i%len(oracleFilters)]

		got, err := Stats(ctx, db, StatsParams{From: from, To: to, Kind: f.Kind, Queue: f.Queue, MaxRawSpan: -1})
		require.NoError(t, err)
		wantKinds, wantTotal := rawKindStats(t, db, from, to, f)
		require.Equal(t, wantKinds, got.Kinds, "window %s..%s filter %+v", from, to, f)
		require.Equal(t, wantTotal, got.Total, "window %s..%s filter %+v", from, to, f)

		// Series are checked on a subset, over at most a day: each raw bucket is a
		// query on both sides of the comparison, and a day of buckets already
		// straddles the watermark and both unaligned edges.
		if i%5 == 0 {
			seriesTo := to
			if day := from.Add(24 * time.Hour); day.Before(seriesTo) {
				seriesTo = day
			}
			checkSeriesOracle(t, db, from, seriesTo, f, IntervalHour, time.UTC)
		}
		if i%10 == 0 {
			ny, err := time.LoadLocation("America/New_York")
			require.NoError(t, err)
			checkSeriesOracle(t, db, from, to, f, IntervalDay, ny)
		}
	}
}

// checkSeriesOracle compares every point of a series against the raw
// aggregate of that point's bucket.
func checkSeriesOracle(
	t *testing.T, db *gorm.DB, from, to time.Time, f statsFilter, interval SeriesInterval, loc *time.Location,
) {
	t.Helper()
	points, err := StatsSeries(context.Background(), db, SeriesParams{
		From: from, To: to, Kind: f.Kind, Queue: f.Queue, Interval: interval, Location: loc, MaxRawSpan: -1,
	})
	require.NoError(t, err)
	require.NotEmpty(t, points)
	for _, p := range points {
		_, want := rawKindStats(t, db, p.Start, p.End, f)
		want.Kind = f.Kind
		require.Equal(t, want, p.Stats, "%s bucket %s..%s filter %+v", interval, p.Start, p.End, f)
	}
}

// TestStatsHybridReadEqualsRawOracle is the SQLite half of the oracle.
func TestStatsHybridReadEqualsRawOracle(t *testing.T) {
	t.Parallel()
	oracleSuite(t, sqliteOpener)
}

// TestStatsCoverageDescribesTheSplit pins Coverage: whole rolled hours come from
// rollups, the tail past the watermark and an unaligned head are raw.
func TestStatsCoverageDescribesTheSplit(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	seedHistory(t, db, []histRun{okRun("a", hourAt(rollupBase, 1), 100), okRun("a", hourAt(rollupBase, 6), 100)})
	res := rollupAll(t, db, hourAt(rollupBase, 4).Add(10*time.Minute))
	require.Equal(t, hourAt(rollupBase, 4), res.Watermark)

	from := hourAt(rollupBase, 0).Add(30 * time.Minute)
	to := hourAt(rollupBase, 7)
	got, err := Stats(context.Background(), db, StatsParams{From: from, To: to})
	require.NoError(t, err)
	assert.Equal(t, res.Watermark, got.Coverage.RolledThrough)
	assert.Equal(t, hourAt(rollupBase, 4), got.Coverage.RawFrom)
	assert.Equal(t, to, got.Coverage.RawTo)
	assert.Equal(t, 30*time.Minute+3*time.Hour, got.Coverage.RawSpan, "the half-hour head plus the three-hour tail")
	assert.EqualValues(t, 2, got.Total.Attempts)
}

// TestStatsRefusesAnUnrolledLongWindow proves the raw cap: with no rollup, a
// window longer than MaxRawSpan fails with ErrStatsNotRolledUp naming the fix,
// a shorter one is served raw, and a negative cap removes the limit.
func TestStatsRefusesAnUnrolledLongWindow(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	seedHistory(t, db, []histRun{okRun("a", rollupBase, 100)})
	ctx := context.Background()

	_, err := Stats(ctx, db, StatsParams{From: rollupBase.Add(-30 * 24 * time.Hour), To: rollupBase.Add(time.Hour)})
	require.ErrorIs(t, err, ErrStatsNotRolledUp)
	assert.Contains(t, err.Error(), "StatsRollupInterval")
	assert.Contains(t, err.Error(), "RebuildStats")

	_, err = StatsSeries(ctx, db, SeriesParams{From: rollupBase.Add(-30 * 24 * time.Hour), To: rollupBase})
	require.ErrorIs(t, err, ErrStatsNotRolledUp)

	got, err := Stats(ctx, db, StatsParams{From: rollupBase.Add(-24 * time.Hour), To: rollupBase.Add(time.Hour)})
	require.NoError(t, err)
	assert.EqualValues(t, 1, got.Total.Attempts, "with the rollup off a short window is read raw — correct, only slower")
	assert.True(t, got.Coverage.RolledThrough.IsZero())

	got, err = Stats(ctx, db, StatsParams{
		From: rollupBase.Add(-30 * 24 * time.Hour), To: rollupBase.Add(time.Hour), MaxRawSpan: -1,
	})
	require.NoError(t, err)
	assert.EqualValues(t, 1, got.Total.Attempts)
}

// TestStatsValidatesItsWindow covers the argument checks.
func TestStatsValidatesItsWindow(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	ctx := context.Background()
	_, err := Stats(ctx, db, StatsParams{From: rollupBase, To: rollupBase})
	require.ErrorIs(t, err, ErrValidation)
	_, err = Stats(ctx, db, StatsParams{To: rollupBase})
	require.ErrorIs(t, err, ErrValidation)
	_, err = Stats(ctx, nil, StatsParams{From: rollupBase, To: rollupBase.Add(time.Hour)})
	require.Error(t, err)
	_, err = StatsSeries(ctx, db, SeriesParams{From: rollupBase, To: rollupBase.Add(time.Hour), Interval: "week"})
	require.ErrorIs(t, err, ErrValidation)
	_, err = StatsSeries(ctx, db, SeriesParams{From: rollupBase, To: rollupBase.Add(time.Hour * 24 * 500), MaxRawSpan: -1})
	require.ErrorIs(t, err, ErrValidation, "a series is bounded in points")
}

// TestStatsKindStatsDerivedFields pins the derived job-level numbers on a
// hand-built hour: retries, the success rate, and the slowest run.
func TestStatsKindStatsDerivedFields(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	h := hourAt(rollupBase, 0)
	slow := okRun("k", h.Add(3*time.Minute), 4000)
	slow.ID = "run-slowest"
	retried := histRun{Kind: "k", Outcome: OutcomeError, JobState: StateRetryable, FinishedAt: h.Add(4 * time.Minute), DurationMs: 20, WaitMs: 5}
	crashed := histRun{Kind: "k", Outcome: OutcomeCrashed, JobState: StateAvailable, FinishedAt: h.Add(5 * time.Minute), DurationMs: -1, WaitMs: 5}
	superseded := histRun{Kind: "k", Outcome: OutcomeSuccess, Superseded: true, FinishedAt: h.Add(6 * time.Minute), DurationMs: 9000, WaitMs: 5}
	seedHistory(t, db, []histRun{
		okRun("k", h.Add(time.Minute), 100), okRun("k", h.Add(2*time.Minute), 200), slow,
		discardRun("k", h.Add(3*time.Minute)), retried, crashed, superseded,
	})
	got, err := Stats(context.Background(), db, StatsParams{From: h, To: h.Add(time.Hour)})
	require.NoError(t, err)
	k := mustKind(t, got, "k")
	assert.EqualValues(t, 6, k.Attempts, "the superseded attempt is not an attempt outcome")
	assert.EqualValues(t, 1, k.Superseded)
	assert.EqualValues(t, 3, k.Succeeded)
	assert.EqualValues(t, 1, k.Discarded)
	assert.EqualValues(t, 2, k.Retries, "one retried error plus one crash the sweep reclaimed")
	assert.InDelta(t, 0.75, k.SuccessRate, 1e-9)
	assert.EqualValues(t, 3, k.Duration.Count, "successful, non-superseded attempts only")
	assert.Equal(t, 4*time.Second, k.Duration.Max, "the superseded 9s run is excluded")
	assert.Equal(t, "run-slowest", k.SlowestRunID)
	assert.EqualValues(t, 6, k.QueueWait.Count)
}

// TestStatsSeriesBuckets covers the bucket layout: empty buckets are present,
// a DST day is 23 hours, and a half-hour zone is refused for day buckets.
func TestStatsSeriesBuckets(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	ctx := context.Background()
	seedHistory(t, db, []histRun{okRun("a", hourAt(rollupBase, 2).Add(time.Minute), 100)})

	points, err := StatsSeries(ctx, db, SeriesParams{From: hourAt(rollupBase, 0), To: hourAt(rollupBase, 5)})
	require.NoError(t, err)
	require.Len(t, points, 5, "every hour is a point, empty or not")
	assert.Zero(t, points[0].Stats.Attempts)
	assert.EqualValues(t, 1, points[2].Stats.Attempts)

	ny, err := time.LoadLocation("America/New_York")
	require.NoError(t, err)
	dst := time.Date(2026, 3, 8, 12, 0, 0, 0, ny) // the spring-forward day
	points, err = StatsSeries(ctx, db, SeriesParams{
		From: dst, To: dst.Add(time.Hour), Interval: IntervalDay, Location: ny,
	})
	require.NoError(t, err)
	require.Len(t, points, 1)
	assert.Equal(t, 23*time.Hour, points[0].End.Sub(points[0].Start), "the spring-forward day is 23 hours")
	assert.Equal(t, 0, points[0].Start.Hour(), "and starts at local midnight")

	kolkata, err := time.LoadLocation("Asia/Kolkata")
	require.NoError(t, err)
	_, err = StatsSeries(ctx, db, SeriesParams{
		From: rollupBase, To: rollupBase.Add(time.Hour), Interval: IntervalDay, Location: kolkata,
	})
	require.ErrorIs(t, err, ErrValidation, "a +05:30 day cannot be summed from hourly rollups")
	_, err = StatsSeries(ctx, db, SeriesParams{
		From: rollupBase, To: rollupBase.Add(time.Hour), Interval: IntervalHour, Location: kolkata,
	})
	require.NoError(t, err, "hour buckets are UTC hours, valid in any zone")
}

// TestStatsWindowAcrossADSTChangeSQLite proves the UTC stamping holds on the
// dialect it exists for: runs written by a clock in a DST zone on both sides of
// the spring-forward gap are all counted by a window spanning it, and none by a
// window just outside.
func TestStatsWindowAcrossADSTChangeSQLite(t *testing.T) {
	t.Parallel()
	db, d := sqliteWritePath(t)
	ny, err := time.LoadLocation("America/New_York")
	require.NoError(t, err)
	// 01:30 EST and 03:30 EDT are one hour apart in real time.
	before := time.Date(2026, 3, 8, 1, 30, 0, 0, ny)
	after := time.Date(2026, 3, 8, 3, 30, 0, 0, ny)
	for _, at := range []time.Time{before, after} {
		ctx := fixedClockCtx(at)
		_, err := Enqueue(ctx, NewClient(db), "dst.kind", []byte(`{}`), InsertOpts{Queue: "dq"})
		require.NoError(t, err)
		jobs, err := d.Dequeue(ctx, []string{"dq"}, AnyClass, true, 1, time.Minute)
		require.NoError(t, err)
		require.Len(t, jobs, 1)
		require.NoError(t, d.InsertRunStub(ctx, "run-"+at.Format("1504"), jobs[0], at, "", "x"))
		_, err = d.Finalize(ctx, jobs[0], "run-"+at.Format("1504"), Result{}, nil, at.Add(time.Second))
		require.NoError(t, err)
	}
	got, err := Stats(context.Background(), db, StatsParams{From: before, To: after.Add(time.Minute)})
	require.NoError(t, err)
	assert.EqualValues(t, 2, got.Total.Attempts, "both sides of the gap are in the window")

	got, err = Stats(context.Background(), db, StatsParams{From: before.Add(5 * time.Second), To: after})
	require.NoError(t, err)
	assert.Zero(t, got.Total.Attempts, "and neither is in the hour between them")
}
