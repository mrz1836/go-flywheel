package core

import (
	"context"
	"fmt"
	"math/rand/v2"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mrz1836/go-foundation/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/datatypes"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// coverageSuite pins the covered range job_stats_progress records: which hours
// the rollups speak for, how every writer extends it, and what reads and
// retention do at its edges. It runs against each dialect.
//
//nolint:gocognit,maintidx // one subtest per guarantee the covered range makes
func coverageSuite(t *testing.T, open dbOpener) {
	t.Run("a rebuild past the watermark rolls the gap instead of skipping it", func(t *testing.T) {
		t.Parallel()
		db := open(t)
		var runs []histRun
		for i := range 30 {
			runs = append(runs, okRun("a", hourAt(rollupBase, i).Add(time.Minute), 100))
		}
		seedHistory(t, db, runs)
		now := hourAt(rollupBase, 31)
		ctx := fixedClockCtx(now)

		// The rollup is catching up: five hours in.
		res, err := rollupPass(ctx, db, now, rollupConfig{grace: defaultStatsRollupGrace, maxHours: 5, retention: defaultStatsRetention})
		require.NoError(t, err)
		require.Equal(t, hourAt(rollupBase, 5), res.Watermark)

		// An operator rebuilds the last few hours to see them now.
		rb, err := RebuildStats(ctx, db, RebuildOpts{From: hourAt(rollupBase, 25), To: hourAt(rollupBase, 30)})
		require.NoError(t, err)
		assert.Equal(t, 25, rb.Hours, "the five requested hours, and the twenty with runs between the watermark and them")
		assert.Equal(t, hourAt(rollupBase, 30), rb.RolledThrough)
		assertCoveredHoursExact(t, db)

		got, err := Stats(ctx, db, StatsParams{From: rollupBase, To: hourAt(rollupBase, 30)})
		require.NoError(t, err)
		assert.EqualValues(t, 30, got.Total.Attempts, "no hour between the watermark and the rebuild was skipped")
		assert.Zero(t, got.Coverage.RawSpan, "and every one of them is served from rollups")
	})

	t.Run("a rebuild before any rollup becomes the range, and the rollup extends it both ways", func(t *testing.T) {
		t.Parallel()
		db := open(t)
		var runs []histRun
		for i := range 30 {
			runs = append(runs, okRun("a", hourAt(rollupBase, i).Add(time.Minute), 100))
		}
		seedHistory(t, db, runs)
		now := hourAt(rollupBase, 31)
		ctx := fixedClockCtx(now)

		rb, err := RebuildStats(ctx, db, RebuildOpts{From: hourAt(rollupBase, 10), To: hourAt(rollupBase, 20)})
		require.NoError(t, err)
		assert.Equal(t, hourAt(rollupBase, 10), rb.RolledFrom)
		assert.Equal(t, hourAt(rollupBase, 20), rb.RolledThrough)

		got, err := Stats(ctx, db, StatsParams{From: rollupBase, To: hourAt(rollupBase, 30), MaxRawSpan: -1})
		require.NoError(t, err)
		assert.EqualValues(t, 30, got.Total.Attempts, "hours outside the rebuilt range are read raw, not as zeros")
		assert.Equal(t, 20*time.Hour, got.Coverage.RawSpan)

		res := rollupAll(t, db, now)
		assert.True(t, res.CaughtUp)
		assert.Equal(t, hourAt(rollupBase, 30), res.Watermark, "forward from the rebuilt range")
		assert.Equal(t, floorHour(now.Add(-defaultStatsRetention)), res.RolledFrom, "and back to the retention")
		assertCoveredHoursExact(t, db)
	})

	t.Run("a rebuild below the range rolls the gap up to it", func(t *testing.T) {
		t.Parallel()
		db := open(t)
		var runs []histRun
		for i := range 30 {
			runs = append(runs, okRun("a", hourAt(rollupBase, i).Add(time.Minute), 100))
		}
		seedHistory(t, db, runs)
		now := hourAt(rollupBase, 31)
		ctx := fixedClockCtx(now)
		_, err := RebuildStats(ctx, db, RebuildOpts{From: hourAt(rollupBase, 20), To: hourAt(rollupBase, 25)})
		require.NoError(t, err)

		rb, err := RebuildStats(ctx, db, RebuildOpts{From: hourAt(rollupBase, 2), To: hourAt(rollupBase, 5)})
		require.NoError(t, err)
		assert.Equal(t, hourAt(rollupBase, 2), rb.RolledFrom, "the range reaches down to the rebuilt hours")
		assert.Equal(t, hourAt(rollupBase, 25), rb.RolledThrough)
		assert.Equal(t, 18, rb.Hours, "three requested hours and the fifteen with runs above them")
		assertCoveredHoursExact(t, db)
	})

	t.Run("a window older than the rollups is read raw, not as zeros", func(t *testing.T) {
		t.Parallel()
		db := open(t)
		now := hourAt(rollupBase, 0)
		seedHistory(t, db, []histRun{
			okRun("old", now.Add(-60*24*time.Hour), 100),
			okRun("recent", now.Add(-10*24*time.Hour), 100),
		})
		ctx := fixedClockCtx(now)
		res, err := rollupPass(ctx, db, now, rollupConfig{
			grace: defaultStatsRollupGrace, maxHours: 1000, retention: 30 * 24 * time.Hour,
		})
		require.NoError(t, err)
		require.Equal(t, now.Add(-30*24*time.Hour), res.RolledFrom)

		got, err := Stats(ctx, db, StatsParams{From: now.Add(-61 * 24 * time.Hour), To: now.Add(-59 * 24 * time.Hour)})
		require.NoError(t, err)
		assert.EqualValues(t, 1, got.Total.Attempts, "the run older than the rollups' retention is still counted")

		got, err = Stats(ctx, db, StatsParams{From: now.Add(-40 * 24 * time.Hour), To: now})
		require.NoError(t, err, "the ten days before the rollups' reach hold no run, so cost nothing")
		assert.EqualValues(t, 1, got.Total.Attempts)
		assert.Equal(t, now.Add(-30*24*time.Hour), got.Coverage.RolledFrom)

		_, err = Stats(ctx, db, StatsParams{From: now.Add(-90 * 24 * time.Hour), To: now})
		require.ErrorIs(t, err, ErrStatsNotRolledUp, "thirty days of raw history before the rollups' reach is over the cap")
		assert.Contains(t, err.Error(), "StatsRetention")
		got, err = Stats(ctx, db, StatsParams{From: now.Add(-90 * 24 * time.Hour), To: now, MaxRawSpan: -1})
		require.NoError(t, err)
		assert.EqualValues(t, 2, got.Total.Attempts)

		points, err := StatsSeries(ctx, db, SeriesParams{
			From: now.Add(-61 * 24 * time.Hour), To: now, Interval: IntervalDay, MaxRawSpan: -1,
		})
		require.NoError(t, err)
		var total int64
		for _, p := range points {
			total += p.Stats.Attempts
		}
		assert.EqualValues(t, 2, total, "a series reads the hours before the rollups raw too")
	})

	t.Run("a backfill re-rolls the covered hours it adds runs to", func(t *testing.T) {
		t.Parallel()
		db := open(t)
		h := hourAt(rollupBase, 0)
		seedHistory(t, db, []histRun{okRun("a", h.Add(time.Minute), 100), okRun("a", hourAt(rollupBase, 5), 100)})
		now := hourAt(rollupBase, 8)
		rollupAll(t, db, now)

		// An older binary, still running during a rolling deploy, finalized runs
		// in hours the rollup has already closed: one beside a rolled run, one in
		// a quiet hour the rollup skipped.
		writeUnloggedLegacyRuns(t, db, []histRun{
			{Kind: "a", Outcome: OutcomeSuccess, FinishedAt: h.Add(30 * time.Minute), DurationMs: 200, WaitMs: 5},
			{Kind: "b", Outcome: OutcomeError, JobState: StateDiscarded, FinishedAt: hourAt(rollupBase, 2).Add(time.Minute), DurationMs: 50, WaitMs: 5},
		})
		n, err := BackfillRunFinishes(fixedClockCtx(now), db)
		require.NoError(t, err)
		assert.EqualValues(t, 2, n)
		assertCoveredHoursExact(t, db)

		got, err := Stats(context.Background(), db, StatsParams{From: h, To: hourAt(rollupBase, 6)})
		require.NoError(t, err)
		assert.Zero(t, got.Coverage.RawSpan)
		assert.EqualValues(t, 4, got.Total.Attempts, "both stragglers are in their hours' rollups")
		assert.EqualValues(t, 1, mustKind(t, got, "b").Discarded, "resolved through the job, like any legacy row")
	})

	t.Run("a backfill with nothing to do does not walk job_runs", func(t *testing.T) {
		t.Parallel()
		db := open(t)
		var runs []histRun
		for i := range 50 {
			runs = append(runs, okRun("a", hourAt(rollupBase, i%5).Add(time.Duration(i)*time.Second), 100))
		}
		seedHistory(t, db, runs)
		_, err := SeedRun(context.Background(), db, RunSeed{
			JobID: "no-such-job", Attempt: 1, ExecutorID: "importer", Outcome: OutcomeSuccess,
			StartedAt: rollupBase, FinishedAt: &rollupBase,
		})
		require.NoError(t, err, "a logged orphan seed has an empty kind, and must not look unlogged")

		sqls, counted := countStatements(db)
		n, err := BackfillRunFinishes(context.Background(), counted)
		require.NoError(t, err)
		assert.Zero(t, n)
		for _, q := range *sqls {
			assert.NotContains(t, q, fmt.Sprintf("LIMIT %d", backfillBatchSize), "no batch walk: %s", q)
			assert.NotContains(t, q, "INSERT", "no write: %s", q)
		}
		assert.LessOrEqual(t, len(*sqls), 2, "the probe, and on SQLite the normalize check: %v", *sqls)
	})

	t.Run("retention holds for the range, and for history the rollup is working back through", func(t *testing.T) {
		t.Parallel()
		db := open(t)
		var runs []histRun
		for i := range 40 {
			runs = append(runs, okRun("a", hourAt(rollupBase, i).Add(time.Minute), 100))
		}
		seedHistory(t, db, runs)
		now := hourAt(rollupBase, 48)
		ctx := fixedClockCtx(now)
		hold := RetentionOpts{HoldForStatsRollup: true}
		jobsLeft := func() int64 {
			var n int64
			require.NoError(t, db.Model(&jobRow{}).Count(&n).Error)
			return n
		}

		n, err := DeleteFinishedJobsWithOptions(ctx, db, now, hold)
		require.NoError(t, err)
		assert.Zero(t, n, "nothing is covered, so nothing may go")

		// A rebuild covers hours 30–39, then the rollup arrives with a retention
		// reaching back to hour 8 and a budget of one hour: it closes the quiet
		// hours after 39 and rolls hour 29, working back toward hour 8.
		_, err = RebuildStats(ctx, db, RebuildOpts{From: hourAt(rollupBase, 30), To: hourAt(rollupBase, 40)})
		require.NoError(t, err)
		cfg := rollupConfig{grace: defaultStatsRollupGrace, maxHours: 1, retention: 40 * time.Hour}
		res, err := rollupPass(ctx, db, now, cfg)
		require.NoError(t, err)
		assert.Equal(t, []time.Time{hourAt(rollupBase, 29)}, res.Rolled)
		assert.False(t, res.CaughtUp)
		assert.Equal(t, hourAt(rollupBase, 29), res.RolledFrom)

		cut, ok, err := statsRetentionCap(ctx, db)
		require.NoError(t, err)
		require.True(t, ok)
		assert.Equal(t, hourAt(rollupBase, 8), cut, "the runs between hour 8 and the range are not counted yet")
		n, err = DeleteFinishedJobsWithOptions(ctx, db, hourAt(rollupBase, 35), hold)
		require.NoError(t, err)
		assert.EqualValues(t, 8, n, "only the jobs older than the rollup's retention go")

		cfg.maxHours = 1000
		res, err = rollupPass(ctx, db, now, cfg)
		require.NoError(t, err)
		require.True(t, res.CaughtUp)
		n, err = DeleteFinishedJobsWithOptions(ctx, db, hourAt(rollupBase, 35), hold)
		require.NoError(t, err)
		assert.EqualValues(t, 27, n, "once the history is counted, the cutoff is the caller's")
		assert.EqualValues(t, 5, jobsLeft())

		got, err := Stats(ctx, db, StatsParams{From: hourAt(rollupBase, 8), To: hourAt(rollupBase, 40)})
		require.NoError(t, err)
		assert.EqualValues(t, 32, got.Total.Attempts, "the rollups outlive the runs retention took")
	})

	t.Run("a pass with nothing to roll writes nothing", func(t *testing.T) {
		t.Parallel()
		db := open(t)
		seedHistory(t, db, []histRun{okRun("a", hourAt(rollupBase, 0).Add(time.Minute), 100)})
		now := hourAt(rollupBase, 3).Add(10 * time.Minute)
		rollupAll(t, db, now)
		before, _, err := readStatsProgress(context.Background(), db)
		require.NoError(t, err)

		// The rollup's next tick, a minute later: no hour has closed since.
		sqls, counted := countStatements(db)
		later := now.Add(time.Minute)
		res, err := rollupPass(fixedClockCtx(later), counted, later, rollupConfig{
			grace: defaultStatsRollupGrace, maxHours: 24, retention: defaultStatsRetention,
		})
		require.NoError(t, err)
		assert.True(t, res.CaughtUp)
		for _, q := range *sqls {
			assert.NotContains(t, q, "UPDATE", "an idle pass rewrites nothing: %s", q)
			assert.NotContains(t, q, "INSERT", "an idle pass inserts nothing: %s", q)
		}
		after, _, err := readStatsProgress(context.Background(), db)
		require.NoError(t, err)
		assert.Equal(t, before, after, "so the baselines memo stays valid across idle passes")
	})

	t.Run("a rollup write moves the version the baselines memo keys on", func(t *testing.T) {
		t.Parallel()
		db := open(t)
		h := hourAt(rollupBase, 0)
		seedHistory(t, db, []histRun{okRun("a", h.Add(time.Minute), 100)})
		now := hourAt(rollupBase, 3)
		rollupAll(t, db, now)
		before, _, err := readStatsProgress(context.Background(), db)
		require.NoError(t, err)

		_, err = RebuildStats(fixedClockCtx(now), db, RebuildOpts{From: h, To: h.Add(time.Hour)})
		require.NoError(t, err)
		after, _, err := readStatsProgress(context.Background(), db)
		require.NoError(t, err)
		assert.Equal(t, before.From, after.From)
		assert.Equal(t, before.To, after.To, "a rebuild inside the range does not move it")
		assert.Greater(t, after.Version, before.Version, "but it does move the version")
	})

	t.Run("a seeded failure counts as a discard or a retry by its job", func(t *testing.T) {
		t.Parallel()
		db := open(t)
		ctx := context.Background()
		h := hourAt(rollupBase, 0)
		at := func(m int) *time.Time { v := h.Add(time.Duration(m) * time.Minute); return &v }
		discarded := seedJobAt(t, db, "seed.kind", StateDiscarded, 2)
		retrying := seedJobAt(t, db, "seed.kind", StateRetryable, 1)
		for _, s := range []RunSeed{
			{JobID: discarded, Attempt: 1, Outcome: OutcomeError, FinishedAt: at(1)},
			{JobID: discarded, Attempt: 2, Outcome: OutcomeTimeout, FinishedAt: at(2)},
			{JobID: retrying, Attempt: 1, Outcome: OutcomeError, FinishedAt: at(3)},
			{JobID: "gone", Attempt: 1, Outcome: OutcomeError, FinishedAt: at(4)},
		} {
			s.ExecutorID, s.StartedAt = "importer", h
			_, err := SeedRun(ctx, db, s)
			require.NoError(t, err)
		}
		got, err := Stats(ctx, db, StatsParams{From: h, To: h.Add(time.Hour)})
		require.NoError(t, err)
		assert.EqualValues(t, 1, got.Total.Discarded, "the attempt that discarded its job")
		assert.EqualValues(t, 3, got.Total.Retries, "every other failed attempt was retried, or its job is gone")
		assert.Zero(t, got.Total.SuccessRate)

		_, err = RebuildStats(fixedClockCtx(h.Add(2*time.Hour)), db, RebuildOpts{From: h, To: h.Add(time.Hour)})
		require.NoError(t, err)
		rolled, err := Stats(ctx, db, StatsParams{From: h, To: h.Add(time.Hour)})
		require.NoError(t, err)
		assert.Zero(t, rolled.Coverage.RawSpan)
		assert.Equal(t, got.Total, rolled.Total, "the rollup counts it the same way")
	})

	t.Run("random maintenance keeps every covered hour exact", func(t *testing.T) {
		t.Parallel()
		for seed := range uint64(4) {
			randomMaintenance(t, open(t), seed)
		}
	})
}

// TestCoverageSuite is the SQLite half of the coverage suite.
func TestCoverageSuite(t *testing.T) {
	t.Parallel()
	coverageSuite(t, sqliteOpener)
}

// randomMaintenance runs a random mix of the operations that write rollups —
// bounded rollup passes at a moving clock and with changing retentions,
// rebuilds of random ranges, and backfills of stragglers an older binary wrote
// — and after every one asserts that each covered hour holds exactly what a
// fresh recompute of it would, and that random windows read equal to raw.
func randomMaintenance(t *testing.T, db *gorm.DB, seed uint64) {
	t.Helper()
	rng := rand.New(rand.NewPCG(seed, 31)) //nolint:gosec // deterministic test data
	seedHistory(t, db, randomHistory(rng, 400, rollupBase, 96*time.Hour))
	now := rollupBase.Add(time.Duration(8+rng.IntN(20)) * time.Hour)
	randHour := func(lo, hi int) time.Time { return hourAt(rollupBase, lo+rng.IntN(hi-lo)) }

	for step := range 40 {
		ctx := fixedClockCtx(now)
		var op string
		switch r := rng.IntN(100); {
		case r < 35:
			retention := defaultStatsRetention
			if rng.IntN(3) == 0 {
				retention = time.Duration(10+rng.IntN(40)) * time.Hour
			}
			op = fmt.Sprintf("rollupPass(retention %s)", retention)
			_, err := rollupPass(ctx, db, now, rollupConfig{
				grace: defaultStatsRollupGrace, maxHours: 1 + rng.IntN(6), retention: retention,
			})
			require.NoError(t, err, "seed %d step %d %s", seed, step, op)
		case r < 65:
			from := randHour(-4, 100)
			to := from.Add(time.Duration(1+rng.IntN(30)) * time.Hour)
			op = fmt.Sprintf("RebuildStats(%s, %s)", from.Format(time.RFC3339), to.Format(time.RFC3339))
			_, err := RebuildStats(ctx, db, RebuildOpts{From: from, To: to})
			require.NoError(t, err, "seed %d step %d %s", seed, step, op)
		case r < 80:
			at := rollupBase.Add(time.Duration(rng.Int64N(int64(now.Sub(rollupBase)))))
			op = "backfill straggler at " + at.Format(time.RFC3339)
			writeUnloggedLegacyRuns(t, db, []histRun{{
				Kind: "straggler", Outcome: OutcomeSuccess, FinishedAt: at, DurationMs: 10 + rng.IntN(1000), WaitMs: 1,
			}})
			_, err := BackfillRunFinishes(ctx, db)
			require.NoError(t, err, "seed %d step %d %s", seed, step, op)
		default:
			now = now.Add(time.Duration(rng.IntN(8)) * time.Hour)
			continue
		}
		assertCoveredHoursExact(t, db, "seed %d step %d after %s", seed, step, op)
		if step%8 == 7 {
			for range 3 {
				from := rollupBase.Add(time.Duration(rng.IntN(100*60)-120) * time.Minute)
				to := from.Add(time.Duration(1+rng.IntN(48*60)) * time.Minute)
				got, err := Stats(context.Background(), db, StatsParams{From: from, To: to, MaxRawSpan: -1})
				require.NoError(t, err)
				_, want := rawKindStats(t, db, from, to, statsFilter{})
				require.Equal(t, want, got.Total, "seed %d step %d window %s..%s", seed, step, from, to)
			}
		}
	}
}

// assertCoveredHoursExact asserts the covered range's invariant: every hour in
// it stores exactly the rows a fresh recompute of the hour renders — no rows
// for an hour without runs — and no rollup row exists past its upper edge.
func assertCoveredHoursExact(t *testing.T, db *gorm.DB, msgAndArgs ...any) {
	t.Helper()
	p, ok, err := readStatsProgress(context.Background(), db)
	require.NoError(t, err)
	if !ok || !p.covered() {
		return
	}
	// Histograms are compared as histograms: PostgreSQL's jsonb re-renders the
	// stored text.
	canonical := func(rows []jobStatsHourlyRow) []jobStatsHourlyRow {
		for i := range rows {
			for _, h := range []*datatypes.JSON{&rows[i].DurHist, &rows[i].WaitHist} {
				parsed, err := parseSparseHist(*h)
				require.NoError(t, err)
				*h = parsed.marshal()
			}
			rows[i].RolledAt = time.Time{}
		}
		return rows
	}
	stored := map[int64][]jobStatsHourlyRow{}
	for _, r := range canonical(statsRows(t, db)) {
		stored[r.BucketStartUnix] = append(stored[r.BucketStartUnix], r)
		require.True(t, p.covers(unixHour(r.BucketStartUnix)), "a rollup row outside the covered range %s..%s: %s %v",
			p.From.Format(time.RFC3339), p.To.Format(time.RFC3339), unixHour(r.BucketStartUnix).Format(time.RFC3339),
			msgAndArgs)
	}
	// Only an hour with a run, or with stored rows, can differ from its
	// recompute; every other covered hour recomputes to no rows and stores none
	// (the loop above proved no row lies outside the range). Visiting just those
	// keeps the check proportional to the history rather than to a 400-day range.
	var finishes []time.Time
	require.NoError(t, db.Model(&jobRunFinishRow{}).
		Where("finished_at >= ? AND finished_at < ?", p.From.UTC(), p.To.UTC()).Pluck("finished_at", &finishes).Error)
	hours := map[int64]struct{}{}
	for _, f := range finishes {
		hours[floorHour(f).Unix()] = struct{}{}
	}
	for bucket := range stored {
		hours[bucket] = struct{}{}
	}
	for sec := range hours {
		h := unixHour(sec)
		aggs, err := aggregateRuns(context.Background(), db, h, h.Add(time.Hour), statsFilter{})
		require.NoError(t, err)
		want := canonical(hourRows(h, aggs, time.Time{}))
		require.Equal(t, want, stored[h.Unix()], "covered hour %s: %v", h.Format(time.RFC3339), msgAndArgs)
	}
}

// writeUnloggedLegacyRuns writes runs the way an older binary did — kind,
// queue, and job state left at the column defaults, no finish-log entry — each
// with a job carrying its kind (and its discard), so a backfill and the read
// path have what they need.
func writeUnloggedLegacyRuns(t *testing.T, db *gorm.DB, runs []histRun) {
	t.Helper()
	marked := make([]histRun, len(runs))
	ids := make([]string, len(runs))
	for i := range runs {
		marked[i] = runs[i]
		marked[i].Legacy = true
		marked[i].ID = models.NewID()
		ids[i] = marked[i].ID
	}
	writeHistory(t, db, marked)
	require.NoError(t, db.Model(&jobRunRow{}).Where("id IN ?", ids).Update("job_state", nil).Error)
}

// seedJobAt inserts one job of kind at state on attempt, for SeedRun to attach
// runs to.
func seedJobAt(t *testing.T, db *gorm.DB, kind string, state JobState, attempt int) string {
	t.Helper()
	id, err := Enqueue(context.Background(), NewClient(db), kind, []byte(`{}`), InsertOpts{MaxAttempts: 5})
	require.NoError(t, err)
	require.NoError(t, db.Model(&jobRow{}).Where("id = ?", id).
		Updates(map[string]any{"state": string(state), "attempt": attempt}).Error)
	return id
}

// countStatements returns a session of db that records the SQL of every
// statement it runs.
func countStatements(db *gorm.DB) (*[]string, *gorm.DB) {
	rec := &statementRecorder{Interface: logger.Discard}
	return &rec.sqls, db.Session(&gorm.Session{Logger: rec})
}

// statementRecorder is a GORM logger that keeps the SQL it traces.
type statementRecorder struct {
	logger.Interface

	mu   sync.Mutex
	sqls []string
}

// Trace records one statement.
func (r *statementRecorder) Trace(_ context.Context, _ time.Time, fc func() (string, int64), _ error) {
	sql, _ := fc()
	r.mu.Lock()
	defer r.mu.Unlock()
	r.sqls = append(r.sqls, strings.Join(strings.Fields(sql), " "))
}

// LogMode keeps the recorder when GORM asks for a different level.
func (r *statementRecorder) LogMode(logger.LogLevel) logger.Interface { return r }

// TestBaselineMemoKeyIsThePool pins the memo's database identity: a
// WithContext copy, a session, and a transaction of one handle share a key, so a
// dashboard passing db.WithContext(r.Context()) hits the memo; another database
// does not.
func TestBaselineMemoKeyIsThePool(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	key, ok := newBaselineKey(db, time.Hour)
	require.True(t, ok)
	for name, h := range map[string]*gorm.DB{
		"WithContext": db.WithContext(context.Background()),
		"Session":     db.Session(&gorm.Session{}),
		"Prepared":    db.Session(&gorm.Session{PrepareStmt: true}),
	} {
		got, ok := newBaselineKey(h, time.Hour)
		require.True(t, ok, name)
		assert.Equal(t, key, got, name)
	}
	require.NoError(t, db.Transaction(func(tx *gorm.DB) error {
		got, ok := newBaselineKey(tx, time.Hour)
		require.True(t, ok)
		assert.Equal(t, key, got, "a transaction reads the same database")
		return nil
	}))
	other, ok := newBaselineKey(newDB(t), time.Hour)
	require.True(t, ok)
	assert.NotEqual(t, key, other)
	wider, _ := newBaselineKey(db, 2*time.Hour)
	assert.NotEqual(t, key, wider, "and the window is part of the key")
}

// TestBaselineMemoInvalidatesOnProgress pins the memo's validity: an entry
// answers only at the exact progress — range and version — it was taken at.
func TestBaselineMemoInvalidatesOnProgress(t *testing.T) {
	t.Parallel()
	c := &baselineCache{}
	key := baselineKey{window: time.Hour}
	p := statsProgress{From: rollupBase, To: rollupBase.Add(time.Hour), Version: 3}
	c.put(key, p, []Baseline{{Kind: "a"}})
	got, ok := c.get(key, p)
	require.True(t, ok)
	assert.Equal(t, []Baseline{{Kind: "a"}}, got)

	bumped := p
	bumped.Version++
	_, ok = c.get(key, bumped)
	assert.False(t, ok, "a rebuild in any process moves the version")
	moved := p
	moved.To = moved.To.Add(time.Hour)
	_, ok = c.get(key, moved)
	assert.False(t, ok, "and a rolled hour moves the range")

	for i := range baselineMemoLimit + 3 {
		c.put(baselineKey{window: time.Duration(i+2) * time.Hour}, p, nil)
		c.put(key, p, []Baseline{{Kind: "a"}})
	}
	assert.LessOrEqual(t, len(c.entries), baselineMemoLimit, "the memo is bounded")
}

// TestRunnerAndSchedulerRefuseAMissingFinishLog pins the startup probe on the
// table every finalize and every sweep writes: without job_run_finishes both
// fail fast rather than claiming, or sweeping, into a failing write.
func TestRunnerAndSchedulerRefuseAMissingFinishLog(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	require.NoError(t, db.Exec(`DROP TABLE job_run_finishes`).Error)
	ctx := context.Background()
	id, err := Enqueue(ctx, NewClient(db), "stats.write", []byte(`{}`), InsertOpts{})
	require.NoError(t, err)

	err = rwRunner(t, db, NewRegistry()).RunUntilIdle(ctx)
	require.ErrorIs(t, err, ErrSchemaOutdated)
	assert.Contains(t, err.Error(), "job_run_finishes (table)")
	assert.Equal(t, string(StateAvailable), jobState(t, db, id), "nothing was claimed")

	s := newSchedulerCfg(t, SchedulerConfig{DB: db, Client: NewClient(db)})
	runCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	err = s.Run(runCtx)
	require.ErrorIs(t, err, ErrSchemaOutdated, "the sweep writes the finish log too")
	assert.Contains(t, err.Error(), "job_run_finishes (table)")
}
