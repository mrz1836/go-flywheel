package core

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// rollupBase anchors the rollup tests at a fixed UTC hour.
//
//nolint:gochecknoglobals // shared fixture instant
var rollupBase = time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)

// rollupSuite is the rollup's maintenance contract, run against one dialect.
//
//nolint:gocognit,maintidx // one subtest per guarantee the rollup makes
func rollupSuite(t *testing.T, open dbOpener) {
	t.Run("rolling an hour twice stores identical rows", func(t *testing.T) {
		t.Parallel()
		db := open(t)
		h := hourAt(rollupBase, 0)
		seedHistory(t, db, []histRun{
			okRun("a", h.Add(10*time.Minute), 120), okRun("a", h.Add(20*time.Minute), 900),
			discardRun("a", h.Add(30*time.Minute)), okRun("b", h.Add(40*time.Minute), 30),
		})
		ctx := fixedClockCtx(h.Add(2 * time.Hour))
		_, err := rollupHour(ctx, db, h, h.Add(2*time.Hour), false, coverageStep{})
		require.NoError(t, err)
		first := statsRows(t, db)
		_, err = rollupHour(ctx, db, h, h.Add(3*time.Hour), false, coverageStep{})
		require.NoError(t, err)
		assert.Equal(t, first, statsRows(t, db), "a re-roll is a replace with the same values")
		require.Len(t, first, 5, "two groups, their two per-kind totals, and the hour's total")
		groups := groupStatsRows(t, db)
		require.Len(t, groups, 2)
		assert.EqualValues(t, 3, groups[0].Attempts)
		assert.EqualValues(t, 1, groups[0].Discarded)
		assert.EqualValues(t, 2, groups[0].DurCount)
		assert.EqualValues(t, 900, groups[0].DurMaxMs)
		assert.Equal(t, histVersion, groups[0].HistVersion)
		total := first[0]
		require.Equal(t, statsTotalKey, total.Kind)
		assert.EqualValues(t, 4, total.Attempts, "the hour's total sums every group")
		assert.EqualValues(t, 3, total.DurCount)
		assert.EqualValues(t, 900, total.DurMaxMs)
	})

	t.Run("a recompute that loses a group deletes it", func(t *testing.T) {
		t.Parallel()
		db := open(t)
		h := hourAt(rollupBase, 0)
		gone := okRun("gone", h.Add(5*time.Minute), 100)
		gone.ID = "run-gone"
		seedHistory(t, db, []histRun{okRun("kept", h.Add(5*time.Minute), 100), gone})
		ctx := fixedClockCtx(h.Add(2 * time.Hour))
		_, err := rollupHour(ctx, db, h, h, false, coverageStep{})
		require.NoError(t, err)
		require.Len(t, groupStatsRows(t, db), 2)

		require.NoError(t, db.Where("id = ?", "run-gone").Delete(&jobRunRow{}).Error)
		_, err = rollupHour(ctx, db, h, h, false, coverageStep{})
		require.NoError(t, err)
		rows := groupStatsRows(t, db)
		require.Len(t, rows, 1, "replace semantics: a group the recompute no longer produces disappears")
		assert.Equal(t, "kept", rows[0].Kind)
		for _, r := range statsRows(t, db) {
			assert.NotEqual(t, "gone", r.Kind, "and so does its per-kind total")
		}
	})

	t.Run("the open hour and the grace window are left alone", func(t *testing.T) {
		t.Parallel()
		db := open(t)
		h := hourAt(rollupBase, 0)
		seedHistory(t, db, []histRun{okRun("a", h.Add(59*time.Minute), 100)})

		res := rollupAll(t, db, h.Add(time.Hour+4*time.Minute))
		assert.Zero(t, res.Hours, "the hour ended four minutes ago, inside the five-minute grace")
		assert.Equal(t, h, res.Watermark, "only the empty hours before it are recorded as processed")
		assert.Empty(t, statsRows(t, db), "an empty hour stores no rows; the progress row records it")

		res = rollupAll(t, db, h.Add(time.Hour+5*time.Minute))
		assert.Equal(t, 1, res.Hours, "past the grace the hour closes")
		assert.Equal(t, []time.Time{h}, res.Rolled)
	})

	t.Run("empty hours are skipped and the watermark advances through them", func(t *testing.T) {
		t.Parallel()
		db := open(t)
		seedHistory(t, db, []histRun{
			okRun("a", hourAt(rollupBase, 0).Add(time.Minute), 100),
			okRun("a", hourAt(rollupBase, 10).Add(time.Minute), 100),
		})
		now := hourAt(rollupBase, 20).Add(10 * time.Minute)
		res := rollupAll(t, db, now)
		assert.Equal(t, []time.Time{hourAt(rollupBase, 0), hourAt(rollupBase, 10)}, res.Rolled,
			"only hours with runs are rolled; the quiet ones are skipped in one probe each")
		assert.True(t, res.CaughtUp)
		assert.Equal(t, hourAt(rollupBase, 20), res.Watermark,
			"the quiet stretch after the last run is recorded, so the watermark reaches the last closed hour")

		res = rollupAll(t, db, now.Add(5*time.Hour))
		assert.Empty(t, res.Rolled)
		assert.Equal(t, hourAt(rollupBase, 25), res.Watermark)
		for _, r := range statsRows(t, db) {
			assert.NotZero(t, r.Attempts, "a quiet hour stores no row, so nothing accumulates")
		}
		assert.Len(t, statsRows(t, db), 6, "two rolled hours, each a group, a per-kind total, and a total")
	})

	t.Run("a pass stops at its ceiling and the next one resumes", func(t *testing.T) {
		t.Parallel()
		db := open(t)
		var runs []histRun
		for i := range 30 {
			runs = append(runs, okRun("a", hourAt(rollupBase, i).Add(time.Minute), 100))
		}
		seedHistory(t, db, runs)
		now := hourAt(rollupBase, 31)
		cfg := rollupConfig{grace: defaultStatsRollupGrace, maxHours: 24, retention: defaultStatsRetention}

		res, err := rollupPass(fixedClockCtx(now), db, now, cfg)
		require.NoError(t, err)
		assert.Equal(t, 24, res.Hours)
		assert.False(t, res.CaughtUp)
		assert.Equal(t, hourAt(rollupBase, 24), res.Watermark)

		res, err = rollupPass(fixedClockCtx(now), db, now, cfg)
		require.NoError(t, err)
		assert.Equal(t, 6, res.Hours)
		assert.True(t, res.CaughtUp)
	})

	t.Run("the first pass reaches back no further than the retention", func(t *testing.T) {
		t.Parallel()
		db := open(t)
		now := hourAt(rollupBase, 0)
		seedHistory(t, db, []histRun{
			okRun("ancient", now.Add(-500*24*time.Hour), 100),
			okRun("recent", now.Add(-2*time.Hour), 100),
		})
		rollupAll(t, db, now)
		rows := statsRows(t, db)
		for _, r := range rows {
			assert.NotEqual(t, "ancient", r.Kind, "history older than the retention is never rolled up")
		}
	})

	t.Run("rows older than the retention are pruned by key range", func(t *testing.T) {
		t.Parallel()
		db := open(t)
		h := hourAt(rollupBase, 0)
		seedHistory(t, db, []histRun{okRun("a", h.Add(time.Minute), 100), okRun("a", h.Add(49*time.Hour), 100)})
		now := h.Add(51 * time.Hour)
		rollupAll(t, db, now)
		require.Len(t, groupStatsRows(t, db), 2)

		res, err := rollupPass(fixedClockCtx(now), db, now,
			rollupConfig{grace: defaultStatsRollupGrace, maxHours: 24, retention: 24 * time.Hour})
		require.NoError(t, err)
		assert.EqualValues(t, 3, res.Pruned, "the hour's group, per-kind total, and total")
		rows := groupStatsRows(t, db)
		require.Len(t, rows, 1)
		assert.Equal(t, hourAt(rollupBase, 49).Unix(), rows[0].BucketStartUnix)
	})

	t.Run("RebuildStats clamps to closed hours and refuses to shrink a rollup", func(t *testing.T) {
		t.Parallel()
		db := open(t)
		h := hourAt(rollupBase, 0)
		late := okRun("a", h.Add(30*time.Minute), 100)
		late.ID = "run-pruned"
		seedHistory(t, db, []histRun{okRun("a", h.Add(10*time.Minute), 100), late})
		now := h.Add(3 * time.Hour)
		ctx := fixedClockCtx(now)
		rollupAll(t, db, now)

		res, err := RebuildStats(ctx, db, RebuildOpts{From: h.Add(-time.Hour), To: now.Add(24 * time.Hour)})
		require.NoError(t, err)
		assert.Equal(t, hourAt(rollupBase, 2), res.To, "To is clamped to the last closed hour")
		assert.Equal(t, 3, res.Hours)

		// Retention (or a host) deletes a run after its hour was rolled up.
		require.NoError(t, db.Where("id = ?", "run-pruned").Delete(&jobRunRow{}).Error)
		_, err = RebuildStats(ctx, db, RebuildOpts{From: h, To: h.Add(time.Hour)})
		require.ErrorIs(t, err, ErrValidation)
		assert.Contains(t, err.Error(), "Force")
		assert.EqualValues(t, 2, groupStatsRows(t, db)[0].Attempts, "the complete rollup is kept")

		_, err = RebuildStats(ctx, db, RebuildOpts{From: h, To: h.Add(time.Hour), Force: true})
		require.NoError(t, err)
		assert.EqualValues(t, 1, groupStatsRows(t, db)[0].Attempts, "Force rebuilds it anyway")

		_, err = RebuildStats(ctx, db, RebuildOpts{From: h})
		require.ErrorIs(t, err, ErrValidation)
	})

	t.Run("RebuildStats picks up a run that landed after its hour closed", func(t *testing.T) {
		t.Parallel()
		db := open(t)
		h := hourAt(rollupBase, 0)
		seedHistory(t, db, []histRun{okRun("a", h.Add(10*time.Minute), 100)})
		now := h.Add(3 * time.Hour)
		rollupAll(t, db, now)

		seedHistory(t, db, []histRun{okRun("a", h.Add(20*time.Minute), 200)}) // e.g. a SeedRun import
		res, err := Stats(fixedClockCtx(now), db, StatsParams{From: h, To: h.Add(time.Hour)})
		require.NoError(t, err)
		assert.EqualValues(t, 1, res.Total.Attempts, "the closed hour does not see the late run")

		_, err = RebuildStats(fixedClockCtx(now), db, RebuildOpts{From: h, To: h.Add(time.Hour)})
		require.NoError(t, err)
		res, err = Stats(fixedClockCtx(now), db, StatsParams{From: h, To: h.Add(time.Hour)})
		require.NoError(t, err)
		assert.EqualValues(t, 2, res.Total.Attempts, "until it is rebuilt")
	})

	t.Run("retention waits for the rollup and never passes its watermark", func(t *testing.T) {
		t.Parallel()
		db := open(t)
		now := hourAt(rollupBase, 48)
		var runs []histRun
		for i := range 40 {
			runs = append(runs, okRun("a", hourAt(rollupBase, i).Add(time.Minute), 100))
		}
		seedHistory(t, db, runs)
		s := newSchedulerCfg(t, SchedulerConfig{
			DB: db, Client: NewClient(db), Driver: driverForTest(t, db),
			RetentionMaxAge: 2 * time.Hour, StatsRollupInterval: time.Minute, StatsMaxHoursPerPass: 10,
		})
		ctx := fixedClockCtx(now)

		n, err := s.PruneRetention(ctx)
		require.NoError(t, err)
		assert.Zero(t, n, "nothing is pruned until the rollup has run")

		res, err := s.RollupStats(ctx)
		require.NoError(t, err)
		require.Equal(t, hourAt(rollupBase, 10), res.Watermark)

		n, err = s.PruneRetention(ctx)
		require.NoError(t, err)
		assert.EqualValues(t, 10, n, "only jobs finalized before the watermark go, not everything older than 2h")

		var left int64
		require.NoError(t, db.Model(&jobRunRow{}).Count(&left).Error)
		assert.EqualValues(t, 30, left, "the runs the rollup has not counted yet are kept")
	})
}

// TestRollupSuite is the SQLite half of the rollup suite.
func TestRollupSuite(t *testing.T) {
	t.Parallel()
	rollupSuite(t, sqliteOpener)
}

// TestRetentionShorterThanTheRollupNeedsIsRejected pins the coupling rule: with
// the rollup on, a retention window that deletes runs before an hour can close
// would make every rollup partial, so the configuration is refused.
func TestRetentionShorterThanTheRollupNeedsIsRejected(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	_, err := NewSchedulerWithConfig(SchedulerConfig{
		DB: db, Client: NewClient(db), Driver: NewSQLiteDriver(db),
		RetentionMaxAge: 30 * time.Minute, StatsRollupInterval: time.Minute,
	})
	require.ErrorIs(t, err, ErrValidation)
	assert.Contains(t, err.Error(), "StatsRollupGrace")

	_, err = NewSchedulerWithConfig(SchedulerConfig{
		DB: db, Client: NewClient(db), Driver: NewSQLiteDriver(db), RetentionMaxAge: 30 * time.Minute,
	})
	require.NoError(t, err, "without the rollup a short retention is the host's business")
}

// TestStatsActivityIsOptIn pins the structural opt-in: no interval, no
// activity — the same convention as the health heartbeat.
func TestStatsActivityIsOptIn(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	names := func(s *Scheduler) []string {
		var out []string
		for _, a := range s.activities() {
			out = append(out, a.name)
		}
		return out
	}
	off := newSchedulerCfg(t, SchedulerConfig{DB: db, Client: NewClient(db)})
	assert.NotContains(t, names(off), "stats")
	on := newSchedulerCfg(t, SchedulerConfig{DB: db, Client: NewClient(db), StatsRollupInterval: time.Minute})
	assert.Contains(t, names(on), "stats")
}

// TestRollupStatsWorksWithTheActivityOff proves RollupStats is callable by a
// host that drives its own maintenance loop, with the defaults filled in.
func TestRollupStatsWorksWithTheActivityOff(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	h := hourAt(rollupBase, 0)
	seedHistory(t, db, []histRun{okRun("a", h.Add(time.Minute), 100)})
	s := newSchedulerCfg(t, SchedulerConfig{DB: db, Client: NewClient(db)})
	res, err := s.RollupStats(fixedClockCtx(h.Add(2 * time.Hour)))
	require.NoError(t, err)
	assert.Equal(t, []time.Time{h}, res.Rolled)
}

// TestRollupPassHonoursCancellation proves a cancelled pass stops between
// hours with the hours it finished intact.
func TestRollupPassHonoursCancellation(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	seedHistory(t, db, []histRun{okRun("a", hourAt(rollupBase, 0).Add(time.Minute), 100)})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	now := hourAt(rollupBase, 5)
	_, err := rollupPass(ctx, db, now, rollupConfig{grace: time.Minute, maxHours: 24, retention: defaultStatsRetention})
	require.ErrorIs(t, err, context.Canceled)
}
