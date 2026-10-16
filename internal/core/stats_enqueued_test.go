package core

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// seedEnqueued inserts one waiting job of kind on queue per instant, each with
// the id the runtime would have minted then, and no run. It returns the ids.
func seedEnqueued(t testing.TB, db *gorm.DB, kind, queue string, at ...time.Time) []string {
	t.Helper()
	ids := make([]string, len(at))
	for i, a := range at {
		ids[i] = mintIDAt(a)
		seedJob(t, db, jobRow{
			ID: ids[i], Kind: kind, Queue: queue, State: string(StateAvailable), MaxAttempts: 1,
			CreatedAt: a.UTC(), UpdatedAt: a.UTC(), ScheduledAt: a.UTC(),
		})
	}
	return ids
}

// hourStatsRows reads the stored rollup rows of one hour, rolled_at zeroed.
func hourStatsRows(t testing.TB, db *gorm.DB, hour time.Time) []jobStatsHourlyRow {
	t.Helper()
	var out []jobStatsHourlyRow
	for _, r := range statsRows(t, db) {
		if r.BucketStartUnix == hour.Unix() {
			out = append(out, r)
		}
	}
	return out
}

// enqueuedByRow keys an hour's rows' enqueue counts by (kind, queue).
func enqueuedByRow(rows []jobStatsHourlyRow) map[[2]string]int64 {
	out := map[[2]string]int64{}
	for _, r := range rows {
		out[[2]string{r.Kind, r.Queue}] = r.Enqueued
	}
	return out
}

// enqueuedCountsSuite proves a job counts as enqueued in the hour its id names:
// in every row class the rollup writes for that hour, in Stats read raw and
// read from rollups alike, and for a job whose created_at a fixed clock set to
// another hour, in its id's hour and not created_at's.
func enqueuedCountsSuite(t *testing.T, open dbOpener) {
	t.Helper()
	db := open(t)
	ctx := context.Background()
	h1, h2, h3 := hourAt(rollupBase, 1), hourAt(rollupBase, 2), hourAt(rollupBase, 3)
	var runs []histRun
	for i, at := range []time.Time{
		h1.Add(5 * time.Minute), h1.Add(20 * time.Minute), h1.Add(59 * time.Minute),
		h2.Add(10 * time.Minute), h2.Add(40 * time.Minute),
	} {
		r := okRun("alpha", h3.Add(time.Duration(i+1)*time.Minute), 100)
		r.Queue, r.EnqueuedAt = "bulk", at
		runs = append(runs, r)
	}
	seedHistory(t, db, runs)
	clocked, err := Enqueue(fixedClockCtx(h1.Add(30*time.Minute)), NewClient(db), "clocked", []byte(`{}`),
		InsertOpts{Queue: "bulk"})
	require.NoError(t, err)
	window := StatsParams{From: h1, To: h2.Add(time.Hour), MaxRawSpan: -1}

	raw, err := Stats(ctx, db, window)
	require.NoError(t, err)
	assert.True(t, raw.Coverage.RolledThrough.IsZero(), "nothing is rolled up yet")
	require.Len(t, raw.Kinds, 1, "the fixed-clock job is not in its created_at's hours")
	assert.Equal(t, "alpha", raw.Kinds[0].Kind)
	assert.EqualValues(t, 5, raw.Kinds[0].Enqueued)
	assert.EqualValues(t, 5, raw.Total.Enqueued)
	assert.Zero(t, raw.Total.Attempts, "the jobs finished later")

	rollupAll(t, db, hourAt(rollupBase, 4).Add(10*time.Minute))
	want := func(n int64) map[[2]string]int64 {
		return map[[2]string]int64{{"alpha", "bulk"}: n, {"alpha", statsTotalKey}: n, {statsTotalKey, statsTotalKey}: n}
	}
	assert.Equal(t, want(3), enqueuedByRow(hourStatsRows(t, db, h1)), "every row class of the first hour")
	assert.Equal(t, want(2), enqueuedByRow(hourStatsRows(t, db, h2)), "and of the second")
	for _, r := range hourStatsRows(t, db, h1) {
		assert.Zero(t, r.Attempts, "the hour holds inserts and no finished run")
	}
	assert.Equal(t, want(0), enqueuedByRow(hourStatsRows(t, db, h3)), "the hour the runs finished in counts no insert")

	rolled, err := Stats(ctx, db, window)
	require.NoError(t, err)
	assert.False(t, rolled.Coverage.RolledThrough.Before(window.To), "the window is served from rollups")
	assert.Equal(t, raw.Kinds, rolled.Kinds, "raw and rolled reads agree")
	assert.Equal(t, raw.Total, rolled.Total)

	at, ok := idTime(clocked)
	require.True(t, ok)
	hour := floorHour(at)
	require.False(t, hour.Equal(h1), "the id was minted by the wall clock")
	own, err := Stats(ctx, db, StatsParams{From: hour, To: hour.Add(time.Hour), Kind: "clocked", MaxRawSpan: -1})
	require.NoError(t, err)
	assert.EqualValues(t, 1, own.Total.Enqueued, "the job counts in the hour its id names")
}

// TestEnqueuedCountsEachJobInItsInsertHour runs the suite on SQLite.
func TestEnqueuedCountsEachJobInItsInsertHour(t *testing.T) {
	t.Parallel()
	enqueuedCountsSuite(t, sqliteOpener)
}

// TestTheRollupWalkRollsAnHourWithOnlyEnqueues proves an hour that received
// inserts and finished nothing is rolled, walking forward or backward, so its
// enqueue counts are stored and the covered range moves past it.
func TestTheRollupWalkRollsAnHourWithOnlyEnqueues(t *testing.T) {
	t.Parallel()
	h1, h5 := hourAt(rollupBase, 1), hourAt(rollupBase, 5)
	now := hourAt(rollupBase, 7).Add(10 * time.Minute)
	seed := func(t *testing.T) *gorm.DB {
		t.Helper()
		db := newDB(t)
		seedEnqueued(t, db, "alpha", "bulk", h1.Add(time.Minute), h1.Add(2*time.Minute), h1.Add(3*time.Minute),
			h1.Add(4*time.Minute))
		seedHistory(t, db, []histRun{okRun("alpha", h5.Add(time.Minute), 100)})
		return db
	}
	check := func(t *testing.T, db *gorm.DB) {
		t.Helper()
		rows := hourStatsRows(t, db, h1)
		require.NotEmpty(t, rows, "the hour of only inserts has rows")
		for _, r := range rows {
			assert.EqualValues(t, 4, r.Enqueued)
			assert.Zero(t, r.Attempts)
		}
		p, _, err := readStatsProgress(context.Background(), db)
		require.NoError(t, err)
		assert.True(t, p.covers(h1), "the covered range reaches past it")
	}

	t.Run("forward", func(t *testing.T) {
		t.Parallel()
		db := seed(t)
		res := rollupAll(t, db, now)
		assert.Equal(t, []time.Time{h1, h5}, res.Rolled, "the walk visits the insert hour and the finish hour")
		check(t, db)
	})
	t.Run("backward", func(t *testing.T) {
		t.Parallel()
		db := seed(t)
		ctx := fixedClockCtx(now)
		_, err := RebuildStats(ctx, db, RebuildOpts{From: h5, To: h5.Add(time.Hour)})
		require.NoError(t, err, "a rebuild starts the covered range above the insert hour")
		res := rollupAll(t, db, now)
		assert.Contains(t, res.Rolled, h1, "the walk back to the retention finds it")
		check(t, db)
	})
}

// TestRebuildRefusesAnHourWhoseEnqueuesShrank proves the shrink guard covers
// enqueue counts: an hour rolled before they existed (stored as zero) is filled
// by a rebuild, a rebuild that would count fewer jobs than the hour stores —
// one was deleted since — is refused and leaves the hour as it was, and Force
// rebuilds it anyway.
func TestRebuildRefusesAnHourWhoseEnqueuesShrank(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	h1 := hourAt(rollupBase, 1)
	ids := seedEnqueued(t, db, "alpha", "bulk", h1.Add(time.Minute), h1.Add(2*time.Minute), h1.Add(3*time.Minute))
	now := hourAt(rollupBase, 3).Add(10 * time.Minute)
	ctx := fixedClockCtx(now)
	rollupAll(t, db, now)
	enqueued := func() int64 {
		t.Helper()
		got, err := Stats(ctx, db, StatsParams{From: h1, To: h1.Add(time.Hour)})
		require.NoError(t, err)
		require.False(t, got.Coverage.RolledThrough.Before(h1.Add(time.Hour)), "served from the rollup")
		return got.Total.Enqueued
	}
	require.EqualValues(t, 3, enqueued())

	require.NoError(t, db.Model(&jobStatsHourlyRow{}).Where("bucket_start_unix = ?", h1.Unix()).
		Update("enqueued", 0).Error)
	assert.Zero(t, enqueued(), "an hour rolled before enqueue counts reads zero")
	_, err := RebuildStats(ctx, db, RebuildOpts{From: h1, To: h1.Add(time.Hour)})
	require.NoError(t, err, "a rebuild that counts more is no shrink")
	assert.EqualValues(t, 3, enqueued(), "and stores the count")

	require.NoError(t, db.Exec("DELETE FROM jobs WHERE id = ?", ids[0]).Error)
	_, err = RebuildStats(ctx, db, RebuildOpts{From: h1, To: h1.Add(time.Hour)})
	require.ErrorIs(t, err, ErrValidation)
	assert.Contains(t, err.Error(), "enqueued", "the refusal says what shrank")
	assert.EqualValues(t, 3, enqueued(), "the hour is unchanged")

	_, err = RebuildStats(ctx, db, RebuildOpts{From: h1, To: h1.Add(time.Hour), Force: true})
	require.NoError(t, err)
	assert.EqualValues(t, 2, enqueued(), "Force replaces it")
}

// TestBaselinesIgnoreHoursWithOnlyEnqueues proves a rolled row that holds only
// inserts changes no baseline: a kind with inserts and no finished attempt is
// not listed, and a kind's baseline is the same whether or not its rows also
// hold insert-only hours.
func TestBaselinesIgnoreHoursWithOnlyEnqueues(t *testing.T) {
	t.Parallel()
	now := hourAt(rollupBase, 8).Add(10 * time.Minute)
	build := func(t *testing.T, withInserts bool) []Baseline {
		t.Helper()
		db := newDB(t)
		var runs []histRun
		for h := range 6 {
			runs = append(runs, okRun("alpha", hourAt(rollupBase, h).Add(time.Minute), 100*(h+1)),
				discardRun("alpha", hourAt(rollupBase, h).Add(2*time.Minute)))
		}
		seedHistory(t, db, runs)
		if withInserts {
			seedEnqueued(t, db, "omega", defaultQueue, hourAt(rollupBase, 1), hourAt(rollupBase, 2))
			seedEnqueued(t, db, "alpha", defaultQueue, hourAt(rollupBase, 6), hourAt(rollupBase, 7))
		}
		rollupAll(t, db, now)
		got, err := Baselines(fixedClockCtx(now), db, 7*24*time.Hour)
		require.NoError(t, err)
		return got
	}

	with, without := build(t, true), build(t, false)
	require.Len(t, with, 1, "a kind with inserts alone has no baseline")
	assert.Equal(t, "alpha", with[0].Kind)
	assert.Equal(t, without, with, "and the inserts change no kind's baseline")
}
