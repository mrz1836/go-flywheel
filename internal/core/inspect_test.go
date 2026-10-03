package core

import (
	"context"
	"testing"
	"time"

	"github.com/mrz1836/go-foundation/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// inspectT0 anchors the live-read tests.
//
//nolint:gochecknoglobals // shared fixture instant
var inspectT0 = time.Date(2026, 9, 2, 12, 0, 0, 0, time.UTC)

// seedRunningJob writes a running job and, when startedAgo is non-negative, the
// stub of its current attempt started that long before inspectT0.
func seedRunning(t *testing.T, db *gorm.DB, id, kind, queue string, startedAgo, leaseLeft time.Duration) {
	t.Helper()
	leased := inspectT0.Add(leaseLeft)
	token := models.NewID()
	seedJob(t, db, jobRow{
		ID: id, Kind: kind, Queue: queue, State: string(StateRunning), Attempt: 2, MaxAttempts: 5,
		LeasedUntil: &leased, LeaseToken: &token, CreatedAt: inspectT0.Add(-time.Hour),
		UpdatedAt: inspectT0, ScheduledAt: inspectT0.Add(-time.Hour),
	})
	if startedAgo < 0 {
		return
	}
	seedRun(t, db, jobRunRow{
		ID: id + "-run", JobID: id, Attempt: 2, Kind: kind, Queue: queue, ExecutorClass: "gpu",
		ExecutorID: "host-1", StartedAt: inspectT0.Add(-startedAgo), Outcome: string(OutcomeStarted),
		CreatedAt: inspectT0.Add(-startedAgo),
	})
	// The previous attempt's row must not be mistaken for the current one.
	finished := inspectT0.Add(-2 * time.Hour)
	seedRun(t, db, jobRunRow{
		ID: id + "-run-1", JobID: id, Attempt: 1, Kind: kind, Queue: queue, ExecutorID: "host-0",
		StartedAt: inspectT0.Add(-3 * time.Hour), FinishedAt: &finished, Outcome: string(OutcomeError),
		CreatedAt: inspectT0.Add(-3 * time.Hour),
	})
}

// inspectSuite covers the live and recent read APIs against one dialect.
//
//nolint:gocognit,maintidx // one subtest per API
func inspectSuite(t *testing.T, open dbOpener) {
	t.Run("ListRunning reports each attempt, longest first, starting last", func(t *testing.T) {
		t.Parallel()
		db := open(t)
		seedRunning(t, db, "run-short", "k", "q1", time.Minute, time.Minute)
		seedRunning(t, db, "run-long", "k", "q2", time.Hour, -time.Second)
		seedRunning(t, db, "run-starting", "k", "q1", -1, time.Minute)
		seedJob(t, db, jobRow{ID: "not-running", Kind: "k", State: string(StateAvailable)})

		got, err := ListRunning(fixedClockCtx(inspectT0), db, ListRunningParams{})
		require.NoError(t, err)
		require.Len(t, got, 3)
		assert.Equal(t, []string{"run-long", "run-short", "run-starting"},
			[]string{got[0].JobID, got[1].JobID, got[2].JobID})

		long := got[0]
		assert.Equal(t, "run-long-run", long.RunID, "joined to the current attempt, not an earlier one")
		assert.Equal(t, time.Hour, long.Elapsed)
		assert.True(t, long.LeaseExpired, "its lease lapsed a second ago")
		assert.Equal(t, "gpu", long.ExecutorClass)
		assert.Equal(t, "host-1", long.ExecutorID)
		assert.Equal(t, 2, long.Attempt)
		assert.Equal(t, 5, long.MaxAttempts)
		assert.False(t, got[1].LeaseExpired)

		starting := got[2]
		assert.True(t, starting.Starting, "claimed but its audit row has not landed")
		assert.Nil(t, starting.StartedAt, "never inferred from updated_at")
		assert.Zero(t, starting.Elapsed)

		filtered, err := ListRunning(fixedClockCtx(inspectT0), db, ListRunningParams{Queue: "q1", Limit: 1})
		require.NoError(t, err)
		require.Len(t, filtered, 1)
		assert.Equal(t, "run-short", filtered[0].JobID)
		none, err := ListRunning(fixedClockCtx(inspectT0), db, ListRunningParams{Kind: "other"})
		require.NoError(t, err)
		assert.Empty(t, none)
	})

	t.Run("ListRunning flags a run far past its kind's baseline", func(t *testing.T) {
		t.Parallel()
		db := open(t)
		// A baseline of 300 one-second runs: p99 ~1s, so the slow line is
		// max(3s, 1s+1m) = 61s.
		var hist []histRun
		for i := range 300 {
			hist = append(hist, okRun("k", inspectT0.Add(-3*time.Hour).Add(time.Duration(i)*time.Second), 1000))
		}
		seedHistory(t, db, hist)
		rollupAll(t, db, inspectT0)
		seedRunning(t, db, "slow", "k", "q", 2*time.Minute, time.Minute)
		seedRunning(t, db, "fine", "k", "q", 30*time.Second, time.Minute)
		seedRunning(t, db, "nobase", "unseen", "q", time.Hour, time.Minute)

		got, err := ListRunning(fixedClockCtx(inspectT0), db, ListRunningParams{WithBaseline: true})
		require.NoError(t, err)
		byID := map[string]RunningJob{}
		for _, r := range got {
			byID[r.JobID] = r
		}
		require.NotNil(t, byID["slow"].BaselineP99)
		assert.InDelta(t, float64(time.Second), float64(*byID["slow"].BaselineP99), float64(100*time.Millisecond))
		assert.True(t, byID["slow"].Slow)
		assert.False(t, byID["fine"].Slow)
		assert.Nil(t, byID["nobase"].BaselineP99, "a kind with no baseline is never called slow")
		assert.False(t, byID["nobase"].Slow)
	})

	t.Run("ListFinished merges the terminal states newest first and pages", func(t *testing.T) {
		t.Parallel()
		db := open(t)
		states := []JobState{StateSucceeded, StateDiscarded, StateCancelled}
		for i := range 9 {
			fin := inspectT0.Add(time.Duration(i) * time.Minute)
			seedJob(t, db, jobRow{
				ID: "fin-" + string(rune('a'+i)), Kind: []string{"x", "y"}[i%2], Queue: "q",
				State: string(states[i%3]), FinalizedAt: &fin,
			})
		}
		seedJob(t, db, jobRow{ID: "in-flight", Kind: "x", State: string(StateRunning)})

		page, err := ListFinished(context.Background(), db, ListFinishedParams{Limit: 4})
		require.NoError(t, err)
		require.Len(t, page, 4)
		assert.Equal(t, []string{"fin-i", "fin-h", "fin-g", "fin-f"}, viewIDs(page))
		require.NotNil(t, page[0].FinalizedAt)

		last := page[len(page)-1]
		next, err := ListFinished(context.Background(), db, ListFinishedParams{
			Limit: 4, Before: &FinishedCursor{FinalizedAt: *last.FinalizedAt, ID: last.ID},
		})
		require.NoError(t, err)
		assert.Equal(t, []string{"fin-e", "fin-d", "fin-c", "fin-b"}, viewIDs(next), "the cursor continues exactly")

		failed, err := ListFinished(context.Background(), db, ListFinishedParams{States: []JobState{StateDiscarded}})
		require.NoError(t, err)
		assert.Equal(t, []string{"fin-h", "fin-e", "fin-b"}, viewIDs(failed))

		since, err := ListFinished(context.Background(), db, ListFinishedParams{
			Kind: "y", Since: inspectT0.Add(4 * time.Minute),
		})
		require.NoError(t, err)
		assert.Equal(t, []string{"fin-h", "fin-f"}, viewIDs(since))

		_, err = ListFinished(context.Background(), db, ListFinishedParams{States: []JobState{StateRunning}})
		require.ErrorIs(t, err, ErrValidation)
	})

	t.Run("ListJobs pages by id with a queue filter", func(t *testing.T) {
		t.Parallel()
		db := open(t)
		var ids []string
		for i := range 5 {
			id := models.NewID()
			ids = append(ids, id)
			seedJob(t, db, jobRow{ID: id, Kind: "k", Queue: []string{"a", "b"}[i%2], State: string(StateAvailable)})
		}
		page, err := ListJobs(context.Background(), db, ListJobsParams{Limit: 2})
		require.NoError(t, err)
		assert.Equal(t, []string{ids[4], ids[3]}, viewIDs(page), "newest id first")
		next, err := ListJobs(context.Background(), db, ListJobsParams{Limit: 2, BeforeID: ids[3]})
		require.NoError(t, err)
		assert.Equal(t, []string{ids[2], ids[1]}, viewIDs(next))
		qa, err := ListJobs(context.Background(), db, ListJobsParams{Queue: "a"})
		require.NoError(t, err)
		assert.Equal(t, []string{ids[4], ids[2], ids[0]}, viewIDs(qa))
		assert.Equal(t, "a", qa[0].Queue)
		assert.Equal(t, []string{}, qa[0].Tags, "tags decode to an empty list, never null")
	})

	t.Run("SlowRuns ranks a recent window by duration", func(t *testing.T) {
		t.Parallel()
		db := open(t)
		timeout := histRun{
			Kind: "k", Outcome: OutcomeTimeout, JobState: StateRetryable,
			FinishedAt: inspectT0.Add(-time.Hour), DurationMs: 9000, WaitMs: 1,
		}
		old := okRun("k", inspectT0.Add(-48*time.Hour), 99_000)
		crash := histRun{Kind: "k", Outcome: OutcomeCrashed, FinishedAt: inspectT0.Add(-time.Hour), DurationMs: -1, WaitMs: 1}
		legacy := okRun("legacy", inspectT0.Add(-2*time.Hour), 5000)
		legacy.Legacy = true
		seedHistory(t, db, []histRun{
			okRun("k", inspectT0.Add(-time.Hour), 100), okRun("other", inspectT0.Add(-time.Hour), 8000),
			timeout, old, crash, legacy,
		})
		got, err := SlowRuns(fixedClockCtx(inspectT0), db, SlowRunsParams{})
		require.NoError(t, err)
		require.Len(t, got, 4, "crashed runs have no duration to rank; the old one is outside the day")
		assert.Equal(t, 9000, *got[0].DurationMs)
		assert.Equal(t, string(OutcomeTimeout), got[0].Outcome, "a timeout is a slow run")

		k, err := SlowRuns(fixedClockCtx(inspectT0), db, SlowRunsParams{Kind: "legacy"})
		require.NoError(t, err)
		require.Len(t, k, 1, "the kind filter resolves an old row's kind through its job")
		minDur, err := SlowRuns(fixedClockCtx(inspectT0), db, SlowRunsParams{MinDuration: 6 * time.Second, Limit: 1})
		require.NoError(t, err)
		require.Len(t, minDur, 1)
		assert.Equal(t, 9000, *minDur[0].DurationMs)
	})

	t.Run("QueueDepths and CountActiveByKind break the backlog down", func(t *testing.T) {
		t.Parallel()
		db := open(t)
		past, future := inspectT0.Add(-10*time.Minute), inspectT0.Add(time.Hour)
		seedJob(t, db, jobRow{ID: "r1", Kind: "k1", Queue: "a", State: string(StateAvailable), ScheduledAt: past})
		seedJob(t, db, jobRow{ID: "r2", Kind: "k1", Queue: "a", State: string(StateRetryable), ScheduledAt: past.Add(5 * time.Minute)})
		seedJob(t, db, jobRow{ID: "s1", Kind: "k2", Queue: "a", State: string(StateScheduled), ScheduledAt: future})
		seedJob(t, db, jobRow{ID: "run1", Kind: "k2", Queue: "b", State: string(StateRunning), ScheduledAt: past})
		seedJob(t, db, jobRow{ID: "p1", Kind: "k2", Queue: "b", State: string(StatePaused), ScheduledAt: past})
		seedJob(t, db, jobRow{ID: "done", Kind: "k1", Queue: "c", State: string(StateSucceeded), ScheduledAt: past})

		depths, err := QueueDepths(fixedClockCtx(inspectT0), db)
		require.NoError(t, err)
		require.Len(t, depths, 2, "a queue with only finished work has no depth")
		assert.Equal(t, QueueDepth{Queue: "a", Ready: 2, ScheduledAhead: 1, OldestReadyAge: 10 * time.Minute}, depths[0])
		assert.Equal(t, QueueDepth{Queue: "b", Running: 1, Paused: 1}, depths[1])

		counts, err := CountActiveByKind(context.Background(), db)
		require.NoError(t, err)
		assert.Equal(t, []ActiveCount{
			{Kind: "k1", Queue: "a", State: StateAvailable, Count: 1},
			{Kind: "k1", Queue: "a", State: StateRetryable, Count: 1},
			{Kind: "k2", Queue: "a", State: StateScheduled, Count: 1},
			{Kind: "k2", Queue: "b", State: StatePaused, Count: 1},
			{Kind: "k2", Queue: "b", State: StateRunning, Count: 1},
		}, counts)
	})

	t.Run("ListRuns returns the enriched view", func(t *testing.T) {
		t.Parallel()
		db := open(t)
		seedRunning(t, db, "j", "k", "q", time.Minute, time.Minute)
		runs, err := ListRuns(context.Background(), db, "j", ListRunsParams{})
		require.NoError(t, err)
		require.Len(t, runs, 2)
		assert.Equal(t, "j-run", runs[0].ID, "newest first")
		assert.Equal(t, 2, runs[0].Attempt)
		assert.Equal(t, "k", runs[0].Kind)
		assert.Equal(t, "q", runs[0].Queue)
		assert.Equal(t, "host-1", runs[0].ExecutorID)
		assert.Equal(t, "j", runs[0].JobID)

		older, err := ListRuns(context.Background(), db, "j", ListRunsParams{Before: runs[0].StartedAt.In(time.FixedZone("x", 7200))})
		require.NoError(t, err)
		require.Len(t, older, 1, "a cursor in any zone compares as the instant it is")
		assert.Equal(t, "j-run-1", older[0].ID)
	})
}

// viewIDs extracts ids from a page of job views.
func viewIDs(views []JobView) []string {
	out := make([]string, len(views))
	for i, v := range views {
		out[i] = v.ID
	}
	return out
}

// TestInspectSuite is the SQLite half of the live-read suite.
func TestInspectSuite(t *testing.T) {
	t.Parallel()
	inspectSuite(t, sqliteOpener)
}

// TestReadAPIsRejectANilDB covers the nil-handle guard every new read shares.
func TestReadAPIsRejectANilDB(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	_, err := ListRunning(ctx, nil, ListRunningParams{})
	require.Error(t, err)
	_, err = ListFinished(ctx, nil, ListFinishedParams{})
	require.Error(t, err)
	_, err = SlowRuns(ctx, nil, SlowRunsParams{})
	require.Error(t, err)
	_, err = QueueDepths(ctx, nil)
	require.Error(t, err)
	_, err = CountActiveByKind(ctx, nil)
	require.Error(t, err)
	_, err = Baselines(ctx, nil, 0)
	require.Error(t, err)
	_, err = Anomalies(ctx, nil, AnomalyParams{})
	require.Error(t, err)
	_, err = RebuildStats(ctx, nil, RebuildOpts{})
	require.Error(t, err)
	_, err = NormalizeRunTimestamps(ctx, nil)
	require.Error(t, err)
	_, err = StatsSeries(ctx, nil, SeriesParams{})
	require.Error(t, err)
}

// TestBaselinesMemoTracksTheWatermark proves the memo is invisible except in
// cost: a cached baseline is returned only while the watermark is unchanged, a
// rollup that closes another hour is seen at once, and a RebuildStats in the
// same process is too.
func TestBaselinesMemoTracksTheWatermark(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	var hist []histRun
	for i := range 250 {
		hist = append(hist, okRun("k", rollupBase.Add(time.Duration(i)*time.Second), 1000))
	}
	hist[0].ID = "memo-first"
	seedHistory(t, db, hist)
	rollupAll(t, db, rollupBase.Add(2*time.Hour))
	ctx := context.Background()

	first, err := Baselines(ctx, db, 0)
	require.NoError(t, err)
	require.Len(t, first, 1)
	assert.EqualValues(t, 250, first[0].Samples)
	first[0].Samples = -1 // a caller mutating its copy must not reach the memo
	again, err := Baselines(ctx, db, 0)
	require.NoError(t, err)
	assert.EqualValues(t, 250, again[0].Samples)

	// A later hour moves the watermark: the memo is not consulted.
	seedHistory(t, db, []histRun{okRun("k", rollupBase.Add(2*time.Hour+time.Minute), 1000)})
	rollupAll(t, db, rollupBase.Add(4*time.Hour))
	moved, err := Baselines(ctx, db, 0)
	require.NoError(t, err)
	assert.EqualValues(t, 251, moved[0].Samples)

	// A rebuild rewrites hours under an unchanged watermark, and clears the memo.
	require.NoError(t, db.Where("id = ?", "memo-first").Delete(&jobRunRow{}).Error)
	_, err = RebuildStats(fixedClockCtx(rollupBase.Add(4*time.Hour)), db,
		RebuildOpts{From: rollupBase, To: rollupBase.Add(time.Hour), Force: true})
	require.NoError(t, err)
	rebuilt, err := Baselines(ctx, db, 0)
	require.NoError(t, err)
	assert.EqualValues(t, 250, rebuilt[0].Samples)
}
