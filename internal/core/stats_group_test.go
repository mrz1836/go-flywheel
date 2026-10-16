package core

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// groupedHistory is two kinds on two queues across four hours, with a retry,
// a discard, a crash, and a superseded attempt, so every counter has something
// to sum.
func groupedHistory() []histRun {
	at := func(n int, m time.Duration) time.Time { return hourAt(rollupBase, n).Add(m) }
	runs := []histRun{
		okRun("alpha", at(0, time.Minute), 100), okRun("alpha", at(1, 2*time.Minute), 300),
		okRun("beta", at(1, 3*time.Minute), 200), okRun("beta", at(3, 4*time.Minute), 900),
		discardRun("alpha", at(2, 5*time.Minute)), discardRun("beta", at(3, 6*time.Minute)),
		{Kind: "alpha", Outcome: OutcomeError, JobState: StateRetryable, FinishedAt: at(1, 7*time.Minute), DurationMs: 20, WaitMs: 5},
		{Kind: "beta", Outcome: OutcomeCrashed, JobState: StateAvailable, FinishedAt: at(2, 8*time.Minute), DurationMs: -1, WaitMs: 5},
		{Kind: "alpha", Outcome: OutcomeSuccess, Superseded: true, FinishedAt: at(3, 9*time.Minute), DurationMs: 50, WaitMs: 5},
	}
	for i := range runs {
		runs[i].CostMicros = int64(10 * (i + 1))
		runs[i].EnqueuedAt = runs[i].FinishedAt.Add(-30 * time.Minute)
		if i%2 == 1 {
			runs[i].Queue = "bulk"
		}
	}
	return runs
}

// sumEntries adds up every counter of entries, for comparison with Total.
func sumEntries(entries []KindStats) KindStats {
	var s KindStats
	for _, e := range entries {
		s.Attempts += e.Attempts
		s.Outcomes.Success += e.Outcomes.Success
		s.Outcomes.Error += e.Outcomes.Error
		s.Outcomes.Timeout += e.Outcomes.Timeout
		s.Outcomes.Snooze += e.Outcomes.Snooze
		s.Outcomes.Cancelled += e.Outcomes.Cancelled
		s.Outcomes.Crashed += e.Outcomes.Crashed
		s.Succeeded += e.Succeeded
		s.Discarded += e.Discarded
		s.Retries += e.Retries
		s.Superseded += e.Superseded
		s.CostMicros += e.CostMicros
		s.Enqueued += e.Enqueued
		s.Duration.Count += e.Duration.Count
		s.QueueWait.Count += e.QueueWait.Count
	}
	return s
}

// TestStatsGroupedByQueueSumToTheTotal proves the two queue groupings: one
// entry per queue, or per kind and queue, each naming its group, sorted, and
// summing to the total, enqueue counts included — over rolled hours and the
// raw tail alike, and equal to
// the per-kind read's total. GroupBy left zero is the per-kind read.
func TestStatsGroupedByQueueSumToTheTotal(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	seedHistory(t, db, groupedHistory())
	res := rollupAll(t, db, hourAt(rollupBase, 2).Add(10*time.Minute))
	require.Equal(t, hourAt(rollupBase, 2), res.Watermark, "two hours rolled, two read raw")
	ctx := context.Background()
	window := StatsParams{From: hourAt(rollupBase, 0), To: hourAt(rollupBase, 4)}

	byKind, err := Stats(ctx, db, window)
	require.NoError(t, err)
	explicit := window
	explicit.GroupBy = StatsByKind
	same, err := Stats(ctx, db, explicit)
	require.NoError(t, err)
	assert.Equal(t, byKind, same, "GroupBy zero is StatsByKind")
	require.Len(t, byKind.Kinds, 2)
	for _, k := range byKind.Kinds {
		assert.Empty(t, k.Queue, "a per-kind entry names no queue")
	}

	byQueueParams := window
	byQueueParams.GroupBy = StatsByQueue
	byQueue, err := Stats(ctx, db, byQueueParams)
	require.NoError(t, err)
	require.Len(t, byQueue.Kinds, 2)
	assert.Equal(t, "bulk", byQueue.Kinds[0].Queue, "sorted by queue")
	assert.Equal(t, defaultQueue, byQueue.Kinds[1].Queue)
	for _, q := range byQueue.Kinds {
		assert.Empty(t, q.Kind, "a per-queue entry names no kind")
	}
	assert.EqualValues(t, 4, byQueue.Kinds[0].Attempts, "bulk: three finished attempts and a crash")
	assert.EqualValues(t, 4, byQueue.Kinds[1].Attempts)
	assert.EqualValues(t, 4, byQueue.Kinds[0].Enqueued, "each job inserted half an hour before its run finished")
	assert.EqualValues(t, 4, byQueue.Kinds[1].Enqueued, "the first hour's job was inserted before the window")

	pairParams := window
	pairParams.GroupBy = StatsByKindQueue
	pairs, err := Stats(ctx, db, pairParams)
	require.NoError(t, err)
	var keys [][2]string
	for _, p := range pairs.Kinds {
		keys = append(keys, [2]string{p.Kind, p.Queue})
	}
	assert.Equal(t, [][2]string{{"alpha", "bulk"}, {"alpha", defaultQueue}, {"beta", "bulk"}, {"beta", defaultQueue}}, keys,
		"sorted by kind, then queue")

	for name, got := range map[string]StatsResult{"by kind": byKind, "by queue": byQueue, "by kind and queue": pairs} {
		assert.Equal(t, byKind.Total, got.Total, "%s: the total is the same however the window is grouped", name)
		want := sumEntries([]KindStats{got.Total})
		assert.Equal(t, want, sumEntries(got.Kinds), "%s: the entries sum to the total", name)
	}

	// A queue filter narrows a grouping to that queue.
	filtered := byQueueParams
	filtered.Queue = "bulk"
	one, err := Stats(ctx, db, filtered)
	require.NoError(t, err)
	require.Len(t, one.Kinds, 1)
	assert.Equal(t, byQueue.Kinds[0], one.Kinds[0])
}

// TestStatsRefusesAnUnknownGroupBy proves a grouping outside the three is
// refused before anything is read: the database here has no tables, so a read
// would fail differently.
func TestStatsRefusesAnUnknownGroupBy(t *testing.T) {
	t.Parallel()
	_, err := Stats(context.Background(), newBareSQLite(t), StatsParams{
		From: rollupBase, To: rollupBase.Add(time.Hour), GroupBy: "lane",
	})
	require.ErrorIs(t, err, ErrValidation)
	var ve *ValidationError
	require.True(t, errors.As(err, &ve))
	assert.Equal(t, "group_by", ve.Field)
}
