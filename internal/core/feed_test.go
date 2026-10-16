package core

import (
	"context"
	"encoding/base64"
	"fmt"
	"testing"
	"time"

	"github.com/mrz1836/go-foundation/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// feedNow is the feed fixture's clock: every finish is placed against it.
//
//nolint:gochecknoglobals // shared fixture instant
var feedNow = time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)

// feedCtx returns a context whose clock reads at.
func feedCtx(at time.Time) context.Context {
	return models.WithClock(context.Background(), models.NewFixedClock(at))
}

// feedRun is a successful run of kind, with the run id id, finishing at at.
func feedRun(id, kind string, at time.Time) histRun {
	r := okRun(kind, at, 120)
	r.ID = id
	return r
}

// feedRunIDs returns the run ids of a page, in page order.
func feedRunIDs(page RunFinishPage) []string {
	out := make([]string, len(page.Finishes))
	for i, f := range page.Finishes {
		out[i] = f.RunID
	}
	return out
}

// runJobID reads the job id a seeded run belongs to.
func runJobID(t *testing.T, db *gorm.DB, runID string) string {
	t.Helper()
	var jobID string
	require.NoError(t, db.Model(&jobRunRow{}).Where("id = ?", runID).Pluck("job_id", &jobID).Error)
	require.NotEmpty(t, jobID)
	return jobID
}

// TestFinishFeedNeverSkipsALateCommitWithinTheSettleWindow proves the feed holds
// back the newest Settle of finishes: a finalize that lands after a poll, stamped
// before a finish the database already held, is read by a later poll instead of
// being passed by the cursor. It also proves what each entry carries, and that a
// poll from the latest cursor returns nothing until another run finishes.
func TestFinishFeedNeverSkipsALateCommitWithinTheSettleWindow(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	superseded := histRun{
		ID: "run-a", Kind: "alpha", Outcome: OutcomeSuccess, Superseded: true,
		FinishedAt: feedNow.Add(-30 * time.Second), DurationMs: 120, WaitMs: 10,
	}
	failed := histRun{
		ID: "run-f", Kind: "beta", Queue: "bulk", Outcome: OutcomeError, JobState: StateRetryable,
		FinishedAt: feedNow.Add(-20 * time.Second), DurationMs: 50, WaitMs: 10,
		ErrorClass: ErrorTransient, ErrorMessage: "upstream refused",
	}
	writeHistory(t, db, []histRun{superseded, failed, feedRun("run-b", "alpha", feedNow.Add(-2*time.Second))})

	first, err := ListRunFinishes(feedCtx(feedNow), db, ListRunFinishesParams{Since: feedNow.Add(-time.Hour)})
	require.NoError(t, err)
	assert.Equal(t, []string{"run-a", "run-f"}, feedRunIDs(first), "run-b finished inside the settle window, so it waits")
	assert.True(t, first.Horizon.Equal(feedNow.Add(-DefaultFeedSettle)), "the horizon is now minus the settle: %v", first.Horizon)
	require.NotEmpty(t, first.NextCursor)
	require.Len(t, first.Finishes, 2)

	a := first.Finishes[0]
	assert.True(t, a.Superseded, "a superseded attempt is flagged")
	assert.Empty(t, a.JobState, "a superseded attempt applied no state")
	f := first.Finishes[1]
	assert.True(t, f.FinishedAt.Equal(failed.FinishedAt), "finished at %v", f.FinishedAt)
	assert.Equal(t, runJobID(t, db, "run-f"), f.JobID)
	assert.Equal(t, "beta", f.Kind)
	assert.Equal(t, "bulk", f.Queue)
	assert.Equal(t, 1, f.Attempt)
	assert.Equal(t, string(OutcomeError), f.Outcome)
	assert.Equal(t, string(StateRetryable), f.JobState)
	assert.Equal(t, string(ErrorTransient), f.ErrorClass)
	require.NotNil(t, f.DurationMs)
	assert.Equal(t, 50, *f.DurationMs)
	assert.False(t, f.Superseded)

	// A finalize stamped before run-b's finish commits only now.
	writeHistory(t, db, []histRun{feedRun("run-x", "alpha", feedNow.Add(-3*time.Second))})

	held, err := ListRunFinishes(feedCtx(feedNow), db, ListRunFinishesParams{After: first.NextCursor})
	require.NoError(t, err)
	assert.Empty(t, held.Finishes, "both newer finishes are still inside the settle window")
	assert.Equal(t, first.NextCursor, held.NextCursor, "a poll that scanned nothing keeps its position")

	later := feedCtx(feedNow.Add(10 * time.Second))
	settled, err := ListRunFinishes(later, db, ListRunFinishesParams{After: first.NextCursor})
	require.NoError(t, err)
	assert.Equal(t, []string{"run-x", "run-b"}, feedRunIDs(settled), "the late commit is read, in finish order")

	idle, err := ListRunFinishes(later, db, ListRunFinishesParams{After: settled.NextCursor})
	require.NoError(t, err)
	assert.Empty(t, idle.Finishes, "nothing finished since")
	assert.Equal(t, settled.NextCursor, idle.NextCursor)

	byQueue, err := ListRunFinishes(later, db, ListRunFinishesParams{Since: feedNow.Add(-time.Hour), Queues: []string{"bulk"}})
	require.NoError(t, err)
	assert.Equal(t, []string{"run-f"}, feedRunIDs(byQueue))
	byKind, err := ListRunFinishes(later, db, ListRunFinishesParams{
		Since: feedNow.Add(-time.Hour), Kinds: []string{"alpha", "gamma"},
	})
	require.NoError(t, err)
	assert.Equal(t, []string{"run-a", "run-x", "run-b"}, feedRunIDs(byKind))
}

// TestFinishFeedResumesExactlyFromItsCursor proves the cursor is exact: five
// finishes sharing one finished_at, read two at a time, each appear once, in
// (finished_at, run_id) order, with the finishes on either side of them.
func TestFinishFeedResumesExactlyFromItsCursor(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	tie := feedNow.Add(-time.Minute)
	runs := []histRun{feedRun("run-0", "alpha", tie.Add(-time.Second))}
	for _, id := range []string{"run-3", "run-1", "run-5", "run-2", "run-4"} {
		runs = append(runs, feedRun(id, "alpha", tie))
	}
	runs = append(runs, feedRun("run-6", "alpha", tie.Add(time.Second)))
	writeHistory(t, db, runs)

	ctx := feedCtx(feedNow)
	var got []string
	p := ListRunFinishesParams{Since: feedNow.Add(-time.Hour), Limit: 2}
	for range len(runs) + 1 {
		page, err := ListRunFinishes(ctx, db, p)
		require.NoError(t, err)
		require.NotEmpty(t, page.NextCursor, "a page always has a cursor to poll from")
		require.LessOrEqual(t, len(page.Finishes), 2)
		if len(page.Finishes) == 0 {
			break
		}
		got = append(got, feedRunIDs(page)...)
		p = ListRunFinishesParams{After: page.NextCursor, Limit: 2}
	}
	assert.Equal(t, []string{"run-0", "run-1", "run-2", "run-3", "run-4", "run-5", "run-6"}, got)
}

// TestFinishFeedScansAtMostOneThousandEntriesAPage proves a page scans at most
// 1,000 finish-log entries and its cursor passes every one it scanned: behind
// 1,500 finishes of another kind, a filtered read returns a short page with a
// cursor past the 1,000th entry, and the next one reaches the match. The match
// is a row an older binary wrote, so it also proves the filter reads the kind
// resolved through the job.
func TestFinishFeedScansAtMostOneThousandEntriesAPage(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	start := feedNow.Add(-2 * time.Hour)
	runs := make([]histRun, 0, 1501)
	for i := range 1500 {
		runs = append(runs, feedRun(fmt.Sprintf("beta-%04d", i), "beta", start.Add(time.Duration(i)*time.Millisecond)))
	}
	legacy := feedRun("alpha-old", "alpha", feedNow.Add(-time.Hour))
	legacy.Legacy = true
	runs = append(runs, legacy)
	seedHistory(t, db, runs)

	ctx := feedCtx(feedNow)
	first, err := ListRunFinishes(ctx, db, ListRunFinishesParams{Since: feedNow.Add(-3 * time.Hour), Kinds: []string{"alpha"}})
	require.NoError(t, err)
	assert.Empty(t, first.Finishes, "the match lies past the page's scan")
	pos, err := decodeFeedCursor(first.NextCursor)
	require.NoError(t, err)
	assert.Equal(t, "beta-0999", pos.RunID, "the cursor passes the 1,000th entry scanned")
	assert.True(t, pos.At.Equal(start.Add(999*time.Millisecond)), "at %v", pos.At)

	second, err := ListRunFinishes(ctx, db, ListRunFinishesParams{After: first.NextCursor, Kinds: []string{"alpha"}})
	require.NoError(t, err)
	require.Equal(t, []string{"alpha-old"}, feedRunIDs(second))
	assert.Equal(t, "alpha", second.Finishes[0].Kind, "the kind an older binary left empty is read through the job")
	assert.Equal(t, defaultQueue, second.Finishes[0].Queue)
}

// TestFinishFeedStartsAtTheHorizon proves a read with no cursor and no Since
// starts at the horizon: it returns nothing older, and a later poll from its
// cursor returns what has settled since.
func TestFinishFeedStartsAtTheHorizon(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	writeHistory(t, db, []histRun{
		feedRun("run-old", "alpha", feedNow.Add(-time.Hour)),
		feedRun("run-recent", "alpha", feedNow.Add(-10*time.Second)),
		feedRun("run-new", "alpha", feedNow.Add(-4*time.Second)),
	})

	first, err := ListRunFinishes(feedCtx(feedNow), db, ListRunFinishesParams{})
	require.NoError(t, err)
	assert.Empty(t, first.Finishes, "nothing at or before the horizon is new, and run-new has not settled")
	assert.True(t, first.Horizon.Equal(feedNow.Add(-DefaultFeedSettle)))
	require.NotEmpty(t, first.NextCursor)

	next, err := ListRunFinishes(feedCtx(feedNow.Add(10*time.Second)), db, ListRunFinishesParams{After: first.NextCursor})
	require.NoError(t, err)
	assert.Equal(t, []string{"run-new"}, feedRunIDs(next))

	settle, err := ListRunFinishes(feedCtx(feedNow), db, ListRunFinishesParams{Settle: 30 * time.Second})
	require.NoError(t, err)
	assert.True(t, settle.Horizon.Equal(feedNow.Add(-30*time.Second)), "a Settle of its own moves the horizon")
}

// TestFinishFeedPassesEntriesWhoseRunIsGone proves an entry whose run retention
// has deleted is scanned and skipped, and the cursor still moves past it.
func TestFinishFeedPassesEntriesWhoseRunIsGone(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	writeHistory(t, db, []histRun{
		feedRun("run-gone-1", "alpha", feedNow.Add(-30*time.Second)),
		feedRun("run-kept", "alpha", feedNow.Add(-25*time.Second)),
		feedRun("run-gone-2", "alpha", feedNow.Add(-20*time.Second)),
	})
	require.NoError(t, db.Exec(`DELETE FROM job_runs WHERE id IN ?`, []string{"run-gone-1", "run-gone-2"}).Error)

	ctx := feedCtx(feedNow)
	first, err := ListRunFinishes(ctx, db, ListRunFinishesParams{Since: feedNow.Add(-time.Hour)})
	require.NoError(t, err)
	assert.Equal(t, []string{"run-kept"}, feedRunIDs(first))
	pos, err := decodeFeedCursor(first.NextCursor)
	require.NoError(t, err)
	assert.Equal(t, "run-gone-2", pos.RunID, "the cursor passes the last entry scanned, its run gone or not")

	writeHistory(t, db, []histRun{feedRun("run-next", "alpha", feedNow.Add(-10*time.Second))})
	next, err := ListRunFinishes(ctx, db, ListRunFinishesParams{After: first.NextCursor})
	require.NoError(t, err)
	assert.Equal(t, []string{"run-next"}, feedRunIDs(next))
}

// TestFinishFeedRefusesABadRequest proves a malformed cursor and a negative
// Settle are refused before any statement runs, each naming its field, and that
// Limit is clamped rather than refused.
func TestFinishFeedRefusesABadRequest(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	ctx := feedCtx(feedNow)
	encoded := func(body string) string { return "rf1." + base64.RawURLEncoding.EncodeToString([]byte(body)) }

	refused := []struct {
		name  string
		p     ListRunFinishesParams
		field string
	}{
		{"not_a_cursor", ListRunFinishesParams{After: "nope"}, "after"},
		{"another_version", ListRunFinishesParams{After: "rf2." + base64.RawURLEncoding.EncodeToString([]byte("1:run"))}, "after"},
		{"bad_base64", ListRunFinishesParams{After: "rf1.%%%"}, "after"},
		{"no_separator", ListRunFinishesParams{After: encoded("1700000000")}, "after"},
		{"not_a_time", ListRunFinishesParams{After: encoded("soon:run-1")}, "after"},
		{"negative_settle", ListRunFinishesParams{Settle: -time.Second}, "settle"},
	}
	for _, c := range refused {
		t.Run(c.name, func(t *testing.T) {
			session, rec := recordStatements(db)
			_, err := ListRunFinishes(ctx, session, c.p)
			requireFieldRefusal(t, err, c.field)
			assert.Empty(t, rec.stmt, "a refusal runs no statement")
		})
	}

	t.Run("a_limit_is_clamped", func(t *testing.T) {
		runs := make([]histRun, 250)
		for i := range runs {
			runs[i] = feedRun(fmt.Sprintf("run-%03d", i), "alpha", feedNow.Add(-time.Hour).Add(time.Duration(i)*time.Second))
		}
		writeHistory(t, db, runs)
		for _, c := range []struct{ limit, want int }{{0, 100}, {-3, 100}, {7, 7}, {500, MaxReadLimit}} {
			page, err := ListRunFinishes(ctx, db, ListRunFinishesParams{Since: feedNow.Add(-2 * time.Hour), Limit: c.limit})
			require.NoError(t, err)
			assert.Len(t, page.Finishes, c.want, "Limit %d", c.limit)
		}
	})
}
