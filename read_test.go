package flywheel

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/datatypes"
	"gorm.io/gorm"
)

// seedJob writes a jobs row directly through the unexported struct so read tests
// control id/kind/state/parent without going through the producer path.
func seedJob(t testing.TB, db *gorm.DB, row jobRow) {
	t.Helper()
	if len(row.Args) == 0 {
		row.Args = datatypes.JSON("{}")
	}
	require.NoError(t, db.Create(&row).Error)
}

// seedRun writes a job_runs row directly for ListRuns ordering/cursor tests.
func seedRun(t testing.TB, db *gorm.DB, row jobRunRow) {
	t.Helper()
	require.NoError(t, db.Create(&row).Error)
}

func TestFindJobHitReturnsView(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	parent := "parent-1"
	enq := time.Date(2026, 6, 19, 12, 0, 0, 0, time.UTC)
	seedJob(t, db, jobRow{
		ID:          "job-1",
		Kind:        "test.kind",
		State:       string(StateRunning),
		Attempt:     3,
		ParentJobID: &parent,
		CreatedAt:   enq,
	})

	view, err := FindJob(context.Background(), db, "job-1")
	require.NoError(t, err)
	assert.Equal(t, "job-1", view.ID)
	assert.Equal(t, "test.kind", view.Kind)
	assert.Equal(t, string(StateRunning), view.State)
	assert.Equal(t, "parent-1", view.ParentJobID)
	assert.Equal(t, 3, view.Attempt)
	assert.True(t, view.EnqueuedAt.Equal(enq))
}

func TestFindJobMissReturnsErrJobNotFound(t *testing.T) {
	t.Parallel()
	db := newDB(t)

	_, err := FindJob(context.Background(), db, "missing")
	require.ErrorIs(t, err, ErrJobNotFound)
}

func TestFindJobSoftDeletedIsExcluded(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	seedJob(t, db, jobRow{ID: "job-del", Kind: "k", State: string(StateAvailable)})
	require.NoError(t, db.Where("id = ?", "job-del").Delete(&jobRow{}).Error)

	_, err := FindJob(context.Background(), db, "job-del")
	require.ErrorIs(t, err, ErrJobNotFound)
}

func TestFindJobNilParentRendersEmptyString(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	seedJob(t, db, jobRow{ID: "job-np", Kind: "k", State: string(StateAvailable)})

	view, err := FindJob(context.Background(), db, "job-np")
	require.NoError(t, err)
	assert.Equal(t, "", view.ParentJobID)
}

func TestListRunsNewestFirstOrdering(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	base := time.Date(2026, 6, 19, 9, 0, 0, 0, time.UTC)
	seedRun(t, db, jobRunRow{ID: "r1", JobID: "j", Attempt: 1, ExecutorClass: "local", ExecutorID: "e", Outcome: string(OutcomeStarted), StartedAt: base, CreatedAt: base})
	seedRun(t, db, jobRunRow{ID: "r2", JobID: "j", Attempt: 2, ExecutorClass: "local", ExecutorID: "e", Outcome: string(OutcomeSuccess), StartedAt: base.Add(time.Minute), CreatedAt: base.Add(time.Minute)})
	seedRun(t, db, jobRunRow{ID: "rOther", JobID: "other", Attempt: 1, ExecutorClass: "local", ExecutorID: "e", Outcome: string(OutcomeStarted), StartedAt: base, CreatedAt: base})

	runs, err := ListRuns(context.Background(), db, "j", ListRunsParams{})
	require.NoError(t, err)
	require.Len(t, runs, 2)
	assert.Equal(t, "r2", runs[0].ID, "newest run first")
	assert.Equal(t, "r1", runs[1].ID)
	assert.Equal(t, string(OutcomeSuccess), runs[0].Outcome)
}

func TestListRunsBeforeCursorAndLimit(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	base := time.Date(2026, 6, 19, 9, 0, 0, 0, time.UTC)
	for i := 0; i < 3; i++ {
		ts := base.Add(time.Duration(i) * time.Minute)
		seedRun(t, db, jobRunRow{
			ID: string(rune('a' + i)), JobID: "j", Attempt: i + 1,
			ExecutorClass: "local", ExecutorID: "e",
			Outcome: string(OutcomeStarted), StartedAt: ts, CreatedAt: ts,
		})
	}

	// Cursor strictly excludes rows at/after the newest row's created_at.
	runs, err := ListRuns(context.Background(), db, "j", ListRunsParams{Before: base.Add(2 * time.Minute)})
	require.NoError(t, err)
	require.Len(t, runs, 2)
	assert.Equal(t, "b", runs[0].ID)
	assert.Equal(t, "a", runs[1].ID)

	// Limit caps the page (newest first).
	limited, err := ListRuns(context.Background(), db, "j", ListRunsParams{Limit: 1})
	require.NoError(t, err)
	require.Len(t, limited, 1)
	assert.Equal(t, "c", limited[0].ID)
}

func TestOverviewGroupsByStateWithTotal(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	seedJob(t, db, jobRow{ID: "o1", Kind: "alpha", State: string(StateAvailable)})
	seedJob(t, db, jobRow{ID: "o2", Kind: "alpha", State: string(StateAvailable)})
	seedJob(t, db, jobRow{ID: "o3", Kind: "alpha", State: string(StateSucceeded)})
	seedJob(t, db, jobRow{ID: "o4", Kind: "beta", State: string(StateRunning)})

	all, err := Overview(context.Background(), db, OverviewParams{})
	require.NoError(t, err)
	assert.Equal(t, 4, all.Total)
	assert.Equal(t, 2, all.CountsByState[string(StateAvailable)])
	assert.Equal(t, 1, all.CountsByState[string(StateSucceeded)])
	assert.Equal(t, 1, all.CountsByState[string(StateRunning)])
}

func TestOverviewKindFilter(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	seedJob(t, db, jobRow{ID: "k1", Kind: "alpha", State: string(StateAvailable)})
	seedJob(t, db, jobRow{ID: "k2", Kind: "alpha", State: string(StateSucceeded)})
	seedJob(t, db, jobRow{ID: "k3", Kind: "beta", State: string(StateAvailable)})

	view, err := Overview(context.Background(), db, OverviewParams{Kind: "alpha"})
	require.NoError(t, err)
	assert.Equal(t, 2, view.Total)
	assert.Equal(t, 1, view.CountsByState[string(StateAvailable)])
	assert.Equal(t, 1, view.CountsByState[string(StateSucceeded)])
	assert.NotContains(t, view.CountsByState, string(StateRunning))
}

func TestOverviewExcludesSoftDeleted(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	seedJob(t, db, jobRow{ID: "s1", Kind: "alpha", State: string(StateAvailable)})
	seedJob(t, db, jobRow{ID: "s2", Kind: "alpha", State: string(StateAvailable)})
	require.NoError(t, db.Where("id = ?", "s2").Delete(&jobRow{}).Error)

	view, err := Overview(context.Background(), db, OverviewParams{})
	require.NoError(t, err)
	assert.Equal(t, 1, view.Total)
}

func TestNonTerminalStatesExcludesTerminal(t *testing.T) {
	t.Parallel()
	states := NonTerminalStates()
	for _, s := range states {
		require.True(t, s.Valid())
	}
	set := map[JobState]bool{}
	for _, s := range states {
		set[s] = true
	}
	for _, terminal := range []JobState{StateSucceeded, StateCancelled, StateDiscarded} {
		assert.False(t, set[terminal], "terminal state %q must not be non-terminal", terminal)
	}
	for _, active := range []JobState{StateAvailable, StateRunning, StateRetryable, StateScheduled} {
		assert.True(t, set[active], "active state %q must be non-terminal", active)
	}
}

func TestListActiveByKindReturnsNonTerminalWithArgs(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	seedJob(t, db, jobRow{ID: "a1", Kind: "investigate", State: string(StateAvailable), Args: datatypes.JSON(`{"person_id":"p1"}`)})
	seedJob(t, db, jobRow{ID: "a2", Kind: "investigate", State: string(StateRunning), Args: datatypes.JSON(`{"person_id":"p2"}`)})
	// Terminal job of the same kind must be excluded.
	seedJob(t, db, jobRow{ID: "a3", Kind: "investigate", State: string(StateSucceeded), Args: datatypes.JSON(`{"person_id":"p3"}`)})
	// Different kind must be excluded.
	seedJob(t, db, jobRow{ID: "b1", Kind: "other", State: string(StateAvailable)})

	views, err := ListActiveByKind(context.Background(), db, "investigate")
	require.NoError(t, err)
	require.Len(t, views, 2)
	got := map[string]string{}
	for _, v := range views {
		assert.Equal(t, "investigate", v.Kind)
		got[v.ID] = string(v.Args)
	}
	assert.JSONEq(t, `{"person_id":"p1"}`, got["a1"])
	assert.JSONEq(t, `{"person_id":"p2"}`, got["a2"])
}

func TestListActiveByKindExcludesSoftDeleted(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	seedJob(t, db, jobRow{ID: "d1", Kind: "investigate", State: string(StateAvailable)})
	require.NoError(t, db.Where("id = ?", "d1").Delete(&jobRow{}).Error)

	views, err := ListActiveByKind(context.Background(), db, "investigate")
	require.NoError(t, err)
	assert.Empty(t, views)
}

func TestCountRunsCountsEveryAttempt(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	base := time.Date(2026, 6, 19, 9, 0, 0, 0, time.UTC)
	seedRun(t, db, jobRunRow{ID: "cr1", JobID: "j", Attempt: 1, ExecutorClass: "local", ExecutorID: "e", Outcome: string(OutcomeStarted), StartedAt: base, CreatedAt: base})
	seedRun(t, db, jobRunRow{ID: "cr2", JobID: "j", Attempt: 2, ExecutorClass: "local", ExecutorID: "e", Outcome: string(OutcomeSuccess), StartedAt: base, CreatedAt: base})

	n, err := CountRuns(context.Background(), db)
	require.NoError(t, err)
	assert.EqualValues(t, 2, n)
}

func TestCountActiveJobsCountsOnlyNonTerminal(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	seedJob(t, db, jobRow{ID: "ca1", Kind: "k", State: string(StateAvailable)})
	seedJob(t, db, jobRow{ID: "ca2", Kind: "k", State: string(StateRunning)})
	seedJob(t, db, jobRow{ID: "ca3", Kind: "k", State: string(StateSucceeded)})
	seedJob(t, db, jobRow{ID: "ca4", Kind: "k", State: string(StateDiscarded)})

	n, err := CountActiveJobs(context.Background(), db)
	require.NoError(t, err)
	assert.EqualValues(t, 2, n)
}

func TestListJobsFiltersOrdersAndLimits(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	base := time.Date(2026, 6, 19, 9, 0, 0, 0, time.UTC)
	seedJob(t, db, jobRow{ID: "lj1", Kind: "alpha", State: string(StateAvailable), CreatedAt: base})
	seedJob(t, db, jobRow{ID: "lj2", Kind: "alpha", State: string(StateSucceeded), CreatedAt: base.Add(time.Minute)})
	seedJob(t, db, jobRow{ID: "lj3", Kind: "beta", State: string(StateAvailable), CreatedAt: base.Add(2 * time.Minute)})

	all, err := ListJobs(context.Background(), db, ListJobsParams{})
	require.NoError(t, err)
	require.Len(t, all, 3)
	assert.Equal(t, "lj3", all[0].ID, "jobs are returned newest first")

	avail, err := ListJobs(context.Background(), db, ListJobsParams{State: string(StateAvailable)})
	require.NoError(t, err)
	assert.Len(t, avail, 2, "the state filter is applied")

	limited, err := ListJobs(context.Background(), db, ListJobsParams{Kind: "alpha", Limit: 1})
	require.NoError(t, err)
	require.Len(t, limited, 1, "the kind filter and limit are applied")
	assert.Equal(t, "lj2", limited[0].ID, "the newest alpha job is returned")
}

func TestListJobsExcludesSoftDeleted(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	seedJob(t, db, jobRow{ID: "ljd", Kind: "k", State: string(StateAvailable)})
	require.NoError(t, db.Where("id = ?", "ljd").Delete(&jobRow{}).Error)

	got, err := ListJobs(context.Background(), db, ListJobsParams{})
	require.NoError(t, err)
	assert.Empty(t, got)
}
