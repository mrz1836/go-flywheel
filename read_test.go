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
	seedRun(t, db, jobRunRow{ID: "r1", JobID: "j", Attempt: 1, ExecutorKind: string(ExecutorLocal), ExecutorID: "e", Outcome: string(OutcomeStarted), StartedAt: base, CreatedAt: base})
	seedRun(t, db, jobRunRow{ID: "r2", JobID: "j", Attempt: 2, ExecutorKind: string(ExecutorLocal), ExecutorID: "e", Outcome: string(OutcomeSuccess), StartedAt: base.Add(time.Minute), CreatedAt: base.Add(time.Minute)})
	seedRun(t, db, jobRunRow{ID: "rOther", JobID: "other", Attempt: 1, ExecutorKind: string(ExecutorLocal), ExecutorID: "e", Outcome: string(OutcomeStarted), StartedAt: base, CreatedAt: base})

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
			ExecutorKind: string(ExecutorLocal), ExecutorID: "e",
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
