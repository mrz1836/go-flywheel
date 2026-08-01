package core

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/mrz1836/go-foundation/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// seedChild writes one child of parent in the given state, scheduled at sched.
func seedChild(t testing.TB, db *gorm.DB, id, parent, kind string, state JobState, sched time.Time) {
	t.Helper()
	p := parent
	seedJob(t, db, jobRow{
		ID: id, Kind: kind, State: string(state), ParentJobID: &p, ScheduledAt: sched,
	})
}

// all returns a copy of every statement the recorder captured. It counts the
// reads a rollup issues, which is what proves ProgressMany does not scale its
// query count with the number of parents.
func (r *stmtRecorder) all() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.sqls...)
}

// countSelects returns how many of the recorded statements are SELECTs.
func (r *stmtRecorder) countSelects() int {
	n := 0
	for _, sql := range r.all() {
		if strings.HasPrefix(strings.ToUpper(strings.TrimSpace(sql)), "SELECT") {
			n++
		}
	}
	return n
}

func TestProgressRollsUpChildrenByState(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	base := time.Now().UTC().Truncate(time.Second)
	ctx := models.WithClock(context.Background(), models.NewFixedClock(base))

	seedJob(t, db, jobRow{ID: "P", Kind: "coordinator", State: string(StateRunning)})
	// Four succeeded, one running, two available, one discarded child.
	for i, st := range []JobState{
		StateSucceeded, StateSucceeded, StateSucceeded, StateSucceeded,
		StateRunning, StateAvailable, StateAvailable, StateDiscarded,
	} {
		seedChild(t, db, "c"+string(rune('a'+i)), "P", "leaf", st, base)
	}

	bp, err := Progress(ctx, db, "P")
	require.NoError(t, err)

	assert.Equal(t, "P", bp.ParentJobID)
	assert.Equal(t, StateRunning, bp.ParentState, "the parent's own state is reported")
	assert.Equal(t, 8, bp.Total)
	assert.Equal(t, 5, bp.Terminal, "four succeeded plus one discarded are terminal")
	assert.Equal(t, 3, bp.Pending, "one running plus two available are pending")
	assert.Equal(t, 4, bp.CountsByState["succeeded"])
	assert.Equal(t, 2, bp.CountsByState["available"])
	assert.Equal(t, 1, bp.CountsByState["running"])
	assert.Equal(t, 1, bp.CountsByState["discarded"])
}

func TestProgressOldestPendingAge(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	base := time.Now().UTC().Truncate(time.Second)
	ctx := models.WithClock(context.Background(), models.NewFixedClock(base))

	seedJob(t, db, jobRow{ID: "P", Kind: "coordinator", State: string(StateRunning)})
	// The oldest pending child was scheduled 30m ago; a terminal child scheduled
	// even earlier must not count, and a newer pending child must not win.
	seedChild(t, db, "old", "P", "leaf", StateAvailable, base.Add(-30*time.Minute))
	seedChild(t, db, "new", "P", "leaf", StateRetryable, base.Add(-5*time.Minute))
	seedChild(t, db, "done", "P", "leaf", StateSucceeded, base.Add(-2*time.Hour))

	bp, err := Progress(ctx, db, "P")
	require.NoError(t, err)
	assert.Equal(t, 30*time.Minute, bp.OldestPendingAge,
		"the age is measured from the oldest pending child, ignoring terminal ones")

	// With no pending work the age is zero.
	require.NoError(t, db.Model(&jobRow{}).Where("id IN ?", []string{"old", "new"}).
		Update("state", string(StateSucceeded)).Error)
	bp, err = Progress(ctx, db, "P")
	require.NoError(t, err)
	assert.Zero(t, bp.Pending)
	assert.Zero(t, bp.OldestPendingAge, "a complete batch has no pending age")
}

func TestProgressUnknownParentIsNotAnError(t *testing.T) {
	t.Parallel()
	db := newDB(t)

	bp, err := Progress(context.Background(), db, "no-such-parent")
	require.NoError(t, err, "an unknown parent is a zero rollup, not an error")
	assert.Empty(t, bp.ParentState, "an unknown parent has no state")
	assert.Zero(t, bp.Total)
}

func TestProgressReportsChildrenWhenTheParentIsGone(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	ctx := context.Background()

	// Children whose parent row does not exist — the shape retention leaves when it
	// prunes a terminal parent while its children remain.
	seedChild(t, db, "c1", "pruned-parent", "leaf", StateSucceeded, time.Now())
	seedChild(t, db, "c2", "pruned-parent", "leaf", StateAvailable, time.Now())

	bp, err := Progress(ctx, db, "pruned-parent")
	require.NoError(t, err)
	assert.Empty(t, bp.ParentState, "the parent is gone, so its state is empty")
	assert.Equal(t, 2, bp.Total, "the children are still rolled up")
	assert.Equal(t, 1, bp.Pending)
}

func TestProgressExcludesSoftDeletedChildren(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	ctx := context.Background()

	seedJob(t, db, jobRow{ID: "P", Kind: "coordinator", State: string(StateRunning)})
	seedChild(t, db, "live", "P", "leaf", StateAvailable, time.Now())
	seedChild(t, db, "gone", "P", "leaf", StateAvailable, time.Now())
	require.NoError(t, db.Delete(&jobRow{}, "id = ?", "gone").Error)

	bp, err := Progress(ctx, db, "P")
	require.NoError(t, err)
	assert.Equal(t, 1, bp.Total, "a soft-deleted child is not part of the batch")
}

func TestProgressManyReturnsEntryPerParent(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	ctx := context.Background()

	seedJob(t, db, jobRow{ID: "P1", Kind: "coordinator", State: string(StateRunning)})
	seedJob(t, db, jobRow{ID: "P2", Kind: "coordinator", State: string(StateSucceeded)})
	seedChild(t, db, "a", "P1", "leaf", StateSucceeded, time.Now())
	seedChild(t, db, "b", "P1", "leaf", StateAvailable, time.Now())
	// P2 exists but has no children; P3 is an unknown id.

	out, err := ProgressMany(ctx, db, []string{"P1", "P2", "P3"})
	require.NoError(t, err)
	require.Len(t, out, 3, "every requested parent gets an entry")

	assert.Equal(t, 2, out["P1"].Total)
	assert.Equal(t, 1, out["P1"].Pending)
	assert.Equal(t, StateRunning, out["P1"].ParentState)

	assert.Zero(t, out["P2"].Total, "a childless parent yields a zero-Total entry")
	assert.Equal(t, StateSucceeded, out["P2"].ParentState, "but its own state is still reported")

	assert.Zero(t, out["P3"].Total)
	assert.Empty(t, out["P3"].ParentState, "an unknown parent is distinguished by an empty state")
}

// TestProgressManyIssuesConstantQueryCount is A2: the multi-parent form must not
// scale its query count with the number of parents. It issues the same two reads
// for fifty parents as for two.
func TestProgressManyIssuesConstantQueryCount(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	ctx := context.Background()

	many := make([]string, 50)
	for i := range many {
		id := "P" + string(rune('A'+i%26)) + string(rune('a'+i/26))
		many[i] = id
		seedJob(t, db, jobRow{ID: id, Kind: "coordinator", State: string(StateRunning)})
		seedChild(t, db, id+"-c", id, "leaf", StateAvailable, time.Now())
	}

	countFor := func(ids []string) int {
		rec := &stmtRecorder{}
		prev := db.Logger
		db.Logger = rec
		defer func() { db.Logger = prev }()
		_, err := ProgressMany(ctx, db, ids)
		require.NoError(t, err)
		return rec.countSelects()
	}

	two := countFor(many[:2])
	fifty := countFor(many)
	assert.Equal(t, 2, two, "ProgressMany reads parent states and child counts — two SELECTs")
	assert.Equal(t, two, fifty, "the query count does not grow with the number of parents")
}

func TestProgressByKindGroupsByKind(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	ctx := context.Background()

	seedJob(t, db, jobRow{ID: "s1", Kind: "scrape", State: string(StateSucceeded)})
	seedJob(t, db, jobRow{ID: "s2", Kind: "scrape", State: string(StateAvailable)})
	seedJob(t, db, jobRow{ID: "s3", Kind: "scrape", State: string(StateRunning)})
	seedJob(t, db, jobRow{ID: "w1", Kind: "weather", State: string(StateDiscarded)})

	out, err := ProgressByKind(ctx, db, []string{"scrape", "weather", "absent"})
	require.NoError(t, err)
	require.Len(t, out, 3, "every requested kind gets an entry, including one with no jobs")

	assert.Equal(t, 3, out["scrape"].Total)
	assert.Equal(t, 1, out["scrape"].Terminal)
	assert.Equal(t, 2, out["scrape"].Pending)
	assert.Empty(t, out["scrape"].ParentJobID, "a kind is not a parent job")
	assert.Empty(t, out["scrape"].ParentState)

	assert.Equal(t, 1, out["weather"].Total)
	assert.Equal(t, 1, out["weather"].Terminal)

	assert.Zero(t, out["absent"].Total, "a kind with no jobs yields a zero-Total entry")
}

func TestProgressEmptyInputs(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	ctx := context.Background()

	many, err := ProgressMany(ctx, db, nil)
	require.NoError(t, err)
	assert.Empty(t, many)

	byKind, err := ProgressByKind(ctx, db, nil)
	require.NoError(t, err)
	assert.Empty(t, byKind)
}

// --- progress read error branches -------------------------------------------

// TestProgressReadsSurfaceDBErrors covers the DB-error exit of each progress
// entry point: a closed database makes every read return its wrapped error rather
// than a partial (and misleading) rollup.
func TestProgressReadsSurfaceDBErrors(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	closeDB(t, db)
	ctx := context.Background()

	_, err := Progress(ctx, db, "p")
	require.Error(t, err, "Progress surfaces a read failure")
	_, err = ProgressMany(ctx, db, []string{"p"})
	require.Error(t, err, "ProgressMany surfaces a read failure")
	_, err = ProgressByKind(ctx, db, []string{"k"})
	require.Error(t, err, "ProgressByKind surfaces a read failure")
	_, err = ChildOutputs(ctx, db, "p", ListRunsParams{})
	require.Error(t, err, "ChildOutputs surfaces a read failure")
}
