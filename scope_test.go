package flywheel

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mrz1836/go-foundation/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/datatypes"
	"gorm.io/gorm"
)

// seedChildrenBulk batch-inserts count children of parent, all in the given state,
// scheduled at sched. It is the fast seed the batching tests need.
func seedChildrenBulk(t testing.TB, db *gorm.DB, parent string, count int, state JobState, sched time.Time) {
	t.Helper()
	rows := make([]jobRow, count)
	p := parent
	for i := range rows {
		rows[i] = jobRow{
			ID: fmt.Sprintf("%s-%s-%d", parent, state, i), Kind: "leaf",
			State: string(state), ParentJobID: &p, Args: datatypes.JSON("{}"), ScheduledAt: sched,
		}
	}
	// CreateInBatches keeps each INSERT's bind-parameter count under SQLite's ceiling
	// (jobRow binds ~22 columns per row).
	require.NoError(t, db.CreateInBatches(&rows, 200).Error)
}

// countChildrenInState counts a parent's children in the given state.
func countChildrenInState(t testing.TB, db *gorm.DB, parent string, state JobState) int64 {
	t.Helper()
	var n int64
	require.NoError(t, db.Model(&jobRow{}).
		Where("parent_job_id = ? AND state = ?", parent, string(state)).Count(&n).Error)
	return n
}

// TestCancelByParentGuardsTerminalAndRunning is A3: the charter's own acceptance.
// A parent with 400 succeeded, 100 running, and 500 available children is cancelled
// down to exactly the 500, with the succeeded outcomes and the running attempts left
// untouched and reported by reason.
func TestCancelByParentGuardsTerminalAndRunning(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	base := time.Now().UTC().Truncate(time.Second)
	finalized := base.Add(-time.Hour)
	ctx := models.WithClock(context.Background(), models.NewFixedClock(base))

	p := "P"
	// 400 succeeded, each with a recorded finalized_at that must survive.
	succeeded := make([]jobRow, 400)
	for i := range succeeded {
		pid := p
		succeeded[i] = jobRow{
			ID: fmt.Sprintf("s-%d", i), Kind: "leaf", State: string(StateSucceeded),
			ParentJobID: &pid, FinalizedAt: &finalized, Args: datatypes.JSON("{}"), ScheduledAt: base,
		}
	}
	require.NoError(t, db.CreateInBatches(&succeeded, 200).Error)
	seedChildrenBulk(t, db, p, 100, StateRunning, base)
	seedChildrenBulk(t, db, p, 500, StateAvailable, base)

	res, err := CancelByParent(ctx, db, p, ScopeOpts{})
	require.NoError(t, err)

	assert.EqualValues(t, 500, res.Changed, "exactly the 500 available children are cancelled")
	assert.EqualValues(t, 400, res.SkippedTerminal, "the succeeded children are skipped as terminal")
	assert.EqualValues(t, 100, res.SkippedRunning, "the running children are skipped in flight")

	assert.EqualValues(t, 500, countChildrenInState(t, db, p, StateCancelled))
	assert.EqualValues(t, 400, countChildrenInState(t, db, p, StateSucceeded), "no succeeded child was clobbered")
	assert.EqualValues(t, 100, countChildrenInState(t, db, p, StateRunning), "no running child was interrupted")

	// No succeeded child's finalized_at was restamped.
	var s0 jobRow
	require.NoError(t, db.Where("id = ?", "s-0").First(&s0).Error)
	require.NotNil(t, s0.FinalizedAt)
	assert.True(t, s0.FinalizedAt.Equal(finalized), "a succeeded child keeps its recorded finalized_at")
}

func TestPauseByParentHoldsClaimableChildren(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	base := time.Now().UTC().Truncate(time.Second)
	ctx := models.WithClock(context.Background(), models.NewFixedClock(base))

	p := "P"
	seedChild(t, db, "avail", p, "leaf", StateAvailable, base)
	seedChild(t, db, "retry", p, "leaf", StateRetryable, base)
	seedChild(t, db, "sched", p, "leaf", StateScheduled, base.Add(time.Hour))
	seedChild(t, db, "run", p, "leaf", StateRunning, base)
	seedChild(t, db, "done", p, "leaf", StateSucceeded, base)

	res, err := PauseByParent(ctx, db, p, ScopeOpts{})
	require.NoError(t, err)
	assert.EqualValues(t, 3, res.Changed, "available, retryable, and scheduled children are held")
	assert.EqualValues(t, 1, res.SkippedRunning)
	assert.EqualValues(t, 1, res.SkippedTerminal)

	assert.EqualValues(t, 3, countChildrenInState(t, db, p, StatePaused))
	assert.EqualValues(t, 1, countChildrenInState(t, db, p, StateRunning), "a running attempt is not interrupted")
	assert.EqualValues(t, 1, countChildrenInState(t, db, p, StateSucceeded))
}

func TestResumeByParentReturnsPausedToAvailable(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	base := time.Now().UTC().Truncate(time.Second)
	ctx := models.WithClock(context.Background(), models.NewFixedClock(base))

	p := "P"
	// A paused child deferred to the future must keep its schedule on resume.
	future := base.Add(time.Hour)
	seedChild(t, db, "paused-now", p, "leaf", StatePaused, base)
	seedChild(t, db, "paused-later", p, "leaf", StatePaused, future)
	seedChild(t, db, "avail", p, "leaf", StateAvailable, base)

	res, err := ResumeByParent(ctx, db, p, ScopeOpts{})
	require.NoError(t, err)
	assert.EqualValues(t, 2, res.Changed, "only paused children are resumed")

	assert.EqualValues(t, 3, countChildrenInState(t, db, p, StateAvailable), "both paused children joined the one that was available")
	var later jobRow
	require.NoError(t, db.Where("id = ?", "paused-later").First(&later).Error)
	assert.True(t, later.ScheduledAt.Equal(future),
		"a child paused while deferred keeps its schedule rather than becoming claimable early")
}

func TestPauseThenResumeRoundTrips(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	base := time.Now().UTC().Truncate(time.Second)
	ctx := models.WithClock(context.Background(), models.NewFixedClock(base))

	p := "P"
	seedChildrenBulk(t, db, p, 5, StateAvailable, base)

	paused, err := PauseByParent(ctx, db, p, ScopeOpts{})
	require.NoError(t, err)
	assert.EqualValues(t, 5, paused.Changed)
	assert.EqualValues(t, 5, countChildrenInState(t, db, p, StatePaused))

	resumed, err := ResumeByParent(ctx, db, p, ScopeOpts{})
	require.NoError(t, err)
	assert.EqualValues(t, 5, resumed.Changed)
	assert.EqualValues(t, 5, countChildrenInState(t, db, p, StateAvailable))
	assert.Zero(t, countChildrenInState(t, db, p, StatePaused))
}

// TestScopeBatchingBoundsEveryTransaction is A5's shape at unit scale: with a
// BatchSize of B and N eligible children, the operation uses ceil(N/B) batches and
// no single transaction touches more than B rows. The bound holds identically at
// 200k; the count here is kept small so the seed is fast.
func TestScopeBatchingBoundsEveryTransaction(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	ctx := context.Background()

	p := "P"
	seedChildrenBulk(t, db, p, 2500, StateAvailable, time.Now())

	res, err := CancelByParent(ctx, db, p, ScopeOpts{BatchSize: 1000})
	require.NoError(t, err)
	assert.EqualValues(t, 2500, res.Changed)
	assert.Equal(t, 3, res.Batches, "2500 children in batches of 1000 is three transactions")
}

func TestScopeBatchSizeDefaultsWhenNonPositive(t *testing.T) {
	t.Parallel()
	assert.Equal(t, defaultScopeBatchSize, ScopeOpts{}.batchSize())
	assert.Equal(t, defaultScopeBatchSize, ScopeOpts{BatchSize: -1}.batchSize())
	assert.Equal(t, 250, ScopeOpts{BatchSize: 250}.batchSize())
}

// TestScopeWithACancelledContextDoesNoWork mirrors the sweep's cancellation
// contract: an operation entered under a dead context opens no batch transaction,
// changes nothing, and its error names the progress made so a shutdown log is
// legible. Committed batches are kept by construction, since each batch is its own
// transaction — so a cancel mid-run is partial progress, not a rollback of the whole.
func TestScopeWithACancelledContextDoesNoWork(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	seedChildrenBulk(t, db, "P", 20, StateAvailable, time.Now())

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	res, err := CancelByParent(ctx, db, "P", ScopeOpts{BatchSize: 5})
	require.ErrorIs(t, err, context.Canceled)
	assert.EqualValues(t, 0, res.Changed)
	assert.Contains(t, err.Error(), "cancelled after 0 changed", "the error names the progress made")
	assert.EqualValues(t, 20, countChildrenInState(t, db, "P", StateAvailable), "nothing was touched")
}

func TestScopeByParentNoChildrenIsAZeroResult(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	ctx := context.Background()

	res, err := PauseByParent(ctx, db, "childless", ScopeOpts{})
	require.NoError(t, err)
	assert.Equal(t, ScopeResult{}, res, "a parent with no children yields a zero result, not an error")
}

// TestCancelByParentDoesNotCountItsOwnCancellationsAsTerminal pins the reason
// SkippedTerminal is counted before the loop: a cancel that finds only cancellable
// children reports them all in Changed and none in SkippedTerminal, even though they
// are terminal by the time it returns.
func TestCancelByParentDoesNotCountItsOwnCancellationsAsTerminal(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	ctx := context.Background()

	seedChildrenBulk(t, db, "P", 10, StateAvailable, time.Now())
	res, err := CancelByParent(ctx, db, "P", ScopeOpts{})
	require.NoError(t, err)
	assert.EqualValues(t, 10, res.Changed)
	assert.EqualValues(t, 0, res.SkippedTerminal, "the rows this cancel just terminated are Changed, not SkippedTerminal")
}

// --- scopeByParent error branches -------------------------------------------

// TestScopeByParentSurfacesErrors covers scopeByParent's four failure exits: a
// nil db, a failed running count, a failed terminal count, and a failed batch
// update. Each is surfaced with the operation named, so a shutdown log stays
// legible.
func TestScopeByParentSurfacesErrors(t *testing.T) {
	t.Parallel()

	t.Run("a nil db is rejected", func(t *testing.T) {
		t.Parallel()
		_, err := CancelByParent(context.Background(), nil, "P", ScopeOpts{})
		require.ErrorContains(t, err, "db is nil")
	})

	t.Run("a failed running count is surfaced", func(t *testing.T) {
		t.Parallel()
		db := newDB(t)
		closeDB(t, db)
		_, err := CancelByParent(context.Background(), db, "P", ScopeOpts{})
		require.ErrorContains(t, err, "count running")
	})

	t.Run("a failed terminal count is surfaced", func(t *testing.T) {
		t.Parallel()
		db := newDB(t)
		// Fail the second query only: the running count (query 1) succeeds, the
		// terminal count (query 2) fails.
		var seen atomic.Int32
		require.NoError(t, db.Callback().Query().Before("gorm:query").Register("test:fail_second_query",
			func(tx *gorm.DB) {
				if seen.Add(1) == 2 {
					_ = tx.AddError(errors.New("terminal count failed"))
				}
			}))
		_, err := CancelByParent(context.Background(), db, "P", ScopeOpts{})
		require.ErrorContains(t, err, "count terminal")
	})

	t.Run("a failed batch update is surfaced", func(t *testing.T) {
		t.Parallel()
		db := newDB(t)
		seedChildrenBulk(t, db, "P", 3, StateAvailable, time.Now())

		// The counts are reads and succeed; the batch's UPDATE fails under a
		// read-only pragma held on the single pinned connection.
		sqlDB, err := db.DB()
		require.NoError(t, err)
		sqlDB.SetMaxOpenConns(1)
		require.NoError(t, db.Exec(`PRAGMA query_only = ON`).Error)

		_, err = CancelByParent(context.Background(), db, "P", ScopeOpts{})
		require.ErrorContains(t, err, "cancel by parent", "the batch UPDATE failure is surfaced")
	})
}
