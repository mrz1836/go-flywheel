package flywheel

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/mrz1836/go-foundation/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// barrierFollowUps builds n Parent child follow-ups of the given kind.
func barrierFollowUps(childKind string, n int) []FollowUp {
	fus := make([]FollowUp, n)
	for i := range fus {
		fus[i] = FollowUp{Kind: childKind, Parent: true, Args: map[string]int{"i": i}}
	}
	return fus
}

// claimOneFrom claims exactly one job from the named queue.
//
//nolint:revive // ctx-as-second-arg matches the testing.TB-first helper convention
func claimOneFrom(t testing.TB, d Driver, ctx context.Context, queue string) RawJob {
	t.Helper()
	batch, err := d.Dequeue(ctx, []string{queue}, AnyClass, true, 1, time.Minute)
	require.NoError(t, err)
	require.Len(t, batch, 1, "a job must be claimable from %q", queue)
	return batch[0]
}

// claimOne claims one job from the default queue.
//
//nolint:revive // ctx-as-second-arg matches the testing.TB-first helper convention
func claimOne(t testing.TB, d Driver, ctx context.Context) RawJob {
	return claimOneFrom(t, d, ctx, "default")
}

// finalizeJob runs the stub-and-finalize half of a claim for raw.
//
//nolint:revive // ctx-as-second-arg matches the testing.TB-first helper convention
func finalizeJob(t testing.TB, d Driver, ctx context.Context, raw RawJob, result Result, workErr error) FinalizeOutcome {
	t.Helper()
	runID := models.NewID()
	require.NoError(t, d.InsertRunStub(ctx, runID, raw, time.Now(), "local", "exec"))
	out, err := d.Finalize(ctx, raw, runID, result, workErr, time.Now())
	require.NoError(t, err)
	return out
}

// declareBarrierParent seeds a parent, claims it, and finalizes it with a barrier
// of contKind over childCount children of childKind. It returns the parent id;
// afterwards the children are available on the default queue.
//
//nolint:revive // ctx-as-second-arg matches the testing.TB-first helper convention
func declareBarrierParent(t testing.TB, d Driver, db *gorm.DB, ctx context.Context, childKind, contKind string, childCount int) string {
	t.Helper()
	ids := seedClaimable(t, db, 1)
	parent := claimOneFrom(t, d, ctx, "default")
	require.Equal(t, ids[0], parent.ID)
	finalizeJob(t, d, ctx, parent, Result{
		FollowUps: barrierFollowUps(childKind, childCount),
		Barrier:   &Barrier{Kind: contKind},
	}, nil)
	return parent.ID
}

// countJobsOfKind counts non-deleted jobs of a kind.
func countJobsOfKind(t testing.TB, db *gorm.DB, kind string) int64 {
	t.Helper()
	var n int64
	require.NoError(t, db.Model(&jobRow{}).Where("kind = ?", kind).Count(&n).Error)
	return n
}

// permanentErr is a permanent-classified error, so one finalize discards the job.
func permanentErr() error {
	return &classifiedError{cause: errors.New("permanent boom"), class: ErrorPermanent}
}

func TestBarrierFiresWhenLastChildCompletes(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	d := NewSQLiteDriver(db)
	ctx := context.Background()

	parent := declareBarrierParent(t, d, db, ctx, "child", "finalize", 3)

	bp, err := Progress(ctx, db, parent)
	require.NoError(t, err)
	assert.Equal(t, 3, bp.Total, "the parent has three children")
	assert.EqualValues(t, 0, countJobsOfKind(t, db, "finalize"), "no continuation before the children finish")

	// The first two children finalize — the barrier must not fire yet.
	for range 2 {
		finalizeJob(t, d, ctx, claimOne(t, d, ctx), Result{}, nil)
	}
	assert.EqualValues(t, 0, countJobsOfKind(t, db, "finalize"), "the barrier waits for the last child")

	// The last child completes the generation → the continuation is enqueued once.
	finalizeJob(t, d, ctx, claimOne(t, d, ctx), Result{}, nil)
	assert.EqualValues(t, 1, countJobsOfKind(t, db, "finalize"), "the last child fires the barrier exactly once")

	// The continuation is a child of the parent, keyed on the parent id.
	var cont jobRow
	require.NoError(t, db.Where("kind = ?", "finalize").First(&cont).Error)
	require.NotNil(t, cont.ParentJobID)
	assert.Equal(t, parent, *cont.ParentJobID, "the continuation continues the generation, not a sibling")
	require.NotNil(t, cont.UniqueKey)
	assert.Equal(t, "barrier:"+parent, *cont.UniqueKey)

	// The parent's barrier columns are cleared once fired.
	var p jobRow
	require.NoError(t, db.Where("id = ?", parent).First(&p).Error)
	assert.Nil(t, p.BarrierKind, "the barrier gate is cleared once it fires")
}

// TestBarrierFiresOnAnyTerminalState is FR-06-12: a half-failed generation still
// gets its finalizer. The three children end succeeded, discarded, and cancelled,
// and the barrier fires when the last of them reaches its terminal state.
func TestBarrierFiresOnAnyTerminalState(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	d := NewSQLiteDriver(db)
	ctx := context.Background()

	declareBarrierParent(t, d, db, ctx, "child", "finalize", 3)

	finalizeJob(t, d, ctx, claimOne(t, d, ctx), Result{}, nil)            // succeeded
	finalizeJob(t, d, ctx, claimOne(t, d, ctx), Result{}, permanentErr()) // discarded
	assert.EqualValues(t, 0, countJobsOfKind(t, db, "finalize"), "two of three terminal is not complete")
	finalizeJob(t, d, ctx, claimOne(t, d, ctx), Result{Cancel: true}, nil) // cancelled

	assert.EqualValues(t, 1, countJobsOfKind(t, db, "finalize"),
		"the barrier fires when the last child reaches any terminal state")
}

// TestBarrierWaitsForARetryingChild proves a non-terminal transition keeps the
// barrier open: a child that retries is still pending, so the barrier fires only
// once that child finally reaches a terminal state.
func TestBarrierWaitsForARetryingChild(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	d := NewSQLiteDriver(db)
	ctx := context.Background()

	declareBarrierParent(t, d, db, ctx, "child", "finalize", 2)

	// Child A retries with a negligible backoff so it is immediately re-claimable.
	a := claimOne(t, d, ctx)
	finalizeJob(t, d, ctx, a, Result{},
		&classifiedError{cause: errors.New("transient"), class: ErrorTransient, retryDelay: time.Nanosecond})
	// Child B succeeds. The barrier must not fire: A is retryable, not terminal.
	finalizeJob(t, d, ctx, claimOne(t, d, ctx), Result{}, nil)
	assert.EqualValues(t, 0, countJobsOfKind(t, db, "finalize"), "a retryable child keeps the barrier open")

	// A's retry succeeds → the generation is now complete → the barrier fires.
	require.Eventually(t, func() bool {
		batch, err := d.Dequeue(ctx, []string{"default"}, AnyClass, true, 1, time.Minute)
		require.NoError(t, err)
		if len(batch) == 0 {
			return false
		}
		finalizeJob(t, d, ctx, batch[0], Result{}, nil)
		return true
	}, 2*time.Second, 5*time.Millisecond, "the retrying child must become claimable")

	assert.EqualValues(t, 1, countJobsOfKind(t, db, "finalize"), "the barrier fires once the retry completes")
}

// TestBarrierEnqueuesAtMostOneContinuation proves the unique key holds: even if the
// completion check somehow ran twice, the barrier-keyed unique key means only one
// continuation ever exists. Here the fired continuation itself reaches terminal and
// its own finalize re-examines the (now cleared) barrier without producing a second.
func TestBarrierEnqueuesAtMostOneContinuation(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	d := NewSQLiteDriver(db)
	ctx := context.Background()

	declareBarrierParent(t, d, db, ctx, "child", "finalize", 2)
	finalizeJob(t, d, ctx, claimOne(t, d, ctx), Result{}, nil)
	finalizeJob(t, d, ctx, claimOne(t, d, ctx), Result{}, nil)
	require.EqualValues(t, 1, countJobsOfKind(t, db, "finalize"))

	// Finalize the continuation itself: its own barrier check must not spawn another.
	finalizeJob(t, d, ctx, claimOne(t, d, ctx), Result{}, nil)
	assert.EqualValues(t, 1, countJobsOfKind(t, db, "finalize"),
		"the continuation's own finalize does not re-fire the barrier")
}

func TestBarrierContinuationInheritsParentRouting(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	d := NewSQLiteDriver(db)
	ctx := context.Background()

	// A parent on a bespoke queue/class/priority, with a barrier that overrides none.
	now := time.Now().Add(-time.Minute)
	seedJob(t, db, jobRow{
		ID: "P", Kind: "coordinator", Queue: "special", ExecutorClass: "gpu", Priority: 5,
		State: string(StateAvailable), MaxAttempts: 5, ScheduledAt: now,
	})
	parent := claimOneFrom(t, d, ctx, "special")
	finalizeJob(t, d, ctx, parent, Result{
		FollowUps: barrierFollowUps("child", 1),
		Barrier:   &Barrier{Kind: "finalize"}, // no queue/class/priority overrides
	}, nil)

	// The one child (on the default queue) completes the generation.
	finalizeJob(t, d, ctx, claimOne(t, d, ctx), Result{}, nil)

	var cont jobRow
	require.NoError(t, db.Where("kind = ?", "finalize").First(&cont).Error)
	assert.Equal(t, "special", cont.Queue, "the continuation inherits the parent's queue")
	assert.Equal(t, "gpu", cont.ExecutorClass, "and its executor class")
	assert.Equal(t, 5, cont.Priority, "and its priority")
}

func TestBarrierRejectsTooWide(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	d := NewSQLiteDriverWithOptions(db, DriverOpts{BarrierMaxChildren: 2})
	ctx := context.Background()

	seedClaimable(t, db, 1)
	parent := claimOne(t, d, ctx)
	runID := models.NewID()
	require.NoError(t, d.InsertRunStub(ctx, runID, parent, time.Now(), "local", "exec"))
	_, err := d.Finalize(ctx, parent, runID, Result{
		FollowUps: barrierFollowUps("child", 3),
		Barrier:   &Barrier{Kind: "finalize"},
	}, nil, time.Now())

	require.ErrorIs(t, err, ErrBarrierTooWide)
	assert.EqualValues(t, 0, countJobsOfKind(t, db, "child"), "the whole finalize rolled back — no children enqueued")
	assert.Equal(t, string(StateRunning), jobState(t, db, parent.ID), "and the parent is not advanced")
}

func TestBarrierRejectsNoChildren(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	d := NewSQLiteDriver(db)
	ctx := context.Background()

	seedClaimable(t, db, 1)
	parent := claimOne(t, d, ctx)
	runID := models.NewID()
	require.NoError(t, d.InsertRunStub(ctx, runID, parent, time.Now(), "local", "exec"))
	// FollowUps present, but none is a Parent child — the barrier has nothing to gate.
	_, err := d.Finalize(ctx, parent, runID, Result{
		FollowUps: []FollowUp{{Kind: "sibling"}},
		Barrier:   &Barrier{Kind: "finalize"},
	}, nil, time.Now())
	require.ErrorIs(t, err, ErrBarrierNoChildren)
}

func TestBarrierRejectsNoKind(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	d := NewSQLiteDriver(db)
	ctx := context.Background()

	seedClaimable(t, db, 1)
	parent := claimOne(t, d, ctx)
	runID := models.NewID()
	require.NoError(t, d.InsertRunStub(ctx, runID, parent, time.Now(), "local", "exec"))
	_, err := d.Finalize(ctx, parent, runID, Result{
		FollowUps: barrierFollowUps("child", 1),
		Barrier:   &Barrier{Kind: ""},
	}, nil, time.Now())
	require.ErrorIs(t, err, ErrValidation)
}

// TestBarrierExactlyOnceDeterministic drives the full generation serially through
// a mix of terminal outcomes, fifty times, asserting the continuation lands exactly
// once each time. The concurrent form runs against Postgres — see the _pg_ twin —
// but the serial loop pins the state machine independent of a database.
func TestBarrierExactlyOnceDeterministic(t *testing.T) {
	t.Parallel()
	for rep := range 50 {
		db := newDB(t)
		d := NewSQLiteDriver(db)
		ctx := context.Background()

		declareBarrierParent(t, d, db, ctx, "child", "finalize", 6)
		// A mix: some succeed, one discards, one cancels; order varies by repetition.
		outcomes := []func(RawJob){
			func(r RawJob) { finalizeJob(t, d, ctx, r, Result{}, nil) },
			func(r RawJob) { finalizeJob(t, d, ctx, r, Result{}, nil) },
			func(r RawJob) { finalizeJob(t, d, ctx, r, Result{}, permanentErr()) },
			func(r RawJob) { finalizeJob(t, d, ctx, r, Result{}, nil) },
			func(r RawJob) { finalizeJob(t, d, ctx, r, Result{Cancel: true}, nil) },
			func(r RawJob) { finalizeJob(t, d, ctx, r, Result{}, nil) },
		}
		for i := range 6 {
			outcomes[(i+rep)%len(outcomes)](claimOne(t, d, ctx))
		}
		require.EqualValues(t, 1, countJobsOfKind(t, db, "finalize"),
			"rep %d: the barrier fires exactly once regardless of outcome order", rep)
	}
}
