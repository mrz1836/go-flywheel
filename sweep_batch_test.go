package flywheel

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// seedExpiredLeases writes n running jobs whose leases expired an hour before
// now, each with the started run stub a live attempt would have committed. It is
// the shape an executor pool leaves when it dies holding its whole lease set.
func seedExpiredLeases(t testing.TB, db *gorm.DB, n int, now time.Time) {
	t.Helper()
	expired := now.Add(-time.Hour)
	for i := range n {
		id := fmt.Sprintf("expired-%04d", i)
		token := "token-" + id
		require.NoError(t, db.Create(&jobRow{
			ID: id, CreatedAt: expired, UpdatedAt: expired,
			Kind: "sweep.batch", Queue: "default", Args: []byte(`{}`),
			Priority: 100, State: string(StateRunning), Attempt: 1, MaxAttempts: 25,
			ScheduledAt: expired, LeasedUntil: &expired, LeaseToken: &token,
			Tags: []byte(`[]`), Metadata: []byte(`{}`),
		}).Error)
		require.NoError(t, db.Create(&jobRunRow{
			ID: "run-" + id, JobID: id, Attempt: 1, ExecutorClass: "local",
			ExecutorID: "exec-1", StartedAt: expired,
			Outcome: string(OutcomeStarted), CreatedAt: expired,
		}).Error)
	}
}

// countJobsInState returns how many jobs currently hold the given state.
func countJobsInState(t testing.TB, db *gorm.DB, state JobState) int64 {
	t.Helper()
	var n int64
	require.NoError(t, db.Model(&jobRow{}).Where("state = ?", string(state)).Count(&n).Error)
	return n
}

// countRunsWithOutcome returns how many job_runs rows hold the given outcome.
func countRunsWithOutcome(t testing.TB, db *gorm.DB, outcome RunOutcome) int64 {
	t.Helper()
	var n int64
	require.NoError(t, db.Model(&jobRunRow{}).Where("outcome = ?", string(outcome)).Count(&n).Error)
	return n
}

// TestSweepReclaimsEveryExpiredLeaseAcrossBatches proves the loop runs to
// exhaustion rather than stopping at one batch. A backlog several times the
// batch size is the case the bound exists for, and a sweep that reclaimed only
// its first batch would leave the rest stalled until the next tick.
func TestSweepReclaimsEveryExpiredLeaseAcrossBatches(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	now := time.Now().UTC().Truncate(time.Second)
	seedExpiredLeases(t, db, 250, now)

	d := NewSQLiteDriverWithOptions(db, DriverOpts{SweepBatchSize: 40})
	reclaimed, err := d.Sweep(context.Background(), now)
	require.NoError(t, err)

	assert.Equal(t, 250, reclaimed, "every expired lease is reclaimed across batches")
	assert.EqualValues(t, 250, countJobsInState(t, db, StateAvailable))
	assert.EqualValues(t, 0, countJobsInState(t, db, StateRunning))
	assert.EqualValues(t, 250, countRunsWithOutcome(t, db, OutcomeCrashed),
		"every stale stub is crashed, not just the first batch's")
}

// TestSweepReclaimsABacklogThatIsAnExactMultipleOfTheBatch covers the boundary
// the `n < batchSize` termination condition turns on. At an exact multiple the
// final full batch is followed by an empty one, and a loop that stopped on
// `n == 0` alone or terminated on the full batch would be off by one batch in
// opposite directions.
func TestSweepReclaimsABacklogThatIsAnExactMultipleOfTheBatch(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	now := time.Now().UTC().Truncate(time.Second)
	seedExpiredLeases(t, db, 100, now)

	d := NewSQLiteDriverWithOptions(db, DriverOpts{SweepBatchSize: 25})
	reclaimed, err := d.Sweep(context.Background(), now)
	require.NoError(t, err)

	assert.Equal(t, 100, reclaimed)
	assert.EqualValues(t, 0, countJobsInState(t, db, StateRunning))
}

// TestSweepCommitsEachBatchIndependently proves the per-batch transaction is
// real: work completed before a failure survives it. Without independent
// commits a backlog whose tail cannot be swept would roll back its head too, so
// a permanently-failing sweep would make no progress at all rather than
// draining what it can.
func TestSweepCommitsEachBatchIndependently(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	now := time.Now().UTC().Truncate(time.Second)
	seedExpiredLeases(t, db, 30, now)

	// A reclaim that succeeds twice and then fails leaves two batches committed.
	calls := 0
	failing := func(ctx context.Context, tx *gorm.DB, at time.Time, limit int) ([]string, error) {
		calls++
		if calls > 2 {
			return nil, errors.New("reclaim exploded")
		}
		lite := &sqliteDriver{baseDriver{db: db}}
		return lite.reclaimExpired(ctx, tx, at, limit)
	}

	d := &sqliteDriver{baseDriver{db: db, opts: DriverOpts{SweepBatchSize: 10}}}
	reclaimed, err := d.sweep(context.Background(), now, failing)

	require.Error(t, err)
	assert.Equal(t, 20, reclaimed, "the count reports the batches that committed before the failure")
	assert.EqualValues(t, 20, countJobsInState(t, db, StateAvailable),
		"committed batches are not rolled back by a later batch's failure")
	assert.EqualValues(t, 10, countJobsInState(t, db, StateRunning),
		"the failed batch left its rows untouched")
}

// TestSweepDrainsTheBacklogOnSQLiteWithoutSkipLocked is the FR-mandated
// documented fallback: SQLite has no SKIP LOCKED and needs none, and the
// batched path must still drain a backlog completely and clear every fence
// token.
func TestSweepDrainsTheBacklogOnSQLiteWithoutSkipLocked(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	now := time.Now().UTC().Truncate(time.Second)
	seedExpiredLeases(t, db, 75, now)

	d := NewSQLiteDriverWithOptions(db, DriverOpts{SweepBatchSize: 10})
	reclaimed, err := d.Sweep(context.Background(), now)
	require.NoError(t, err)
	assert.Equal(t, 75, reclaimed)

	var stillTokened int64
	require.NoError(t, db.Model(&jobRow{}).
		Where("lease_token IS NOT NULL").Count(&stillTokened).Error)
	assert.EqualValues(t, 0, stillTokened,
		"a reclaimed job holds no token, or the expired attempt can finalize over the next claim")
}

// TestSweepLeavesSoftDeletedJobsAlone pins the soft-delete scope. The SQLite
// reclaim inherits it from Model(&jobRow{}); the PostgreSQL one spells it out
// by hand. Both must agree, and this is the assertion that says so on the
// dialect where it is implicit.
func TestSweepLeavesSoftDeletedJobsAlone(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	now := time.Now().UTC().Truncate(time.Second)
	seedExpiredLeases(t, db, 3, now)
	require.NoError(t, db.Delete(&jobRow{}, "id = ?", "expired-0000").Error)

	d := NewSQLiteDriver(db)
	reclaimed, err := d.Sweep(context.Background(), now)
	require.NoError(t, err)

	assert.Equal(t, 2, reclaimed, "a soft-deleted running job is not reclaimed")
}

// TestSweepBatchSizeIsNeverUnbounded is the FR-03-04 guarantee stated directly
// against the resolver, so no value a caller can supply — zero, negative, or
// absurd — selects an unbounded transaction.
func TestSweepBatchSizeIsNeverUnbounded(t *testing.T) {
	t.Parallel()

	assert.Equal(t, defaultSweepBatchSize, DriverOpts{}.sweepBatchSize(),
		"a zero value selects the documented default")
	assert.Equal(t, defaultSweepBatchSize, DriverOpts{SweepBatchSize: -1}.sweepBatchSize(),
		"a negative value selects the default rather than disabling the bound")
	assert.Equal(t, 7, DriverOpts{SweepBatchSize: 7}.sweepBatchSize(),
		"a positive value is honored verbatim")
	assert.Positive(t, defaultSweepBatchSize, "the default is itself a bound")
}

// TestSweepCancelledReportsPartialProgress covers FR-03-06. A cancelled sweep
// must report what it committed rather than discarding it, and must wrap
// context.Canceled so a caller can tell a shutdown from a database failure.
//
// The cancellation lands during the second batch, so this also pins the other
// half of the contract: the batch in flight when the cancel arrives is rolled
// back, not half-applied. Its rows stay running and the next sweep collects
// them, which is why losing an in-flight batch is work redone rather than work
// lost — and why the sweep, unlike Finalize, does not detach its transaction
// from cancellation.
func TestSweepCancelledReportsPartialProgress(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	now := time.Now().UTC().Truncate(time.Second)
	seedExpiredLeases(t, db, 50, now)

	ctx, cancel := context.WithCancel(context.Background())
	batches := 0
	counting := func(rctx context.Context, tx *gorm.DB, at time.Time, limit int) ([]string, error) {
		batches++
		if batches == 2 {
			cancel()
		}
		lite := &sqliteDriver{baseDriver{db: db}}
		return lite.reclaimExpired(rctx, tx, at, limit)
	}

	d := &sqliteDriver{baseDriver{db: db, opts: DriverOpts{SweepBatchSize: 10}}}
	reclaimed, err := d.sweep(ctx, now, counting)

	require.ErrorIs(t, err, context.Canceled, "a cancelled sweep wraps the context error")
	assert.Equal(t, 10, reclaimed, "the committed batch is reported, not discarded")
	assert.EqualValues(t, 10, countJobsInState(t, db, StateAvailable),
		"the committed batch is not rolled back by the cancellation")
	assert.EqualValues(t, 40, countJobsInState(t, db, StateRunning),
		"the in-flight batch rolls back whole; the next sweep collects it")
}

// TestSweepWithACancelledContextDoesNoWork covers the loop's own cancellation
// check, which the test above cannot reach: there, the cancel is caught by the
// in-flight transaction first, and the window between a commit and the next
// loop iteration is not addressable from a test without a hook that exists only
// to be hooked.
//
// A context already cancelled on entry reaches it deterministically, and pins
// what matters about that branch: a sweep under a dead context opens no
// transaction at all, and its error carries the progress count an operator
// reading a shutdown log needs.
func TestSweepWithACancelledContextDoesNoWork(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	now := time.Now().UTC().Truncate(time.Second)
	seedExpiredLeases(t, db, 20, now)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	batches := 0
	counting := func(rctx context.Context, tx *gorm.DB, at time.Time, limit int) ([]string, error) {
		batches++
		lite := &sqliteDriver{baseDriver{db: db}}
		return lite.reclaimExpired(rctx, tx, at, limit)
	}

	d := &sqliteDriver{baseDriver{db: db, opts: DriverOpts{SweepBatchSize: 10}}}
	reclaimed, err := d.sweep(ctx, now, counting)

	require.ErrorIs(t, err, context.Canceled)
	assert.Equal(t, 0, batches, "no transaction is opened under a dead context")
	assert.Equal(t, 0, reclaimed)
	assert.Contains(t, err.Error(), "cancelled after 0 reclaimed",
		"the error names the progress made, in the loop's own vocabulary")
	assert.EqualValues(t, 20, countJobsInState(t, db, StateRunning), "nothing was touched")
}

// TestSweepOnAnEmptyQueueRunsExactlyOneBatch guards the steady-state cost. A
// scheduler sweeps on a fixed cadence whether or not anything expired, so the
// no-op sweep is the one a deployment pays for continuously, and it must cost
// one round trip rather than one per batch-size.
func TestSweepOnAnEmptyQueueRunsExactlyOneBatch(t *testing.T) {
	t.Parallel()
	db := newDB(t)

	batches := 0
	counting := func(ctx context.Context, tx *gorm.DB, at time.Time, limit int) ([]string, error) {
		batches++
		lite := &sqliteDriver{baseDriver{db: db}}
		return lite.reclaimExpired(ctx, tx, at, limit)
	}

	d := &sqliteDriver{baseDriver{db: db, opts: DriverOpts{SweepBatchSize: 10}}}
	reclaimed, err := d.sweep(context.Background(), time.Now(), counting)
	require.NoError(t, err)

	assert.Equal(t, 0, reclaimed)
	assert.Equal(t, 1, batches, "an empty backlog costs exactly one round trip")
}
