package flywheel

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// realworld_stress_test.go drives bounded throughput and chaos scenarios sized
// for a single-writer in-memory SQLite database: hundreds–low-thousands of jobs,
// short wall time, fully deterministic. The exactly-once guarantees are the same
// ones a data-intelligence platform leans on when it drains a backlog or recovers
// from a crashed executor.

// exactlyOnceTracker records, per job id, how many times the work ran, so a test
// can assert each job ran exactly once and flag any double-dispatch.
type exactlyOnceTracker struct {
	seen sync.Map // job id -> *atomic.Int32
	dups atomic.Int64
	ran  atomic.Int64
}

// mark records one run for id and returns false if id had already been seen.
func (t *exactlyOnceTracker) mark(id string) bool {
	t.ran.Add(1)
	counter, _ := t.seen.LoadOrStore(id, &atomic.Int32{})
	if counter.(*atomic.Int32).Add(1) > 1 {
		t.dups.Add(1)
		return false
	}
	return true
}

// distinct returns how many unique job ids ran.
func (t *exactlyOnceTracker) distinct() int {
	n := 0
	t.seen.Range(func(_, _ any) bool {
		n++
		return true
	})
	return n
}

type drainArgs struct {
	N int `json:"n"`
}

func (drainArgs) Kind() string { return "rw.stress.drain" }

type drainWorker struct{ tracker *exactlyOnceTracker }

func (*drainWorker) Kind() string { return "rw.stress.drain" }
func (w *drainWorker) Work(_ context.Context, job *Job[drainArgs]) (Result, error) {
	w.tracker.mark(job.ID)
	return Result{}, nil
}

// TestRealWorldStressBacklogDrainExactlyOnce enqueues a large backlog and drains
// it with one runner, asserting every job ran exactly once and reached succeeded.
func TestRealWorldStressBacklogDrainExactlyOnce(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	ctx := context.Background()

	const total = 800

	tracker := &exactlyOnceTracker{}
	reg := NewRegistry()
	Register(reg, &drainWorker{tracker: tracker})
	r := newRunner(t, db, reg)

	client := NewClient(db)
	for i := range total {
		_, err := Insert(ctx, client, drainArgs{N: i}, InsertOpts{})
		require.NoError(t, err)
	}

	runToIdle(t, ctx, r)

	assert.EqualValues(t, total, tracker.ran.Load(), "every job ran")
	assert.Equal(t, total, tracker.distinct(), "every job is distinct")
	assert.EqualValues(t, 0, tracker.dups.Load(), "no job ran twice")

	overview, err := Overview(ctx, db, OverviewParams{})
	require.NoError(t, err)
	assert.Equal(t, total, overview.CountsByState[string(StateSucceeded)], "the whole backlog succeeded")
	assert.Equal(t, total, overview.Total)

	runs, err := CountRuns(ctx, db)
	require.NoError(t, err)
	assert.EqualValues(t, total, runs, "one audit row per job — no duplicate attempts")
}

// --- mixed-outcome storm -----------------------------------------------------

type stormSuccessArgs struct{ I int }

func (stormSuccessArgs) Kind() string { return "rw.storm.success" }

type stormRetryArgs struct{ I int }

func (stormRetryArgs) Kind() string { return "rw.storm.retry" }

type stormPermanentArgs struct{ I int }

func (stormPermanentArgs) Kind() string { return "rw.storm.permanent" }

type stormSnoozeArgs struct{ I int }

func (stormSnoozeArgs) Kind() string { return "rw.storm.snooze" }

type stormSuccessWorker struct{ ran atomic.Int64 }

func (*stormSuccessWorker) Kind() string { return "rw.storm.success" }
func (w *stormSuccessWorker) Work(_ context.Context, _ *Job[stormSuccessArgs]) (Result, error) {
	w.ran.Add(1)
	return Result{}, nil
}

// stormRetryWorker fails once per job (keyed by id) then succeeds.
type stormRetryWorker struct{ attempts sync.Map }

func (*stormRetryWorker) Kind() string { return "rw.storm.retry" }
func (w *stormRetryWorker) Work(_ context.Context, job *Job[stormRetryArgs]) (Result, error) {
	counter, _ := w.attempts.LoadOrStore(job.ID, &atomic.Int32{})
	if counter.(*atomic.Int32).Add(1) == 1 {
		return Result{}, assert.AnError
	}
	return Result{}, nil
}

type stormPermanentWorker struct{}

func (*stormPermanentWorker) Kind() string              { return "rw.storm.permanent" }
func (*stormPermanentWorker) Classify(error) ErrorClass { return ErrorPermanent }
func (*stormPermanentWorker) Work(context.Context, *Job[stormPermanentArgs]) (Result, error) {
	return Result{}, assert.AnError
}

// stormSnoozeWorker snoozes once per job then succeeds.
type stormSnoozeWorker struct{ attempts sync.Map }

func (*stormSnoozeWorker) Kind() string { return "rw.storm.snooze" }
func (w *stormSnoozeWorker) Work(_ context.Context, job *Job[stormSnoozeArgs]) (Result, error) {
	counter, _ := w.attempts.LoadOrStore(job.ID, &atomic.Int32{})
	if counter.(*atomic.Int32).Add(1) == 1 {
		d := time.Millisecond
		return Result{Snooze: &d}, nil
	}
	return Result{}, nil
}

// TestRealWorldStressMixedOutcomeStorm interleaves four outcome classes (success,
// transient-retry, permanent-fail, snooze) and reconciles the final overview
// against the expected tallies.
func TestRealWorldStressMixedOutcomeStorm(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	ctx := context.Background()

	const each = 100

	success := &stormSuccessWorker{}
	reg := NewRegistry()
	Register(reg, success)
	Register(reg, &stormRetryWorker{})
	Register(reg, &stormPermanentWorker{})
	Register(reg, &stormSnoozeWorker{})
	r := rwRunner(t, db, reg, func(c *RunnerConfig) { c.RetryBackoffBase = time.Millisecond })

	client := NewClient(db)
	for i := range each {
		// Interleave the kinds so the runner sees a churned queue, not blocks.
		_, err := Insert(ctx, client, stormSuccessArgs{I: i}, InsertOpts{})
		require.NoError(t, err)
		_, err = Insert(ctx, client, stormRetryArgs{I: i}, InsertOpts{})
		require.NoError(t, err)
		_, err = Insert(ctx, client, stormPermanentArgs{I: i}, InsertOpts{})
		require.NoError(t, err)
		_, err = Insert(ctx, client, stormSnoozeArgs{I: i}, InsertOpts{})
		require.NoError(t, err)
	}

	runToIdle(t, ctx, r)

	overview, err := Overview(ctx, db, OverviewParams{})
	require.NoError(t, err)
	assert.Equal(t, 4*each, overview.Total, "every enqueued job reached a terminal state")
	// success + retry + snooze all end succeeded; permanent ends discarded.
	assert.Equal(t, 3*each, overview.CountsByState[string(StateSucceeded)], "success, retry, and snooze kinds all succeed")
	assert.Equal(t, each, overview.CountsByState[string(StateDiscarded)], "permanent-fail kind discards")

	// Per-kind tallies.
	assert.Equal(t, each, kindStateCount(t, db, "rw.storm.success", StateSucceeded))
	assert.Equal(t, each, kindStateCount(t, db, "rw.storm.retry", StateSucceeded))
	assert.Equal(t, each, kindStateCount(t, db, "rw.storm.snooze", StateSucceeded))
	assert.Equal(t, each, kindStateCount(t, db, "rw.storm.permanent", StateDiscarded))
	assert.EqualValues(t, each, success.ran.Load())
}

// kindStateCount counts jobs of a kind in a given state.
func kindStateCount(t *testing.T, db *gorm.DB, kind string, state JobState) int {
	t.Helper()
	overview, err := Overview(context.Background(), db, OverviewParams{Kind: kind})
	require.NoError(t, err)
	return overview.CountsByState[string(state)]
}

// --- chaos via lease expiry --------------------------------------------------

// TestRealWorldStressChaosLeaseExpiry simulates an executor that claims a chunk
// of work and dies mid-batch: the leases expire, a Sweep reclaims them, and a
// fresh runner finishes the whole backlog — every job completes exactly once.
func TestRealWorldStressChaosLeaseExpiry(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	driver := NewSQLiteDriver(db)
	base := time.Now().UTC().Truncate(time.Second)

	const total = 300
	const crashed = 120

	tracker := &exactlyOnceTracker{}
	reg := NewRegistry()
	Register(reg, &drainWorker{tracker: tracker})

	// Insert under the same fixed clock the crash-claim uses, so scheduled_at is
	// exactly base and the claim is deterministic regardless of wall-clock drift
	// or stored-time precision.
	claimCtx := clockCtx(context.Background(), NewFixedClock(base))
	client := NewClient(db)
	for i := range total {
		_, err := Insert(claimCtx, client, drainArgs{N: i}, InsertOpts{})
		require.NoError(t, err)
	}

	// A "crashed" executor claims a chunk and writes run stubs, then dies: the
	// jobs sit running with leases held by a process that will never finalize.
	claimedTotal := 0
	for claimedTotal < crashed {
		batch, err := driver.Dequeue(claimCtx, []string{"default"}, "local", true, crashed-claimedTotal, 30*time.Second)
		require.NoError(t, err)
		require.NotEmpty(t, batch, "the crashed executor must be able to claim work")
		for i := range batch {
			require.NoError(t, driver.InsertRunStub(claimCtx, NewID(), batch[i], base, "local", "deadbeef:1"))
		}
		claimedTotal += len(batch)
	}

	var running int64
	require.NoError(t, db.Table("jobs").Where("state = ?", string(StateRunning)).Count(&running).Error)
	assert.EqualValues(t, crashed, running, "the crashed chunk is leased and in flight")

	// The leases expire; one sweep reclaims the whole crashed chunk.
	sweepCtx := clockCtx(context.Background(), NewFixedClock(base.Add(time.Hour)))
	sched := NewScheduler(db, client)
	reclaimed, err := sched.Sweep(sweepCtx)
	require.NoError(t, err)
	assert.Equal(t, crashed, reclaimed, "every expired lease is reclaimed in one sweep")

	// A fresh runner drains everything — the reclaimed chunk plus the work the
	// crashed executor never touched — driven on a fixed clock past the lease so
	// the whole test stays in consistent UTC (the stored scheduled_at is base; a
	// wall-clock runner in a non-UTC zone would compare it inconsistently).
	r := newRunner(t, db, reg)
	runToIdle(t, sweepCtx, r)

	assert.Equal(t, total, tracker.distinct(), "every job ran")
	assert.EqualValues(t, 0, tracker.dups.Load(), "no job ran twice despite the crash and reclaim")

	overview, err := Overview(context.Background(), db, OverviewParams{})
	require.NoError(t, err)
	assert.Equal(t, total, overview.CountsByState[string(StateSucceeded)], "the whole backlog completed")

	// The crashed chunk left exactly `crashed` stale stubs, all marked crashed.
	var crashedRuns int64
	require.NoError(t, db.Table("job_runs").Where("outcome = ?", string(OutcomeCrashed)).Count(&crashedRuns).Error)
	assert.EqualValues(t, crashed, crashedRuns, "each crashed claim left exactly one crashed stub")
}

// restartWorker processes jobs normally but cancels the runner's context after a
// fixed number of jobs, deterministically simulating a runner that stops
// mid-backlog (a rolling restart).
type restartWorker struct {
	tracker *exactlyOnceTracker
	cancel  context.CancelFunc
	stopAt  int64
	count   atomic.Int64
}

func (*restartWorker) Kind() string { return "rw.stress.drain" }
func (w *restartWorker) Work(_ context.Context, job *Job[drainArgs]) (Result, error) {
	w.tracker.mark(job.ID)
	if w.count.Add(1) == w.stopAt {
		w.cancel() // stop the runner cleanly after this job finalizes
	}
	return Result{}, nil
}

// TestRealWorldStressRunnerRestartResumesCleanly drives a runner that stops
// partway through a backlog, then restarts a fresh runner, and asserts the
// backlog finishes with every job run exactly once — no lost or duplicated work
// across the restart.
func TestRealWorldStressRunnerRestartResumesCleanly(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	ctx := context.Background()

	// Pin one connection open so the shared-cache in-memory database survives the
	// brief idle gap between the two runners (the cache is dropped when its last
	// connection closes).
	sqlDB, err := db.DB()
	require.NoError(t, err)
	keepAlive, err := sqlDB.Conn(ctx)
	require.NoError(t, err)
	defer func() { _ = keepAlive.Close() }()

	const total = 400
	const stopAfter = 100

	tracker := &exactlyOnceTracker{}
	stopCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	reg := NewRegistry()
	Register(reg, &restartWorker{tracker: tracker, cancel: cancel, stopAt: stopAfter})

	client := NewClient(db)
	for i := range total {
		_, insErr := Insert(ctx, client, drainArgs{N: i}, InsertOpts{})
		require.NoError(t, insErr)
	}

	// First runner: stops itself after exactly stopAfter jobs.
	r1 := newRunner(t, db, reg)
	err = r1.RunUntilIdle(stopCtx)
	require.ErrorIs(t, err, context.Canceled, "the first runner stops on cancel")
	require.EqualValues(t, stopAfter, tracker.distinct(), "the first runner stopped mid-backlog")

	// Second runner resumes and finishes the rest.
	r2 := newRunner(t, db, reg)
	runToIdle(t, ctx, r2)

	assert.Equal(t, total, tracker.distinct(), "every job ran across the restart")
	assert.EqualValues(t, 0, tracker.dups.Load(), "no job ran twice across the restart")
	overview, err := Overview(ctx, db, OverviewParams{})
	require.NoError(t, err)
	assert.Equal(t, total, overview.CountsByState[string(StateSucceeded)], "the backlog completed after restart")
}
