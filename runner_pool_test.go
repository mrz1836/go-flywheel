package flywheel

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// --- doubles ----------------------------------------------------------------

// poolDriver serves jobs from a backlog, honoring the claim limit it is given
// and recording every limit it was asked for.
//
// It is what fakeDriver cannot be: fakeDriver serves its whole batch once and
// then reports empty, which is enough to exercise a single poll and nothing
// about a loop that claims repeatedly to refill slots. Recording the limits is
// how "no Dequeue ever asks for more than the free-slot count" becomes an
// assertion instead of a reading of the code.
type poolDriver struct {
	mu      sync.Mutex
	pending []RawJob
	limits  []int
	// observeClaim, when set, is called with the limit at the top of every
	// Dequeue. It is set after the Runner exists — the driver has to be built
	// first — which is safe because no claim happens until the loop starts.
	observeClaim func(limit int)
}

// newPoolDriver builds a backlog of n jobs whose args carry their ordinal.
func newPoolDriver(n int) *poolDriver {
	pending := make([]RawJob, n)
	for i := range pending {
		pending[i] = RawJob{
			ID:          fmt.Sprintf("pool-job-%d", i),
			Kind:        "pool.peak",
			Queue:       "default",
			Args:        fmt.Appendf(nil, `{"N":%d}`, i),
			Attempt:     1,
			MaxAttempts: 1,
		}
	}
	return &poolDriver{pending: pending}
}

// Dequeue serves up to limit jobs off the backlog.
func (d *poolDriver) Dequeue(
	_ context.Context, _ []string, _ ExecutorClass, _ bool, limit int, _ time.Duration,
) ([]RawJob, error) {
	if d.observeClaim != nil {
		d.observeClaim(limit)
	}

	d.mu.Lock()
	defer d.mu.Unlock()
	d.limits = append(d.limits, limit)
	if limit <= 0 || len(d.pending) == 0 {
		return nil, nil
	}
	n := min(limit, len(d.pending))
	batch := d.pending[:n]
	d.pending = d.pending[n:]
	return batch, nil
}

func (*poolDriver) InsertRunStub(
	context.Context, string, RawJob, time.Time, ExecutorClass, string,
) error {
	return nil
}

func (*poolDriver) Finalize(
	context.Context, RawJob, string, Result, error, time.Time,
) (FinalizeOutcome, error) {
	return FinalizeOutcome{State: StateSucceeded, RunOutcome: OutcomeSuccess}, nil
}

func (*poolDriver) RenewLease(context.Context, string, string, time.Time) (bool, error) {
	return true, nil
}

func (*poolDriver) InsertChild(context.Context, *gorm.DB, FollowUp, string) error { return nil }

func (*poolDriver) Sweep(context.Context, time.Time) (int, error) { return 0, nil }

// claimLimits returns every limit a Dequeue was asked for, in order.
func (d *poolDriver) claimLimits() []int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]int(nil), d.limits...)
}

type peakArgs struct{ N int }

func (peakArgs) Kind() string { return "pool.peak" }

// peakWorker records the high-water mark of concurrent executions and lets a
// test hold individual jobs by ordinal.
type peakWorker struct {
	// hold, when set, runs inside the worker body and is what a test uses to
	// make one job slow without making all of them slow.
	hold func(ctx context.Context, n int)

	current atomic.Int32
	peak    atomic.Int32
	done    atomic.Int32
}

func (*peakWorker) Kind() string { return "pool.peak" }

func (w *peakWorker) Work(ctx context.Context, job *Job[peakArgs]) (Result, error) {
	cur := w.current.Add(1)
	for {
		peak := w.peak.Load()
		if cur <= peak || w.peak.CompareAndSwap(peak, cur) {
			break
		}
	}
	if w.hold != nil {
		w.hold(ctx, job.Args.N)
	}
	w.current.Add(-1)
	w.done.Add(1)
	return Result{}, nil
}

// newPoolRunner builds a Runner over d at the given pool size and claim batch
// size. Its DB is a real (empty) SQLite database: nothing the poolDriver serves
// reaches it, which is what makes pendingCount read zero while jobs are in
// flight — the condition the RunUntilIdle guarantee has to survive.
func newPoolRunner(t testing.TB, d Driver, w *peakWorker, concurrency, batch int) *Runner {
	t.Helper()
	reg := NewRegistry()
	Register(reg, w)
	r, err := NewRunner(RunnerConfig{
		DB:             newDB(t),
		Driver:         d,
		Registry:       reg,
		Queues:         []string{"default"},
		ExecutorClass:  "local",
		Concurrency:    concurrency,
		ClaimBatchSize: batch,
		PollInterval:   time.Millisecond,
	})
	require.NoError(t, err)
	return r
}

// runInBackground starts r.Run on its own goroutine and returns a stop function
// that cancels it and waits for the loop to return.
func runInBackground(t *testing.T, r *Runner) func() {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	return func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("Run did not return after cancel")
		}
	}
}

// --- FR-04-01 / FR-04-02: slots refill independently ------------------------

// TestPoolFreesASlotWhenItsJobFinalizes proves a slot becomes claimable the
// moment its own job finishes, with no reference to any sibling.
func TestPoolFreesASlotWhenItsJobFinalizes(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	p := newPool(2)

	n, err := p.reserve(ctx, 2)
	require.NoError(t, err)
	require.Equal(t, 2, n, "both slots are reserved")

	entered := make(chan struct{})
	release := make(chan struct{})
	p.start(func() error {
		close(entered)
		<-release
		return nil
	})
	<-entered

	// One reservation is running and one is still held, so the pool is full.
	full, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	_, err = p.reserve(full, 1)
	require.ErrorIs(t, err, context.DeadlineExceeded, "a full pool hands out nothing")

	close(release)

	got, err := p.reserve(ctx, 1)
	require.NoError(t, err, "the finished job's slot is claimable immediately")
	assert.Equal(t, 1, got)
	p.release(got + 1)
}

// TestPoolNeverExceedsConcurrencyInFlight proves the pool is a real bound: a
// deep backlog and a worker that counts its own concurrency never observe more
// than Concurrency executions at once.
func TestPoolNeverExceedsConcurrencyInFlight(t *testing.T) {
	t.Parallel()
	const concurrency = 4
	const jobs = 60

	w := &peakWorker{hold: func(context.Context, int) { time.Sleep(time.Millisecond) }}
	d := newPoolDriver(jobs)
	r := newPoolRunner(t, d, w, concurrency, 0)

	stop := runInBackground(t, r)
	require.Eventually(t, func() bool { return w.done.Load() == jobs },
		10*time.Second, 5*time.Millisecond, "the whole backlog drains")
	stop()

	assert.LessOrEqual(t, w.peak.Load(), int32(concurrency),
		"the pool never runs more than Concurrency jobs at once")
	assert.Greater(t, w.peak.Load(), int32(1),
		"the pool did run jobs concurrently, so the bound above is not vacuous")
}

// TestRunnerKeepsClaimingWhileOneJobIsSlow is the anti-barrier test.
//
// Under the old semantics Concurrency was a barrier: pollOnce claimed a batch of
// N and blocked on all N before the loop could claim again. With one job that
// never returns, that loop claims exactly one batch and stops — so the jobs
// beyond the first batch would never run at all.
//
// The assertion is therefore not "it is faster". It is that eight jobs complete
// while a ninth, claimed in the first batch, is still holding its slot.
func TestRunnerKeepsClaimingWhileOneJobIsSlow(t *testing.T) {
	t.Parallel()
	const concurrency = 4
	const fastJobs = 8

	release := make(chan struct{})
	w := &peakWorker{hold: func(_ context.Context, n int) {
		if n == 0 {
			<-release
		}
	}}
	// Job 0 is the straggler and is claimed in the very first batch.
	d := newPoolDriver(fastJobs + 1)
	r := newPoolRunner(t, d, w, concurrency, 0)

	stop := runInBackground(t, r)
	require.Eventually(t, func() bool { return w.done.Load() == fastJobs },
		10*time.Second, 5*time.Millisecond,
		"the fast jobs finish while the straggler still holds its slot")

	assert.EqualValues(t, 1, w.current.Load(),
		"exactly one job — the straggler — is still in flight")

	close(release)
	require.Eventually(t, func() bool { return w.done.Load() == fastJobs+1 },
		5*time.Second, 5*time.Millisecond, "the straggler finishes too")
	stop()
}

// --- FR-04-03: claim batch decoupled from the worker count ------------------

// TestClaimLimitClampsToConcurrency covers the arithmetic on its own:
// ClaimBatchSize may lower the claim and may never raise it.
func TestClaimLimitClampsToConcurrency(t *testing.T) {
	t.Parallel()
	tests := map[string]struct {
		concurrency, batch, want int
	}{
		"unset batch claims the pool size":  {8, 0, 8},
		"a smaller batch lowers the claim":  {8, 2, 2},
		"a larger batch does not raise it":  {8, 16, 8},
		"an equal batch changes nothing":    {8, 8, 8},
		"a negative batch is treated as 0":  {8, -1, 8},
		"a single-slot pool claims one job": {1, 4, 1},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			r := &Runner{cfg: RunnerConfig{Concurrency: tc.concurrency, ClaimBatchSize: tc.batch}}
			assert.Equal(t, tc.want, r.claimLimit())
		})
	}
}

// TestClaimAsksForTheConfiguredLimit pins the limit at the Driver boundary: one
// poll against an idle pool asks for exactly claimLimit, no more and no less.
func TestClaimAsksForTheConfiguredLimit(t *testing.T) {
	t.Parallel()
	fd := &fakeDriver{}
	r := newFakeRunner(t, fd, 4)
	r.cfg.ClaimBatchSize = 3

	_, err := r.pollOnce(context.Background())
	require.NoError(t, err)
	assert.Equal(t, []int{3}, fd.dequeueLimits(),
		"the claim asks for ClaimBatchSize, not the pool size")
	assert.Equal(t, 1, fd.dequeueCalls(), "one poll issues one claim")
}

// TestClaimBatchSizeCapsTheDequeueLimit proves the configured cap reaches the
// driver: with eight slots and a batch size of two, no claim asks for more.
func TestClaimBatchSizeCapsTheDequeueLimit(t *testing.T) {
	t.Parallel()
	const jobs = 40

	w := &peakWorker{hold: func(context.Context, int) { time.Sleep(time.Millisecond) }}
	d := newPoolDriver(jobs)
	r := newPoolRunner(t, d, w, 8, 2)

	stop := runInBackground(t, r)
	require.Eventually(t, func() bool { return w.done.Load() == jobs },
		10*time.Second, 5*time.Millisecond, "the backlog drains")
	stop()

	limits := d.claimLimits()
	require.NotEmpty(t, limits)
	for i, limit := range limits {
		assert.LessOrEqualf(t, limit, 2, "claim %d asked for %d, above ClaimBatchSize", i, limit)
	}
}

// TestClaimBatchSizeNeverRaisesAboveFreeSlots is the other half: a batch size
// above the pool size is ignored rather than honored, because a claimed job the
// runner has no slot for is a lease burning in a queue.
func TestClaimBatchSizeNeverRaisesAboveFreeSlots(t *testing.T) {
	t.Parallel()
	const jobs = 20

	w := &peakWorker{hold: func(context.Context, int) { time.Sleep(time.Millisecond) }}
	d := newPoolDriver(jobs)
	r := newPoolRunner(t, d, w, 2, 16)

	stop := runInBackground(t, r)
	require.Eventually(t, func() bool { return w.done.Load() == jobs },
		10*time.Second, 5*time.Millisecond, "the backlog drains")
	stop()

	for i, limit := range d.claimLimits() {
		assert.LessOrEqualf(t, limit, 2, "claim %d asked for %d, above the pool size", i, limit)
	}
}

// TestDequeueLimitNeverExceedsFreeSlots is A2's second half, asserted at the
// instant of each claim rather than after the fact: the limit asked for plus the
// jobs already executing never exceeds the pool size.
//
// The two quantities are disjoint by construction — a reservation being claimed
// into is not yet running — so their sum is a lower bound on the reservations
// the pool holds, and the pool holds at most Concurrency.
func TestDequeueLimitNeverExceedsFreeSlots(t *testing.T) {
	t.Parallel()
	const concurrency = 4
	const jobs = 40

	w := &peakWorker{hold: func(context.Context, int) { time.Sleep(2 * time.Millisecond) }}
	d := newPoolDriver(jobs)
	r := newPoolRunner(t, d, w, concurrency, 0)

	var overclaims atomic.Int32
	d.observeClaim = func(limit int) {
		if limit+r.pool.inFlight() > concurrency {
			overclaims.Add(1)
		}
	}

	stop := runInBackground(t, r)
	require.Eventually(t, func() bool { return w.done.Load() == jobs },
		10*time.Second, 5*time.Millisecond, "the backlog drains")
	stop()

	assert.Zero(t, overclaims.Load(),
		"no claim asked for more than the pool had free at that instant")
}

// --- FR-04-06: RunUntilIdle still means "every job is terminal" -------------

// TestRunUntilIdleDoesNotReturnWhileAJobIsStillRunning is A4, and it is written
// to fail against an implementation that counts pending rows without draining
// the pool first.
//
// The setup is what makes it sharp: the poolDriver's jobs never reach the
// runner's own database, so pendingCount reads zero from the very first poll. An
// implementation that trusted that count would declare the queue idle while a
// job was mid-worker — which is exactly the regression this guards, since
// 'running' being one of nonTerminalStates is what would otherwise be quietly
// doing the work.
//
// The load-bearing assertion is therefore not on the return value — the loop
// drains its pool on the way out regardless, so a caller cannot observe the
// difference there. It is on *when the decision was taken*: a query callback on
// the runner's own database records how many jobs were executing each time the
// pending count ran, and the invariant is that the runner never asks "is the
// queue idle?" while its own pool is busy.
func TestRunUntilIdleDoesNotReturnWhileAJobIsStillRunning(t *testing.T) {
	t.Parallel()

	w := &peakWorker{hold: func(context.Context, int) { time.Sleep(150 * time.Millisecond) }}
	d := newPoolDriver(1)
	r := newPoolRunner(t, d, w, 2, 0)

	// Nothing else queries this database during the run, so every callback here
	// is a pending count. It reads the pool rather than the worker's own counter
	// because the pool is incremented synchronously by the dispatching loop,
	// where the worker's counter is only incremented once the worker body has
	// been entered — a marker that arrives too late to catch the window.
	var busyAtCount atomic.Int32
	require.NoError(t, r.cfg.DB.Callback().Query().Before("gorm:query").
		Register("test:pending_count_probe", func(*gorm.DB) {
			if n := r.pool.inFlight(); n > 0 {
				busyAtCount.Add(int32(n)) //nolint:gosec // a pool size, bounded by Concurrency
			}
		}))

	require.NoError(t, r.RunUntilIdle(context.Background()))

	assert.Zero(t, busyAtCount.Load(),
		"the queue was declared idle while this runner's own pool still had a job in flight")
	assert.EqualValues(t, 1, w.done.Load(), "the job reached a terminal state")
	assert.Zero(t, w.current.Load(), "nothing is still executing")
}

// TestRunUntilIdleSurfacesADispatchErrorFromAPoolGoroutine proves a per-job
// failure raised on a pool goroutine — not on the loop's own — still reaches the
// caller. It is deterministic rather than racy because the drain wait happens
// before the collector is read, and a dispatch deposits its error before it
// releases its slot.
func TestRunUntilIdleSurfacesADispatchErrorFromAPoolGoroutine(t *testing.T) {
	t.Parallel()
	fd := &fakeDriver{
		batch:   []RawJob{{ID: "a", Kind: "test.success", Args: []byte(`{}`)}},
		stubErr: errors.New("stub failed"),
	}
	r := newFakeRunner(t, fd, 2)

	err := r.RunUntilIdle(context.Background())
	require.ErrorContains(t, err, "stub failed",
		"a dispatch error raised on a pool goroutine reaches the caller")
}

// --- FR-04-04 / FR-04-05: Concurrency 1 is unchanged ------------------------

// TestPoolStartRunsInlineAtLimitOne is A3 at the pool level: at limit 1 there is
// no goroutine boundary, and above it there is.
func TestPoolStartRunsInlineAtLimitOne(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	t.Run("limit one runs on the caller's goroutine", func(t *testing.T) {
		t.Parallel()
		p := newPool(1)
		_, err := p.reserve(ctx, 1)
		require.NoError(t, err)

		// Written and read with no synchronization on purpose: a goroutine here
		// would be both a wrong answer and a data race the -race build reports.
		ran := false
		p.start(func() error { ran = true; return nil })
		assert.True(t, ran, "start ran the job before returning: no goroutine, no handoff")
		assert.Zero(t, p.inFlight(), "the slot is released when the job returns")
	})

	t.Run("above one the job runs on its own goroutine", func(t *testing.T) {
		t.Parallel()
		p := newPool(2)
		_, err := p.reserve(ctx, 1)
		require.NoError(t, err)

		entered := make(chan struct{})
		release := make(chan struct{})
		p.start(func() error {
			close(entered)
			<-release
			return nil
		})
		// start returned while the job is still blocked, so it did not run inline.
		<-entered
		assert.Equal(t, 1, p.inFlight())

		close(release)
		require.NoError(t, p.waitIdle(ctx))
		assert.Zero(t, p.inFlight())
	})
}

// TestConcurrencyOneRunsInlineOnTheLoopGoroutine is A3 through the Runner: the
// worker's own stack carries the dispatch loop's frame, which it could not if a
// goroutine sat between them.
func TestConcurrencyOneRunsInlineOnTheLoopGoroutine(t *testing.T) {
	t.Parallel()

	var stack atomic.Value
	w := &peakWorker{hold: func(context.Context, int) {
		buf := make([]byte, 16<<10)
		stack.Store(string(buf[:runtime.Stack(buf, false)]))
	}}
	d := newPoolDriver(1)
	r := newPoolRunner(t, d, w, 1, 0)

	stop := runInBackground(t, r)
	require.Eventually(t, func() bool { return w.done.Load() == 1 },
		5*time.Second, 5*time.Millisecond, "the job runs")
	stop()

	got, ok := stack.Load().(string)
	require.True(t, ok, "the worker captured its stack")
	assert.Contains(t, got, "flywheel.(*Runner).run",
		"at Concurrency 1 the worker runs on the dispatch loop's own goroutine")
	assert.EqualValues(t, 1, w.peak.Load(), "one job in flight at a time")
}

// TestNewRunnerRejectsSQLiteConcurrencyRegardlessOfClaimBatchSize proves the new
// knob cannot be used to talk the guard out of its verdict — the pool size is
// what SQLite constrains, not the size of the claim.
func TestNewRunnerRejectsSQLiteConcurrencyRegardlessOfClaimBatchSize(t *testing.T) {
	t.Parallel()
	for name, batch := range map[string]int{"unset": 0, "one": 1, "larger": 8} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			db := newDB(t)
			_, err := NewRunner(RunnerConfig{
				DB:             db,
				Driver:         NewSQLiteDriver(db),
				Registry:       NewRegistry(),
				Queues:         []string{"default"},
				Concurrency:    2,
				ClaimBatchSize: batch,
			})
			require.ErrorIs(t, err, ErrSQLiteConcurrency)
		})
	}
}

// TestPoolAdmitsAnOverServingDriver proves a Driver that returns more jobs than
// the limit it was given still has every one of them accounted for. Those jobs
// are leased and must run; a dispatch the pool has not counted is one a drain
// would report as finished while it was still executing.
func TestPoolAdmitsAnOverServingDriver(t *testing.T) {
	t.Parallel()
	fd := &fakeDriver{batch: []RawJob{
		{ID: "a", Kind: "test.success", Args: []byte(`{}`)},
		{ID: "b", Kind: "test.success", Args: []byte(`{}`)},
		{ID: "c", Kind: "test.success", Args: []byte(`{}`)},
	}}
	// A pool of two against a driver that serves three regardless of the limit.
	r := newFakeRunner(t, fd, 2)

	n, err := r.pollOnce(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 3, n, "every served job is dispatched, not truncated")
	assert.Equal(t, 3, fd.finalized, "every served job finalized")
	assert.Zero(t, r.pool.inFlight(), "the pool is empty afterwards")

	r.pool.mu.Lock()
	held := r.pool.held
	r.pool.mu.Unlock()
	assert.Zero(t, held, "every reservation, including the over-served ones, was returned")
}

// TestConcurrencyOneDispatchesSequentially pins the ordering half of FR-04-04:
// at Concurrency 1 a batch runs one job after another, never overlapping.
func TestConcurrencyOneDispatchesSequentially(t *testing.T) {
	t.Parallel()
	var order []int
	var mu sync.Mutex

	w := &peakWorker{hold: func(_ context.Context, n int) {
		mu.Lock()
		order = append(order, n)
		mu.Unlock()
	}}
	d := newPoolDriver(5)
	r := newPoolRunner(t, d, w, 1, 0)

	stop := runInBackground(t, r)
	require.Eventually(t, func() bool { return w.done.Load() == 5 },
		5*time.Second, 5*time.Millisecond, "every job runs")
	stop()

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, []int{0, 1, 2, 3, 4}, order, "jobs ran in claim order, one at a time")
	assert.EqualValues(t, 1, w.peak.Load(), "never more than one in flight")
	for i, limit := range d.claimLimits() {
		assert.Equalf(t, 1, limit, "claim %d asked for %d; a single-slot pool claims one job", i, limit)
	}
}
