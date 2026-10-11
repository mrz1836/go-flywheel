package core

import (
	"context"
	"errors"
	"log/slog"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// The cooperative-cancel tests run a real Runner against a real database, with a
// heartbeat fast enough to keep the suite quick and a lease long enough that
// nothing but the test itself takes the claim.
const (
	coopHeartbeat = 50 * time.Millisecond
	coopLease     = 5 * time.Second
	// coopSlack bounds the statements and the scheduling around a heartbeat on a
	// loaded -race run. "Within one heartbeat" is pinned by counting renewals,
	// which no amount of load can blur; the wall clock only bounds the wait.
	coopSlack = 2 * time.Second
)

// walFileOpener opens a WAL file database: the heartbeat writes on one
// connection while the test writes on another, which the shared-cache fixture
// turns into an intermittent "database is locked" (see
// TestLeaseHeartbeatStopsOnClaimLoss).
func walFileOpener(t *testing.T) *gorm.DB {
	t.Helper()
	return newWALFileDB(t)
}

// --- fixtures ---------------------------------------------------------------

// coopArgs is the args of the job most of these tests run.
type coopArgs struct{ V string }

func (coopArgs) Kind() string { return "test.coop" }

// coopChildArgs decodes the args declareBarrierParent gives each child.
type coopChildArgs struct {
	I int `json:"i"`
}

func (coopChildArgs) Kind() string { return "child" }

// coopWorker holds its attempt open until the test lets it go, and records why
// its context ended. When its context ends it records the cause and parks until
// let go, so a test can read what the database shows while the attempt is still
// running; then it returns the context's error, or success when finishAnyway is
// set, as a worker that ignores its context does.
type coopWorker[A Args] struct {
	kind         string
	finishAnyway bool

	entered     chan struct{}
	ctxDone     chan struct{}
	release     chan struct{}
	enterOnce   sync.Once
	doneOnce    sync.Once
	releaseOnce sync.Once

	mu    sync.Mutex
	cause error
}

func newCoopWorker[A Args](kind string) *coopWorker[A] {
	return &coopWorker[A]{
		kind:    kind,
		entered: make(chan struct{}),
		ctxDone: make(chan struct{}),
		release: make(chan struct{}),
	}
}

func (w *coopWorker[A]) Kind() string { return w.kind }

func (w *coopWorker[A]) Work(ctx context.Context, _ *Job[A]) (Result, error) {
	w.enterOnce.Do(func() { close(w.entered) })
	select {
	case <-w.release:
		return Result{}, nil
	case <-ctx.Done():
	}
	w.mu.Lock()
	w.cause = context.Cause(ctx)
	w.mu.Unlock()
	w.doneOnce.Do(func() { close(w.ctxDone) })
	<-w.release
	if w.finishAnyway {
		return Result{}, nil
	}
	return Result{}, ctx.Err()
}

// letGo releases the worker. It is safe to call more than once.
func (w *coopWorker[A]) letGo() { w.releaseOnce.Do(func() { close(w.release) }) }

// contextCause is what context.Cause reported when the worker's context ended,
// nil while it has not.
func (w *coopWorker[A]) contextCause() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.cause
}

// waitEntered waits for the worker's body to start.
func (w *coopWorker[A]) waitEntered(t *testing.T) {
	t.Helper()
	select {
	case <-w.entered:
	case <-time.After(coopSlack):
		t.Fatal("the worker never started")
	}
}

// waitContextDone waits up to within for the worker's context to end.
func (w *coopWorker[A]) waitContextDone(t *testing.T, within time.Duration, msg string) {
	t.Helper()
	select {
	case <-w.ctxDone:
	case <-time.After(within):
		t.Fatal(msg)
	}
}

// assertContextAlive waits for d and fails if the worker's context ended.
func (w *coopWorker[A]) assertContextAlive(t *testing.T, d time.Duration, msg string) {
	t.Helper()
	select {
	case <-w.ctxDone:
		t.Fatalf("%s: the worker's context ended with %v", msg, w.contextCause())
	case <-time.After(d):
	}
}

// gatedRenewDriver counts lease renewals and can hold them, so a test can write
// what a path writes while no renewal is in flight, then let the next renewal
// find the claim gone and the heartbeat read the state the path left.
type gatedRenewDriver struct {
	Driver
	renewals atomic.Int64

	mu       sync.Mutex
	cond     *sync.Cond
	held     bool
	inFlight int
}

func newGatedRenewDriver(d Driver) *gatedRenewDriver {
	g := &gatedRenewDriver{Driver: d}
	g.cond = sync.NewCond(&g.mu)
	return g
}

func (d *gatedRenewDriver) RenewLease(
	ctx context.Context, jobID, leaseToken string, until time.Time,
) (bool, error) {
	d.mu.Lock()
	for d.held {
		d.cond.Wait()
	}
	d.inFlight++
	d.mu.Unlock()
	defer func() {
		d.mu.Lock()
		d.inFlight--
		d.cond.Broadcast()
		d.mu.Unlock()
	}()
	d.renewals.Add(1)
	return d.Driver.RenewLease(ctx, jobID, leaseToken, until)
}

// hold stops new renewals from starting and waits out the one in flight, if any.
func (d *gatedRenewDriver) hold() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.held = true
	for d.inFlight > 0 {
		d.cond.Wait()
	}
}

// resume lets renewals run again.
func (d *gatedRenewDriver) resume() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.held = false
	d.cond.Broadcast()
}

// dispatchCtxKey marks the context a test dispatches with. The heartbeat's reads
// carry it, since the heartbeat detaches its context from cancellation and not
// from its values; the test's own reads do not.
type dispatchCtxKey struct{}

// errInjectedStateRead is the failure stateReadProbe injects.
var errInjectedStateRead = errors.New("injected state read failure")

// stateReadProbe counts the reads of a job's state made on the dispatch's
// context: the heartbeat's read after a lost claim, and a superseded finalize's
// read-back. It fails the next failNext of them.
type stateReadProbe struct {
	reads    atomic.Int64
	failNext atomic.Int64
}

func probeStateReads(t *testing.T, db *gorm.DB) *stateReadProbe {
	t.Helper()
	p := &stateReadProbe{}
	require.NoError(t, db.Callback().Query().Before("gorm:query").Register("test:state_read_probe",
		func(tx *gorm.DB) {
			ctx := tx.Statement.Context
			if ctx == nil || ctx.Value(dispatchCtxKey{}) == nil {
				return
			}
			if tx.Statement.Table != "jobs" || !slices.Equal(tx.Statement.Selects, []string{"state"}) {
				return
			}
			p.reads.Add(1)
			for {
				n := p.failNext.Load()
				if n <= 0 {
					return
				}
				if p.failNext.CompareAndSwap(n, n-1) {
					_ = tx.AddError(errInjectedStateRead)
					return
				}
			}
		}))
	return p
}

// coopRunner builds a Runner over db and d with the cooperative-cancel tests'
// heartbeat and lease.
func coopRunner(
	t *testing.T, db *gorm.DB, d Driver, reg *Registry, cooperative bool, obs Observer, logs slog.Handler,
) *Runner {
	t.Helper()
	r, err := NewRunner(RunnerConfig{
		DB: db, Driver: d, Registry: reg,
		Queues: []string{"default"}, ClaimAnyClass: true, Concurrency: 1,
		LeaseDuration:     coopLease,
		HeartbeatInterval: coopHeartbeat,
		PollInterval:      5 * time.Millisecond,
		CooperativeCancel: cooperative,
		Observer:          obs,
		Logger:            slog.New(logs),
	})
	require.NoError(t, err)
	return r
}

// coopDispatch is one pollOnce running in the background.
type coopDispatch struct {
	done chan struct{}
	err  error
}

// dispatchOne runs one claim-to-completion of r on the marked context. Its
// cleanup resumes renewals and lets the worker go, then waits for the dispatch,
// so a failed test never leaves the heartbeat blocked or the database in use.
func dispatchOne(t *testing.T, r *Runner, d *gatedRenewDriver, letGo func()) *coopDispatch {
	t.Helper()
	cd := &coopDispatch{done: make(chan struct{})}
	ctx := context.WithValue(context.Background(), dispatchCtxKey{}, true)
	go func() {
		defer close(cd.done)
		_, cd.err = r.pollOnce(ctx)
	}()
	t.Cleanup(func() {
		d.resume()
		letGo()
		select {
		case <-cd.done:
		case <-time.After(10 * time.Second):
			t.Error("the dispatch did not return")
		}
	})
	return cd
}

// wait waits for the dispatch to return and requires that it succeeded.
func (cd *coopDispatch) wait(t *testing.T) {
	t.Helper()
	select {
	case <-cd.done:
		require.NoError(t, cd.err)
	case <-time.After(coopSlack):
		t.Fatal("the dispatch did not return after the worker was let go")
	}
}

// coopAttempt is one running attempt of a test.coop job.
type coopAttempt struct {
	db       *gorm.DB
	driver   *gatedRenewDriver
	worker   *coopWorker[coopArgs]
	probe    *stateReadProbe
	obs      *recordingObserver
	logs     *recordingHandler
	jobID    string
	dispatch *coopDispatch
}

// startCoopAttempt enqueues one test.coop job on db and dispatches it on a Runner
// with or without CooperativeCancel, returning once the worker is running.
func startCoopAttempt(
	t *testing.T, db *gorm.DB, cooperative bool, w *coopWorker[coopArgs], opts InsertOpts,
) *coopAttempt {
	t.Helper()
	a := &coopAttempt{
		db:     db,
		driver: newGatedRenewDriver(driverForTest(t, db)),
		worker: w,
		probe:  probeStateReads(t, db),
		obs:    &recordingObserver{},
		logs:   &recordingHandler{},
	}
	reg := NewRegistry()
	Register(reg, w)
	r := coopRunner(t, db, a.driver, reg, cooperative, a.obs, a.logs)

	id, err := Insert(context.Background(), NewClient(db), coopArgs{V: "x"}, opts)
	require.NoError(t, err)
	a.jobID = id
	a.dispatch = dispatchOne(t, r, a.driver, w.letGo)
	w.waitEntered(t)
	return a
}

// finish lets the worker return and waits for its attempt to finalize.
func (a *coopAttempt) finish(t *testing.T) {
	t.Helper()
	a.worker.letGo()
	a.dispatch.wait(t)
}

// latestRun reads the job's newest attempt through the public read.
func (a *coopAttempt) latestRun(t *testing.T) JobRunView {
	t.Helper()
	run, ok, err := LatestRun(context.Background(), a.db, a.jobID)
	require.NoError(t, err)
	require.True(t, ok, "the job has a run")
	return run
}

// logged returns the log records with message msg.
func (a *coopAttempt) logged(msg string) []map[string]any {
	var out []map[string]any
	for _, rec := range a.logs.records() {
		if rec["msg"] == msg {
			out = append(out, rec)
		}
	}
	return out
}

// The log lines the cooperative cancel writes.
const (
	logCancellingAttempt = "jobs: job cancelled; cancelling its running attempt"
	logStateReadFailed   = "jobs: job state read failed after a lost claim; retrying at the next heartbeat"
)

// --- with the opt-in ----------------------------------------------------------

// cooperativeCancelSuite proves a cancel stops a cooperative Runner's worker on
// the first renewal after it commits, with the cause ErrJobCancelled, and that
// the attempt is recorded as the cancel it was.
func cooperativeCancelSuite(t *testing.T, open dbOpener) {
	t.Helper()

	t.Run("a worker that stops is recorded as cancelled", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		w := newCoopWorker[coopArgs]("test.coop")
		a := startCoopAttempt(t, open(t), true, w, InsertOpts{})

		require.NoError(t, CancelJob(ctx, a.db, a.jobID))
		afterCancel := a.driver.renewals.Load()
		w.waitContextDone(t, coopHeartbeat+coopSlack, "the worker's context must end within one heartbeat of the cancel")

		require.ErrorIs(t, w.contextCause(), ErrJobCancelled, "context.Cause names the cancel")
		atDone := a.driver.renewals.Load()
		assert.LessOrEqual(t, atDone-afterCancel, int64(1),
			"the first renewal after the cancel found the claim gone, so the cancel landed within one heartbeat")
		assert.EqualValues(t, 1, a.probe.reads.Load(), "one read of the job's state")
		finalizedAt := jobRowByID(t, a.db, a.jobID).FinalizedAt
		require.NotNil(t, finalizedAt)

		a.finish(t)
		assert.Equal(t, atDone, a.driver.renewals.Load(), "renewal never resumed after the lost claim")

		run := a.latestRun(t)
		assert.Equal(t, string(OutcomeCancelled), run.Outcome, "the attempt is recorded as the cancel it was")
		assert.True(t, run.Superseded, "the cancel had already taken its claim")
		require.NotNil(t, run.Error)
		assert.Equal(t, "jobs: job cancelled while running: context canceled", *run.Error)
		assert.Empty(t, run.ErrorClass, "a cancel is not a failure, so it carries no error class")
		assert.Empty(t, run.JobState, "a superseded finalize applies no state")
		require.NotNil(t, run.FinishedAt)

		row := jobRowByID(t, a.db, a.jobID)
		assert.Equal(t, string(StateCancelled), row.State, "the job stays cancelled")
		require.NotNil(t, row.FinalizedAt)
		assert.True(t, finalizedAt.Equal(*row.FinalizedAt), "the late finalize leaves the cancel's stamp alone")
		assert.Nil(t, row.LeaseToken)

		_, _, finishes, retries := a.obs.snapshot()
		assert.Empty(t, finishes, "a superseded attempt is not reported as finished")
		assert.Empty(t, retries)
		supersedes := a.obs.snapshotSupersedes()
		require.Len(t, supersedes, 1)
		assert.Equal(t, OutcomeCancelled, supersedes[0].Outcome)
		assert.Equal(t, StateCancelled, supersedes[0].State)

		lines := a.logged(logCancellingAttempt)
		require.Len(t, lines, 1, "the cancel is logged once")
		assert.Equal(t, a.jobID, lines[0]["job_id"])
		assert.Equal(t, supersedes[0].RunID, lines[0]["run_id"])
		assert.Equal(t, "test.coop", lines[0]["kind"])
		assert.EqualValues(t, 1, lines[0]["attempt"])
		assert.Equal(t, slog.LevelInfo.String(), lines[0]["level"])
		assert.Empty(t, a.logged(logStateReadFailed))
	})

	t.Run("a failed state read is retried at the next heartbeat", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		w := newCoopWorker[coopArgs]("test.coop")
		a := startCoopAttempt(t, open(t), true, w, InsertOpts{})
		a.probe.failNext.Store(2)

		require.NoError(t, CancelJob(ctx, a.db, a.jobID))
		afterCancel := a.driver.renewals.Load()
		w.waitContextDone(t, 3*coopHeartbeat+coopSlack, "a failed read must not lose the cancel")

		require.ErrorIs(t, w.contextCause(), ErrJobCancelled)
		assert.EqualValues(t, 3, a.probe.reads.Load(), "two failed reads, then the one that answered")
		assert.LessOrEqual(t, a.driver.renewals.Load()-afterCancel, int64(1),
			"the heartbeat retried the read, not the renewal")
		warns := a.logged(logStateReadFailed)
		require.Len(t, warns, 1, "a streak of failed reads warns once")
		assert.Equal(t, slog.LevelWarn.String(), warns[0]["level"])
		assert.Equal(t, a.jobID, warns[0]["job_id"])

		a.finish(t)
		assert.Equal(t, string(OutcomeCancelled), a.latestRun(t).Outcome)
		assert.Len(t, a.logged(logCancellingAttempt), 1)
	})

	t.Run("a worker that finishes anyway keeps its success", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		w := newCoopWorker[coopArgs]("test.coop")
		w.finishAnyway = true
		a := startCoopAttempt(t, open(t), true, w, InsertOpts{})

		require.NoError(t, CancelJob(ctx, a.db, a.jobID))
		w.waitContextDone(t, coopHeartbeat+coopSlack, "the worker's context must end within one heartbeat of the cancel")
		require.ErrorIs(t, w.contextCause(), ErrJobCancelled)
		a.finish(t)

		run := a.latestRun(t)
		assert.Equal(t, string(OutcomeSuccess), run.Outcome, "the work it finished is reported as done")
		assert.True(t, run.Superseded, "and its result is not applied to the job")
		assert.Nil(t, run.Error)
		assert.Empty(t, run.ErrorClass)
		assert.Equal(t, string(StateCancelled), jobState(t, a.db, a.jobID))
	})
}

// TestCooperativeCancelCancelsTheWorkerWithinOneHeartbeat proves that with
// CooperativeCancel a cancelled job's worker sees its context end on the first
// renewal after the cancel, with errors.Is(context.Cause(ctx), ErrJobCancelled);
// that a failed state read is retried at the next heartbeat without renewing;
// and that the attempt records outcome cancelled, superseded, with no error
// class, unless its worker finished anyway.
func TestCooperativeCancelCancelsTheWorkerWithinOneHeartbeat(t *testing.T) {
	t.Parallel()
	cooperativeCancelSuite(t, walFileOpener)
}

// reclaimNeverCancelsSuite takes a running attempt's claim each way other than a
// cancel, as each path writes it, and proves the heartbeat reads the state it
// left and leaves the worker running: a lost claim alone never asks a worker to
// stop.
func reclaimNeverCancelsSuite(t *testing.T, open dbOpener) {
	t.Helper()
	pastTheLease := func() time.Time { return time.Now().Add(2 * coopLease) }

	for _, tc := range []struct {
		name        string
		maxAttempts int
		take        func(t *testing.T, ctx context.Context, a *coopAttempt)
		want        JobState
	}{
		{
			name: "the lease sweep's reclaim",
			take: func(t *testing.T, ctx context.Context, a *coopAttempt) {
				n, err := a.driver.Sweep(ctx, pastTheLease())
				require.NoError(t, err)
				require.Equal(t, 1, n)
			},
			want: StateAvailable,
		},
		{
			name: "a reclaim claimed again under a new token",
			take: func(t *testing.T, ctx context.Context, a *coopAttempt) {
				n, err := a.driver.Sweep(ctx, pastTheLease())
				require.NoError(t, err)
				require.Equal(t, 1, n)
				claimed, err := a.driver.Dequeue(ctx, []string{"default"}, AnyClass, true, 1, time.Hour)
				require.NoError(t, err)
				require.Len(t, claimed, 1)
			},
			want: StateRunning,
		},
		{
			name:        "the lease sweep's crash discard",
			maxAttempts: 1,
			take: func(t *testing.T, ctx context.Context, a *coopAttempt) {
				n, err := a.driver.Sweep(ctx, pastTheLease())
				require.NoError(t, err)
				require.Equal(t, 1, n)
			},
			want: StateDiscarded,
		},
		{
			name: "a retry with AllowRunning",
			take: func(t *testing.T, ctx context.Context, a *coopAttempt) {
				require.NoError(t, RetryJobWithOptions(ctx, a.db, a.jobID, RetryOpts{AllowRunning: true}))
			},
			want: StateAvailable,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			w := newCoopWorker[coopArgs]("test.coop")
			a := startCoopAttempt(t, open(t), true, w, InsertOpts{MaxAttempts: tc.maxAttempts})

			// Write what the path writes while no renewal is in flight, so the
			// next renewal finds the claim gone and reads the state it left.
			a.driver.hold()
			tc.take(t, ctx, a)
			require.Equal(t, string(tc.want), jobState(t, a.db, a.jobID))
			a.driver.resume()

			require.Eventually(t, func() bool { return a.probe.reads.Load() >= 1 },
				coopSlack, time.Millisecond, "the heartbeat reads the job's state once its claim is lost")
			renewals := a.driver.renewals.Load()
			w.assertContextAlive(t, 10*coopHeartbeat, "a lost claim that is not a cancel must not stop the worker")
			assert.Equal(t, renewals, a.driver.renewals.Load(), "renewal stopped with the lost claim")
			assert.EqualValues(t, 1, a.probe.reads.Load(), "one read answered, and the heartbeat ended")
			assert.Empty(t, a.logged(logCancellingAttempt))

			a.finish(t)
			require.NoError(t, w.contextCause(), "the worker's context never ended")
			run := a.latestRun(t)
			assert.Equal(t, 1, run.Attempt)
			assert.Equal(t, string(OutcomeSuccess), run.Outcome, "the attempt keeps the outcome its worker returned")
			assert.True(t, run.Superseded)
			assert.Equal(t, string(tc.want), jobState(t, a.db, a.jobID), "the superseded finalize changed nothing")
		})
	}
}

// TestAReclaimNeverCancelsTheWorker proves that with CooperativeCancel only a
// cancel stops a worker: the lease sweep's reclaim, a re-claim under a new
// token, the sweep's crash discard, and a retry with AllowRunning each take the
// claim, and the worker runs on as it would without the setting.
func TestAReclaimNeverCancelsTheWorker(t *testing.T) {
	t.Parallel()
	reclaimNeverCancelsSuite(t, walFileOpener)
}

// --- without the opt-in -------------------------------------------------------

// withoutCooperativeCancelSuite proves that a Runner without CooperativeCancel
// behaves as it always has: a cancel takes the attempt's claim, renewal stops,
// nothing reads the job's state, and the worker runs to its end, its result
// recorded superseded.
func withoutCooperativeCancelSuite(t *testing.T, db *gorm.DB) {
	t.Helper()
	ctx := context.Background()
	w := newCoopWorker[coopArgs]("test.coop")
	a := startCoopAttempt(t, db, false, w, InsertOpts{})

	require.NoError(t, CancelJob(ctx, a.db, a.jobID))
	w.assertContextAlive(t, 10*coopHeartbeat, "without the opt-in a cancel never stops the worker")
	require.Eventually(t, func() bool {
		before := a.driver.renewals.Load()
		time.Sleep(3 * coopHeartbeat)
		return a.driver.renewals.Load() == before
	}, coopSlack, time.Millisecond, "renewal stops once the claim is lost")
	assert.Zero(t, a.probe.reads.Load(), "nothing reads the job's state")

	a.finish(t)
	require.NoError(t, w.contextCause())
	run := a.latestRun(t)
	assert.Equal(t, string(OutcomeSuccess), run.Outcome)
	assert.True(t, run.Superseded, "its result is discarded")
	assert.Equal(t, string(StateCancelled), jobState(t, a.db, a.jobID))
	assert.Empty(t, a.logged(logCancellingAttempt))
}

// TestWithoutCooperativeCancelACancelledWorkerRunsOn proves the setting is off
// by default and changes nothing while off.
func TestWithoutCooperativeCancelACancelledWorkerRunsOn(t *testing.T) {
	t.Parallel()
	withoutCooperativeCancelSuite(t, newWALFileDB(t))
}

// TestNewRunnerRefusesCooperativeCancelWithoutAHeartbeat proves the setting is
// refused where nothing would ever notice a cancel: a negative HeartbeatInterval
// disables renewal, and with it the read that sees the cancel.
func TestNewRunnerRefusesCooperativeCancelWithoutAHeartbeat(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	build := func(cooperative bool, interval time.Duration) error {
		_, err := NewRunner(RunnerConfig{
			DB: db, Driver: NewSQLiteDriver(db), Registry: NewRegistry(), Queues: []string{"default"},
			HeartbeatInterval: interval, CooperativeCancel: cooperative,
		})
		return err
	}

	require.EqualError(t, build(true, -1),
		"jobs: runner config: CooperativeCancel needs the lease heartbeat, and HeartbeatInterval is negative")
	require.NoError(t, build(true, 0), "the derived interval renews")
	require.NoError(t, build(true, time.Second))
	require.NoError(t, build(false, -1), "without the setting a disabled heartbeat is allowed, as before")
}

// --- what a reader sees -------------------------------------------------------

// assertCancelWaiting checks how a job reads through FindJob and its runs: a
// cancel still waiting on a running attempt is a cancelled job whose run at the
// job's attempt still reads outcome started. Once that attempt returns, its run
// is finished and superseded, and the cancel waits on nothing.
func assertCancelWaiting(t *testing.T, db *gorm.DB, jobID string, waiting bool) {
	t.Helper()
	ctx := context.Background()
	job, err := FindJob(ctx, db, jobID)
	require.NoError(t, err)
	assert.Equal(t, string(StateCancelled), job.State)
	require.NotNil(t, job.FinalizedAt, "the job is terminal from the cancel on")

	run, ok, err := LatestRun(ctx, db, jobID)
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, job.Attempt, run.Attempt, "the latest run is the one at the job's attempt")
	runs, err := ListRuns(ctx, db, jobID, ListRunsParams{})
	require.NoError(t, err)
	require.NotEmpty(t, runs)
	assert.Equal(t, run, runs[0], "ListRuns reads the same attempt first")

	if waiting {
		assert.Equal(t, string(OutcomeStarted), run.Outcome, "the attempt has not returned")
		assert.Nil(t, run.FinishedAt)
		assert.False(t, run.Superseded)
		return
	}
	assert.NotEqual(t, string(OutcomeStarted), run.Outcome, "the attempt has returned")
	assert.NotNil(t, run.FinishedAt)
	assert.True(t, run.Superseded)
}

// cancelWaitingSuite pins how a cancel still waiting on a running attempt reads,
// with and without the opt-in: from the cancel until the worker returns, even
// once a cooperative worker has seen its context end.
func cancelWaitingSuite(t *testing.T, open dbOpener) {
	t.Helper()
	for _, cooperative := range []bool{true, false} {
		name := "without CooperativeCancel"
		if cooperative {
			name = "with CooperativeCancel"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			w := newCoopWorker[coopArgs]("test.coop")
			a := startCoopAttempt(t, open(t), cooperative, w, InsertOpts{})

			require.NoError(t, CancelJob(ctx, a.db, a.jobID))
			assertCancelWaiting(t, a.db, a.jobID, true)
			if cooperative {
				w.waitContextDone(t, coopHeartbeat+coopSlack, "the cooperative cancel reaches the worker")
			} else {
				w.assertContextAlive(t, 3*coopHeartbeat, "without the opt-in the worker runs on")
			}
			assertCancelWaiting(t, a.db, a.jobID, true)

			a.finish(t)
			assertCancelWaiting(t, a.db, a.jobID, false)
		})
	}
}

// TestACancelWaitingOnARunningAttemptIsVisible proves the existing reads show a
// cancel that still waits on a running attempt, with or without
// CooperativeCancel: FindJob reads the job cancelled while LatestRun (and
// ListRuns) read its attempt started, until the attempt returns.
func TestACancelWaitingOnARunningAttemptIsVisible(t *testing.T) {
	t.Parallel()
	cancelWaitingSuite(t, walFileOpener)
}

// --- the barrier ------------------------------------------------------------

// cooperativeCancelBarrierSuite cancels a running child, the last of its
// generation, under a cooperative Runner. The cancel fires the parent's barrier;
// the worker stops and its attempt finalizes as cancelled, superseded, and fires
// nothing more.
func cooperativeCancelBarrierSuite(t *testing.T, db *gorm.DB) {
	t.Helper()
	ctx := context.Background()
	d := newGatedRenewDriver(driverForTest(t, db))
	parentID := declareBarrierParent(t, d, db, ctx, "child", "finalize", 3)
	finalizeJob(t, d, ctx, claimOne(t, d, ctx), Result{}, nil)
	finalizeJob(t, d, ctx, claimOne(t, d, ctx), Result{}, nil)

	w := newCoopWorker[coopChildArgs]("child")
	reg := NewRegistry()
	Register(reg, w)
	r := coopRunner(t, db, d, reg, true, &recordingObserver{}, &recordingHandler{})
	cd := dispatchOne(t, r, d, w.letGo)
	w.waitEntered(t)

	running := pendingChildIDs(t, db, parentID, StateRunning)
	require.Len(t, running, 1, "the last child is running")
	require.NoError(t, CancelJob(ctx, db, running[0]))
	assertBarrierContinuations(t, db, parentID, 1)

	w.waitContextDone(t, coopHeartbeat+coopSlack, "the cancelled child's worker is asked to stop")
	require.ErrorIs(t, w.contextCause(), ErrJobCancelled)
	w.letGo()
	cd.wait(t)

	assertBarrierContinuations(t, db, parentID, 1)
	run, ok, err := LatestRun(ctx, db, running[0])
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, string(OutcomeCancelled), run.Outcome)
	assert.True(t, run.Superseded, "the late finalize was fenced, so it fired nothing")
}

// TestACooperativeCancelFiresItsParentsBarrierOnce proves the cooperative cancel
// keeps the barrier's contract: the cancel fires it, and the stopped worker's
// late finalize enqueues no second continuation.
func TestACooperativeCancelFiresItsParentsBarrierOnce(t *testing.T) {
	t.Parallel()
	cooperativeCancelBarrierSuite(t, newWALFileDB(t))
}
