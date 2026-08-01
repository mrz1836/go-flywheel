package core

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// --- fixtures ---------------------------------------------------------------

// leaseArgs is the args for a worker that waits to be released.
type leaseArgs struct{ V string }

func (leaseArgs) Kind() string { return "test.block" }

// leaseWorker blocks until released, so a test can hold an attempt open
// across several lease renewals and several sweeps.
//
// It counts entries rather than completions: the assertion these tests care
// about is "the body ran once", and a second entry is the double execution the
// heartbeat exists to prevent — it must be visible even if neither entry ever
// returns.
type leaseWorker struct {
	release chan struct{}
	entries atomic.Int64
	panics  bool
	hang    bool
}

func (*leaseWorker) Kind() string { return "test.block" }

func (w *leaseWorker) Work(ctx context.Context, _ *Job[leaseArgs]) (Result, error) {
	w.entries.Add(1)
	if w.panics {
		panic("blocking worker panicked")
	}
	if w.hang {
		// Ignore the release and run until the execution timeout cancels ctx.
		<-ctx.Done()
		return Result{}, ctx.Err()
	}
	select {
	case <-w.release:
	case <-ctx.Done():
		return Result{}, ctx.Err()
	}
	return Result{}, nil
}

// countingRenewDriver counts RenewLease calls, so a test can assert that
// renewal stopped rather than that it merely slowed down.
type countingRenewDriver struct {
	Driver
	renewals atomic.Int64
}

func (d *countingRenewDriver) RenewLease(
	ctx context.Context, jobID, leaseToken string, until time.Time,
) (bool, error) {
	d.renewals.Add(1)
	return d.Driver.RenewLease(ctx, jobID, leaseToken, until)
}

// heartbeatRunner builds a SQLite runner with a short lease and a fast
// heartbeat.
//
// The plan's acceptance is written at a 5 s lease and a 30 s worker; this runs
// the same shapes three orders of magnitude faster so the suite stays fast. The
// only property the scaling relies on is that the interval divides the lease
// several times over, which it does.
func heartbeatRunner(t *testing.T, db *gorm.DB, driver Driver, reg *Registry, tune func(*RunnerConfig)) *Runner {
	t.Helper()
	cfg := RunnerConfig{
		DB: db, Driver: driver, Registry: reg,
		Queues: []string{"default"}, ClaimAnyClass: true, Concurrency: 1,
		LeaseDuration:     150 * time.Millisecond,
		HeartbeatInterval: 15 * time.Millisecond,
		PollInterval:      5 * time.Millisecond,
		Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	if tune != nil {
		tune(&cfg)
	}
	r, err := NewRunner(cfg)
	require.NoError(t, err)
	return r
}

// runSweeps sweeps expired leases on a tight ticker until the test ends, which
// is what turns "the lease was renewed" into "the job was never reclaimed".
func runSweeps(t *testing.T, ctx context.Context, driver Driver) { //nolint:revive // ctx-after-t matches the package's test helpers
	t.Helper()
	var wg sync.WaitGroup
	wg.Go(func() {
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				_, _ = driver.Sweep(ctx, time.Now())
			}
		}
	})
	t.Cleanup(wg.Wait)
}

// --- A1: the heartbeat prevents reclaim -------------------------------------

// TestLeaseHeartbeatPreventsReclaim is A1. A worker that outlives its lease
// several times over, with a sweeper running throughout, is not reclaimed and
// is not re-dispatched: its lease advances under it, and its body runs exactly
// once.
//
// Without renewal this is the library's default configuration plus a job that
// takes longer than 30 seconds — the failure is not exotic, and this test is
// the same shape scaled down.
func TestLeaseHeartbeatPreventsReclaim(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	driver := NewSQLiteDriver(db)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	worker := &leaseWorker{release: make(chan struct{})}
	reg := NewRegistry()
	Register(reg, worker)
	r := heartbeatRunner(t, db, driver, reg, nil)

	id, err := Insert(ctx, NewClient(db), leaseArgs{V: "x"}, InsertOpts{})
	require.NoError(t, err)

	runSweeps(t, ctx, driver)

	drained := make(chan error, 1)
	go func() { drained <- r.RunUntilIdle(ctx) }()

	// Watch the lease move. Three distinct expiries means the heartbeat ran at
	// least three times, which is more than the lease's own lifetime — so the
	// job survived a window in which a fixed lease would have expired twice.
	seen := map[time.Time]bool{}
	require.Eventually(t, func() bool {
		if until := leasedUntil(t, db, id); until != nil {
			seen[*until] = true
		}
		return len(seen) >= 3
	}, 5*time.Second, 5*time.Millisecond, "leased_until must advance while the worker runs")

	assert.Equal(t, string(StateRunning), jobState(t, db, id), "the job was never reclaimed")

	close(worker.release)
	select {
	case err := <-drained:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("the drain did not finish after the worker was released")
	}

	assert.Equal(t, string(StateSucceeded), jobState(t, db, id))
	assert.EqualValues(t, 1, worker.entries.Load(), "the worker body ran exactly once")
	assert.EqualValues(t, 1, runCount(t, db, id), "and was audited exactly once")
}

// --- A2: renewal stops when the attempt ends --------------------------------

// TestLeaseHeartbeatStopsWhenTheAttemptEnds is A2. Renewal must stop on every
// exit path from the attempt, not just the tidy one — a heartbeat that outlived
// its worker would hold a lease against a job nobody is running.
//
// The three paths are asserted the same way: let the attempt finish, snapshot
// the renewal count, wait several intervals, and require the count has not
// moved. A count frozen across ten intervals is renewal having stopped, not
// renewal being slow.
func TestLeaseHeartbeatStopsWhenTheAttemptEnds(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name   string
		worker *leaseWorker
		tune   func(*RunnerConfig)
	}{
		{name: "returns", worker: &leaseWorker{release: closedChan()}},
		{name: "panics", worker: &leaseWorker{release: make(chan struct{}), panics: true}},
		{
			name:   "times out",
			worker: &leaseWorker{release: make(chan struct{}), hang: true},
			tune:   func(c *RunnerConfig) { c.DefaultTimeout = 30 * time.Millisecond },
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			db := newDB(t)
			driver := &countingRenewDriver{Driver: NewSQLiteDriver(db)}
			ctx := context.Background()

			reg := NewRegistry()
			Register(reg, tc.worker)
			r := heartbeatRunner(t, db, driver, reg, tc.tune)

			_, err := Insert(ctx, NewClient(db), leaseArgs{V: "x"}, InsertOpts{})
			require.NoError(t, err)

			// One poll: claim, dispatch, finish. The attempt is over by the time
			// this returns, heartbeat included — stop waits for the goroutine.
			_, err = r.pollOnce(ctx)
			require.NoError(t, err)
			require.EqualValues(t, 1, tc.worker.entries.Load(), "the worker ran")

			settled := driver.renewals.Load()
			time.Sleep(10 * 15 * time.Millisecond)
			assert.Equal(t, settled, driver.renewals.Load(),
				"renewal stopped with the attempt: no further renewals after it ended")
		})
	}
}

// closedChan returns an already-closed channel, so a leaseWorker returns
// immediately instead of waiting to be released.
func closedChan() chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}

// --- A3: a lost claim stops renewal -----------------------------------------

// TestLeaseHeartbeatStopsOnClaimLoss is A3. A job forcibly reclaimed mid-run
// leaves its original attempt holding a token that matches nothing: the next
// renewal reports the claim lost, renewal stops, and — the half that matters —
// the stale attempt cannot extend the lease the *new* claim now holds.
//
// That last clause is why renewal is fenced at all. An unfenced heartbeat would
// have the superseded attempt holding the new attempt's job open for as long as
// it kept ticking.
func TestLeaseHeartbeatStopsOnClaimLoss(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	driver := &countingRenewDriver{Driver: NewSQLiteDriver(db)}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	worker := &leaseWorker{release: make(chan struct{})}
	reg := NewRegistry()
	Register(reg, worker)
	r := heartbeatRunner(t, db, driver, reg, nil)

	id, err := Insert(ctx, NewClient(db), leaseArgs{V: "x"}, InsertOpts{})
	require.NoError(t, err)

	dispatched := make(chan struct{})
	go func() {
		defer close(dispatched)
		_, _ = r.pollOnce(ctx)
	}()

	// Wait for the claim, then take it away the way a sweep does.
	require.Eventually(t, func() bool { return worker.entries.Load() == 1 },
		5*time.Second, 5*time.Millisecond, "the worker must be running before its claim is revoked")
	require.NoError(t, RetryJob(ctx, db, id))

	// A fresh claim, under a fresh token, with its own lease.
	stolen, err := driver.Dequeue(ctx, []string{"default"}, AnyClass, true, 1, time.Hour)
	require.NoError(t, err)
	require.Len(t, stolen, 1)
	newExpiry := leasedUntil(t, db, id)
	require.NotNil(t, newExpiry)

	// The old attempt's heartbeat notices and stops.
	var settled int64
	require.Eventually(t, func() bool {
		n := driver.renewals.Load()
		if n == settled {
			return settled > 0
		}
		settled = n
		return false
	}, 5*time.Second, 20*time.Millisecond, "renewal must stop once the claim is lost")

	close(worker.release)
	<-dispatched

	still := leasedUntil(t, db, id)
	require.NotNil(t, still)
	assert.WithinDuration(t, *newExpiry, *still, time.Second,
		"the superseded attempt did not extend the lease the new claim holds")
	assert.Equal(t, string(StateRunning), jobState(t, db, id),
		"nor did its finalize advance the job out from under that claim")
}

// --- A9: OnLeaseRenewed ------------------------------------------------------

// TestLeaseHeartbeatCallsOnLeaseRenewed is A9. The callback fires once per
// successful renewal, carrying the expiry the lease was actually extended to,
// and an error from it is logged without stopping renewal.
//
// The no-stop half is the load-bearing one: the lease is already extended by the
// time the callback runs, so a callback error cannot un-extend it — and
// stopping renewal over a failure in the host's own bookkeeping would strand a
// job whose worker is still running.
func TestLeaseHeartbeatCallsOnLeaseRenewed(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	driver := NewSQLiteDriver(db)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	worker := &leaseWorker{release: make(chan struct{})}
	reg := NewRegistry()
	Register(reg, worker)

	var mu sync.Mutex
	var renewals []LeaseRenewal
	r := heartbeatRunner(t, db, driver, reg, func(c *RunnerConfig) {
		c.OnLeaseRenewed = func(_ context.Context, renewal LeaseRenewal) error {
			mu.Lock()
			defer mu.Unlock()
			renewals = append(renewals, renewal)
			return errors.New("the host's own bookkeeping failed")
		}
	})

	id, err := Insert(ctx, NewClient(db), leaseArgs{V: "x"}, InsertOpts{})
	require.NoError(t, err)

	dispatched := make(chan struct{})
	go func() {
		defer close(dispatched)
		_, _ = r.pollOnce(ctx)
	}()

	// A returning error does not stop renewal: the callback keeps being called.
	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(renewals) >= 3
	}, 5*time.Second, 5*time.Millisecond, "a callback error must not stop renewal")

	close(worker.release)
	<-dispatched

	mu.Lock()
	defer mu.Unlock()
	for i, renewal := range renewals {
		assert.Equal(t, id, renewal.JobID)
		assert.Equal(t, "test.block", renewal.Kind)
		assert.Equal(t, 1, renewal.Attempt)
		assert.NotEmpty(t, renewal.RunID)
		assert.NotEmpty(t, renewal.LeaseToken)
		assert.Equal(t, renewal.RenewedAt.Add(150*time.Millisecond), renewal.ExpiresAt,
			"renewal %d reports the expiry the lease was extended to", i)
	}
}

// --- interval resolution -----------------------------------------------------

// TestHeartbeatIntervalResolution covers the three cases of the cadence rule,
// including the floor's documented consequence at a very short lease.
func TestHeartbeatIntervalResolution(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name          string
		lease, config time.Duration
		want          time.Duration
	}{
		{"derived from the lease", 30 * time.Second, 0, 10 * time.Second},
		{"explicit beats derived", 30 * time.Second, 2 * time.Second, 2 * time.Second},
		{"explicit is not floored", 30 * time.Second, 20 * time.Millisecond, 20 * time.Millisecond},
		{"negative disables", 30 * time.Second, -1, 0},
		{"derived is floored at a second", 900 * time.Millisecond, 0, time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			r := &Runner{cfg: RunnerConfig{LeaseDuration: tc.lease, HeartbeatInterval: tc.config}}
			assert.Equal(t, tc.want, r.heartbeatInterval())
		})
	}
}

// --- SQLite parity ------------------------------------------------------------

// TestLeaseHeartbeatOnSingleConnectionSQLite is the SQLite-parity check the
// heartbeat most needs, and it is a deadlock test rather than a feature test.
//
// The heartbeat is a second goroutine issuing writes while a worker runs, and on
// a one-connection pool every write serializes through the same connection the
// dispatch path uses. If renewal ever held that connection across the worker
// body — or waited on something the worker could only release after renewing —
// this configuration would hang rather than fail, so it is asserted here
// explicitly instead of being assumed from the shared-cache fixture the rest of
// the package uses.
func TestLeaseHeartbeatOnSingleConnectionSQLite(t *testing.T) {
	t.Parallel()
	db := newSingleConnMemoryDB(t)
	driver := &countingRenewDriver{Driver: NewSQLiteDriver(db)}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	worker := &leaseWorker{release: make(chan struct{})}
	reg := NewRegistry()
	Register(reg, worker)
	r := heartbeatRunner(t, db, driver, reg, nil)

	id, err := Insert(ctx, NewClient(db), leaseArgs{V: "x"}, InsertOpts{})
	require.NoError(t, err)

	dispatched := make(chan struct{})
	go func() {
		defer close(dispatched)
		_, _ = r.pollOnce(ctx)
	}()

	require.Eventually(t, func() bool { return driver.renewals.Load() >= 3 },
		5*time.Second, 5*time.Millisecond,
		"the heartbeat must renew against the same single connection the worker's dispatch uses")

	close(worker.release)
	select {
	case <-dispatched:
	case <-time.After(5 * time.Second):
		t.Fatal("dispatch did not return: the heartbeat and the worker deadlocked on the single connection")
	}
	assert.Equal(t, string(StateSucceeded), jobState(t, db, id))
}

// TestNewRunnerStillRejectsSQLiteConcurrency guards the constraint every plan
// touching the runner must preserve: the SQLite claim is a serialized
// SELECT-then-UPDATE with no SKIP LOCKED, so it is correct only at Concurrency
// 1. The heartbeat adds goroutines to the dispatch path and must not have
// loosened it.
func TestNewRunnerStillRejectsSQLiteConcurrency(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	_, err := NewRunner(RunnerConfig{
		DB: db, Driver: NewSQLiteDriver(db), Registry: NewRegistry(),
		Queues: []string{"default"}, Concurrency: 2,
	})
	require.ErrorIs(t, err, ErrSQLiteConcurrency)
}

// TestLeaseHeartbeatDisabledMakesNoRenewals proves the escape hatch actually
// escapes: a negative interval restores the fixed-lease behavior, with no
// renewal traffic at all.
func TestLeaseHeartbeatDisabledMakesNoRenewals(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	driver := &countingRenewDriver{Driver: NewSQLiteDriver(db)}
	ctx := context.Background()

	worker := &leaseWorker{release: closedChan()}
	reg := NewRegistry()
	Register(reg, worker)
	r := heartbeatRunner(t, db, driver, reg, func(c *RunnerConfig) { c.HeartbeatInterval = -1 })

	_, err := Insert(ctx, NewClient(db), leaseArgs{V: "x"}, InsertOpts{})
	require.NoError(t, err)
	_, err = r.pollOnce(ctx)
	require.NoError(t, err)

	assert.Zero(t, driver.renewals.Load(), "a disabled heartbeat issues no renewals")
}
