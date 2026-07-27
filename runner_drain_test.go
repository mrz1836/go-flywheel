package flywheel

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// blockingClaimDriver holds its first Dequeue open until released, so a test can
// land Stop while a claim is in flight — the one window in which Stop cannot
// prevent a job being leased.
type blockingClaimDriver struct {
	*poolDriver
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (d *blockingClaimDriver) Dequeue(
	ctx context.Context, queues []string, class ExecutorClass,
	claimAny bool, limit int, lease time.Duration,
) ([]RawJob, error) {
	d.once.Do(func() {
		close(d.entered)
		<-d.release
	})
	return d.poolDriver.Dequeue(ctx, queues, class, claimAny, limit, lease)
}

// heldWorker blocks every job until released and announces each entry, so a test
// can put an exact number of jobs in flight and hold them there.
func heldWorker(started chan<- struct{}, release <-chan struct{}) *peakWorker {
	return &peakWorker{hold: func(context.Context, int) {
		started <- struct{}{}
		<-release
	}}
}

// --- Stop -------------------------------------------------------------------

// TestStopIsNonBlockingAndHaltsFurtherClaims covers both halves of Stop's
// contract: it returns at once, and the loop stops claiming.
func TestStopIsNonBlockingAndHaltsFurtherClaims(t *testing.T) {
	t.Parallel()
	d := newPoolDriver(0) // nothing to serve; the loop polls an empty queue
	w := &peakWorker{}
	r := newPoolRunner(t, d, w, 4, 0)

	runErr := make(chan error, 1)
	go func() { runErr <- r.Run(context.Background()) }()

	require.Eventually(t, func() bool { return len(d.claimLimits()) > 2 },
		5*time.Second, time.Millisecond, "the loop is polling")

	start := time.Now()
	r.Stop()
	assert.Less(t, time.Since(start), 100*time.Millisecond, "Stop does not block")

	select {
	case err := <-runErr:
		require.NoError(t, err, "a requested stop is how Run is meant to end")
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after Stop")
	}

	claims := len(d.claimLimits())
	time.Sleep(50 * time.Millisecond)
	assert.Equal(t, claims, len(d.claimLimits()), "no claim is issued after the loop returns")
}

// TestStopIsIdempotentAndSafeBeforeRun proves Stop and Drain are total: callable
// repeatedly, and callable before the loop ever runs.
func TestStopIsIdempotentAndSafeBeforeRun(t *testing.T) {
	t.Parallel()
	d := newPoolDriver(3)
	w := &peakWorker{}
	r := newPoolRunner(t, d, w, 4, 0)

	r.Stop()
	r.Stop()
	r.Stop()
	assert.Zero(t, r.InFlight())
	require.NoError(t, r.Drain(context.Background()), "draining an empty pool is immediate")

	require.NoError(t, r.Run(context.Background()), "Run on a stopped Runner returns at once")
	require.ErrorIs(t, r.RunUntilIdle(context.Background()), ErrRunnerStopped,
		"RunUntilIdle reports that it never drained the queue")

	assert.Empty(t, d.claimLimits(), "a stopped Runner claims nothing at all")
	assert.Zero(t, w.done.Load(), "and runs nothing")
}

// TestStopDispatchesAClaimAlreadyInFlight pins the boundary Stop draws. It bounds
// when the next claim is *issued*, not what happens to one already in flight: a
// batch that came back from Dequeue after Stop landed is already leased, and
// stranding it would leave the job invisible until the lease sweep reclaimed it.
func TestStopDispatchesAClaimAlreadyInFlight(t *testing.T) {
	t.Parallel()
	d := &blockingClaimDriver{
		poolDriver: newPoolDriver(1),
		entered:    make(chan struct{}),
		release:    make(chan struct{}),
	}
	w := &peakWorker{}
	r := newPoolRunner(t, d, w, 2, 0)

	runErr := make(chan error, 1)
	go func() { runErr <- r.Run(context.Background()) }()

	<-d.entered // a claim is in flight
	r.Stop()
	close(d.release) // it returns a leased job

	select {
	case err := <-runErr:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after Stop")
	}
	assert.EqualValues(t, 1, w.done.Load(),
		"the already-leased job ran rather than being stranded for the sweep")
}

// --- Drain ------------------------------------------------------------------

// TestDrainWaitsForInFlightJobsThenReturnsNil is A7's happy half: three jobs in
// flight and a deadline longer than they need.
func TestDrainWaitsForInFlightJobsThenReturnsNil(t *testing.T) {
	t.Parallel()
	const jobs = 3

	started := make(chan struct{}, jobs)
	release := make(chan struct{})
	w := heldWorker(started, release)
	d := newPoolDriver(jobs)
	r := newPoolRunner(t, d, w, 4, 0)

	runErr := make(chan error, 1)
	go func() { runErr <- r.Run(context.Background()) }()
	for range jobs {
		<-started
	}
	claimsBefore := len(d.claimLimits())

	drainErr := make(chan error, 1)
	go func() { drainErr <- r.Drain(context.Background()) }()

	select {
	case err := <-drainErr:
		t.Fatalf("Drain returned while %d jobs were in flight: %v", jobs, err)
	case <-time.After(100 * time.Millisecond):
	}

	close(release)
	require.NoError(t, <-drainErr, "Drain returns nil once the pool is empty")
	assert.EqualValues(t, jobs, w.done.Load(), "every job finalized")
	assert.Zero(t, r.InFlight())
	// One claim may have been in flight when Stop landed; none after that.
	assert.LessOrEqual(t, len(d.claimLimits()), claimsBefore+1, "no further claim was made")

	require.NoError(t, <-runErr)
}

// TestDrainReportsTheTimeoutAndTheInFlightCount is A7's timeout half: the count
// is what turns a warning into a diagnostic, and it is taken at the instant the
// deadline arrived rather than re-read afterwards.
func TestDrainReportsTheTimeoutAndTheInFlightCount(t *testing.T) {
	t.Parallel()
	const jobs = 2

	started := make(chan struct{}, jobs)
	release := make(chan struct{})
	w := heldWorker(started, release)
	d := newPoolDriver(jobs)
	r := newPoolRunner(t, d, w, 4, 0)

	runErr := make(chan error, 1)
	go func() { runErr <- r.Run(context.Background()) }()
	for range jobs {
		<-started
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	err := r.Drain(ctx)

	require.ErrorIs(t, err, context.DeadlineExceeded, "the deadline error stays reachable")
	var timeout *DrainTimeoutError
	require.ErrorAs(t, err, &timeout)
	assert.Equal(t, jobs, timeout.InFlight, "the error names how many jobs were still running")
	assert.Contains(t, err.Error(), "2 jobs in flight")

	close(release)
	require.NoError(t, <-runErr)
}

// TestDrainConcurrentWithRunIsRaceFree hammers the drain surface from several
// goroutines while the loop claims and dispatches. Its assertion is the -race
// build: Stop, Drain, and InFlight are documented as safe before, during, and
// after Run, and that is only true if they share no unsynchronized state with it.
func TestDrainConcurrentWithRunIsRaceFree(t *testing.T) {
	t.Parallel()
	w := &peakWorker{hold: func(context.Context, int) { time.Sleep(time.Millisecond) }}
	d := newPoolDriver(200)
	r := newPoolRunner(t, d, w, 4, 0)

	runErr := make(chan error, 1)
	go func() { runErr <- r.Run(context.Background()) }()

	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for range 20 {
				_ = r.InFlight()
				time.Sleep(time.Millisecond)
			}
			r.Stop()
			_ = r.Drain(context.Background())
		})
	}
	wg.Wait()

	require.NoError(t, <-runErr)
	assert.Zero(t, r.InFlight(), "every concurrent drain agreed the pool was empty")
}

// TestRunUntilIdleReturnsErrRunnerStoppedWhenStopped proves the two entry points
// report a stop differently. Run ends nil because a requested stop is how it is
// meant to end; RunUntilIdle promised a drained queue and must say it did not
// deliver one, or a caller distinguishing a clean drain from an interrupted one
// cannot.
func TestRunUntilIdleReturnsErrRunnerStoppedWhenStopped(t *testing.T) {
	t.Parallel()
	w := &peakWorker{hold: func(context.Context, int) { time.Sleep(2 * time.Millisecond) }}
	d := newPoolDriver(500)
	r := newPoolRunner(t, d, w, 2, 0)

	idleErr := make(chan error, 1)
	go func() { idleErr <- r.RunUntilIdle(context.Background()) }()

	require.Eventually(t, func() bool { return w.done.Load() > 0 },
		5*time.Second, time.Millisecond, "the drain is underway")
	r.Stop()

	select {
	case err := <-idleErr:
		require.ErrorIs(t, err, ErrRunnerStopped)
	case <-time.After(5 * time.Second):
		t.Fatal("RunUntilIdle did not return after Stop")
	}
	assert.Zero(t, r.InFlight(), "it still waited for its own in-flight jobs")
}

// TestStopDoesNotCancelInFlightWork proves Stop is not an abort: a worker that
// watches its own context is not disturbed by it. That is what makes "no new
// claims, then wait" a contract a rolling deploy can rely on.
func TestStopDoesNotCancelInFlightWork(t *testing.T) {
	t.Parallel()
	started := make(chan struct{})
	release := make(chan struct{})
	var sawCancel, ranToCompletion bool
	var mu sync.Mutex

	w := &peakWorker{hold: func(ctx context.Context, _ int) {
		close(started)
		select {
		case <-release:
			mu.Lock()
			ranToCompletion = true
			mu.Unlock()
		case <-ctx.Done():
			mu.Lock()
			sawCancel = true
			mu.Unlock()
		}
	}}
	d := newPoolDriver(1)
	r := newPoolRunner(t, d, w, 2, 0)

	runErr := make(chan error, 1)
	go func() { runErr <- r.Run(context.Background()) }()
	<-started

	r.Stop()
	time.Sleep(50 * time.Millisecond) // give a cancel, if there were one, time to land

	mu.Lock()
	assert.False(t, sawCancel, "Stop must not reach the worker's context")
	mu.Unlock()

	close(release)
	require.NoError(t, <-runErr)

	mu.Lock()
	defer mu.Unlock()
	assert.True(t, ranToCompletion, "the worker finished its own way")
}
