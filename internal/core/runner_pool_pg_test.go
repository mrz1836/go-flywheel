//go:build integration

package core

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mrz1836/go-foundation/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

type pgPoolArgs struct{ Slow bool }

func (pgPoolArgs) Kind() string { return "pg.pool" }

// pgPoolWorker runs fast jobs quickly and holds the straggler open until the test
// releases it, tracking concurrency and completions.
type pgPoolWorker struct {
	fast time.Duration
	// stragglerIn closes when the straggler enters Work; release lets it finish.
	stragglerIn chan struct{}
	release     chan struct{}

	current atomic.Int32
	peak    atomic.Int32
	// fastDone counts completed fast jobs. The straggler is excluded so the count
	// is a clean "everything else drained around it".
	fastDone atomic.Int32
	// stragglerDone records that the straggler's Work returned. It is the
	// deterministic form of "it is still holding its slot": InFlight can read 2 for
	// the moment the last fast job spends between its worker returning and its
	// finalize committing.
	stragglerDone atomic.Bool
}

func (*pgPoolWorker) Kind() string { return "pg.pool" }

func (w *pgPoolWorker) Work(_ context.Context, job *Job[pgPoolArgs]) (Result, error) {
	cur := w.current.Add(1)
	for {
		peak := w.peak.Load()
		if cur <= peak || w.peak.CompareAndSwap(peak, cur) {
			break
		}
	}
	defer w.current.Add(-1)

	if job.Args.Slow {
		close(w.stragglerIn)
		<-w.release
		w.stragglerDone.Store(true)
		return Result{}, nil
	}
	time.Sleep(w.fast)
	w.fastDone.Add(1)
	return Result{}, nil
}

// seedStraggler enqueues the one job that will not finish until released, at a
// priority that puts it at the head of the claim order.
//
// Ordering is load-bearing. The claim is ORDER BY priority, scheduled_at, so a
// lower priority number is claimed first — and the straggler has to land in the
// *first* batch for this to test anything. If it were claimed last, a
// batch-and-barrier loop would have already finished every other job and the
// assertion would pass against the very behavior it exists to reject.
func seedStraggler(t *testing.T, db *gorm.DB) {
	t.Helper()
	_, err := Insert(context.Background(), NewClient(db), pgPoolArgs{Slow: true},
		InsertOpts{Priority: -1})
	require.NoError(t, err)
}

// seedFastJobs enqueues n jobs that finish on their own.
func seedFastJobs(t *testing.T, db *gorm.DB, n int) {
	t.Helper()
	ctx := context.Background()
	client := NewClient(db)
	for range n {
		_, err := Insert(ctx, client, pgPoolArgs{}, InsertOpts{})
		require.NoError(t, err)
	}
}

// TestRunnerPGKeepsClaimingAroundAStragglerPostgres is A1 against real SKIP
// LOCKED: one job that will not finish must not stop the others.
//
// It asserts the property rather than a percentage, and that choice came from a
// failure. The first version measured slot utilization — worker-busy time over
// slots × wall time — and required it to clear 50 %. That passed at 76 % on a
// developer laptop and failed at 27.7 % on CI, where a containerized PostgreSQL
// makes each job's stub and finalize round trips large relative to a 5 ms sleep.
// The pool was behaving identically in both; the threshold was measuring the
// runner's hardware. Absolute utilization is published in docs/BENCHMARKS.md from
// a deliberate run on a named machine, which is the only place a number like that
// means anything.
//
// What is left is hardware-independent and still rejects the batch-and-barrier
// loop outright. The straggler is claimed in the first batch, so a loop that
// waited for its whole batch before claiming again would stop there: at
// Concurrency 4 it would finish at most three more jobs, never forty.
func TestRunnerPGKeepsClaimingAroundAStragglerPostgres(t *testing.T) {
	db := NewPostgresIsolatedDB(t)
	const (
		concurrency = 4
		fastJobs    = 40
	)

	release := make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })

	w := &pgPoolWorker{
		fast:        time.Millisecond,
		stragglerIn: make(chan struct{}),
		release:     release,
	}
	reg := NewRegistry()
	Register(reg, w)

	seedStraggler(t, db)
	seedFastJobs(t, db, fastJobs)

	r, err := NewRunner(RunnerConfig{
		DB:            db,
		Driver:        NewPostgresDriver(db),
		Registry:      reg,
		Queues:        []string{"default"},
		ClaimAnyClass: true,
		Concurrency:   concurrency,
		PollInterval:  2 * time.Millisecond,
		LeaseDuration: time.Minute,
	})
	require.NoError(t, err)

	runCtx, cancelRun := context.WithCancel(context.Background())
	defer cancelRun()
	runErr := make(chan error, 1)
	go func() { runErr <- r.Run(runCtx) }()

	// The straggler is confirmed in flight before anything is asserted about the
	// rest, so "they drained around it" cannot be satisfied by it having finished.
	<-w.stragglerIn

	require.Eventually(t, func() bool { return w.fastDone.Load() == fastJobs },
		60*time.Second, 10*time.Millisecond,
		"every other job drained while the straggler held its slot")

	assert.False(t, w.stragglerDone.Load(), "the straggler is still holding its slot")
	require.Eventually(t, func() bool { return r.InFlight() == 1 },
		10*time.Second, 10*time.Millisecond,
		"exactly the straggler remains in flight once the last fast job finalizes")
	assert.LessOrEqual(t, w.peak.Load(), int32(concurrency), "the pool bound held throughout")
	assert.Greater(t, w.peak.Load(), int32(1), "and the run was genuinely concurrent")

	releaseOnce.Do(func() { close(release) })
	require.NoError(t, r.Drain(context.Background()), "the straggler finishes on the drain")

	var unfinished int64
	require.NoError(t, db.Model(&jobRow{}).
		Where("state <> ?", string(StateSucceeded)).Count(&unfinished).Error)
	assert.Zero(t, unfinished, "every job reached a terminal success")

	cancelRun()
	<-runErr
}

// TestRunnerPGDrainStrandsNothingTheSweepCannotReclaim is A7's durability half
// against the real claim: when a drain times out, the jobs it abandoned are left
// in exactly the shape the lease sweep recovers.
//
// The point is that a timed-out drain is not a lost job. It is the same row shape
// a process kill leaves — state running, a held lease, an audit row reading
// started — and the sweep is what returns it to available.
func TestRunnerPGDrainStrandsNothingTheSweepCannotReclaim(t *testing.T) {
	db := NewPostgresIsolatedDB(t)
	const (
		concurrency = 4
		jobs        = 3
	)

	started := make(chan struct{}, jobs)
	release := make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })

	blocked := &pgBlockedWorker{started: started, release: release}
	reg := NewRegistry()
	Register(reg, blocked)
	seedFastJobs(t, db, jobs)

	driver := NewPostgresDriver(db)
	r, err := NewRunner(RunnerConfig{
		DB:            db,
		Driver:        driver,
		Registry:      reg,
		Queues:        []string{"default"},
		ClaimAnyClass: true,
		Concurrency:   concurrency,
		PollInterval:  2 * time.Millisecond,
		// Short enough that the sweep below has something expired to find, and the
		// heartbeat is off so nothing renews it out from under the sweep.
		LeaseDuration:     500 * time.Millisecond,
		HeartbeatInterval: -1,
	})
	require.NoError(t, err)

	runErr := make(chan error, 1)
	go func() { runErr <- r.Run(context.Background()) }()
	for range jobs {
		<-started
	}

	drainCtx, cancelDrain := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancelDrain()
	drainErr := r.Drain(drainCtx)

	require.ErrorIs(t, drainErr, context.DeadlineExceeded)
	var timeout *DrainTimeoutError
	require.ErrorAs(t, drainErr, &timeout)
	assert.Equal(t, jobs, timeout.InFlight, "the drain named every job it abandoned")

	// The abandoned jobs are running with a lease held — the shape a kill leaves.
	var running int64
	require.NoError(t, db.Model(&jobRow{}).
		Where("state = ? AND lease_token IS NOT NULL", string(StateRunning)).
		Count(&running).Error)
	assert.EqualValues(t, jobs, running, "every abandoned job is claimed and leased, not lost")

	// Once the leases expire, the sweep reclaims all of them.
	require.Eventually(t, func() bool {
		reclaimed, sweepErr := driver.Sweep(context.Background(), models.ClockFrom(context.Background()).Now(context.Background()))
		require.NoError(t, sweepErr)
		return reclaimed == jobs
	}, 5*time.Second, 50*time.Millisecond, "the lease sweep reclaimed every abandoned job")

	var available int64
	require.NoError(t, db.Model(&jobRow{}).
		Where("state = ? AND lease_token IS NULL", string(StateAvailable)).Count(&available).Error)
	assert.EqualValues(t, jobs, available,
		"and returned them for another attempt with their stale tokens cleared")

	releaseOnce.Do(func() { close(release) })
	require.NoError(t, <-runErr, "Run returns nil for a requested stop")
}

// pgBlockedWorker holds every job until released, announcing each entry.
type pgBlockedWorker struct {
	started chan<- struct{}
	release <-chan struct{}
}

func (*pgBlockedWorker) Kind() string { return "pg.pool" }

func (w *pgBlockedWorker) Work(context.Context, *Job[pgPoolArgs]) (Result, error) {
	w.started <- struct{}{}
	<-w.release
	return Result{}, nil
}
