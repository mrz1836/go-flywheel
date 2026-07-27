//go:build integration

package flywheel

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

// pgPoolWorker sleeps for one of two durations and accumulates the time it spent
// executing, so a run can be turned into a slot-utilization number.
type pgPoolWorker struct {
	fast, slow time.Duration

	busyNanos atomic.Int64
	current   atomic.Int32
	peak      atomic.Int32
	done      atomic.Int32
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

	d := w.fast
	if job.Args.Slow {
		d = w.slow
	}
	started := time.Now()
	time.Sleep(d)
	w.busyNanos.Add(time.Since(started).Nanoseconds())

	w.current.Add(-1)
	w.done.Add(1)
	return Result{}, nil
}

// seedPoolJobs enqueues n jobs of which every stride-th is slow, and returns how
// many slow ones it wrote.
func seedPoolJobs(t *testing.T, db *gorm.DB, n, stride int) int {
	t.Helper()
	ctx := context.Background()
	client := NewClient(db)
	slow := 0
	for i := range n {
		isSlow := stride > 0 && i%stride == 0
		if isSlow {
			slow++
		}
		_, err := Insert(ctx, client, pgPoolArgs{Slow: isSlow}, InsertOpts{})
		require.NoError(t, err)
	}
	return slow
}

// TestRunnerPGSlotUtilizationUnderMixedSpeed is A1 against real SKIP LOCKED: with
// one job in ten running twenty times longer than the rest, the pool must keep its
// slots busy rather than idling them behind a straggler.
//
// It is the barrier stated as a number. Under the old semantics the loop claimed
// eight, waited for the slowest of the eight, and claimed eight more — so a batch
// containing a 20× job left seven slots idle for the whole of it, and utilization
// could not exceed roughly 1/8 on the batches that contained one. Measured as
// worker-busy time over slots × wall time, that ceiling is what this asserts is
// gone.
func TestRunnerPGSlotUtilizationUnderMixedSpeed(t *testing.T) {
	db := NewPostgresIsolatedDB(t)
	const (
		concurrency = 8
		jobs        = 240
		stride      = 10 // one job in ten is slow
		fast        = 5 * time.Millisecond
		slow        = 20 * fast
	)

	w := &pgPoolWorker{fast: fast, slow: slow}
	reg := NewRegistry()
	Register(reg, w)
	slowJobs := seedPoolJobs(t, db, jobs, stride)

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

	start := time.Now()
	require.NoError(t, r.RunUntilIdle(context.Background()))
	elapsed := time.Since(start)

	require.EqualValues(t, jobs, w.done.Load(), "every job ran exactly once")

	// Slot utilization: worker-busy time over the capacity the run had available.
	// It understates true occupancy — the stub, finalize, and claim windows are
	// capacity the pool was using and this does not count — so it is a floor.
	capacity := float64(concurrency) * float64(elapsed.Nanoseconds())
	utilization := float64(w.busyNanos.Load()) / capacity
	t.Logf("slot utilization %.1f%% over %s (%d jobs, %d slow, peak in flight %d)",
		utilization*100, elapsed, jobs, slowJobs, w.peak.Load())

	// The serial lower bound: the barrier could not have beaten this.
	assert.Greater(t, utilization, 0.5,
		"the pool kept its slots busy through the stragglers rather than idling behind them")
	assert.LessOrEqual(t, w.peak.Load(), int32(concurrency),
		"and never exceeded its bound while doing it")
	assert.Greater(t, w.peak.Load(), int32(1), "the run was genuinely concurrent")
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
	seedPoolJobs(t, db, jobs, 0)

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
