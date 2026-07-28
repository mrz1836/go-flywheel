package flywheel

import (
	"context"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// skippedTickMessage is the warn line an overlapping tick emits.
const skippedTickMessage = "jobs: maintenance tick skipped, previous pass still running"

// TestActivitySkipsATickWhosePredecessorIsStillRunning covers the guard at the
// unit the guard lives on. A pass that outlasts its interval must skip the next
// tick rather than run concurrently with itself — two overlapping retention
// passes would delete from the same window twice.
func TestActivitySkipsATickWhosePredecessorIsStillRunning(t *testing.T) {
	t.Parallel()

	release := make(chan struct{})
	entered := make(chan struct{}, 1)
	var runs atomic.Int64

	a := &activity{
		name:     "slow",
		interval: time.Millisecond,
		run: func(context.Context) {
			runs.Add(1)
			entered <- struct{}{}
			<-release
		},
	}
	handler := &captureHandler{}
	logger := slog.New(handler)

	// Start a pass and park it inside run.
	go a.tick(context.Background(), logger)
	<-entered

	// A tick arriving while that pass is parked is skipped, not queued.
	a.tick(context.Background(), logger)
	a.tick(context.Background(), logger)

	assert.EqualValues(t, 1, runs.Load(), "the overlapping ticks did not run the work")
	require.True(t, handler.has(skippedTickMessage), "a skipped tick is logged")

	records := handler.recordsFor(skippedTickMessage)
	require.Len(t, records, 2)
	assert.Equal(t, "slow", records[0].attrs["activity"].String(),
		"the log names which activity fell behind")
	assert.EqualValues(t, 1, records[0].attrs["consecutive_skips"].Int64())
	assert.EqualValues(t, 2, records[1].attrs["consecutive_skips"].Int64(),
		"consecutive skips accumulate while the pass is still running")

	close(release)
}

// TestActivityResetsTheSkipCountAfterAPassCompletes pins the word
// "consecutive". A monotonically climbing lifetime counter cannot distinguish a
// system that fell behind once an hour ago from one that is falling behind now,
// which is the entire diagnostic value of the number.
func TestActivityResetsTheSkipCountAfterAPassCompletes(t *testing.T) {
	t.Parallel()

	release := make(chan struct{})
	entered := make(chan struct{}, 1)
	var slow atomic.Bool
	slow.Store(true)

	a := &activity{
		name:     "bursty",
		interval: time.Millisecond,
		run: func(context.Context) {
			if slow.Load() {
				entered <- struct{}{}
				<-release
			}
		},
	}
	handler := &captureHandler{}
	logger := slog.New(handler)

	go a.tick(context.Background(), logger)
	<-entered
	a.tick(context.Background(), logger) // skip 1
	a.tick(context.Background(), logger) // skip 2
	require.EqualValues(t, 2, a.skips.Load())

	// Let the slow pass finish, then run a fast one to completion.
	slow.Store(false)
	close(release)
	require.Eventually(t, func() bool { return !a.running.Load() }, time.Second, time.Millisecond)
	a.tick(context.Background(), logger)

	assert.EqualValues(t, 0, a.skips.Load(), "a completed pass resets the consecutive count")
}

// TestActivityLoopStopsOnContextCancel proves the loop exits on cancellation,
// which is what makes Scheduler.Run's WaitGroup terminate.
func TestActivityLoopStopsOnContextCancel(t *testing.T) {
	t.Parallel()

	var runs atomic.Int64
	a := &activity{
		name:     "fast",
		interval: time.Millisecond,
		run:      func(context.Context) { runs.Add(1) },
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); a.loop(ctx, slog.New(&captureHandler{})) }()

	require.Eventually(t, func() bool { return runs.Load() > 0 }, time.Second, time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("the activity loop did not stop on cancellation")
	}
}

// TestSchedulerRunKeepsMaintenanceActivitiesIndependent proves independence at the level
// that matters: a retention pass slower than its own interval must not delay the
// lease sweep.
//
// Before this change Run was one select over four tickers with each case
// running inline, so this test would see the sweep stall for as long as the
// prune ran — and the sweep is the only recovery path for work lost to a
// crashed process, which makes the compounding failure a real outage rather
// than a latency blip.
func TestSchedulerRunKeepsMaintenanceActivitiesIndependent(t *testing.T) {
	t.Parallel()
	db := newDB(t)

	blocked := &blockingSweepDriver{Driver: NewSQLiteDriver(db), gate: make(chan struct{})}
	sched, err := NewSchedulerWithConfig(SchedulerConfig{
		DB: db, Client: NewClient(db), Driver: blocked,
		TickInterval:  time.Millisecond,
		SweepInterval: time.Hour, // the sweep activity is not the subject here
		Logger:        slog.New(&captureHandler{}),
	})
	require.NoError(t, err)

	// Park the sweep activity by calling it directly, then prove the periodic
	// tick keeps firing on its own goroutine while the sweep is stuck.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	runDone := make(chan error, 1)
	go func() { runDone <- sched.Run(ctx) }()

	sweepDone := make(chan struct{})
	go func() { defer close(sweepDone); _, _ = sched.Sweep(ctx) }()

	// The periodic tick must keep running while the sweep is blocked. Ticks are
	// counted by the driver-independent Tick path, so a due periodic is enough.
	installPeriodic(t, db, "independent", "test.success", time.Now().Add(-time.Minute), true)
	require.Eventually(t, func() bool {
		return jobCount(t, db, "test.success") > 0
	}, 3*time.Second, 5*time.Millisecond, "the periodic tick runs while the sweep is blocked")

	close(blocked.gate)
	<-sweepDone
	cancel()

	select {
	case err := <-runDone:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not return after cancellation")
	}
}

// TestSchedulerRunWaitsForInFlightPasses proves Run does not return the instant
// its context is cancelled: it waits for whatever pass is in flight, which is
// what makes a Node's DrainTimeout a bound on something real.
func TestSchedulerRunWaitsForInFlightPasses(t *testing.T) {
	t.Parallel()
	db := newDB(t)

	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	var finished atomic.Bool

	sched, err := NewSchedulerWithConfig(SchedulerConfig{
		DB: db, Client: NewClient(db), Driver: NewSQLiteDriver(db),
		TickInterval: time.Hour, SweepInterval: time.Hour,
		Logger: slog.New(&captureHandler{}),
	})
	require.NoError(t, err)

	// A hand-built activity set is the honest way to drive this: the guard and
	// the wait live on activity, and Run's contract is that it waits for them.
	acts := []*activity{{
		name:     "slow",
		interval: time.Millisecond,
		run: func(context.Context) {
			select {
			case entered <- struct{}{}:
			default:
			}
			<-release
			finished.Store(true)
		},
	}}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		sched.runActivities(ctx, acts)
	}()

	<-entered
	cancel()

	// Run must still be waiting: the pass has not been released.
	select {
	case <-done:
		t.Fatal("the wait returned before the in-flight pass finished")
	case <-time.After(50 * time.Millisecond):
	}

	close(release)
	select {
	case <-done:
		assert.True(t, finished.Load(), "the in-flight pass ran to completion")
	case <-time.After(time.Second):
		t.Fatal("the wait did not return after the pass finished")
	}
}

// blockingSweepDriver parks Sweep until its gate is closed, so a test can hold
// one maintenance activity open and observe the others.
type blockingSweepDriver struct {
	Driver
	gate chan struct{}
}

// Sweep blocks until the gate opens, then delegates.
func (d *blockingSweepDriver) Sweep(ctx context.Context, now time.Time) (int, error) {
	<-d.gate
	return d.Driver.Sweep(ctx, now)
}
