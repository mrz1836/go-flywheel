package core

import (
	"context"
	"testing"
	"time"

	"github.com/mrz1836/go-foundation/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// sweepRecorder is an Observer that keeps every SweepEvent it is given.
type sweepRecorder struct {
	noopObserver

	events []SweepEvent
}

// OnSweep records the event.
func (r *sweepRecorder) OnSweep(_ context.Context, ev SweepEvent) { r.events = append(r.events, ev) }

// opaqueSweepDriver is a custom Driver whose Sweep reaches no built-in driver: it
// reports a count and nothing else.
type opaqueSweepDriver struct {
	Driver

	reclaimed int
}

// Sweep reports the configured count without touching the database.
func (d *opaqueSweepDriver) Sweep(context.Context, time.Time) (int, error) { return d.reclaimed, nil }

// sweepEventSuite crashes three jobs and sweeps them once through a Scheduler
// built on wrap(the dialect's driver):
//
//   - one with a single attempt, which the sweep discards;
//   - one with attempts left, which it returns to the queue;
//   - one claimed with no run stub, which it returns too, with no run to name.
//
// The leases expire in that order, so that is the reclaim order the event lists
// them in.
func sweepEventSuite(t *testing.T, db *gorm.DB, wrap func(Driver) Driver) {
	t.Helper()
	d := driverForTest(t, db)
	rec := &sweepRecorder{}
	s := newSchedulerCfg(t, SchedulerConfig{DB: db, Client: NewClient(db), Driver: wrap(d), Observer: rec})
	ctx := context.Background()

	seedCrashJob(t, db, "swept-last", "sweep-last", 1)
	seedCrashJob(t, db, "swept-left", "sweep-left", 3)
	seedCrashJob(t, db, "swept-bare", "sweep-bare", 3)

	claim := func(queue string, lease time.Duration) RawJob {
		batch, err := d.Dequeue(ctx, []string{queue}, AnyClass, true, 1, lease)
		require.NoError(t, err)
		require.Len(t, batch, 1)
		return batch[0]
	}
	last := claim("sweep-last", 30*time.Second)
	lastRun := models.NewID()
	require.NoError(t, d.InsertRunStub(ctx, lastRun, last, time.Now(), "local", "exec"))
	left := claim("sweep-left", 60*time.Second)
	leftRun := models.NewID()
	require.NoError(t, d.InsertRunStub(ctx, leftRun, left, time.Now(), "local", "exec"))
	claim("sweep-bare", 90*time.Second) // the process died before it wrote a stub

	sweepPastTheLease(t, s)

	require.Len(t, rec.events, 1, "one pass, one event")
	ev := rec.events[0]
	assert.Equal(t, 3, ev.Reclaimed)
	assert.Equal(t, 1, ev.Discarded)
	assert.Equal(t, []SweptJob{
		{
			JobEvent:    JobEvent{JobID: "swept-last", RunID: lastRun, Kind: "crash.loop", Queue: "sweep-last", Attempt: 1},
			MaxAttempts: 1, State: StateDiscarded,
		},
		{
			JobEvent:    JobEvent{JobID: "swept-left", RunID: leftRun, Kind: "crash.loop", Queue: "sweep-left", Attempt: 1},
			MaxAttempts: 3, State: StateAvailable,
		},
		{
			JobEvent:    JobEvent{JobID: "swept-bare", Kind: "crash.loop", Queue: "sweep-bare", Attempt: 1},
			MaxAttempts: 3, State: StateAvailable,
		},
	}, ev.Jobs, "one entry per reclaimed job, oldest lease first")

	// A pass with nothing to reclaim reports nothing.
	sweepPastTheLease(t, s)
	require.Len(t, rec.events, 2)
	assert.Zero(t, rec.events[1].Reclaimed)
	assert.Zero(t, rec.events[1].Discarded)
	assert.Empty(t, rec.events[1].Jobs)
}

// TestSweepEventNamesEachReclaimedJob proves the sweep reports each job it
// reclaimed, with the crashed run and the state it left the job in, through the
// built-in driver and through a Driver that wraps it and passes its context on.
func TestSweepEventNamesEachReclaimedJob(t *testing.T) {
	t.Parallel()

	t.Run("built-in driver", func(t *testing.T) {
		t.Parallel()
		sweepEventSuite(t, newDB(t), func(d Driver) Driver { return d })
	})
	t.Run("a wrapper that passes its context on", func(t *testing.T) {
		t.Parallel()
		sweepEventSuite(t, newDB(t), func(d Driver) Driver { return &countingDriver{Driver: d} })
	})
	t.Run("a custom driver reports its count alone", func(t *testing.T) {
		t.Parallel()
		db := newDB(t)
		rec := &sweepRecorder{}
		s := newSchedulerCfg(t, SchedulerConfig{
			DB: db, Client: NewClient(db), Driver: &opaqueSweepDriver{Driver: NewSQLiteDriver(db), reclaimed: 2}, Observer: rec,
		})
		n, err := s.Sweep(context.Background())
		require.NoError(t, err)
		assert.Equal(t, 2, n)
		require.Len(t, rec.events, 1)
		assert.Equal(t, 2, rec.events[0].Reclaimed)
		assert.Zero(t, rec.events[0].Discarded)
		assert.Empty(t, rec.events[0].Jobs)
	})
}
