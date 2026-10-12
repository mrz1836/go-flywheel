package core

import (
	"context"
	"testing"
	"time"

	"github.com/mrz1836/go-foundation/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// finishWaitT0 is when the finish-wait cases' job becomes claimable.
//
//nolint:gochecknoglobals // shared fixture instant
var finishWaitT0 = time.Date(2026, 3, 8, 6, 30, 0, 0, time.UTC)

type finishWaitArgs struct{}

func (finishWaitArgs) Kind() string { return "finish_wait.worker" }

type finishWaitWorker struct{}

func (finishWaitWorker) Kind() string { return "finish_wait.worker" }

func (finishWaitWorker) Work(context.Context, *Job[finishWaitArgs]) (Result, error) {
	return Result{}, nil
}

// unscheduledDriver hands the Runner each claimed job without its ScheduledAt,
// as a custom Driver that never reads the column would.
type unscheduledDriver struct {
	Driver
}

func (d unscheduledDriver) Dequeue(
	ctx context.Context, queues []string, class ExecutorClass, claimAny bool, limit int, lease time.Duration,
) ([]RawJob, error) {
	jobs, err := d.Driver.Dequeue(ctx, queues, class, claimAny, limit, lease)
	for i := range jobs {
		jobs[i].ScheduledAt = time.Time{}
	}
	return jobs, err
}

// runFinishWaitJob inserts one job claimable at finishWaitT0, runs it on a
// runner over driver whose clock reads three seconds later, and returns the
// finish event the observer saw and the run the attempt recorded.
func runFinishWaitJob(t *testing.T, wrap func(Driver) Driver) (FinishEvent, JobRunView) {
	t.Helper()
	db := newDB(t)
	clk := models.NewSimulatedClock(finishWaitT0)
	ctx := clockCtx(context.Background(), clk)

	reg := NewRegistry()
	Register[finishWaitArgs](reg, finishWaitWorker{})
	obs := &recordingObserver{}
	r := rwRunner(t, db, reg, func(cfg *RunnerConfig) {
		cfg.Driver = wrap(cfg.Driver)
		cfg.Observer = obs
	})

	id, err := Insert(ctx, NewClient(db), finishWaitArgs{}, InsertOpts{})
	require.NoError(t, err)
	clk.Advance(3 * time.Second)
	n, err := r.pollOnce(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, n, "the job was claimable")

	_, _, finishes, _ := obs.snapshot()
	require.Len(t, finishes, 1, "the attempt finished once")
	return finishes[0], latestRunView(t, db, id)
}

// TestFinishEventCarriesTheAttemptsQueueWait expects a finish event to report how
// long its attempt waited for a worker, the value the run row records, so an
// observer reads it without a query; and zero when the claimed job carries no
// ScheduledAt to measure from.
func TestFinishEventCarriesTheAttemptsQueueWait(t *testing.T) {
	t.Parallel()

	t.Run("a job claimable three seconds before its start", func(t *testing.T) {
		t.Parallel()
		ev, run := runFinishWaitJob(t, func(d Driver) Driver { return d })

		require.NotNil(t, run.QueueWaitMs, "the run records the wait")
		assert.Equal(t, 3000, *run.QueueWaitMs)
		assert.Equal(t, 3*time.Second, ev.QueueWait, "the event reports the attempt's wait")
		assert.Equal(t, time.Duration(*run.QueueWaitMs)*time.Millisecond, ev.QueueWait,
			"the event's wait is the one the run records")
	})

	t.Run("a claimed job without ScheduledAt", func(t *testing.T) {
		t.Parallel()
		ev, run := runFinishWaitJob(t, func(d Driver) Driver { return unscheduledDriver{Driver: d} })

		assert.Nil(t, run.QueueWaitMs, "the run records no wait")
		assert.Zero(t, ev.QueueWait, "nor does the event")
	})
}
