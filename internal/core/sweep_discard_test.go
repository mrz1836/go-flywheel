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

// crashLease is the lease the crash tests claim with. Each sweep runs under a
// clock twice that far ahead, so the lease it finds has always expired.
const crashLease = time.Minute

// seedCrashJob writes one claimable job on queue with the given attempt budget
// and returns its id. scheduled_at is backdated in the driver's own zone, as
// seedClaimable explains, so a Dequeue picks the job up at once.
func seedCrashJob(t testing.TB, db *gorm.DB, id, queue string, maxAttempts int) string {
	t.Helper()
	at := time.Now().Add(-time.Minute)
	seedJob(t, db, jobRow{
		ID: id, Kind: "crash.loop", Queue: queue, State: string(StateAvailable),
		MaxAttempts: maxAttempts, CreatedAt: at, UpdatedAt: at, ScheduledAt: at,
	})
	return id
}

// crashAttempt claims one job from queue and commits its run stub, then stops:
// nothing finalizes the attempt, which is what a process that died mid-attempt
// leaves behind. It returns the claim and the stub's run id.
//
//nolint:revive // ctx-as-second-arg matches the testing.TB-first helper convention
func crashAttempt(t testing.TB, ctx context.Context, d Driver, queue string) (RawJob, string) {
	t.Helper()
	batch, err := d.Dequeue(ctx, []string{queue}, AnyClass, true, 1, crashLease)
	require.NoError(t, err)
	require.Len(t, batch, 1, "a job must be claimable from %q", queue)
	runID := models.NewID()
	require.NoError(t, d.InsertRunStub(ctx, runID, batch[0], time.Now(), "local", "exec-crash"))
	return batch[0], runID
}

// sweepPastTheLease runs one Scheduler sweep under a clock past every lease the
// crash tests take, and returns the instant the sweep ran at.
func sweepPastTheLease(t testing.TB, s *Scheduler) time.Time {
	t.Helper()
	now := time.Now().Add(2 * crashLease)
	_, err := s.Sweep(clockCtx(context.Background(), models.NewFixedClock(now)))
	require.NoError(t, err)
	return now
}

// crashScheduler builds a Scheduler over db that sweeps through d, the driver
// the test claims with.
func crashScheduler(t testing.TB, db *gorm.DB, d Driver) *Scheduler {
	t.Helper()
	return newSchedulerCfg(t, SchedulerConfig{DB: db, Client: NewClient(db), Driver: d})
}

// sweepDiscardSuite crashes a two-attempt job twice. The first reclaim returns it
// to available; the second, which reclaims the attempt that spent its budget,
// discards it as a finalize would have: finalized at the sweep's instant, with
// no lease and no claim left.
func sweepDiscardSuite(t *testing.T, db *gorm.DB) {
	t.Helper()
	d := driverForTest(t, db)
	s := crashScheduler(t, db, d)
	ctx := context.Background()
	id := seedCrashJob(t, db, "crash-loop-last", "default", 2)

	crashAttempt(t, ctx, d, "default")
	sweepPastTheLease(t, s)
	require.Equal(t, string(StateAvailable), jobRowByID(t, db, id).State,
		"a crash with attempts left returns the job to the queue")

	crashAttempt(t, ctx, d, "default")
	sweptAt := sweepPastTheLease(t, s)

	row := jobRowByID(t, db, id)
	assert.Equal(t, string(StateDiscarded), row.State, "the crashed attempt was the last, so the job ends")
	assert.Equal(t, 2, row.Attempt, "the attempt counter never passes the budget")
	require.NotNil(t, row.FinalizedAt, "a discarded job carries its finalization instant")
	assert.WithinDuration(t, sweptAt, *row.FinalizedAt, time.Millisecond, "the job is finalized at the sweep's instant")
	assert.Nil(t, row.LeasedUntil, "the discard releases the lease")
	assert.Nil(t, row.LeaseToken, "the discard clears the fence token")

	// Nothing can claim it again: a discarded job is terminal.
	batch, err := d.Dequeue(ctx, []string{"default"}, AnyClass, true, 1, crashLease)
	require.NoError(t, err)
	assert.Empty(t, batch, "a discarded job is never claimed again")
}

// crashedStubSuite crashes a two-attempt job twice and reads each crashed run:
// both carry outcome crashed, and each records the state the sweep applied to the
// job, available for the first and discarded for the last.
func crashedStubSuite(t *testing.T, db *gorm.DB) {
	t.Helper()
	d := driverForTest(t, db)
	s := crashScheduler(t, db, d)
	ctx := context.Background()
	id := seedCrashJob(t, db, "crash-loop-stubs", "default", 2)

	_, firstRun := crashAttempt(t, ctx, d, "default")
	sweepPastTheLease(t, s)
	_, lastRun := crashAttempt(t, ctx, d, "default")
	sweepPastTheLease(t, s)

	runs := runsOf(t, db, id)
	require.Len(t, runs, 2)
	assert.Equal(t, firstRun, runs[0].ID)
	assert.Equal(t, lastRun, runs[1].ID)
	for _, r := range runs {
		assert.Equal(t, string(OutcomeCrashed), r.Outcome, "attempt %d crashed", r.Attempt)
		assert.NotNil(t, r.FinishedAt, "attempt %d is finished", r.Attempt)
	}
	require.NotNil(t, runs[0].JobState)
	assert.Equal(t, string(StateAvailable), *runs[0].JobState, "the first crash returned the job to the queue")
	require.NotNil(t, runs[1].JobState)
	assert.Equal(t, string(StateDiscarded), *runs[1].JobState, "the last crash discarded the job")
}

// TestSweepDiscardsAJobWhoseCrashedAttemptWasItsLast proves a job whose process
// dies on every attempt ends instead of looping: the reclaim of its last attempt
// discards it.
func TestSweepDiscardsAJobWhoseCrashedAttemptWasItsLast(t *testing.T) {
	t.Parallel()
	sweepDiscardSuite(t, newDB(t))
}

// TestSweepReturnsAJobWithAttemptsLeftToAvailable guards the other side: a crash
// that leaves attempts in the budget returns the job to the queue, unfinalized,
// as the sweep always has.
func TestSweepReturnsAJobWithAttemptsLeftToAvailable(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	d := NewSQLiteDriver(db)
	s := crashScheduler(t, db, d)
	id := seedCrashJob(t, db, "crash-loop-left", "default", 3)

	_, runID := crashAttempt(t, context.Background(), d, "default")
	sweepPastTheLease(t, s)

	row := jobRowByID(t, db, id)
	assert.Equal(t, string(StateAvailable), row.State)
	assert.Equal(t, 1, row.Attempt)
	assert.Nil(t, row.FinalizedAt, "a job that runs again is not finalized")
	assert.Nil(t, row.LeasedUntil)
	assert.Nil(t, row.LeaseToken)

	runs := runsOf(t, db, id)
	require.Len(t, runs, 1)
	assert.Equal(t, runID, runs[0].ID)
	assert.Equal(t, string(OutcomeCrashed), runs[0].Outcome)
	require.NotNil(t, runs[0].JobState)
	assert.Equal(t, string(StateAvailable), *runs[0].JobState)
}

// TestCrashedStubRecordsTheStateTheSweepApplied proves the crashed run records
// what the reclaim did to its job, so the runs alone tell a crash that will run
// again from one that ended the job.
func TestCrashedStubRecordsTheStateTheSweepApplied(t *testing.T) {
	t.Parallel()
	crashedStubSuite(t, newDB(t))
}
