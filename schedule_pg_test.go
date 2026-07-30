//go:build integration

package flywheel

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mrz1836/go-foundation/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

type replayContinuityArgs struct{}

func (replayContinuityArgs) Kind() string { return "replay.continuity" }

// replayContinuityWorker succeeds on every attempt and counts its runs, so a
// replayed job re-converges on its first post-replay claim with no backoff to wait
// on.
type replayContinuityWorker struct{ ran atomic.Int32 }

func (*replayContinuityWorker) Kind() string { return "replay.continuity" }
func (w *replayContinuityWorker) Work(context.Context, *Job[replayContinuityArgs]) (Result, error) {
	w.ran.Add(1)
	return Result{}, nil
}

// jobRunAttempts returns a job's recorded attempt numbers in ascending order.
func jobRunAttempts(t *testing.T, db *gorm.DB, jobID string) []int {
	t.Helper()
	var attempts []int
	require.NoError(t, db.Model(&jobRunRow{}).
		Where("job_id = ?", jobID).Order("attempt").Pluck("attempt", &attempts).Error)
	return attempts
}

// TestRetryResetAttemptsKeepsJobRunsContinuousPostgres is A1's core invariant under
// the real FOR UPDATE SKIP LOCKED claim path and real concurrency. A job discarded
// at attempt == max_attempts == 2, whose two failures are recorded in job_runs, is
// retried with ResetAttempts and re-run. Because ResetAttempts raises max_attempts
// rather than rewinding attempt, the re-run's claim lands at attempt 3 and its run
// stub does not collide with the recorded attempts 1 and 2 on the
// job_runs(job_id, attempt) unique index — the audit sequence continues 1,2,3 with
// no gap. Rewinding attempt would drive the next claim back to attempt 1 and fail
// the InsertRunStub with a duplicate-key error, which is exactly the failure the
// headroom design avoids.
func TestRetryResetAttemptsKeepsJobRunsContinuousPostgres(t *testing.T) {
	t.Parallel()
	db := NewPostgresIsolatedDB(t)
	base := time.Now().UTC().Truncate(time.Second)
	seedCtx := models.WithClock(context.Background(), models.NewFixedClock(base))

	const jobs = 40
	ids := make([]string, jobs)
	for i := range ids {
		id := fmt.Sprintf("cont-%d", i)
		ids[i] = id
		// A job discarded at attempt == max_attempts == 2.
		seedJob(t, db, jobRow{
			ID: id, Kind: "replay.continuity", State: string(StateDiscarded),
			Attempt: 2, MaxAttempts: 2, FinalizedAt: &base,
		})
		// Its two recorded failures, byte-identical in shape to what the runtime writes.
		for attempt := 1; attempt <= 2; attempt++ {
			_, err := SeedRun(seedCtx, db, RunSeed{
				JobID: id, Attempt: attempt, ExecutorID: "seed",
				Outcome: OutcomeError, ErrorMessage: "boom",
			})
			require.NoError(t, err)
		}
	}

	// Replay each: restore the budget as headroom (max_attempts -> attempt + old max).
	for _, id := range ids {
		require.NoError(t, RetryJobWithOptions(context.Background(), db, id,
			RetryOpts{Force: true, ResetAttempts: true}))
	}

	// Drain under real concurrency: four runners, concurrency four each — up to
	// 16-way concurrent SKIP LOCKED claims against the replayed cohort.
	worker := &replayContinuityWorker{}
	reg := NewRegistry()
	Register(reg, worker)

	runners := make([]*Runner, 4)
	for i := range runners {
		r, err := NewRunner(RunnerConfig{
			DB: db, Driver: NewPostgresDriver(db), Registry: reg,
			Queues: []string{"default", "periodic"}, ExecutorClass: "local", Concurrency: 4,
		})
		require.NoError(t, err)
		runners[i] = r
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	for _, r := range runners {
		wg.Add(1)
		go func(r *Runner) {
			defer wg.Done()
			_ = r.RunUntilIdle(ctx)
		}(r)
	}
	wg.Wait()

	assert.EqualValues(t, jobs, worker.ran.Load(), "each replayed job ran exactly once more")
	for _, id := range ids {
		row := jobRowByID(t, db, id)
		assert.Equal(t, string(StateSucceeded), row.State, "the replayed job re-converged to succeeded")
		assert.Equal(t, 3, row.Attempt, "the re-run claimed attempt 3, continuing the sequence")
		assert.Equal(t, 4, row.MaxAttempts, "the budget was restored as headroom (attempt 2 + old max 2)")
		assert.Equal(t, []int{1, 2, 3}, jobRunAttempts(t, db, id),
			"job_runs is continuous 1,2,3 with no gap and no unique-index collision")
	}
}
