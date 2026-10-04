//go:build integration

package core

import (
	"context"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// pgNewKindArgs is the job kind only the new release's runner registers.
type pgNewKindArgs struct {
	N int `json:"n"`
}

func (pgNewKindArgs) Kind() string { return "pg.new_kind" }

// pgNewKindWorker records every execution, so a job run twice is caught.
type pgNewKindWorker struct{ tracker *exactlyOnceTracker }

func (*pgNewKindWorker) Kind() string { return "pg.new_kind" }
func (w *pgNewKindWorker) Work(_ context.Context, job *Job[pgNewKindArgs]) (Result, error) {
	w.tracker.mark(job.ID)
	return Result{}, nil
}

// mixedFleetRunner builds one release's runner for the mixed-fleet test: four
// slots and a tight poll.
func mixedFleetRunner(t *testing.T, db *gorm.DB, reg *Registry, class ExecutorClass, logs *recordingHandler) *Runner {
	t.Helper()
	r, err := NewRunner(RunnerConfig{
		DB:            db,
		Driver:        NewPostgresDriver(db),
		Registry:      reg,
		Queues:        []string{"default"},
		ExecutorClass: class,
		ClaimAnyClass: true,
		Concurrency:   4,
		PollInterval:  2 * time.Millisecond,
		Logger:        slog.New(logs),
	})
	require.NoError(t, err)
	return r
}

// TestRunnerMixedFleetRunsNewKindJobsPostgres is issue #61 on real SKIP LOCKED
// claims at a concurrency SQLite cannot run: a runner of the old release and a
// runner of the new one poll the same queue while 100 jobs of a kind only the new
// one registers wait. The old runner claims some of them first and must hand each
// back; the two then claim side by side until the old one drains, as it would in
// a rolling deploy. Every job runs exactly once on the new runner, none is
// discarded, and no deferral spends an attempt.
func TestRunnerMixedFleetRunsNewKindJobsPostgres(t *testing.T) {
	t.Parallel()
	db := NewPostgresIsolatedDB(t)
	ctx := context.Background()

	const total = 100
	for i := range total {
		_, err := Insert(ctx, NewClient(db), pgNewKindArgs{N: i}, InsertOpts{})
		require.NoError(t, err)
	}

	// The old release registers its own kinds, not the new one.
	oldReg := NewRegistry()
	Register(oldReg, &pgConcurrentWorker{})
	oldLogs := &recordingHandler{}
	// Each stop runs once: explicitly once the jobs are settled, or deferred when
	// an assertion ends the test first.
	oldRunner := mixedFleetRunner(t, db, oldReg, "old", oldLogs)
	stopOld := sync.OnceFunc(runInBackground(t, oldRunner))
	defer stopOld()

	// Let the old runner settle some jobs before the new release arrives, so the
	// overlap is real rather than a race the new runner might win outright.
	// The conditions use assert, not require: Eventually runs them off the test
	// goroutine, where FailNow must not be called.
	require.Eventually(t, func() bool {
		var settled int64
		err := db.Table("job_runs").
			Where("executor_class = ? AND outcome <> ?", "old", string(OutcomeStarted)).Count(&settled).Error
		return assert.NoError(t, err) && settled > 0
	}, 30*time.Second, 5*time.Millisecond, "the old runner settles jobs of the kind it lacks")

	tracker := &exactlyOnceTracker{}
	newReg := NewRegistry()
	Register(newReg, &pgNewKindWorker{tracker: tracker})
	stopNew := sync.OnceFunc(runInBackground(t, mixedFleetRunner(t, db, newReg, "new", &recordingHandler{})))
	defer stopNew()

	// Both releases claim side by side until the new one has run a quarter of the
	// jobs; then the old one drains, as a rolling deploy retires it. A deferred job
	// is due again a second after its first deferral, two after its second, so
	// retiring the old runner bounds how long the rest wait. It is drained, not
	// cancelled: a cancel can land between a claim and its run stub and strand
	// that job until a lease sweep, which this test does not run.
	require.Eventually(t, func() bool { return tracker.distinct() >= total/4 },
		30*time.Second, 5*time.Millisecond, "the new runner runs jobs alongside the old one")
	drainCtx, cancelDrain := context.WithTimeout(ctx, 10*time.Second)
	defer cancelDrain()
	require.NoError(t, oldRunner.Drain(drainCtx))
	stopOld()

	require.Eventually(t, func() bool {
		var pending int64
		err := db.Table("jobs").Where("state IN ?", nonTerminalStates).Count(&pending).Error
		return assert.NoError(t, err) && pending == 0
	}, 60*time.Second, 10*time.Millisecond, "every job reaches a terminal state")
	stopNew()

	assert.EqualValues(t, total, countByState(t, db, "pg.new_kind", "succeeded"), "every job ran")
	assert.Zero(t, countByState(t, db, "pg.new_kind", "discarded"), "the old runner discarded nothing")
	assert.Zero(t, tracker.dups.Load(), "no job ran twice")
	assert.Equal(t, total, tracker.distinct())

	var spent int64
	require.NoError(t, db.Table("jobs").
		Where("kind = ? AND max_attempts - attempt <> ?", "pg.new_kind", defaultMaxAttempts-1).Count(&spent).Error)
	assert.Zero(t, spent, "every job keeps a first-attempt success's headroom: no deferral spent an attempt")

	var snoozes []jobRunRow
	require.NoError(t, db.Where("kind = ? AND outcome = ?", "pg.new_kind", string(OutcomeSnooze)).Find(&snoozes).Error)
	require.NotEmpty(t, snoozes, "the old runner deferred jobs")
	for _, run := range snoozes {
		assert.Equal(t, "old", run.ExecutorClass, "only the old runner defers")
		require.NotNil(t, run.ErrorMessage)
		assert.Equal(t, unknownKindMessage, *run.ErrorMessage)
		assert.Nil(t, run.ErrorClass)
	}
	assert.Len(t, logsFor(oldLogs, slog.LevelWarn, deferWarnMsg), 1, "the old runner warned once for the kind")
}
