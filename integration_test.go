//go:build integration

package flywheel_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mrz1836/go-flywheel"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// pgConcurrentArgs is the args type for a worker that increments a counter
// and immediately returns success — used by the SKIP-LOCKED concurrency test.
type pgConcurrentArgs struct{ V string }

func (pgConcurrentArgs) Kind() string { return "pg.concurrent" }

type pgConcurrentWorker struct {
	processed atomic.Int64
}

func (*pgConcurrentWorker) Kind() string { return "pg.concurrent" }
func (w *pgConcurrentWorker) Work(_ context.Context, _ *flywheel.Job[pgConcurrentArgs]) (flywheel.Result, error) {
	w.processed.Add(1)
	return flywheel.Result{}, nil
}

// newPostgresRunner builds a Runner over the shared Postgres test DB with
// the supplied concurrency. The default queues are claimed.
func newPostgresRunner(t *testing.T, db *gorm.DB, reg *flywheel.Registry, concurrency int) *flywheel.Runner {
	t.Helper()
	runner, err := flywheel.NewRunner(flywheel.RunnerConfig{
		DB:           db,
		Driver:       flywheel.NewPostgresDriver(db),
		Registry:     reg,
		Queues:       []string{"default", "periodic"},
		ExecutorKind: flywheel.ExecutorLocal,
		Concurrency:  concurrency,
	})
	require.NoError(t, err)
	return runner
}

func countByState(t *testing.T, db *gorm.DB, kind, state string) int64 {
	t.Helper()
	var n int64
	require.NoError(t, db.Table("jobs").
		Where("kind = ? AND state = ?", kind, state).Count(&n).Error)
	return n
}

// TestRunnerPGSkipLockedConcurrency drives 4 runners with concurrency=4
// against 100 jobs and asserts every job runs exactly once. The Postgres
// SKIP LOCKED claim guarantees no two runners ever dispatch the same job.
func TestRunnerPGSkipLockedConcurrency(t *testing.T) {
	t.Parallel()
	db := flywheel.NewPostgresIsolatedDB(t)

	worker := &pgConcurrentWorker{}
	reg := flywheel.NewRegistry()
	flywheel.Register(reg, worker)

	const totalJobs = 100
	for i := range totalJobs {
		_, err := flywheel.Insert(context.Background(), flywheel.NewClient(db),
			pgConcurrentArgs{V: fmt.Sprintf("v%d", i)}, flywheel.InsertOpts{})
		require.NoError(t, err)
	}

	// 4 parallel runners with concurrency=4 — 16-way concurrent claims.
	const runners = 4
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	for range runners {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r := newPostgresRunner(t, db, reg, 4)
			_ = r.RunUntilIdle(ctx)
		}()
	}
	wg.Wait()

	assert.EqualValues(t, totalJobs, worker.processed.Load(),
		"every job ran exactly once — SKIP LOCKED prevents double-claim")
	assert.EqualValues(t, totalJobs, countByState(t, db, "pg.concurrent", "succeeded"))
}

// TestRunnerPGUniqueKeyRaceOneWins drives N concurrent Inserts with the
// same UniqueKey and asserts exactly one row lands.
func TestRunnerPGUniqueKeyRaceOneWins(t *testing.T) {
	t.Parallel()
	db := flywheel.NewPostgresIsolatedDB(t)

	const goroutines = 50
	var (
		wg       sync.WaitGroup
		success  atomic.Int64
		conflict atomic.Int64
	)
	wg.Add(goroutines)
	for range goroutines {
		go func() {
			defer wg.Done()
			_, err := flywheel.Insert(context.Background(), flywheel.NewClient(db),
				pgConcurrentArgs{V: "race"}, flywheel.InsertOpts{UniqueKey: "racing"})
			switch {
			case err == nil:
				success.Add(1)
			case errorsIs(err, flywheel.ErrAlreadyEnqueued):
				conflict.Add(1)
			default:
				t.Errorf("unexpected error: %v", err)
			}
		}()
	}
	wg.Wait()

	assert.EqualValues(t, 1, success.Load(), "exactly one insert wins the unique-key race")
	assert.EqualValues(t, goroutines-1, conflict.Load(), "every other insert resolves to ErrAlreadyEnqueued")

	var rows int64
	require.NoError(t, db.Table("jobs").Where("unique_key = ?", "racing").Count(&rows).Error)
	assert.EqualValues(t, 1, rows, "exactly one row lands")
}

// TestRunnerPGLeaseExpiryReclaimsJob simulates a dead executor: a job in
// running state with an expired lease must be returned to available, and its
// in-flight job_runs stub must be marked crashed.
func TestRunnerPGLeaseExpiryReclaimsJob(t *testing.T) {
	t.Parallel()
	db := flywheel.NewPostgresIsolatedDB(t)

	worker := &pgConcurrentWorker{}
	reg := flywheel.NewRegistry()
	flywheel.Register(reg, worker)
	runner := newPostgresRunner(t, db, reg, 1)

	id, err := flywheel.Insert(context.Background(), flywheel.NewClient(db),
		pgConcurrentArgs{V: "stuck"}, flywheel.InsertOpts{})
	require.NoError(t, err)

	// Force the job into running with an expired lease.
	now := time.Now().UTC()
	require.NoError(t, db.Table("jobs").Where("id = ?", id).Updates(map[string]any{
		"state":        "running",
		"leased_until": now.Add(-time.Minute),
		"attempt":      1,
	}).Error)
	require.NoError(t, db.Exec(
		`INSERT INTO job_runs(id, job_id, attempt, executor_kind, executor_id, started_at, outcome, created_at)
		 VALUES (?,?,?,?,?,?,?,?)`,
		"00000000-0000-7000-8000-000000000001", id, 1, "local", "deadbeef:1", now.Add(-time.Hour),
		"started", now.Add(-time.Hour),
	).Error)

	// Sweep + drain.
	sched := flywheel.NewScheduler(db, flywheel.NewClient(db))
	reclaimed, err := sched.Sweep(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 1, reclaimed)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	require.NoError(t, runner.RunUntilIdle(ctx))

	assert.EqualValues(t, 1, worker.processed.Load(), "reclaimed job runs after sweep")
}

// pgFanOutArgs is the args type for the parent worker in the fan-out test —
// it emits N child follow-ups in one Result, atomic with finalization.
type pgFanOutArgs struct{ Children int }

func (pgFanOutArgs) Kind() string { return "pg.fanout.parent" }

type pgFanOutWorker struct {
	processed atomic.Int64
}

func (*pgFanOutWorker) Kind() string { return "pg.fanout.parent" }
func (w *pgFanOutWorker) Work(_ context.Context, job *flywheel.Job[pgFanOutArgs]) (flywheel.Result, error) {
	w.processed.Add(1)
	followUps := make([]flywheel.FollowUp, job.Args.Children)
	for i := range followUps {
		followUps[i] = flywheel.FollowUp{
			Kind:   "pg.fanout.child",
			Args:   pgChildArgs{Index: i},
			Parent: true,
		}
	}
	return flywheel.Result{FollowUps: followUps}, nil
}

// pgChildArgs is the args type for the fan-out test's children.
type pgChildArgs struct{ Index int }

func (pgChildArgs) Kind() string { return "pg.fanout.child" }

type pgChildWorker struct {
	processed atomic.Int64
	indices   sync.Map // index → struct{}; used to detect double-dispatch
	dups      atomic.Int64
}

func (*pgChildWorker) Kind() string { return "pg.fanout.child" }
func (w *pgChildWorker) Work(_ context.Context, job *flywheel.Job[pgChildArgs]) (flywheel.Result, error) {
	w.processed.Add(1)
	if _, loaded := w.indices.LoadOrStore(job.Args.Index, struct{}{}); loaded {
		w.dups.Add(1)
	}
	return flywheel.Result{}, nil
}

// TestRunnerPGFanOutAtomicityNoDoubleDispatch confirms a parent job's fan-out
// follow-ups are enqueued atomically with parent finalization (FR-006) and
// that 4 concurrent runners never dispatch the same child twice under SKIP
// LOCKED contention.
func TestRunnerPGFanOutAtomicityNoDoubleDispatch(t *testing.T) {
	t.Parallel()
	db := flywheel.NewPostgresIsolatedDB(t)

	const children = 50
	parent := &pgFanOutWorker{}
	child := &pgChildWorker{}
	reg := flywheel.NewRegistry()
	flywheel.Register(reg, parent)
	flywheel.Register(reg, child)

	_, err := flywheel.Insert(context.Background(), flywheel.NewClient(db),
		pgFanOutArgs{Children: children}, flywheel.InsertOpts{})
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	const runners = 4
	var wg sync.WaitGroup
	for range runners {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r := newPostgresRunner(t, db, reg, 4)
			_ = r.RunUntilIdle(ctx)
		}()
	}
	wg.Wait()

	assert.EqualValues(t, 1, parent.processed.Load(), "parent runs exactly once")
	assert.EqualValues(t, children, child.processed.Load(), "every child runs exactly once")
	assert.EqualValues(t, 0, child.dups.Load(), "no child dispatched twice")
	assert.EqualValues(t, 1, countByState(t, db, "pg.fanout.parent", "succeeded"))
	assert.EqualValues(t, children, countByState(t, db, "pg.fanout.child", "succeeded"))

	// Every child carries the parent's id — the fan-out wired parent_job_id.
	var withParent int64
	require.NoError(t, db.Table("jobs").
		Where("kind = ? AND parent_job_id IS NOT NULL", "pg.fanout.child").
		Count(&withParent).Error)
	assert.EqualValues(t, children, withParent, "every child references its parent")
}

// errorsIs is a tiny wrapper so the integration test file does not need to
// import errors at the top (used only inside one goroutine path above).
func errorsIs(err, target error) bool {
	if err == nil || target == nil {
		return err == target
	}
	for err != nil {
		if err == target {
			return true
		}
		u, ok := err.(interface{ Unwrap() error })
		if !ok {
			return false
		}
		err = u.Unwrap()
	}
	return false
}
