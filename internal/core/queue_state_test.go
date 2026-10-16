package core

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// queueStateTestRefresh is the QueueStateRefresh the queue-pause runner tests
// run with, so a pause takes hold in milliseconds rather than seconds.
const queueStateTestRefresh = 50 * time.Millisecond

// queueStateRefreshFailedMsg is the warning a Runner logs when it cannot read
// the paused set and keeps the last one it read.
const queueStateRefreshFailedMsg = "jobs: queue state refresh failed; keeping the last paused set"

// queueWorkArgs is the job the queue-pause tests run on every queue.
type queueWorkArgs struct{}

func (queueWorkArgs) Kind() string { return "queue.work" }

// queueWorker records, by job id, the queue of every job it runs.
type queueWorker struct{ ran sync.Map }

func (*queueWorker) Kind() string { return "queue.work" }

func (w *queueWorker) Work(_ context.Context, job *Job[queueWorkArgs]) (Result, error) {
	w.ran.Store(job.ID, job.Queue)
	return Result{}, nil
}

// hasRun reports whether the worker ran id.
func (w *queueWorker) hasRun(id string) bool {
	_, ok := w.ran.Load(id)
	return ok
}

// waitRan waits until the worker has run every id.
func (w *queueWorker) waitRan(t *testing.T, ids ...string) {
	t.Helper()
	require.Eventually(t, func() bool {
		for _, id := range ids {
			if !w.hasRun(id) {
				return false
			}
		}
		return true
	}, 10*time.Second, 5*time.Millisecond, "every job ran")
}

// queueWorkRegistry registers w as the queue.work worker.
func queueWorkRegistry(w *queueWorker) *Registry {
	reg := NewRegistry()
	Register[queueWorkArgs](reg, w)
	return reg
}

// enqueueOn inserts one queue.work job on each named queue, in order, and
// returns their ids.
func enqueueOn(t *testing.T, db *gorm.DB, queues ...string) []string {
	t.Helper()
	ids := make([]string, len(queues))
	for i, q := range queues {
		id, err := Insert(context.Background(), NewClient(db), queueWorkArgs{}, InsertOpts{Queue: q})
		require.NoError(t, err)
		ids[i] = id
	}
	return ids
}

// queueRunner builds a Runner on queues with the test refresh and a tight poll.
func queueRunner(t *testing.T, db *gorm.DB, reg *Registry, queues []string, mutate ...func(*RunnerConfig)) *Runner {
	t.Helper()
	cfg := RunnerConfig{
		DB: db, Driver: driverForTest(t, db), Registry: reg, Queues: queues, ClaimAnyClass: true,
		PollInterval: 2 * time.Millisecond, QueueStateRefresh: queueStateTestRefresh,
	}
	for _, m := range mutate {
		m(&cfg)
	}
	r, err := NewRunner(cfg)
	require.NoError(t, err)
	return r
}

// holdsPause reports whether r's last read of the paused set found queue
// paused.
func holdsPause(r *Runner, queue string) bool {
	r.queueState.mu.Lock()
	defer r.queueState.mu.Unlock()
	return r.queueState.paused[queue]
}

// pausedQueueSuite runs two Runners on queues a and b, each with concurrency
// slots, over db, and pauses a while they run: each reads the pause within a
// refresh or so, after which neither claims a job of a while jobs of b keep
// running, and the queue lists as paused by its actor, with one entry.
func pausedQueueSuite(t *testing.T, db *gorm.DB, concurrency int) {
	t.Helper()
	w := &queueWorker{}
	reg := queueWorkRegistry(w)
	slots := func(cfg *RunnerConfig) { cfg.Concurrency = concurrency }
	runners := []*Runner{
		queueRunner(t, db, reg, []string{"a", "b"}, slots),
		queueRunner(t, db, reg, []string{"a", "b"}, slots),
	}
	for _, r := range runners {
		t.Cleanup(runInBackground(t, r))
	}
	w.waitRan(t, enqueueOn(t, db, "a")...)

	operator := WithActor(context.Background(), Actor{Type: ActorUser, ID: "u1", Reason: "provider outage"})
	pausedAt := time.Now()
	require.NoError(t, PauseQueue(operator, db, "a"))
	require.Eventually(t, func() bool { return holdsPause(runners[0], "a") && holdsPause(runners[1], "a") },
		10*time.Second, time.Millisecond, "every runner reads the pause")
	assert.Less(t, time.Since(pausedAt), 40*queueStateTestRefresh, "within a refresh or so")

	held := enqueueOn(t, db, "a", "a", "a")
	w.waitRan(t, enqueueOn(t, db, "b", "b", "b")...)
	time.Sleep(3 * queueStateTestRefresh)
	for _, id := range held {
		assert.False(t, w.hasRun(id), "no runner claims a job of the paused queue")
		assert.Equal(t, string(StateAvailable), jobState(t, db, id))
	}

	queues, err := ListQueues(context.Background(), db)
	require.NoError(t, err)
	var a QueueInfo
	for _, q := range queues {
		if q.Queue == "a" {
			a = q
		}
	}
	assert.True(t, a.IsPaused)
	assert.NotNil(t, a.PausedAt)
	assert.Equal(t, &Actor{Type: ActorUser, ID: "u1", Reason: "provider outage"}, a.PausedBy)
	assert.EqualValues(t, 3, a.Ready, "its jobs wait, ready")
	entries := auditRows(t, db, "a")
	require.Len(t, entries, 1)
	assert.Equal(t, string(AuditPaused), entries[0].Action)
}

// TestAPausedQueueIsNotClaimedWithinOneRefresh runs the suite on SQLite.
func TestAPausedQueueIsNotClaimedWithinOneRefresh(t *testing.T) {
	t.Parallel()
	pausedQueueSuite(t, newWALFileDB(t), 1)
}

// TestResumingAQueueResumesClaims proves a Runner whose every queue is paused
// claims nothing, holds no slot, and asks its limiter for nothing, while
// RunUntilIdle waits for the paused queue's job; that resuming the queue lets
// it claim the job within a refresh, after which RunUntilIdle returns; and that
// RunUntilIdle on a fully paused Runner with no job pending anywhere returns at
// once.
func TestResumingAQueueResumesClaims(t *testing.T) {
	t.Parallel()
	db := newWALFileDB(t)
	w := &queueWorker{}
	operator := WithActor(context.Background(), queueOperator)
	require.NoError(t, PauseQueue(operator, db, "a"))
	job := enqueueOn(t, db, "a")[0]

	limiter := &countingLimiter{next: func(resource string, n int) (Grant, error) {
		return Grant{Resource: resource, N: n}, nil
	}}
	r := queueRunner(t, db, queueWorkRegistry(w), []string{"a"}, func(cfg *RunnerConfig) {
		cfg.Limiter = limiter
		cfg.Resource = "provider:a"
	})
	done := make(chan error, 1)
	go func() { done <- r.RunUntilIdle(context.Background()) }()

	time.Sleep(4 * queueStateTestRefresh)
	select {
	case err := <-done:
		t.Fatalf("RunUntilIdle returned with a paused queue's job pending: %v", err)
	default:
	}
	assert.False(t, w.hasRun(job), "a fully paused runner claims nothing")
	assert.Zero(t, r.InFlight(), "and holds no slot")
	assert.Zero(t, limiter.acquireCount(), "and asks its limiter for nothing")

	resumedAt := time.Now()
	require.NoError(t, ResumeQueue(operator, db, "a"))
	select {
	case err := <-done:
		require.NoError(t, err, "RunUntilIdle returns once the resumed queue's job is done")
	case <-time.After(10 * time.Second):
		t.Fatal("RunUntilIdle did not return after the resume")
	}
	assert.Less(t, time.Since(resumedAt), 40*queueStateTestRefresh, "the resume took hold within a refresh or so")
	assert.Equal(t, string(StateSucceeded), jobState(t, db, job))
	assert.Positive(t, limiter.acquireCount())
	entries := auditRows(t, db, "a")
	require.Len(t, entries, 2)
	assert.Equal(t, string(AuditResumed), entries[1].Action)

	require.NoError(t, PauseQueue(operator, db, "idle"))
	idle := queueRunner(t, db, queueWorkRegistry(&queueWorker{}), []string{"idle"})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, idle.RunUntilIdle(ctx), "with nothing pending anywhere, a fully paused runner is idle")
}

// failQueueStateReads makes every read of job_queues on db fail while fail is
// set, leaving every other statement alone.
func failQueueStateReads(t *testing.T, db *gorm.DB, fail *atomic.Bool) {
	t.Helper()
	require.NoError(t, db.Callback().Query().Before("gorm:query").Register("test:fail_queue_state_"+t.Name(),
		func(tx *gorm.DB) {
			if fail.Load() && tx.Statement.Table == "job_queues" {
				_ = tx.AddError(errors.New("queue state unreadable"))
			}
		}))
}

// TestARunnerKeepsItsLastPausedSetWhenARefreshFails proves a Runner that read
// the paused set once keeps leaving the paused queue out of its claims while
// later reads fail, warning once for the whole streak, and picks up a resume
// once a read succeeds again; and that a Runner whose first read fails claims
// every queue it is configured for.
func TestARunnerKeepsItsLastPausedSetWhenARefreshFails(t *testing.T) {
	t.Parallel()
	t.Run("after a successful read", func(t *testing.T) {
		t.Parallel()
		db := newWALFileDB(t)
		var fail atomic.Bool
		failQueueStateReads(t, db, &fail)
		operator := WithActor(context.Background(), queueOperator)
		require.NoError(t, PauseQueue(operator, db, "a"))
		w := &queueWorker{}
		logs := &captureHandler{}
		t.Cleanup(runInBackground(t, queueRunner(t, db, queueWorkRegistry(w), []string{"a", "b"}, func(cfg *RunnerConfig) {
			cfg.Logger = slog.New(logs)
		})))
		w.waitRan(t, enqueueOn(t, db, "b")...)

		fail.Store(true)
		require.Eventually(t, func() bool { return len(logs.recordsFor(queueStateRefreshFailedMsg)) > 0 },
			10*time.Second, time.Millisecond, "a refresh fails")
		held := enqueueOn(t, db, "a")[0]
		w.waitRan(t, enqueueOn(t, db, "b")...)
		time.Sleep(3 * queueStateTestRefresh)
		assert.False(t, w.hasRun(held), "the last paused set still holds the queue")
		assert.Len(t, logs.recordsFor(queueStateRefreshFailedMsg), 1, "one warning for the whole failure streak")

		fail.Store(false)
		require.NoError(t, ResumeQueue(operator, db, "a"))
		w.waitRan(t, held)
	})
	t.Run("before any read succeeds", func(t *testing.T) {
		t.Parallel()
		db := newWALFileDB(t)
		var fail atomic.Bool
		failQueueStateReads(t, db, &fail)
		require.NoError(t, PauseQueue(WithActor(context.Background(), queueOperator), db, "a"))
		fail.Store(true)
		w := &queueWorker{}
		logs := &captureHandler{}
		t.Cleanup(runInBackground(t, queueRunner(t, db, queueWorkRegistry(w), []string{"a", "b"}, func(cfg *RunnerConfig) {
			cfg.Logger = slog.New(logs)
		})))
		w.waitRan(t, enqueueOn(t, db, "a", "b")...)
		assert.NotEmpty(t, logs.recordsFor(queueStateRefreshFailedMsg))
	})
}

// TestARunnerRefusesToStartWithoutTheQueueTable is the deploy-order guard for
// the paused set: a Runner reads job_queues before it claims, so against a
// schema without it the Runner stops before claiming anything, naming the table
// and the fix; with QueueStateRefresh negative it reads no paused set, needs no
// table, and runs. db carries the current schema; the test drops the table.
func TestARunnerRefusesToStartWithoutTheQueueTable(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	require.NoError(t, db.Exec("DROP TABLE job_queues").Error)
	w := &queueWorker{}
	id := enqueueOn(t, db, defaultQueue)[0]

	err := rwRunner(t, db, queueWorkRegistry(w)).RunUntilIdle(context.Background())
	require.ErrorIs(t, err, ErrSchemaOutdated)
	assert.Contains(t, err.Error(), "job_queues (table)", "the error names the missing table")
	assert.Contains(t, err.Error(), "flywheel migrate", "and how to fix it")
	assert.Equal(t, string(StateAvailable), jobState(t, db, id), "nothing was claimed")

	off := rwRunner(t, db, queueWorkRegistry(w), func(cfg *RunnerConfig) { cfg.QueueStateRefresh = -1 })
	require.NoError(t, off.RunUntilIdle(context.Background()), "a negative refresh turns the check off")
	assert.Equal(t, string(StateSucceeded), jobState(t, db, id))
}
