package flywheel

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

var (
	errTransient        = errors.New("transient")
	errPermanentFailure = errors.New("permanent failure")
)

// successArgs is the args for a worker that always succeeds.
type successArgs struct{ V string }

func (successArgs) Kind() string { return "test.success" }

type retryArgs struct{ V string }

func (retryArgs) Kind() string { return "test.retry" }

type permanentArgs struct{ V string }

func (permanentArgs) Kind() string { return "test.permanent" }

type panicArgs struct{ V string }

func (panicArgs) Kind() string { return "test.panic" }

type snoozeArgs struct{ V string }

func (snoozeArgs) Kind() string { return "test.snooze" }

// successWorker always succeeds.
type successWorker struct{ calls atomic.Int32 }

func (*successWorker) Kind() string { return "test.success" }
func (w *successWorker) Work(_ context.Context, _ *Job[successArgs]) (Result, error) {
	w.calls.Add(1)
	return Result{}, nil
}

// retryWorker fails (transient) the first failuresBefore attempts then succeeds.
type retryWorker struct {
	failuresBefore int32
	calls          atomic.Int32
}

func (*retryWorker) Kind() string { return "test.retry" }
func (w *retryWorker) Work(_ context.Context, _ *Job[retryArgs]) (Result, error) {
	n := w.calls.Add(1)
	if n <= w.failuresBefore {
		return Result{}, &classifiedError{
			cause:      errTransient,
			class:      ErrorTransient,
			retryDelay: time.Millisecond,
		}
	}
	return Result{}, nil
}

// permanentWorker reports its errors as permanent so the runner stops retrying.
type permanentWorker struct{ calls atomic.Int32 }

func (*permanentWorker) Kind() string              { return "test.permanent" }
func (*permanentWorker) Classify(error) ErrorClass { return ErrorPermanent }
func (w *permanentWorker) Work(_ context.Context, _ *Job[permanentArgs]) (Result, error) {
	w.calls.Add(1)
	return Result{}, errPermanentFailure
}

// panicWorker panics on its first attempt, succeeds afterward.
type panicWorker struct{ calls atomic.Int32 }

func (*panicWorker) Kind() string { return "test.panic" }
func (w *panicWorker) Work(_ context.Context, _ *Job[panicArgs]) (Result, error) {
	if w.calls.Add(1) == 1 {
		panic("boom")
	}
	return Result{}, nil
}

// snoozeWorker snoozes on the first attempt, succeeds afterward.
type snoozeWorker struct{ calls atomic.Int32 }

func (*snoozeWorker) Kind() string { return "test.snooze" }
func (w *snoozeWorker) Work(_ context.Context, _ *Job[snoozeArgs]) (Result, error) {
	if w.calls.Add(1) == 1 {
		d := time.Millisecond
		return Result{Snooze: &d}, nil
	}
	return Result{}, nil
}

// jobState reads the state column for jobID — a small helper since the
// runner tests assert lifecycle terminations.
func jobState(t *testing.T, db *gorm.DB, jobID string) string {
	t.Helper()
	var s string
	require.NoError(t, db.Table("jobs").Select("state").Where("id = ?", jobID).Scan(&s).Error)
	return s
}

// runCount returns how many job_runs rows exist for jobID.
func runCount(t *testing.T, db *gorm.DB, jobID string) int64 {
	t.Helper()
	var n int64
	require.NoError(t, db.Table("job_runs").Where("job_id = ?", jobID).Count(&n).Error)
	return n
}

func TestRunnerSuccessfulJobReachesSucceededState(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	w := &successWorker{}
	reg := NewRegistry()
	Register(reg, w)
	r := newRunner(t, db, reg)

	id, err := Insert(context.Background(), NewClient(db), successArgs{V: "ok"}, InsertOpts{})
	require.NoError(t, err)

	runToIdle(t, context.Background(), r)

	assert.Equal(t, string(StateSucceeded), jobState(t, db, id))
	assert.EqualValues(t, 1, runCount(t, db, id), "one job_runs row per attempt")
	assert.EqualValues(t, 1, w.calls.Load())
}

func TestRunnerTransientErrorRetriesThenSucceeds(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	w := &retryWorker{failuresBefore: 2}
	reg := NewRegistry()
	Register(reg, w)
	r := newRunner(t, db, reg)

	id, err := Insert(context.Background(), NewClient(db), retryArgs{V: "x"}, InsertOpts{})
	require.NoError(t, err)

	runToIdle(t, context.Background(), r)

	assert.Equal(t, string(StateSucceeded), jobState(t, db, id))
	assert.EqualValues(t, 3, w.calls.Load(), "two failures + one success")
	assert.EqualValues(t, 3, runCount(t, db, id), "one job_runs row per attempt — append-only audit")
}

func TestRunnerPermanentErrorDiscardsImmediately(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	w := &permanentWorker{}
	reg := NewRegistry()
	Register(reg, w)
	r := newRunner(t, db, reg)

	id, err := Insert(context.Background(), NewClient(db), permanentArgs{V: "x"}, InsertOpts{})
	require.NoError(t, err)

	runToIdle(t, context.Background(), r)

	assert.Equal(t, string(StateDiscarded), jobState(t, db, id), "Permanent → discarded on first attempt")
	assert.EqualValues(t, 1, w.calls.Load())
}

func TestRunnerExceedsMaxAttemptsDiscards(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	w := &retryWorker{failuresBefore: 100}
	reg := NewRegistry()
	Register(reg, w)
	r := newRunner(t, db, reg)

	id, err := Insert(context.Background(), NewClient(db),
		retryArgs{V: "x"}, InsertOpts{MaxAttempts: 3})
	require.NoError(t, err)

	runToIdle(t, context.Background(), r)

	assert.Equal(t, string(StateDiscarded), jobState(t, db, id))
	assert.EqualValues(t, 3, w.calls.Load(), "exactly MaxAttempts attempts before discard")
}

func TestRunnerPanicRecoveryClassifiesAsTransientByDefault(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	w := &panicWorker{}
	reg := NewRegistry()
	Register(reg, w)
	r := newRunner(t, db, reg)

	id, err := Insert(context.Background(), NewClient(db), panicArgs{V: "x"}, InsertOpts{})
	require.NoError(t, err)

	runToIdle(t, context.Background(), r)

	assert.Equal(t, string(StateSucceeded), jobState(t, db, id),
		"the panic is recovered as a transient error; the next attempt succeeds")
	assert.EqualValues(t, 2, w.calls.Load())
}

func TestRunnerSnoozeReschedulesWithoutConsumingAttempt(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	w := &snoozeWorker{}
	reg := NewRegistry()
	Register(reg, w)
	r := newRunner(t, db, reg)

	id, err := Insert(context.Background(), NewClient(db), snoozeArgs{V: "x"}, InsertOpts{MaxAttempts: 2})
	require.NoError(t, err)

	runToIdle(t, context.Background(), r)

	assert.Equal(t, string(StateSucceeded), jobState(t, db, id))
	assert.EqualValues(t, 2, w.calls.Load(), "first snooze + second success")
	// MaxAttempts was 2; the snooze raised it to 3, so the success on attempt 2 fits.
	var maxAttempts int
	require.NoError(t, db.Table("jobs").Select("max_attempts").Where("id = ?", id).Scan(&maxAttempts).Error)
	assert.Equal(t, 3, maxAttempts, "snooze raises max_attempts by 1 to preserve retry headroom")
}

func TestRunnerUnknownKindIsTerminallyDiscarded(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	reg := NewRegistry() // empty; nothing registered
	r := newRunner(t, db, reg)

	id, err := Insert(context.Background(), NewClient(db), successArgs{V: "orphan"}, InsertOpts{})
	require.NoError(t, err)

	runToIdle(t, context.Background(), r)

	assert.Equal(t, string(StateDiscarded), jobState(t, db, id), "an unknown kind is a permanent dispatch error")
}

func TestRunnerCtxCancelStopsRunUntilIdle(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	reg := NewRegistry()
	Register(reg, &successWorker{})
	r := newRunner(t, db, reg)

	// Enqueue one job with ScheduleAt in the far future so it never claims.
	future := time.Now().Add(time.Hour)
	_, err := Insert(context.Background(), NewClient(db), successArgs{V: "later"},
		InsertOpts{ScheduleAt: &future})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err = r.RunUntilIdle(ctx)
	require.Error(t, err, "RunUntilIdle must surface the cancelled ctx")
}

func TestRunnerSqliteDriverRejectsConcurrencyGreaterThanOne(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	reg := NewRegistry()
	Register(reg, &successWorker{})

	_, err := NewRunner(RunnerConfig{
		DB:            db,
		Driver:        NewSQLiteDriver(db),
		Registry:      reg,
		Queues:        []string{"default"},
		ExecutorClass: "local",
		Concurrency:   2,
	})
	require.ErrorIs(t, err, ErrSQLiteConcurrency)
}
