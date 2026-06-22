package flywheel

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fixedTimeout is a Timeouter that always reports the same duration.
type fixedTimeout time.Duration

func (f fixedTimeout) Timeout() time.Duration { return time.Duration(f) }

func TestRunnerResolveTimeoutPrecedence(t *testing.T) {
	t.Parallel()
	r := &Runner{cfg: RunnerConfig{DefaultTimeout: 5 * time.Second}}

	perJob := 100
	assert.Equal(t, 100*time.Millisecond, r.resolveTimeout(registryEntry{}, RawJob{TimeoutMs: &perJob}),
		"a per-job timeout wins over the per-kind and runner default")

	assert.Equal(t, 2*time.Second, r.resolveTimeout(registryEntry{timeouter: fixedTimeout(2 * time.Second)}, RawJob{}),
		"a per-kind Timeouter wins over the runner default")

	assert.Equal(t, 5*time.Second, r.resolveTimeout(registryEntry{}, RawJob{}),
		"the runner default applies when nothing else is set")

	zero := 0
	r0 := &Runner{cfg: RunnerConfig{}}
	assert.Equal(t, time.Duration(0), r0.resolveTimeout(registryEntry{}, RawJob{TimeoutMs: &zero}),
		"a non-positive per-job timeout and no defaults means no timeout")
}

func TestClientPersistsTimeoutMs(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	id, err := Insert(context.Background(), NewClient(db), successArgs{V: "x"}, InsertOpts{Timeout: 250 * time.Millisecond})
	require.NoError(t, err)

	var row struct{ TimeoutMs *int }
	require.NoError(t, db.Table("jobs").Select("timeout_ms").Where("id = ?", id).Scan(&row).Error)
	require.NotNil(t, row.TimeoutMs, "the per-job timeout is persisted")
	assert.Equal(t, 250, *row.TimeoutMs)
}

// timeoutArgs/timeoutWorker park on the first attempt until the execution
// timeout cancels the ctx, then succeed. NextRetry shortens the backoff so the
// test runs fast.
type timeoutArgs struct{}

func (timeoutArgs) Kind() string { return "test.timeout" }

type timeoutWorker struct{ calls atomic.Int32 }

func (*timeoutWorker) Kind() string                       { return "test.timeout" }
func (*timeoutWorker) NextRetry(error, int) time.Duration { return time.Millisecond }
func (w *timeoutWorker) Work(ctx context.Context, _ *Job[timeoutArgs]) (Result, error) {
	if w.calls.Add(1) == 1 {
		<-ctx.Done() // park until the per-job execution timeout fires
		return Result{}, ctx.Err()
	}
	return Result{}, nil
}

func TestRunnerWorkerTimeoutRetriesAndRecordsTimeoutOutcome(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	reg := NewRegistry()
	w := &timeoutWorker{}
	Register(reg, w)
	r := newRunner(t, db, reg)
	ctx := context.Background()

	id, err := Insert(ctx, NewClient(db), timeoutArgs{}, InsertOpts{Timeout: 20 * time.Millisecond})
	require.NoError(t, err)

	runToIdle(t, ctx, r)

	assert.Equal(t, string(StateSucceeded), jobState(t, db, id), "the job times out once, then succeeds")
	assert.EqualValues(t, 2, w.calls.Load())

	var outcomes []string
	require.NoError(t, db.Table("job_runs").Where("job_id = ?", id).Order("attempt").Pluck("outcome", &outcomes).Error)
	require.Len(t, outcomes, 2)
	assert.Equal(t, string(OutcomeTimeout), outcomes[0], "the timed-out attempt records a timeout outcome")
	assert.Equal(t, string(OutcomeSuccess), outcomes[1])

	var classes []string
	require.NoError(t, db.Table("job_runs").
		Where("job_id = ? AND error_class IS NOT NULL", id).Pluck("error_class", &classes).Error)
	require.Len(t, classes, 1)
	assert.Equal(t, string(ErrorTimeout), classes[0], "the timed-out attempt is classified as a timeout")
}

func TestRunnerDefaultTimeoutAppliesWhenNoPerJobTimeout(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	reg := NewRegistry()
	w := &timeoutWorker{}
	Register(reg, w)

	r, err := NewRunner(RunnerConfig{
		DB: db, Driver: NewSQLiteDriver(db), Registry: reg,
		Queues: []string{"default", "periodic"}, ClaimAnyClass: true, Concurrency: 1,
		DefaultTimeout: 20 * time.Millisecond,
	})
	require.NoError(t, err)

	ctx := context.Background()
	id, err := Insert(ctx, NewClient(db), timeoutArgs{}, InsertOpts{})
	require.NoError(t, err)

	runToIdle(t, ctx, r)
	assert.Equal(t, string(StateSucceeded), jobState(t, db, id), "the runner default timeout bounds the first attempt")
	assert.EqualValues(t, 2, w.calls.Load())
}
