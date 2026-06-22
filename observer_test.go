package flywheel

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// recordingObserver captures every lifecycle event for assertion. It is
// concurrency-safe so it can be shared across a concurrent dispatch batch.
type recordingObserver struct {
	mu       sync.Mutex
	claims   []ClaimEvent
	starts   []JobEvent
	finishes []FinishEvent
	retries  []RetryEvent
}

func (o *recordingObserver) OnClaim(_ context.Context, ev ClaimEvent) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.claims = append(o.claims, ev)
}

func (o *recordingObserver) OnStart(_ context.Context, ev JobEvent) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.starts = append(o.starts, ev)
}

func (o *recordingObserver) OnFinish(_ context.Context, ev FinishEvent) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.finishes = append(o.finishes, ev)
}

func (o *recordingObserver) OnRetry(_ context.Context, ev RetryEvent) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.retries = append(o.retries, ev)
}

func (o *recordingObserver) snapshot() (claims []ClaimEvent, starts []JobEvent, finishes []FinishEvent, retries []RetryEvent) {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]ClaimEvent(nil), o.claims...),
		append([]JobEvent(nil), o.starts...),
		append([]FinishEvent(nil), o.finishes...),
		append([]RetryEvent(nil), o.retries...)
}

// newObservedRunner builds a SQLite runner wired to obs.
func newObservedRunner(t *testing.T, db *gorm.DB, reg *Registry, obs Observer) *Runner {
	t.Helper()
	r, err := NewRunner(RunnerConfig{
		DB: db, Driver: NewSQLiteDriver(db), Registry: reg,
		Queues: []string{"default", "periodic"}, ClaimAnyClass: true,
		Concurrency: 1, Observer: obs,
	})
	require.NoError(t, err)
	return r
}

func TestNewRunnerDefaultsObserverToNoop(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	r := newRunner(t, db, NewRegistry())
	assert.NotNil(t, r.cfg.Observer, "a nil Observer is defaulted to the internal no-op")
	assert.IsType(t, noopObserver{}, r.cfg.Observer)
}

func TestObserverReceivesClaimStartFinishOnSuccess(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	reg := NewRegistry()
	Register(reg, &successWorker{})
	obs := &recordingObserver{}
	r := newObservedRunner(t, db, reg, obs)
	ctx := context.Background()

	id, err := Insert(ctx, NewClient(db), successArgs{V: "x"}, InsertOpts{})
	require.NoError(t, err)

	runToIdle(t, ctx, r)

	claims, starts, finishes, retries := obs.snapshot()
	require.NotEmpty(t, claims)
	assert.Equal(t, 1, claims[0].Claimed, "the claim event reports the batch size")

	require.Len(t, starts, 1)
	assert.Equal(t, id, starts[0].JobID)
	assert.Equal(t, "test.success", starts[0].Kind)
	assert.Equal(t, 1, starts[0].Attempt)

	require.Len(t, finishes, 1)
	assert.Equal(t, id, finishes[0].JobID)
	assert.Equal(t, OutcomeSuccess, finishes[0].Outcome)
	assert.NoError(t, finishes[0].Err)
	assert.Empty(t, string(finishes[0].ErrorClass), "success carries no error class")

	assert.Empty(t, retries, "a successful job triggers no retry event")
}

func TestObserverOnRetryFiresForTransientError(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	reg := NewRegistry()
	Register(reg, &retryWorker{failuresBefore: 1})
	obs := &recordingObserver{}
	r := newObservedRunner(t, db, reg, obs)
	ctx := context.Background()

	id, err := Insert(ctx, NewClient(db), retryArgs{V: "x"}, InsertOpts{})
	require.NoError(t, err)

	runToIdle(t, ctx, r)

	_, starts, finishes, retries := obs.snapshot()
	assert.Len(t, starts, 2, "the worker started twice — one failure, one success")
	require.Len(t, retries, 1, "exactly one retry was scheduled")
	assert.Equal(t, id, retries[0].JobID)
	assert.Equal(t, 2, retries[0].NextAttempt, "the retry runs as attempt 2")
	assert.Equal(t, ErrorTransient, retries[0].ErrorClass)
	assert.Positive(t, retries[0].Delay, "the retry carries a positive backoff delay")

	// Two finishes: the first an error (retryable), the second a success.
	require.Len(t, finishes, 2)
	assert.Equal(t, OutcomeError, finishes[0].Outcome)
	assert.Equal(t, OutcomeSuccess, finishes[1].Outcome)
}

func TestObserverOnFinishForUnknownKindHasNoStart(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	obs := &recordingObserver{}
	r := newObservedRunner(t, db, NewRegistry(), obs) // empty registry
	ctx := context.Background()

	id, err := Insert(ctx, NewClient(db), successArgs{V: "orphan"}, InsertOpts{})
	require.NoError(t, err)

	runToIdle(t, ctx, r)

	_, starts, finishes, retries := obs.snapshot()
	assert.Empty(t, starts, "an unknown kind never starts a worker")
	require.Len(t, finishes, 1)
	assert.Equal(t, id, finishes[0].JobID)
	assert.Equal(t, OutcomeError, finishes[0].Outcome)
	assert.Equal(t, ErrorPermanent, finishes[0].ErrorClass, "an unknown kind is a permanent error")
	assert.Empty(t, retries, "a permanent error does not retry")
}

func TestNoopObserverMethodsDoNotPanic(t *testing.T) {
	t.Parallel()
	var o noopObserver
	ctx := context.Background()
	assert.NotPanics(t, func() {
		o.OnClaim(ctx, ClaimEvent{})
		o.OnStart(ctx, JobEvent{})
		o.OnFinish(ctx, FinishEvent{})
		o.OnRetry(ctx, RetryEvent{})
	})
}
