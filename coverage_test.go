package flywheel

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	sqlite3 "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/datatypes"
	"gorm.io/gorm"
)

// --- pure unit tests (no database) -----------------------------------------

func TestDefaultBackoffGrowsThenCapsAtOneMinute(t *testing.T) {
	t.Parallel()
	assert.Equal(t, time.Second, defaultBackoff(0), "attempt below 1 is treated as 1")
	assert.Equal(t, time.Second, defaultBackoff(1))
	assert.Equal(t, 2*time.Second, defaultBackoff(2))
	assert.Equal(t, time.Minute, defaultBackoff(100), "the exponential delay caps at one minute")
}

func TestTruncateCapsAtNBytes(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "abc", truncate("abc", 5), "a short string is returned unchanged")
	assert.Equal(t, "ab", truncate("abcde", 2), "a long string is cut to n bytes")
}

func TestRawFromRowMalformedTagsErrors(t *testing.T) {
	t.Parallel()
	_, err := rawFromRow(jobRow{Tags: datatypes.JSON("not json")}, 1)
	require.Error(t, err, "an undecodable tags blob is surfaced as an error")
}

func TestRawFromRowDecodesTags(t *testing.T) {
	t.Parallel()
	rj, err := rawFromRow(jobRow{ID: "x", Tags: datatypes.JSON(`["a","b"]`)}, 3)
	require.NoError(t, err)
	assert.Equal(t, []string{"a", "b"}, rj.Tags)
	assert.Equal(t, 3, rj.Attempt)
}

func TestClassifiedErrorUnwrapAndError(t *testing.T) {
	t.Parallel()
	cause := errors.New("boom")
	ce := &classifiedError{cause: cause, class: ErrorPermanent}
	assert.Equal(t, "boom", ce.Error(), "Error renders the underlying cause")
	assert.ErrorIs(t, ce, cause, "Unwrap exposes the cause for errors.Is")
}

func TestValidationErrorErrorAndUnwrap(t *testing.T) {
	t.Parallel()
	err := newValidationError("kind", "is required")
	assert.Equal(t, "flywheel: kind is required", err.Error())
	assert.ErrorIs(t, err, ErrValidation)
}

func TestRequestIDContextRoundTrip(t *testing.T) {
	t.Parallel()
	assert.Empty(t, RequestIDFrom(WithRequestID(context.Background(), "")), "an empty id is a no-op")
	assert.Equal(t, "req-1", RequestIDFrom(WithRequestID(context.Background(), "req-1")))
}

func TestMetadataWithRequestID(t *testing.T) {
	t.Parallel()
	assert.JSONEq(t, `{"request_id":"req-1"}`, string(metadataWithRequestID(nil, "req-1")))
	assert.JSONEq(t, `{}`, string(metadataWithRequestID(nil, "")), "no base, no id is an empty object")
	assert.JSONEq(t, `{"foo":"bar","request_id":"req-2"}`,
		string(metadataWithRequestID([]byte(`{"foo":"bar"}`), "req-2")), "base is merged ahead of the id")
}

func TestRequestIDFromMetadata(t *testing.T) {
	t.Parallel()
	assert.Empty(t, requestIDFromMetadata(nil))
	assert.Empty(t, requestIDFromMetadata([]byte("not json")), "a malformed blob tolerates rather than crashes")
	assert.Equal(t, "req-9", requestIDFromMetadata([]byte(`{"request_id":"req-9"}`)))
}

func TestReconcileIndexDDLSupportedDialects(t *testing.T) {
	t.Parallel()
	pg, err := reconcileIndexDDL("postgres")
	require.NoError(t, err)
	assert.NotEmpty(t, pg)
	lite, err := reconcileIndexDDL("sqlite")
	require.NoError(t, err)
	assert.NotEmpty(t, lite)
}

func TestRunnerBackoffWithinJitterBounds(t *testing.T) {
	t.Parallel()
	r := &Runner{cfg: RunnerConfig{RetryBackoffBase: time.Second}}
	low := r.backoff(0)
	assert.Positive(t, low, "attempt below 1 is treated as 1 and still produces a positive delay")

	big := r.backoff(100)
	assert.LessOrEqual(t, big, time.Duration(float64(maxRetryBackoff)*1.25))
	assert.GreaterOrEqual(t, big, time.Duration(float64(maxRetryBackoff)*0.75),
		"a large attempt caps near the max backoff modulo jitter")
}

func TestExecutorIdentityIsNonEmpty(t *testing.T) {
	t.Parallel()
	assert.NotEmpty(t, executorIdentity(), "the identity is host:pid")
}

func TestCronBucketsInvalidExprErrors(t *testing.T) {
	t.Parallel()
	now := time.Now()
	_, _, err := cronBuckets("definitely not a cron", now.Add(-time.Hour), now, 10)
	require.Error(t, err)
}

func TestWrapByMessageForeignKeyAndDefault(t *testing.T) {
	t.Parallel()
	assert.ErrorIs(t, wrapByMessage(errors.New("FOREIGN KEY constraint failed")), ErrForeignKey)
	assert.ErrorIs(t, wrapByMessage(errors.New(`violates foreign key constraint "x"`)), ErrForeignKey)
	assert.ErrorIs(t, wrapByMessage(errors.New("some other failure")), ErrDatabaseError)
}

func TestWrapSqliteErrorForeignKeyAndDefault(t *testing.T) {
	t.Parallel()
	fk := sqlite3.Error{Code: sqlite3.ErrConstraint, ExtendedCode: sqlite3.ErrConstraintForeignKey}
	ok, wrapped := wrapSqliteError(fk)
	require.True(t, ok)
	assert.ErrorIs(t, wrapped, ErrForeignKey)

	other := sqlite3.Error{Code: sqlite3.ErrConstraint, ExtendedCode: sqlite3.ErrConstraintCheck}
	ok, wrapped = wrapSqliteError(other)
	require.True(t, ok)
	assert.ErrorIs(t, wrapped, ErrDatabaseError, "an unmapped constraint falls back to ErrDatabaseError")
}

// --- client -----------------------------------------------------------------

func TestClientWriteDBReturnsConnection(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	assert.Same(t, db, NewClient(db).WriteDB())
}

type badMarshalArgs struct{ Ch chan int }

func (badMarshalArgs) Kind() string { return "cov.badmarshal" }

func TestInsertMarshalErrorReturns(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	_, err := Insert(context.Background(), NewClient(db), badMarshalArgs{Ch: make(chan int)}, InsertOpts{})
	require.Error(t, err, "args that cannot be JSON-marshaled surface an error")
}

// --- registry ---------------------------------------------------------------

type fullArgs struct{ V string }

func (fullArgs) Kind() string { return "cov.full" }

// fullWorker implements every optional worker interface so Register captures
// each of them.
type fullWorker struct{}

func (fullWorker) Kind() string                                         { return "cov.full" }
func (fullWorker) Work(context.Context, *Job[fullArgs]) (Result, error) { return Result{}, nil }
func (fullWorker) NextRetry(error, int) time.Duration                   { return time.Second }
func (fullWorker) Classify(error) ErrorClass                            { return ErrorTransient }
func (fullWorker) Defaults() InsertOpts                                 { return InsertOpts{Queue: "q"} }
func (fullWorker) Timeout() time.Duration                               { return time.Second }

func TestRegisterCapturesOptionalInterfaces(t *testing.T) {
	t.Parallel()
	reg := NewRegistry()
	Register(reg, fullWorker{})
	e, ok := reg.lookup("cov.full")
	require.True(t, ok)
	assert.NotNil(t, e.classifier, "Classifier is captured")
	assert.NotNil(t, e.retryable, "Retryable is captured")
	assert.NotNil(t, e.defaults, "Defaults is captured")
	assert.NotNil(t, e.timeouter, "Timeouter is captured")
}

func TestClassifyUsesClassifierAndRetryableDelay(t *testing.T) {
	t.Parallel()
	reg := NewRegistry()
	Register(reg, fullWorker{})
	e, _ := reg.lookup("cov.full")

	r := &Runner{cfg: RunnerConfig{RetryBackoffBase: time.Second}}
	err := r.classify(e, errors.New("x"), RawJob{Attempt: 1})

	var ce *classifiedError
	require.ErrorAs(t, err, &ce)
	assert.Equal(t, ErrorTransient, ce.class, "the worker Classifier verdict is used")
	assert.Equal(t, time.Second, ce.retryDelay, "the worker NextRetry override is used")
}

// --- NewRunner --------------------------------------------------------------

func TestNewRunnerAppliesZeroValueDefaults(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	r, err := NewRunner(RunnerConfig{
		DB:       db,
		Driver:   NewSQLiteDriver(db),
		Registry: NewRegistry(),
		Queues:   []string{"default"},
		// ExecutorClass, Concurrency, durations and Logger left zero.
	})
	require.NoError(t, err)
	assert.Equal(t, 1, r.cfg.Concurrency)
	assert.Equal(t, AnyClass, r.cfg.ExecutorClass, "executor class defaults to the wildcard")
	assert.Equal(t, defaultLeaseDuration, r.cfg.LeaseDuration)
	assert.Equal(t, defaultPollInterval, r.cfg.PollInterval)
	assert.Equal(t, defaultRetryBackoffBase, r.cfg.RetryBackoffBase)
	assert.NotNil(t, r.cfg.Logger)
}

func TestNewRunnerRejectsMissingDependencies(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	reg := NewRegistry()
	driver := NewSQLiteDriver(db)

	tests := map[string]struct {
		cfg  RunnerConfig
		want error
	}{
		"no db":       {RunnerConfig{Driver: driver, Registry: reg, Queues: []string{"default"}}, errRunnerNeedsDB},
		"no driver":   {RunnerConfig{DB: db, Registry: reg, Queues: []string{"default"}}, errRunnerNeedsDriver},
		"no registry": {RunnerConfig{DB: db, Driver: driver, Queues: []string{"default"}}, errRunnerNeedsRegistry},
		"no queue":    {RunnerConfig{DB: db, Driver: driver, Registry: reg}, errRunnerNeedsQueue},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			_, err := NewRunner(tc.cfg)
			require.ErrorIs(t, err, tc.want)
		})
	}
}

// --- pollOnce / dispatch via a fake driver ----------------------------------

// fakeDriver is an in-memory Driver double that lets the runner be exercised at
// Concurrency > 1 (which the SQLite driver forbids) and lets stub/dequeue error
// paths be forced deterministically.
type fakeDriver struct {
	mu          sync.Mutex
	batch       []RawJob
	served      bool
	dequeueErr  error
	stubErr     error
	finalizeErr error
	finalized   int
}

func (f *fakeDriver) Dequeue(context.Context, []string, ExecutorClass, bool, int, time.Duration) ([]RawJob, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.dequeueErr != nil {
		return nil, f.dequeueErr
	}
	if f.served {
		return nil, nil
	}
	f.served = true
	return f.batch, nil
}

func (f *fakeDriver) InsertRunStub(context.Context, string, RawJob, time.Time, ExecutorClass, string) error {
	return f.stubErr
}

func (f *fakeDriver) Finalize(context.Context, RawJob, string, Result, error, time.Time) error {
	f.mu.Lock()
	f.finalized++
	f.mu.Unlock()
	return f.finalizeErr
}

func (f *fakeDriver) InsertChild(context.Context, *gorm.DB, FollowUp, string) error { return nil }

func (f *fakeDriver) Sweep(context.Context, time.Time) (int, error) { return 0, nil }

func newFakeRunner(t *testing.T, fd *fakeDriver, concurrency int) *Runner {
	t.Helper()
	reg := NewRegistry()
	Register(reg, &successWorker{})
	r, err := NewRunner(RunnerConfig{
		DB:            newDB(t),
		Driver:        fd,
		Registry:      reg,
		Queues:        []string{"default"},
		ExecutorClass: "local",
		Concurrency:   concurrency,
	})
	require.NoError(t, err)
	return r
}

func TestPollOnceConcurrentDispatchRunsWholeBatch(t *testing.T) {
	t.Parallel()
	fd := &fakeDriver{batch: []RawJob{
		{ID: "a", Kind: "test.success", Args: []byte(`{}`)},
		{ID: "b", Kind: "test.success", Args: []byte(`{}`)},
	}}
	r := newFakeRunner(t, fd, 2)

	n, err := r.pollOnce(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 2, n)
	assert.Equal(t, 2, fd.finalized, "every job in the batch is finalized")
}

func TestPollOnceConcurrentJoinsFinalizeErrors(t *testing.T) {
	t.Parallel()
	fd := &fakeDriver{
		batch:       []RawJob{{ID: "a", Kind: "test.success", Args: []byte(`{}`)}},
		finalizeErr: errors.New("finalize failed"),
	}
	r := newFakeRunner(t, fd, 2)

	_, err := r.pollOnce(context.Background())
	require.Error(t, err, "a finalize error from a concurrent dispatch is joined and surfaced")
}

func TestPollOnceSerialStubErrorPropagates(t *testing.T) {
	t.Parallel()
	fd := &fakeDriver{
		batch:   []RawJob{{ID: "a", Kind: "test.success", Args: []byte(`{}`)}},
		stubErr: errors.New("stub failed"),
	}
	r := newFakeRunner(t, fd, 1)

	_, err := r.pollOnce(context.Background())
	require.ErrorContains(t, err, "stub failed")
}

func TestPollOnceDequeueErrorPropagates(t *testing.T) {
	t.Parallel()
	fd := &fakeDriver{dequeueErr: errors.New("dequeue failed")}
	r := newFakeRunner(t, fd, 1)

	_, err := r.pollOnce(context.Background())
	require.ErrorContains(t, err, "dequeue failed")
}

// --- runner.Run loop --------------------------------------------------------

func TestRunnerRunStopsOnContextCancel(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	reg := NewRegistry()
	Register(reg, &successWorker{})
	r := newRunner(t, db, reg)

	ctx, cancel := context.WithCancel(context.Background())
	_, err := Insert(ctx, NewClient(db), successArgs{V: "x"}, InsertOpts{})
	require.NoError(t, err)

	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()

	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not stop after context cancel")
	}
}

// --- scheduler.Run loop -----------------------------------------------------

func TestSchedulerRunStopsOnContextCancel(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	sched := NewScheduler(db, NewClient(db))
	sched.tickInterval = 5 * time.Millisecond
	sched.sweepInterval = 5 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- sched.Run(ctx) }()

	time.Sleep(40 * time.Millisecond) // let both the tick and sweep tickers fire
	cancel()

	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(2 * time.Second):
		t.Fatal("scheduler Run did not stop after context cancel")
	}
}

func TestSchedulerFirePeriodicWithoutScheduleErrors(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	sched := NewScheduler(db, NewClient(db))

	now := time.Now().UTC().Truncate(time.Second)
	ctx := clockCtx(context.Background(), NewFixedClock(now))

	// Seed a row with neither cron_expr nor interval_seconds, bypassing the
	// BeforeSave validation hook with raw SQL.
	require.NoError(t, db.Exec(
		`INSERT INTO job_periodics(id, slug, kind, args_template, queue, next_run_at, is_active, created_at, updated_at)
		 VALUES (?,?,?,?,?,?,?,?,?)`,
		NewID(), "cov-noplan", "cov.k", "{}", "periodic", now.Add(-time.Minute), true, now, now,
	).Error)

	_, err := sched.Tick(ctx)
	require.ErrorIs(t, err, errPeriodicNoSchedule)
}

// --- sqlite driver ----------------------------------------------------------

func TestSQLiteDequeueExecutorClassFilterAndEarlyReturns(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	d := NewSQLiteDriver(db)
	ctx := context.Background()

	out, err := d.Dequeue(ctx, []string{"default"}, AnyClass, true, 0, time.Second)
	require.NoError(t, err)
	assert.Empty(t, out, "a non-positive limit claims nothing")

	out, err = d.Dequeue(ctx, nil, AnyClass, true, 5, time.Second)
	require.NoError(t, err)
	assert.Empty(t, out, "no queues claims nothing")

	id, err := Insert(ctx, NewClient(db), successArgs{V: "x"}, InsertOpts{ExecutorClass: "gpu"})
	require.NoError(t, err)

	// A runner serving a different, non-wildcard class does not claim the
	// gpu-routed job through the executor_class filter.
	none, err := d.Dequeue(ctx, []string{"default"}, ExecutorClass("cpu"), false, 5, time.Second)
	require.NoError(t, err)
	assert.Empty(t, none, "a cpu executor does not claim a gpu-routed job")

	claimed, err := d.Dequeue(ctx, []string{"default"}, ExecutorClass("gpu"), false, 5, time.Second)
	require.NoError(t, err)
	require.Len(t, claimed, 1, "a gpu executor claims a gpu-routed job via the executor_class filter")
	assert.Equal(t, id, claimed[0].ID)
}

// --- baseDriver direct paths ------------------------------------------------

func TestInsertRunStubDuplicateIDErrors(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	d := baseDriver{db: db}
	ctx := context.Background()
	raw := RawJob{ID: "job-1", Attempt: 1}
	runID := NewID()

	require.NoError(t, d.InsertRunStub(ctx, runID, raw, time.Now(), ExecutorClass("local"), "h1"))
	err := d.InsertRunStub(ctx, runID, raw, time.Now(), ExecutorClass("local"), "h1")
	require.Error(t, err, "a duplicate run-stub primary key surfaces an error")
}

func TestFinalizeOutputMarshalErrors(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	d := baseDriver{db: db}
	ctx := context.Background()

	raw := RawJob{ID: "job-x", Attempt: 1, MaxAttempts: 5}
	runID := NewID()
	require.NoError(t, d.InsertRunStub(ctx, runID, raw, time.Now(), ExecutorClass("local"), "h1"))

	err := d.Finalize(ctx, raw, runID, Result{Output: make(chan int)}, nil, time.Now())
	require.Error(t, err, "an unmarshalable worker Output surfaces a finalize error")
}

// --- follow-ups, output, cost, truncation through the runner ----------------

type fanoutParentArgs struct{}

func (fanoutParentArgs) Kind() string { return "cov.parent" }

type fanoutChildArgs struct{ N int }

func (fanoutChildArgs) Kind() string { return "cov.child" }

type fanoutParentWorker struct{}

func (fanoutParentWorker) Kind() string { return "cov.parent" }
func (fanoutParentWorker) Work(context.Context, *Job[fanoutParentArgs]) (Result, error) {
	return Result{
		Output:     map[string]any{"ok": true},
		CostMicros: 42,
		FollowUps: []FollowUp{
			{Kind: "cov.child", Args: fanoutChildArgs{N: 1}, Parent: true},
			{Kind: "cov.child", Args: fanoutChildArgs{N: 2}, UniqueKey: "cov-uk-dup"},
		},
	}, nil
}

type fanoutChildWorker struct{}

func (fanoutChildWorker) Kind() string { return "cov.child" }
func (fanoutChildWorker) Work(context.Context, *Job[fanoutChildArgs]) (Result, error) {
	return Result{}, nil
}

func TestRunnerFollowUpsInsertChildSkipCollisionAndRecordOutput(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	reg := NewRegistry()
	Register(reg, fanoutParentWorker{})
	Register(reg, fanoutChildWorker{})
	r := newRunner(t, db, reg)
	ctx := context.Background()
	c := NewClient(db)

	// Pre-seed a job holding the unique_key one follow-up will collide with, so
	// that follow-up is skipped (not enqueued) rather than aborting finalize.
	_, err := Enqueue(ctx, c, "cov.child", []byte(`{"N":99}`), InsertOpts{UniqueKey: "cov-uk-dup"})
	require.NoError(t, err)

	parentID, err := Insert(ctx, c, fanoutParentArgs{}, InsertOpts{})
	require.NoError(t, err)

	runToIdle(t, ctx, r)

	assert.Equal(t, string(StateSucceeded), jobState(t, db, parentID))

	// The parent run row records the output, cost, and how many children landed.
	var run struct {
		EnqueuedChildren int
		CostMicros       *int64
		Output           []byte
	}
	require.NoError(t, db.Table("job_runs").
		Select("enqueued_children, cost_micros, output").
		Where("job_id = ?", parentID).Scan(&run).Error)
	assert.Equal(t, 1, run.EnqueuedChildren, "the colliding follow-up is skipped; only one child is enqueued")
	require.NotNil(t, run.CostMicros)
	assert.EqualValues(t, 42, *run.CostMicros)
	assert.NotEmpty(t, run.Output, "the worker Output is persisted")

	// Exactly one child carries the parent link (the Parent:true follow-up).
	var withParent int64
	require.NoError(t, db.Table("jobs").
		Where("kind = ? AND parent_job_id = ?", "cov.child", parentID).Count(&withParent).Error)
	assert.EqualValues(t, 1, withParent)
}

type longErrArgs struct{}

func (longErrArgs) Kind() string { return "cov.longerr" }

type longErrWorker struct{}

func (longErrWorker) Kind() string              { return "cov.longerr" }
func (longErrWorker) Classify(error) ErrorClass { return ErrorPermanent }
func (longErrWorker) Work(context.Context, *Job[longErrArgs]) (Result, error) {
	return Result{}, errors.New(strings.Repeat("x", maxErrorMessage+904))
}

func TestRunnerTruncatesLongErrorMessage(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	reg := NewRegistry()
	Register(reg, longErrWorker{})
	r := newRunner(t, db, reg)
	ctx := context.Background()

	id, err := Insert(ctx, NewClient(db), longErrArgs{}, InsertOpts{})
	require.NoError(t, err)

	runToIdle(t, ctx, r)

	assert.Equal(t, string(StateDiscarded), jobState(t, db, id))

	var msg string
	require.NoError(t, db.Table("job_runs").
		Select("error_message").Where("job_id = ?", id).Scan(&msg).Error)
	assert.Len(t, msg, maxErrorMessage, "the stored error message is capped at maxErrorMessage bytes")

	var payload string
	require.NoError(t, db.Table("job_runs").
		Select("error_payload").Where("job_id = ?", id).Scan(&payload).Error)
	assert.NotEmpty(t, payload, "an error payload is recorded alongside the message")
}

type cancelArgs struct{}

func (cancelArgs) Kind() string { return "cov.cancel" }

type cancelWorker struct{}

func (cancelWorker) Kind() string { return "cov.cancel" }
func (cancelWorker) Work(context.Context, *Job[cancelArgs]) (Result, error) {
	return Result{Cancel: true}, nil
}

func TestRunnerCancelResultMovesJobToCancelled(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	reg := NewRegistry()
	Register(reg, cancelWorker{})
	r := newRunner(t, db, reg)
	ctx := context.Background()

	id, err := Insert(ctx, NewClient(db), cancelArgs{}, InsertOpts{})
	require.NoError(t, err)

	runToIdle(t, ctx, r)
	assert.Equal(t, string(StateCancelled), jobState(t, db, id))
}

type reqIDArgs struct{}

func (reqIDArgs) Kind() string { return "cov.reqid" }

// reqIDWorker captures the request_id the runner threaded onto its ctx.
type reqIDWorker struct{ got string }

func (*reqIDWorker) Kind() string { return "cov.reqid" }
func (w *reqIDWorker) Work(ctx context.Context, _ *Job[reqIDArgs]) (Result, error) {
	w.got = RequestIDFrom(ctx)
	return Result{}, nil
}

func TestRunnerThreadsRequestIDFromMetadataToWorkerCtx(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	reg := NewRegistry()
	w := &reqIDWorker{}
	Register(reg, w)
	r := newRunner(t, db, reg)
	ctx := context.Background()

	_, err := Insert(ctx, NewClient(db), reqIDArgs{}, InsertOpts{RequestID: "req-runner"})
	require.NoError(t, err)

	runToIdle(t, ctx, r)
	assert.Equal(t, "req-runner", w.got, "the runner reads request_id from metadata onto the worker ctx")
}

// --- base.go lifecycle hooks -----------------------------------------------

func TestJobRunRowBeforeCreateDefaultsTimestamps(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	row := jobRunRow{JobID: "j", ExecutorClass: "local", ExecutorID: "h", Outcome: string(OutcomeStarted)}
	require.NoError(t, db.Create(&row).Error)
	assert.NotEmpty(t, row.ID, "the id is minted")
	assert.False(t, row.StartedAt.IsZero(), "StartedAt defaults to the clock's now")
	assert.False(t, row.CreatedAt.IsZero(), "CreatedAt defaults to the clock's now")
}

// --- read.go error paths ----------------------------------------------------

func TestReadPathsSurfaceDBErrors(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	sqlDB, err := db.DB()
	require.NoError(t, err)
	require.NoError(t, sqlDB.Close()) // force every subsequent query to error

	ctx := context.Background()
	_, err = FindJob(ctx, db, "x")
	assert.Error(t, err)
	_, err = ListRuns(ctx, db, "x", ListRunsParams{Before: time.Now(), Limit: 5})
	assert.Error(t, err)
	_, err = Overview(ctx, db, OverviewParams{Kind: "k"})
	assert.Error(t, err)
	_, err = ListActiveByKind(ctx, db, "k")
	assert.Error(t, err)
	_, err = CountRuns(ctx, db)
	assert.Error(t, err)
	_, err = CountActiveJobs(ctx, db)
	assert.Error(t, err)
}

func TestPendingCountSurfacesDBError(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	r := newRunner(t, db, NewRegistry())
	sqlDB, err := db.DB()
	require.NoError(t, err)
	require.NoError(t, sqlDB.Close())

	_, err = r.pendingCount(context.Background())
	require.Error(t, err)
}

func TestSweepSurfacesDBError(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	d := baseDriver{db: db}
	sqlDB, err := db.DB()
	require.NoError(t, err)
	require.NoError(t, sqlDB.Close())

	_, err = d.Sweep(context.Background(), time.Now())
	require.Error(t, err)
}

// --- httpdoer ---------------------------------------------------------------

func TestDefaultHTTPDoerPerformsRealRequest(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusTeapot)
		_, _ = io.WriteString(w, "brewing")
	}))
	defer srv.Close()

	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, srv.URL, nil)
	require.NoError(t, err)

	resp, err := DefaultHTTPDoer().Do(req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()

	assert.Equal(t, http.StatusTeapot, resp.StatusCode)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Equal(t, "brewing", string(body))
}

func TestFakeHTTPDoerStubDefaultBodyReadErrorFailsUnstubbedReads(t *testing.T) {
	t.Parallel()
	readErr := errors.New("truncated stream")
	doer := NewFakeHTTPDoer().StubDefaultBodyReadError(http.StatusOK, readErr)

	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "https://example.test/x", nil)
	require.NoError(t, err)

	resp, err := doer.Do(req)
	require.NoError(t, err)
	defer func() { _ = resp.Body.Close() }()

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	_, readBackErr := io.ReadAll(resp.Body)
	require.ErrorIs(t, readBackErr, readErr, "the default body-read error applies to any unstubbed request")
}
