package flywheel

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"os"
	"strconv"
	"sync"
	"time"

	"gorm.io/gorm"
)

// Runner defaults applied when a RunnerConfig field is left zero.
const (
	defaultLeaseDuration    = 30 * time.Second
	defaultPollInterval     = 100 * time.Millisecond
	defaultRetryBackoffBase = time.Second
	maxRetryBackoff         = time.Minute
	backoffJitterSpread     = 0.5 // ±25% — the jitter multiplier spans [0.75, 1.25).
)

// nonTerminalStates are the job states that keep RunUntilIdle polling.
var nonTerminalStates = []string{ //nolint:gochecknoglobals // intentional shared constant slice
	string(StateAvailable), string(StateRunning), string(StateRetryable), string(StateScheduled),
}

// RunnerConfig configures a Runner.
type RunnerConfig struct {
	// DB is the database the Runner reads queue state from (RunUntilIdle).
	DB *gorm.DB
	// Driver claims and finalizes jobs.
	Driver Driver
	// Registry maps job kinds to workers.
	Registry *Registry
	// Queues are the logical queues this Runner claims from.
	Queues []string
	// ExecutorClass is the routing label this Runner serves: it claims jobs whose
	// executor_class equals it (or is the empty wildcard) unless ClaimAnyClass is
	// set, and stamps it on every job_runs row this Runner writes.
	ExecutorClass ExecutorClass
	// ClaimAnyClass, when true, makes this Runner claim jobs of every executor
	// class, not only its own class and the wildcard. A single-node local
	// deployment typically sets it so one Runner drains the whole queue.
	ClaimAnyClass bool
	// LeaseDuration is the visibility timeout on a claimed job.
	LeaseDuration time.Duration
	// PollInterval is the pause between empty polls.
	PollInterval time.Duration
	// Concurrency is the number of jobs claimed and run per poll. A SQLite
	// driver requires 1.
	Concurrency int
	// RetryBackoffBase is the base delay for the exponential retry backoff.
	// Optional; defaults to one second.
	RetryBackoffBase time.Duration
	// DefaultTimeout, when > 0, is the execution ceiling applied to every attempt
	// that specifies no timeout of its own (per-job InsertOpts.Timeout or per-kind
	// Timeouter). Optional; zero means no default timeout.
	DefaultTimeout time.Duration
	// Observer, when set, receives lifecycle events (claim/start/finish/retry) for
	// metrics or tracing. Optional; a nil Observer installs an internal no-op.
	Observer Observer
	// Logger is the base logger bound onto each Job. Optional.
	Logger *slog.Logger
}

// Runner claims jobs from a Driver and dispatches them to registered workers.
type Runner struct {
	cfg        RunnerConfig
	executorID string
}

// NewRunner validates cfg and returns a Runner. It returns ErrSQLiteConcurrency
// when a SQLite driver is wired with Concurrency greater than 1 (FR-039).
//
//nolint:gocognit,gocyclo // straight-line config validation and zero-value defaulting
func NewRunner(cfg RunnerConfig) (*Runner, error) {
	if cfg.DB == nil {
		return nil, errRunnerNeedsDB
	}
	if cfg.Driver == nil {
		return nil, errRunnerNeedsDriver
	}
	if cfg.Registry == nil {
		return nil, errRunnerNeedsRegistry
	}
	if len(cfg.Queues) == 0 {
		return nil, errRunnerNeedsQueue
	}
	if _, isSQLite := cfg.Driver.(*sqliteDriver); isSQLite && cfg.Concurrency > 1 {
		return nil, ErrSQLiteConcurrency
	}

	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 1
	}
	if cfg.LeaseDuration <= 0 {
		cfg.LeaseDuration = defaultLeaseDuration
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = defaultPollInterval
	}
	if cfg.RetryBackoffBase <= 0 {
		cfg.RetryBackoffBase = defaultRetryBackoffBase
	}
	if cfg.Observer == nil {
		cfg.Observer = noopObserver{}
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}

	return &Runner{cfg: cfg, executorID: executorIdentity()}, nil
}

// Run drives the dispatch loop until ctx is cancelled.
func (r *Runner) Run(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("jobs: runner stopped: %w", err)
		}
		claimed, err := r.pollOnce(ctx)
		if err != nil {
			r.cfg.Logger.ErrorContext(ctx, "jobs: poll failed", "error", err)
		}
		if claimed == 0 {
			select {
			case <-ctx.Done():
				return fmt.Errorf("jobs: runner stopped: %w", ctx.Err())
			case <-time.After(r.cfg.PollInterval):
			}
		}
	}
}

// RunUntilIdle drives the dispatch loop until every job has reached a terminal
// state, then returns. It is the deterministic test driver.
//
//nolint:gocognit // a single poll-drain-wait loop; splitting it obscures the flow
func (r *Runner) RunUntilIdle(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("jobs: run-until-idle stopped: %w", err)
		}
		claimed, err := r.pollOnce(ctx)
		if err != nil {
			return err
		}
		if claimed > 0 {
			continue
		}
		pending, err := r.pendingCount(ctx)
		if err != nil {
			return err
		}
		if pending == 0 {
			return nil
		}
		// Jobs remain but none are claimable yet (retry/snooze backoff);
		// wait one interval and poll again.
		select {
		case <-ctx.Done():
			return fmt.Errorf("jobs: run-until-idle stopped: %w", ctx.Err())
		case <-time.After(r.cfg.PollInterval):
		}
	}
}

// pendingCount reports how many jobs are still in a non-terminal state.
func (r *Runner) pendingCount(ctx context.Context) (int64, error) {
	var count int64
	if err := r.cfg.DB.WithContext(ctx).Model(&jobRow{}).
		Where("state IN ?", nonTerminalStates).Count(&count).Error; err != nil {
		return 0, fmt.Errorf("jobs: count pending: %w", err)
	}
	return count, nil
}

// pollOnce claims one batch and dispatches it, returning the batch size.
func (r *Runner) pollOnce(ctx context.Context) (int, error) {
	batch, err := r.cfg.Driver.Dequeue(
		ctx, r.cfg.Queues, r.cfg.ExecutorClass, r.cfg.ClaimAnyClass, r.cfg.Concurrency, r.cfg.LeaseDuration,
	)
	if err != nil {
		return 0, err
	}
	if len(batch) == 0 {
		return 0, nil
	}
	r.cfg.Observer.OnClaim(ctx, ClaimEvent{
		ExecutorClass: r.cfg.ExecutorClass,
		Queues:        r.cfg.Queues,
		Claimed:       len(batch),
	})
	if r.cfg.Concurrency == 1 {
		for i := range batch {
			if dispatchErr := r.dispatch(ctx, batch[i]); dispatchErr != nil {
				return len(batch), dispatchErr
			}
		}
		return len(batch), nil
	}

	errs := make([]error, len(batch))
	var wg sync.WaitGroup
	for i := range batch {
		wg.Go(func() {
			errs[i] = r.dispatch(ctx, batch[i])
		})
	}
	wg.Wait()
	return len(batch), errors.Join(errs...)
}

// dispatch runs one claimed job: it pre-allocates the audit stub, runs the
// worker outside any transaction with panic recovery, then finalizes.
func (r *Runner) dispatch(ctx context.Context, raw RawJob) error {
	runID := NewID()
	startedAt := ClockFrom(ctx).Now(ctx)

	if err := r.cfg.Driver.InsertRunStub(
		ctx, runID, raw, startedAt, r.cfg.ExecutorClass, r.executorID,
	); err != nil {
		return err
	}

	jobEv := JobEvent{JobID: raw.ID, RunID: runID, Kind: raw.Kind, Queue: raw.Queue, Attempt: raw.Attempt}

	entry, known := r.cfg.Registry.lookup(raw.Kind)
	if !known {
		finishedAt := ClockFrom(ctx).Now(ctx)
		unknown := &classifiedError{cause: ErrUnknownKind, class: ErrorPermanent}
		r.observe(ctx, raw, jobEv, Result{}, unknown, startedAt, finishedAt)
		return r.cfg.Driver.Finalize(ctx, raw, runID, Result{}, unknown, finishedAt)
	}

	logger := r.cfg.Logger.With("job_id", raw.ID, "kind", raw.Kind, "run_id", runID)
	if reqID := requestIDFromMetadata(raw.Metadata); reqID != "" {
		ctx = WithRequestID(ctx, reqID)
		logger = logger.With("request_id", reqID)
	}

	in := dispatchInput{
		ID:          raw.ID,
		Kind:        raw.Kind,
		Queue:       raw.Queue,
		RawArgs:     raw.Args,
		Attempt:     raw.Attempt,
		MaxAttempts: raw.MaxAttempts,
		ParentJobID: raw.ParentJobID,
		EnqueuedAt:  raw.ScheduledAt,
		Tags:        raw.Tags,
		Logger:      logger,
		RunID:       runID,
	}

	r.cfg.Observer.OnStart(ctx, jobEv)

	workCtx := ctx
	if d := r.resolveTimeout(entry, raw); d > 0 {
		var cancel context.CancelFunc
		workCtx, cancel = context.WithTimeout(ctx, d)
		defer cancel()
	}

	result, workErr := r.runWork(workCtx, entry, in)
	finishedAt := ClockFrom(ctx).Now(ctx)

	var finalErr error
	if workErr != nil {
		finalErr = r.classify(entry, workErr, raw)
	}
	r.observe(ctx, raw, jobEv, result, finalErr, startedAt, finishedAt)
	// Finalize on the parent ctx, not the (possibly expired) workCtx, so a
	// timed-out attempt still records its outcome.
	return r.cfg.Driver.Finalize(ctx, raw, runID, result, finalErr, finishedAt)
}

// observe emits the OnFinish event (and OnRetry when the attempt will retry) for
// one finalized attempt. It reuses planFinalize so the observer sees the same
// outcome, error class, and retry delay the Driver persists.
func (r *Runner) observe(
	ctx context.Context, raw RawJob, ev JobEvent, result Result, finalErr error, startedAt, finishedAt time.Time,
) {
	plan := planFinalize(raw, result, finalErr, finishedAt)
	finish := FinishEvent{
		JobEvent: ev,
		Outcome:  plan.runOutcome,
		Err:      finalErr,
		Duration: finishedAt.Sub(startedAt),
	}
	if plan.errorClass != nil {
		finish.ErrorClass = *plan.errorClass
	}
	r.cfg.Observer.OnFinish(ctx, finish)

	if plan.jobState == StateRetryable {
		var delay time.Duration
		if plan.scheduledAt != nil {
			delay = plan.scheduledAt.Sub(finishedAt)
		}
		r.cfg.Observer.OnRetry(ctx, RetryEvent{
			JobEvent:    ev,
			NextAttempt: ev.Attempt + 1,
			Delay:       delay,
			ErrorClass:  finish.ErrorClass,
		})
	}
}

// resolveTimeout selects the execution timeout for an attempt, preferring the
// per-job timeout, then the worker's per-kind Timeouter, then the runner's
// DefaultTimeout. A zero result means no timeout is applied.
func (r *Runner) resolveTimeout(entry registryEntry, raw RawJob) time.Duration {
	if raw.TimeoutMs != nil && *raw.TimeoutMs > 0 {
		return time.Duration(*raw.TimeoutMs) * time.Millisecond
	}
	if entry.timeouter != nil {
		if d := entry.timeouter.Timeout(); d > 0 {
			return d
		}
	}
	return r.cfg.DefaultTimeout
}

// runWork invokes the worker, recovering a panic into an error so the executor
// survives it (FR-011, SC-008).
func (r *Runner) runWork(
	ctx context.Context, entry registryEntry, in dispatchInput,
) (result Result, err error) {
	defer func() {
		if rec := recover(); rec != nil {
			result = Result{}
			err = fmt.Errorf("%w: %v", errWorkerPanicked, rec)
		}
	}()
	return entry.dispatch(ctx, in)
}

// classify wraps a worker error with the Runner's verdict — the error class
// (worker Classifier or transient) and the retry delay (worker Retryable or
// the exponential backoff) — for the Driver to apply.
func (r *Runner) classify(entry registryEntry, workErr error, raw RawJob) error {
	class := ErrorTransient
	if entry.classifier != nil {
		if c := entry.classifier.Classify(workErr); c != "" {
			class = c
		}
	}
	// An execution-timeout deadline always classifies as timeout, overriding any
	// worker classifier, so a hung attempt is distinguishable in the audit trail.
	if errors.Is(workErr, context.DeadlineExceeded) {
		class = ErrorTimeout
	}
	var delay time.Duration
	if entry.retryable != nil {
		delay = entry.retryable.NextRetry(workErr, raw.Attempt)
	}
	if delay <= 0 {
		delay = r.backoff(raw.Attempt)
	}
	return &classifiedError{cause: workErr, class: class, retryDelay: delay}
}

// backoff is the exponential retry delay with ±25% jitter.
func (r *Runner) backoff(attempt int) time.Duration {
	delay := expBackoff(r.cfg.RetryBackoffBase, maxRetryBackoff, attempt)
	jitter := (1.0 - backoffJitterSpread/2) + rand.Float64()*backoffJitterSpread //nolint:gosec // jitter, not security
	return time.Duration(float64(delay) * jitter)
}

// executorIdentity returns this process's executor identity (hostname:pid).
func executorIdentity() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "unknown"
	}
	return host + ":" + strconv.Itoa(os.Getpid())
}
