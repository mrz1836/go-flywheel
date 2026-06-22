// Package flywheel is a durable, Postgres- and SQLite-backed background-work
// runtime: it enqueues, dispatches, retries, audits, schedules, and recovers
// typed jobs.
//
// The package is self-contained: it owns its own row and typed structs
// (RawJob, Job[A]) and reaches the database through a two-implementation Driver
// seam, so the runtime code never sees the SQL dialect. It depends only on
// gorm, the cron parser, and the standard library — no host application
// packages — which is what lets it ship as a standalone module.
package flywheel

import (
	"context"
	"log/slog"
	"time"

	"gorm.io/gorm"
)

// ExecutorClass is a free-form routing label that pairs a job with the executor
// pool eligible to run it. It is a convention, not a closed enum: a local or
// SQLite deployment uses "local" (or the empty wildcard), an AWS deployment uses
// "lambda" or "ecs", and a specialized pool uses any string you like ("gpu",
// "high-mem", ...). Its string value is the stable wire form persisted on
// jobs.executor_class and job_runs.executor_class.
//
// The empty class — AnyClass — is the wildcard: a job carrying it may be claimed
// by any runner, and a runner configured with ClaimAnyClass claims jobs of every
// class. Routing a job to a dedicated pool is just a matter of giving the job and
// that pool's runner the same non-empty class.
type ExecutorClass string

// AnyClass is the empty wildcard executor class. A job inserted with AnyClass
// (the default) is claimable by every runner regardless of the runner's own
// class; it is the right choice whenever a job is not pinned to a specific
// executor pool.
const AnyClass ExecutorClass = ""

// ErrorClass classifies a worker error. Permanent and validation errors stop
// retrying; transient and timeout errors are retried.
type ErrorClass string

// Recognized ErrorClass values.
const (
	ErrorTransient  ErrorClass = "transient"
	ErrorPermanent  ErrorClass = "permanent"
	ErrorValidation ErrorClass = "validation"
	ErrorTimeout    ErrorClass = "timeout"
)

// Valid reports whether c is a recognized ErrorClass.
func (c ErrorClass) Valid() bool {
	switch c {
	case ErrorTransient, ErrorPermanent, ErrorValidation, ErrorTimeout:
		return true
	default:
		return false
	}
}

// JobState is a job's lifecycle state. Its string values are the stable wire
// form persisted on jobs rows.
type JobState string

// Recognized JobState values.
const (
	StateAvailable JobState = "available"
	StateRunning   JobState = "running"
	StateRetryable JobState = "retryable"
	StateScheduled JobState = "scheduled"
	StateSucceeded JobState = "succeeded"
	StateCancelled JobState = "cancelled"
	StateDiscarded JobState = "discarded"
)

// Valid reports whether s is a recognized JobState.
func (s JobState) Valid() bool {
	switch s {
	case StateAvailable, StateRunning, StateRetryable, StateScheduled,
		StateSucceeded, StateCancelled, StateDiscarded:
		return true
	default:
		return false
	}
}

// RunOutcome is the outcome of a single job attempt. Its string values are the
// stable wire form persisted on job_runs rows.
type RunOutcome string

// Recognized RunOutcome values.
const (
	OutcomeStarted   RunOutcome = "started"
	OutcomeSuccess   RunOutcome = "success"
	OutcomeError     RunOutcome = "error"
	OutcomeSnooze    RunOutcome = "snooze"
	OutcomeCancelled RunOutcome = "cancelled"
	OutcomeTimeout   RunOutcome = "timeout"
	OutcomeCrashed   RunOutcome = "crashed"
)

// Valid reports whether o is a recognized RunOutcome.
func (o RunOutcome) Valid() bool {
	switch o {
	case OutcomeStarted, OutcomeSuccess, OutcomeError, OutcomeSnooze,
		OutcomeCancelled, OutcomeTimeout, OutcomeCrashed:
		return true
	default:
		return false
	}
}

// Args is any JSON-serializable struct passed to a worker.
type Args any

// Worker is the interface domain authors implement. The runtime ships the
// interface; implementations arrive in later phases.
type Worker[A Args] interface {
	// Kind is the stable worker name, persisted as jobs.kind.
	Kind() string
	// Work runs the job. It executes outside any transaction.
	Work(ctx context.Context, job *Job[A]) (Result, error)
}

// Retryable is an optional worker interface. When implemented, the Runner uses
// NextRetry to compute the backoff delay instead of the default schedule.
type Retryable interface {
	NextRetry(err error, attempt int) time.Duration
}

// Classifier is an optional worker interface. When implemented, the Runner uses
// Classify to decide whether an error is retried.
type Classifier interface {
	Classify(err error) ErrorClass
}

// Defaults is an optional worker interface. When implemented, the returned
// InsertOpts seed a producer's Insert for this kind.
type Defaults interface {
	Defaults() InsertOpts
}

// Timeouter is an optional worker interface. When implemented, the Runner cancels
// the worker's ctx after the returned duration, turning a hung attempt into a
// context.DeadlineExceeded that retries via the normal backoff and records a
// timeout outcome. A zero or negative duration means no per-kind timeout; an
// InsertOpts.Timeout overrides it. A worker that ignores ctx cancellation still
// runs to completion — the lease sweep remains the ultimate backstop.
type Timeouter interface {
	Timeout() time.Duration
}

// Job is what a worker receives. RunID and Logger are injected by the Runner.
type Job[A Args] struct {
	ID          string
	Kind        string
	Queue       string
	Args        A
	Attempt     int
	MaxAttempts int
	ParentJobID *string
	EnqueuedAt  time.Time
	Tags        []string
	Logger      *slog.Logger
	// RunID is the pre-allocated job_runs.id for this attempt. A side-effect
	// row may set its job_run_id to RunID safely — the run row already exists.
	RunID string
}

// RawJob is a claimed jobs row as the Driver returns it, before the Runner
// binds it into a typed Job[A].
type RawJob struct {
	ID          string
	Kind        string
	Queue       string
	Args        []byte
	Attempt     int
	MaxAttempts int
	// TimeoutMs, when non-nil, is this job's per-job execution timeout in
	// milliseconds, applied by the Runner around the worker call.
	TimeoutMs   *int
	ParentJobID *string
	Tags        []string
	ScheduledAt time.Time
	// Metadata is the raw jobs.metadata JSON blob. The Runner uses it to
	// thread request_id through to the worker's ctx and slog attrs; workers
	// generally read the value via [RequestIDFrom] rather than parsing it.
	Metadata []byte
}

// Result is what a worker returns on success.
type Result struct {
	// Output is the worker's structured output, stored on the JobRun.
	Output any
	// Snooze, when non-nil, reschedules the job for the given delay without
	// consuming an attempt.
	Snooze *time.Duration
	// Cancel, when true, moves the job to a terminal cancelled state.
	Cancel bool
	// FollowUps are child jobs enqueued atomically with finalization.
	FollowUps []FollowUp
	// CostMicros is the accumulated external-call cost for this attempt.
	CostMicros int64
	// SourceFetchIDs records side-effect source fetches this attempt produced.
	SourceFetchIDs []string
}

// FollowUp describes a child job a worker requests be enqueued.
type FollowUp struct {
	Kind       string
	Args       any
	Queue      string
	UniqueKey  string
	ScheduleAt *time.Time
	// Parent, when true, sets the child's parent_job_id to the spawning job.
	Parent   bool
	Priority int
	// ExecutorClass routes the child job to a specific executor pool. Empty
	// (AnyClass) leaves the child claimable by any runner.
	ExecutorClass ExecutorClass
}

// InsertOpts configures a single Insert.
type InsertOpts struct {
	Queue string
	// UniqueKey enforces idempotency forever: an insert collides with any job
	// that ever carried the same key, terminal or not. Use it for "enqueue this
	// exact unit of work at most once, ever".
	UniqueKey string
	// UniqueActiveKey enforces idempotency only while a job is active (available,
	// running, retryable, or scheduled): an insert collides only with a still-live
	// job carrying the same key, and the key frees up once that job reaches a
	// terminal state. Use it for "at most one in-flight job for this subject",
	// where a later run is expected once the current one finishes.
	UniqueActiveKey string
	ScheduleAt      *time.Time
	Parent          *string
	Priority        int
	ExecutorClass   ExecutorClass
	MaxAttempts     int
	// Timeout, when > 0, bounds this job's worker execution: the Runner cancels
	// the worker's ctx after it elapses. It overrides the worker's Timeouter and
	// the runner's DefaultTimeout.
	Timeout time.Duration
	// Tx, when set, writes the job row on the caller's transaction (outbox).
	Tx *gorm.DB
	// RequestID, when non-empty, is stamped on the job's metadata so the
	// Runner can thread it through ctx + slog on dequeue. Falls back to
	// [RequestIDFrom] on the caller's ctx when this field is empty.
	RequestID string
}
