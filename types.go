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

// ExecutorKind is the host kind an executor reports. Its string values are the
// stable wire form persisted on job_runs rows.
type ExecutorKind string

// Recognized ExecutorKind values.
const (
	ExecutorLambda ExecutorKind = "lambda"
	ExecutorECS    ExecutorKind = "ecs"
	ExecutorLocal  ExecutorKind = "local"
)

// RunOn is the host kind a job declares it may run on. Its string values are
// the stable wire form persisted on jobs rows.
type RunOn string

// Recognized RunOn values.
const (
	RunOnLambda RunOn = "lambda"
	RunOnECS    RunOn = "ecs"
	RunOnEither RunOn = "either"
)

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
}

// InsertOpts configures a single Insert.
type InsertOpts struct {
	Queue       string
	UniqueKey   string
	ScheduleAt  *time.Time
	Parent      *string
	Priority    int
	RunOn       RunOn
	MaxAttempts int
	// Tx, when set, writes the job row on the caller's transaction (outbox).
	Tx *gorm.DB
	// RequestID, when non-empty, is stamped on the job's metadata so the
	// Runner can thread it through ctx + slog on dequeue. Falls back to
	// [RequestIDFrom] on the caller's ctx when this field is empty.
	RequestID string
}
