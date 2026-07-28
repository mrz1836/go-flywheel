package flywheel

import (
	"errors"
	"fmt"
)

// ErrValidation is the sentinel every lifecycle validation failure wraps, so a
// caller can branch on errors.Is(err, ErrValidation) without depending on a
// host validation package. The runtime owns its own validation seam — it never
// imports a foundation/base-model error type.
var ErrValidation = errors.New("flywheel: validation failed")

// ValidationError is a single field's validation failure raised by a row's
// lifecycle hook (BeforeCreate/BeforeSave). It unwraps to ErrValidation.
type ValidationError struct {
	Field   string
	Message string
}

// Error renders the field and message.
func (e *ValidationError) Error() string {
	return fmt.Sprintf("flywheel: %s %s", e.Field, e.Message)
}

// Unwrap exposes ErrValidation for errors.Is.
func (e *ValidationError) Unwrap() error { return ErrValidation }

// newValidationError builds a ValidationError for field with message msg.
func newValidationError(field, msg string) error {
	return &ValidationError{Field: field, Message: msg}
}

// ErrAlreadyEnqueued is returned by Insert when a job with the same unique_key
// already exists. Callers compare it with errors.Is and treat the work as
// already submitted.
var ErrAlreadyEnqueued = errors.New("jobs: already enqueued")

// ErrRunAlreadyRecorded is returned by SeedRun when a seeded run collides with
// an existing (job_id, attempt) pair. It is the job_runs counterpart of
// ErrAlreadyEnqueued: the database rejected the row, so the attempt is already
// recorded and the caller's write was a duplicate.
var ErrRunAlreadyRecorded = errors.New("flywheel: run already recorded for this attempt")

// ErrUnknownKind is returned by the registry when a job's kind has no
// registered worker.
var ErrUnknownKind = errors.New("jobs: unknown job kind")

// ErrUnsupportedDialect is returned by Migrate, IndexSet, Indexes, and
// InstallIndexes for a dialect that cannot express the runtime's partial
// indexes (anything but postgres and sqlite). It is wrapped with the offending
// dialect name, so every one of those entry points fails identically under
// errors.Is.
var ErrUnsupportedDialect = errors.New("flywheel: unsupported dialect")

// ErrSQLiteConcurrency is returned by NewRunner when a SQLite driver is wired
// with a concurrency greater than 1 — SQLite serializes writers and a second
// concurrent dequeue would deadlock.
var ErrSQLiteConcurrency = errors.New("jobs: sqlite driver requires concurrency 1")

// ErrRunnerStopped is returned by RunUntilIdle when Stop ended the dispatch loop
// before the queue reached a terminal state. It promised a drained queue and did
// not deliver one, so it says so rather than returning nil.
//
// Run returns nil for the same event: a requested stop is how Run is meant to
// end.
var ErrRunnerStopped = errors.New("flywheel: runner stopped before the queue drained")

// DrainTimeoutError is what Drain returns when its deadline arrives before the
// pool empties, carrying how many jobs were still executing at that instant.
//
// The count is the point. "Some in-flight jobs may not have finished" is a
// warning; "3 jobs still in flight" is a diagnostic, and it is the number a host
// needs to decide whether its drain budget is sized for its workload. Those jobs
// keep their leases and are recovered by the lease sweep, exactly as they would
// be after a process kill.
type DrainTimeoutError struct {
	// InFlight is how many jobs were executing when the deadline arrived.
	InFlight int
	// Err is the deadline's own error, so errors.Is reaches
	// context.DeadlineExceeded and context.Canceled through it.
	Err error
}

// Error renders the count alongside the deadline that cut the drain short.
func (e *DrainTimeoutError) Error() string {
	return fmt.Sprintf("flywheel: runner drain left %d jobs in flight: %v", e.InFlight, e.Err)
}

// Unwrap exposes the deadline error for errors.Is.
func (e *DrainTimeoutError) Unwrap() error { return e.Err }

// ErrMissingKind is returned by Insert when the args value does not name its
// job kind. An args type used with Insert must implement Kind() string.
var ErrMissingKind = errors.New("jobs: args value must implement Kind() string")

// RunnerConfig validation errors returned by NewRunner.
var (
	errRunnerNeedsDB       = errors.New("jobs: runner config requires DB")
	errRunnerNeedsDriver   = errors.New("jobs: runner config requires Driver")
	errRunnerNeedsRegistry = errors.New("jobs: runner config requires Registry")
	errRunnerNeedsQueue    = errors.New("jobs: runner config requires at least one queue")
)

// errWorkerPanicked wraps a recovered worker panic.
var errWorkerPanicked = errors.New("jobs: worker panicked")

// SchedulerConfig validation errors returned by NewScheduler and
// NewSchedulerWithConfig.
//
// They are unexported for the same reason errRunnerNeedsDriver is: a
// construction failure is a wiring bug the caller fixes in source, not a
// runtime condition it branches on. An exported sentinel invites a caller to
// handle what it should have prevented.
var (
	errSchedulerNeedsDB     = errors.New("jobs: scheduler config requires DB")
	errSchedulerNeedsClient = errors.New("jobs: scheduler config requires Client")
	errSchedulerNeedsDriver = errors.New("jobs: scheduler config requires Driver")
)

// errNodeNeedsRunner is returned by NewNode for a config with no runners.
//
// There is no companion "node scheduler config" error: NewNode wraps whatever
// NewSchedulerWithConfig returns rather than re-deriving its own verdict, so a
// caller sees the specific field that was missing instead of a generic message
// that names three.
var errNodeNeedsRunner = errors.New("jobs: node config requires at least one runner")

// errPeriodicNoSchedule is returned when a periodic definition has neither a
// cron expression nor an interval.
var errPeriodicNoSchedule = errors.New("jobs: periodic has no schedule")

// ErrPeriodicNotFound is returned by SetPeriodicActive and DeletePeriodic when
// no periodic definition has the requested slug.
var ErrPeriodicNotFound = errors.New("flywheel: periodic not found")

// ErrJobTerminal is returned when an operator action targets a job that has
// already reached a terminal state (succeeded, cancelled, discarded). The
// runtime refuses the write rather than overwriting a recorded outcome and its
// finalized_at stamp.
var ErrJobTerminal = errors.New("flywheel: job has already reached a terminal state")
