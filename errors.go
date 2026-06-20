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

// ErrUnknownKind is returned by the registry when a job's kind has no
// registered worker.
var ErrUnknownKind = errors.New("jobs: unknown job kind")

// ErrSQLiteConcurrency is returned by NewRunner when a SQLite driver is wired
// with a concurrency greater than 1 — SQLite serializes writers and a second
// concurrent dequeue would deadlock.
var ErrSQLiteConcurrency = errors.New("jobs: sqlite driver requires concurrency 1")

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

// errPeriodicNoSchedule is returned when a periodic definition has neither a
// cron expression nor an interval.
var errPeriodicNoSchedule = errors.New("jobs: periodic has no schedule")
