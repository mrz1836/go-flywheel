package flywheel

import "errors"

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
