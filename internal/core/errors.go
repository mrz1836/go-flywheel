package core

import (
	"errors"
	"fmt"
	"strings"
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

// ErrFollowUpLimit is returned by Finalize when a worker returns more follow-ups
// than the configured limit. It is deliberately fatal rather than truncating:
// silently dropping children would lose work with no signal, so the finalize
// transaction rolls back and nothing is enqueued. A fan-out larger than the limit
// is a design error — spawn a fan-out coordinator job instead of returning N
// children from one attempt.
var ErrFollowUpLimit = errors.New("flywheel: follow-up count exceeds the configured limit")

// ErrBarrierTooWide is returned by Finalize when a worker declares a Result.Barrier
// over more children than DriverOpts.BarrierMaxChildren allows. It is separate from
// ErrFollowUpLimit and checked in addition to it: a barrier costs an index-only
// completion count per child finalize — O(children) per child, O(n²) over the
// generation — which an operator may want to cap more tightly than the general
// fan-out ceiling. Like ErrFollowUpLimit it is fatal rather than truncating,
// directing the host to a tree of bounded generations.
var ErrBarrierTooWide = errors.New("flywheel: barrier-bearing generation exceeds the configured limit")

// ErrBarrierNoChildren is returned by Finalize when a worker declares a
// Result.Barrier but returns no FollowUp with Parent set. A barrier is scoped to a
// job's children, so one with no children could never fire — a silently
// never-running continuation is exactly the do-nothing footgun the runtime refuses.
var ErrBarrierNoChildren = errors.New("flywheel: barrier declared with no child follow-ups")

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

// ErrIndexDrift is the sentinel an IndexDriftError unwraps to, so a caller can
// branch on errors.Is(err, ErrIndexDrift) without depending on the error's
// concrete type. It is returned by InstallIndexes and Migrate when an installed
// index's definition has drifted from the runtime's and reconciliation was not
// requested.
var ErrIndexDrift = errors.New("flywheel: installed index definition has drifted")

// IndexDriftError is returned by the installer when one or more installed indexes
// have drifted from the runtime's definitions and reconciliation was not enabled.
// It carries every drift so a host can act on all of them at once rather than
// rediscovering them one failed deploy at a time. It unwraps to ErrIndexDrift.
//
// The default is to return this rather than to reconcile, because correcting the
// drift takes a table-wide ACCESS EXCLUSIVE lock a host should choose to pay: this
// failure is recoverable, while a lock taken on a deploy the host did not ask for
// is a stall under load. Set IndexOpts.Reconcile or MigrateOpts.Reconcile to
// reconcile instead.
type IndexDriftError struct {
	// Drift is every index whose installed definition differs from the runtime's,
	// each naming the installed and the expected definition.
	Drift []IndexDrift
}

// Error names each drifted index, what is installed, and what was expected, so a
// host can act without reading library source.
func (e *IndexDriftError) Error() string {
	var b strings.Builder
	fmt.Fprintf(
		&b,
		"flywheel: %d installed index definition(s) have drifted from the runtime's; "+
			"enable reconciliation (IndexOpts.Reconcile) or correct by hand:",
		len(e.Drift),
	)
	for _, d := range e.Drift {
		fmt.Fprintf(&b, "\n  %s:\n    installed: %s\n    expected:  %s", d.Name, d.Installed, d.Expected)
	}
	return b.String()
}

// Unwrap exposes ErrIndexDrift for errors.Is.
func (e *IndexDriftError) Unwrap() error { return ErrIndexDrift }

// ErrSQLiteConcurrency is returned by NewRunner when a SQLite driver is wired
// with a concurrency greater than 1 — SQLite serializes writers and a second
// concurrent dequeue would deadlock.
var ErrSQLiteConcurrency = errors.New("jobs: sqlite driver requires concurrency 1")

// ErrSQLitePragma reports a SQLite connection missing a pragma the runtime's
// serialized claim needs (WAL for a file database, a positive busy_timeout, a
// safe synchronous level). NewSQLiteDriverWithOptions returns it;
// NewSQLiteDriver logs it through slog.Default() and proceeds, so its
// one-argument, error-free signature is preserved.
var ErrSQLitePragma = errors.New("flywheel: sqlite connection is missing a required pragma")

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
	// errRunnerNeedsResource is returned when a Limiter is configured without a
	// Resource. The gate runs before the claim, so the resource must be knowable
	// without inspecting a job — it is a property of the Runner, not the work.
	errRunnerNeedsResource = errors.New("jobs: runner config requires Resource when Limiter is set")
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

// errPeriodicNoSchedule is returned when a periodic definition has neither a
// cron expression nor an interval.
var errPeriodicNoSchedule = errors.New("jobs: periodic has no schedule")

// ErrPeriodicNotFound is returned by SetPeriodicActive and DeletePeriodic when
// no periodic definition has the requested slug.
var ErrPeriodicNotFound = errors.New("flywheel: periodic not found")

// ErrJobTerminal is returned when an operator action targets a job that has
// already reached a terminal state (succeeded, cancelled, discarded). The
// runtime refuses the write rather than overwriting a recorded outcome and its
// finalized_at stamp. A bulk Replay whose States name StateSucceeded without
// Force is refused with it too: re-running succeeded work must be deliberate.
var ErrJobTerminal = errors.New("flywheel: job has already reached a terminal state")

// ErrReplayUnbounded is returned by Replay when neither Kinds nor FailedSince is
// set. An unscoped replay of every discarded job in the database is almost never
// the intent, so the runtime refuses it rather than doing something enormous by
// accident. Bound it by kind or a failure window, or use ReplayByParent to bound
// it by lineage.
var ErrReplayUnbounded = errors.New("flywheel: replay must be bounded by kind, lineage, or a failure window")
