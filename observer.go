package flywheel

import (
	"context"
	"time"
)

// Observer is the optional lifecycle hook the Runner invokes around each attempt.
// It is the dependency-free telemetry seam: the core never imports a metrics or
// tracing library — a consumer implements Observer against their own stack
// (OpenTelemetry, Prometheus, statsd, slog) and wires it via RunnerConfig.Observer.
//
// Every method is called synchronously on the dispatch path and must not block; an
// implementation that needs to do I/O should buffer and return immediately. All
// methods receive the worker ctx, so a tracing implementation can pull the active
// span and ctxutil.RequestIDFrom(ctx) without extra plumbing.
//
// OnStart fires only for a registered kind; a job whose kind has no worker goes
// straight to OnFinish (with a permanent error) and never OnStart.
//
// # What follows an OnStart
//
// Exactly one of OnFinish or OnSupersede, never both. An attempt whose claim was
// superseded did not advance the job, so reporting it as finished would count an
// outcome the database never took — which is precisely the double-execution
// signal these events exist to surface. OnRetry cannot follow a supersede
// either: no retry was scheduled.
//
// That makes an OnFinish count a count of *state-advancing* finalizations, and
// it makes a nonzero OnSupersede rate the thing to alert on.
//
// OnFinish fires only when the finalize succeeded. A finalize that errored
// persisted nothing, so there is no outcome to report and neither event fires.
//
// # What is and is not ordered
//
// Per job the order is fixed: OnClaim for its batch, then OnStart, then OnFinish
// or OnSupersede, then OnRetry if it will run again.
//
// Across batches at Concurrency greater than 1 there is no order at all. The
// Runner claims to fill its free slots rather than waiting for a whole batch, so
// batch k+1's OnClaim can fire before batch k's last OnFinish. An observer that
// aggregates — counters, histograms, spans keyed on the job — is unaffected; one
// that assumes a batch is closed before the next opens is not, and never had that
// guarantee at any concurrency above 1.
type Observer interface {
	// OnClaim fires once per non-empty claimed batch, after Dequeue and before
	// any dispatch from that batch. It does not imply the previous batch has
	// finished — see the ordering note on Observer.
	OnClaim(ctx context.Context, ev ClaimEvent)
	// OnStart fires immediately before a worker's Work runs.
	OnStart(ctx context.Context, ev JobEvent)
	// OnFinish fires after each attempt is decided and persisted, for every
	// terminal-or-retry outcome the driver actually applied.
	OnFinish(ctx context.Context, ev FinishEvent)
	// OnRetry fires when an attempt is scheduled for another try — a subset of
	// OnFinish — so a metric can count retries without re-deriving the state
	// machine.
	OnRetry(ctx context.Context, ev RetryEvent)
	// OnSupersede fires in OnFinish's place when a finished attempt could not
	// advance the job's state because its claim was no longer held: the job was
	// reclaimed by the lease sweep, cancelled, or retried while the attempt was
	// running. The attempt's audit row is still written; its outcome is
	// discarded.
	//
	// A nonzero rate here means work is being executed twice. The lease is too
	// short for the workload, or the heartbeat is disabled or failing.
	OnSupersede(ctx context.Context, ev SupersedeEvent)
}

// ClaimEvent describes one claimed batch.
type ClaimEvent struct {
	ExecutorClass ExecutorClass
	Queues        []string
	Claimed       int
}

// JobEvent identifies one attempt. It is embedded in the finish and retry events.
type JobEvent struct {
	JobID   string
	RunID   string
	Kind    string
	Queue   string
	Attempt int
}

// FinishEvent reports one completed attempt.
type FinishEvent struct {
	JobEvent
	// Outcome is the attempt's recorded outcome (success, error, snooze,
	// cancelled, or timeout).
	Outcome RunOutcome
	// ErrorClass is the failure classification; it is the zero value on success.
	ErrorClass ErrorClass
	// Err is the worker error, or nil on success.
	Err error
	// Duration is the wall time the attempt took.
	Duration time.Duration
}

// RetryEvent reports an attempt that has been scheduled to retry.
type RetryEvent struct {
	JobEvent
	// NextAttempt is the attempt number the retry will run as.
	NextAttempt int
	// Delay is the backoff before the retry becomes claimable.
	Delay time.Duration
	// ErrorClass is the failure classification that triggered the retry.
	ErrorClass ErrorClass
}

// SupersedeEvent reports one attempt whose outcome was discarded because its
// claim was no longer held.
type SupersedeEvent struct {
	JobEvent
	// Outcome is what the attempt recorded in its audit row — its real outcome,
	// not a placeholder. The attempt happened; what was discarded is its effect
	// on the job.
	Outcome RunOutcome
	// State is the job's state as the superseding claim left it: running when the
	// job was reclaimed and is being retried, cancelled when it was cancelled
	// out from under the attempt. It is empty when the job no longer exists.
	State JobState
	// Duration is the wall time the attempt took before finishing into a lost
	// claim. It is the same measurement OnFinish carries, so a discarded attempt
	// still contributes to a duration distribution rather than vanishing from it.
	Duration time.Duration
	// LeaseToken is the token the attempt held. The job's current token differs —
	// that difference is what made this a supersede.
	LeaseToken string
}

// noopObserver is the default Observer when RunnerConfig.Observer is nil: every
// method is a no-op, so the dispatch hot path never needs a nil check.
type noopObserver struct{}

func (noopObserver) OnClaim(context.Context, ClaimEvent)         {}
func (noopObserver) OnStart(context.Context, JobEvent)           {}
func (noopObserver) OnFinish(context.Context, FinishEvent)       {}
func (noopObserver) OnRetry(context.Context, RetryEvent)         {}
func (noopObserver) OnSupersede(context.Context, SupersedeEvent) {}
