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
// span and RequestIDFrom(ctx) without extra plumbing.
//
// OnStart fires only for a registered kind; a job whose kind has no worker goes
// straight to OnFinish (with a permanent error) and never OnStart.
type Observer interface {
	// OnClaim fires once per non-empty claimed batch, after Dequeue and before
	// any dispatch.
	OnClaim(ctx context.Context, ev ClaimEvent)
	// OnStart fires immediately before a worker's Work runs.
	OnStart(ctx context.Context, ev JobEvent)
	// OnFinish fires after each attempt is decided, for every terminal-or-retry
	// outcome.
	OnFinish(ctx context.Context, ev FinishEvent)
	// OnRetry fires when an attempt is scheduled for another try — a subset of
	// OnFinish — so a metric can count retries without re-deriving the state
	// machine.
	OnRetry(ctx context.Context, ev RetryEvent)
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

// noopObserver is the default Observer when RunnerConfig.Observer is nil: every
// method is a no-op, so the dispatch hot path never needs a nil check.
type noopObserver struct{}

func (noopObserver) OnClaim(context.Context, ClaimEvent)   {}
func (noopObserver) OnStart(context.Context, JobEvent)     {}
func (noopObserver) OnFinish(context.Context, FinishEvent) {}
func (noopObserver) OnRetry(context.Context, RetryEvent)   {}
