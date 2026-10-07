package observers

import (
	"context"
	"log/slog"

	flywheel "github.com/mrz1836/go-flywheel"
)

// The messages of a finished attempt, one per level OnFinish logs it at.
const (
	// msgJobFinished is a routine finish: a success, a snooze, or a cancel.
	msgJobFinished = "flywheel: job finished"

	// msgJobAttemptFailed is a failed attempt that did not end the job: it
	// will retry.
	msgJobAttemptFailed = "flywheel: job attempt failed"

	// msgJobDiscarded is a failed attempt that ended the job: its work is
	// lost until it is replayed.
	msgJobDiscarded = "flywheel: job failed and was discarded"
)

// SlogObserver implements flywheel.Observer by logging every lifecycle event
// through an *slog.Logger. Routine events log at debug, so a daemon running at
// info stays quiet and a `--log debug` run gets a rich, per-attempt trace —
// claim, start, finish, and retry — with structured attributes for filtering.
//
// What an operator must see without turning debug on logs above that: a failed
// attempt at warn, a job a failure discarded at error, a superseded attempt at
// warn, and a sweep that reclaimed leases at info.
type SlogObserver struct {
	logger *slog.Logger
	level  slog.Level
}

// NewSlog returns a SlogObserver that logs routine events through logger at
// debug level, and failures above it (see SlogObserver). A nil logger falls
// back to slog.Default(), so wiring it can never panic on a nil.
func NewSlog(logger *slog.Logger) *SlogObserver {
	if logger == nil {
		logger = slog.Default()
	}
	return &SlogObserver{logger: logger, level: slog.LevelDebug}
}

// Compile-time proof SlogObserver satisfies flywheel.Observer.
var _ flywheel.Observer = (*SlogObserver)(nil)

// OnClaim logs a claimed batch.
func (s *SlogObserver) OnClaim(ctx context.Context, ev flywheel.ClaimEvent) {
	s.logger.LogAttrs(
		ctx, s.level, "flywheel: jobs claimed",
		slog.String("executor_class", string(ev.ExecutorClass)),
		slog.Any("queues", ev.Queues),
		slog.Int("claimed", ev.Claimed),
	)
}

// OnStart logs an attempt about to run.
func (s *SlogObserver) OnStart(ctx context.Context, ev flywheel.JobEvent) {
	s.logger.LogAttrs(
		ctx, s.level, "flywheel: job started",
		slog.String("job_id", ev.JobID),
		slog.String("run_id", ev.RunID),
		slog.String("kind", ev.Kind),
		slog.String("queue", ev.Queue),
		slog.Int("attempt", ev.Attempt),
	)
}

// OnFinish logs a decided attempt, including the error when one occurred, at a
// level that follows what the attempt did to the job.
//
// A success, a snooze, or a cancel is routine and logs at the configured level
// (debug). A failed attempt never is. One the job will retry logs at warn as
// "flywheel: job attempt failed", and one that discarded the job — a permanent
// or validation error, or the attempt budget spent — logs at error as
// "flywheel: job failed and was discarded", because that job's work is lost
// until someone replays it. Both carry the error text, its class, the attempt
// against the budget, and the job's resulting state, so an operator reading
// info-level logs sees what failed and why. The error text is logged as the
// worker returned it, so keep secrets out of worker errors.
func (s *SlogObserver) OnFinish(ctx context.Context, ev flywheel.FinishEvent) {
	attrs := []slog.Attr{
		slog.String("job_id", ev.JobID),
		slog.String("run_id", ev.RunID),
		slog.String("kind", ev.Kind),
		slog.String("queue", ev.Queue),
		slog.String("outcome", string(ev.Outcome)),
		slog.Int("attempt", ev.Attempt),
		slog.Duration("duration", ev.Duration),
	}
	if ev.MaxAttempts > 0 {
		attrs = append(attrs, slog.Int("max_attempts", ev.MaxAttempts))
	}
	if ev.State != "" {
		attrs = append(attrs, slog.String("job_state", string(ev.State)))
	}
	if ev.ErrorClass != "" {
		attrs = append(attrs, slog.String("error_class", string(ev.ErrorClass)))
	}
	if ev.Err != nil {
		attrs = append(attrs, slog.String("error", ev.Err.Error()))
	}
	level, msg := s.finishLevel(ev)
	s.logger.LogAttrs(ctx, level, msg, attrs...)
}

// finishLevel returns the level and message of a finished attempt: error for a
// job the attempt discarded, warn for any other failed attempt (an error, a
// timeout, or a crash), and the configured level for a routine finish.
func (s *SlogObserver) finishLevel(ev flywheel.FinishEvent) (slog.Level, string) {
	switch {
	case ev.State == flywheel.StateDiscarded:
		return slog.LevelError, msgJobDiscarded
	case ev.Outcome == flywheel.OutcomeError, ev.Outcome == flywheel.OutcomeTimeout, ev.Outcome == flywheel.OutcomeCrashed:
		return slog.LevelWarn, msgJobAttemptFailed
	}

	return s.level, msgJobFinished
}

// OnSupersede logs a discarded attempt at warn.
//
// Like a failed attempt, it is not routine enough for the configured level: it
// means a job's work was executed and thrown away, which an operator wants to
// see without having turned debug on first.
func (s *SlogObserver) OnSupersede(ctx context.Context, ev flywheel.SupersedeEvent) {
	s.logger.LogAttrs(
		ctx, slog.LevelWarn, "flywheel: job attempt superseded; its outcome was discarded",
		slog.String("job_id", ev.JobID),
		slog.String("run_id", ev.RunID),
		slog.String("kind", ev.Kind),
		slog.String("queue", ev.Queue),
		slog.String("discarded_outcome", string(ev.Outcome)),
		slog.String("job_state", string(ev.State)),
		slog.Int("attempt", ev.Attempt),
		slog.Duration("duration", ev.Duration),
	)
}

// OnSweep logs a completed stuck-lease reclaim pass. It logs at the configured
// level (debug) when the pass reclaimed nothing — routine maintenance — and at
// info when it reclaimed leases, because a nonzero reclaim means an executor died
// mid-attempt and an operator scanning info-level logs should see that.
func (s *SlogObserver) OnSweep(ctx context.Context, ev flywheel.SweepEvent) {
	level := s.level
	if ev.Reclaimed > 0 {
		level = slog.LevelInfo
	}
	s.logger.LogAttrs(
		ctx, level, "flywheel: lease sweep completed",
		slog.Int("reclaimed", ev.Reclaimed),
		slog.Duration("duration", ev.Duration),
	)
}

// OnRetry logs a scheduled retry.
func (s *SlogObserver) OnRetry(ctx context.Context, ev flywheel.RetryEvent) {
	s.logger.LogAttrs(
		ctx, s.level, "flywheel: job retry scheduled",
		slog.String("job_id", ev.JobID),
		slog.String("kind", ev.Kind),
		slog.Int("next_attempt", ev.NextAttempt),
		slog.Duration("delay", ev.Delay),
		slog.String("error_class", string(ev.ErrorClass)),
	)
}
