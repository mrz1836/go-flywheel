package observers

import (
	"context"
	"log/slog"

	flywheel "github.com/mrz1836/go-flywheel"
)

// SlogObserver implements flywheel.Observer by logging every lifecycle event
// through an *slog.Logger. It logs at debug level so a daemon running at info
// stays quiet, and a `--log debug` run gets a rich, per-attempt trace — claim,
// start, finish, and retry — with structured attributes for filtering.
type SlogObserver struct {
	logger *slog.Logger
	level  slog.Level
}

// NewSlog returns a SlogObserver that logs through logger at debug level. A nil
// logger falls back to slog.Default(), so wiring it can never panic on a nil.
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

// OnFinish logs a decided attempt, including the error when one occurred.
func (s *SlogObserver) OnFinish(ctx context.Context, ev flywheel.FinishEvent) {
	attrs := []slog.Attr{
		slog.String("job_id", ev.JobID),
		slog.String("kind", ev.Kind),
		slog.String("queue", ev.Queue),
		slog.String("outcome", string(ev.Outcome)),
		slog.Int("attempt", ev.Attempt),
		slog.Duration("duration", ev.Duration),
	}
	if ev.ErrorClass != "" {
		attrs = append(attrs, slog.String("error_class", string(ev.ErrorClass)))
	}
	if ev.Err != nil {
		attrs = append(attrs, slog.String("error", ev.Err.Error()))
	}
	s.logger.LogAttrs(ctx, s.level, "flywheel: job finished", attrs...)
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
