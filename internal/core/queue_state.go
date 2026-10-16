package core

import (
	"context"
	"log/slog"
	"slices"
	"sync"
	"time"
)

// defaultQueueStateRefresh is how often a Runner reads which of its queues are
// paused when RunnerConfig.QueueStateRefresh is zero.
const defaultQueueStateRefresh = 5 * time.Second

// queueState is a Runner's view of which of its queues an operator has paused:
// the set it last read from job_queues, when it last tried to read it, and
// whether a read has succeeded yet. Its zero value is ready: nothing paused,
// nothing read.
type queueState struct {
	mu sync.Mutex
	// paused is the paused set the last successful read returned.
	paused map[string]bool
	// read reports that a read has succeeded; before one does, the Runner claims
	// every queue it is configured for.
	read bool
	// lastTry is when the last read was attempted, on the monotonic clock: the
	// refresh is a polling cadence, like PollInterval, not a stored time.
	lastTry time.Time
	// failing reports that the last read failed, so a failure streak logs one
	// warning, at its start.
	failing bool
}

// queueStateRefresh resolves how often the Runner reads the paused set: an
// explicit positive value as given, zero as the default, and a negative value
// as off, which it reports as zero.
func (r *Runner) queueStateRefresh() time.Duration {
	switch {
	case r.cfg.QueueStateRefresh < 0:
		return 0
	case r.cfg.QueueStateRefresh > 0:
		return r.cfg.QueueStateRefresh
	}
	return defaultQueueStateRefresh
}

// claimable returns the queues the Runner claims from now: its configured
// queues without the ones an operator paused, in configuration order. It reads
// the paused set at most once per refresh, when one has passed since its last
// attempt — one primary-key read of job_queues over the Runner's queues. A read
// that fails keeps the set the last successful one returned, and warns once
// for the streak; until a read succeeds, every configured queue is claimable.
// With the refresh off it returns every configured queue and reads nothing.
func (r *Runner) claimable(ctx context.Context) []string {
	refresh := r.queueStateRefresh()
	if refresh <= 0 {
		return r.cfg.Queues
	}
	s := &r.queueState
	s.mu.Lock()
	defer s.mu.Unlock()
	if now := time.Now(); s.lastTry.IsZero() || now.Sub(s.lastTry) >= refresh {
		s.lastTry = now
		var paused []string
		err := r.cfg.DB.WithContext(ctx).Model(&jobQueueRow{}).
			Where("queue IN ? AND paused_at IS NOT NULL", r.cfg.Queues).
			Pluck("queue", &paused).Error
		switch {
		case err != nil && ctx.Err() == nil:
			if !s.failing {
				s.failing = true
				r.cfg.Logger.WarnContext(ctx, "jobs: queue state refresh failed; keeping the last paused set",
					slog.Any("queues", r.cfg.Queues), slog.Any("error", err))
			}
		case err == nil:
			s.failing = false
			s.read = true
			s.paused = make(map[string]bool, len(paused))
			for _, q := range paused {
				s.paused[q] = true
			}
		}
	}
	if !s.read || len(s.paused) == 0 {
		return r.cfg.Queues
	}
	return slices.DeleteFunc(slices.Clone(r.cfg.Queues), func(q string) bool { return s.paused[q] })
}

// untilQueueStateRefresh is how long until the Runner reads the paused set
// again: how long a Runner whose every queue is paused waits before it looks.
func (r *Runner) untilQueueStateRefresh() time.Duration {
	s := &r.queueState
	s.mu.Lock()
	defer s.mu.Unlock()
	return r.queueStateRefresh() - time.Since(s.lastTry)
}
