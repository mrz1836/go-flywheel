package flywheel

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/mrz1836/go-foundation/models"
)

// startHeartbeat renews one attempt's lease until the returned stop is called or
// the claim is lost, and returns the stop.
//
// # Why the context is detached
//
// The goroutine runs on context.WithoutCancel(ctx) plus its own cancel, for the
// same reason baseDriver.Finalize detaches its transaction: a drain cancels the
// worker's context while the attempt is still finalizing, and the window in
// which the outcome is being recorded is exactly the window in which losing the
// lease would be worst. Renewal therefore ends when the attempt ends — when
// dispatch's defer fires — not when the context around it does.
//
// # Why a lost claim does not cancel the worker
//
// It stops renewing and logs, and the worker keeps running. Cancelling it would
// be a new failure mode — a worker interrupted mid-side-effect — and it would
// buy nothing, because the fence discards that worker's result either way. A
// host that does want cancellation on claim loss has OnLeaseRenewed, which sees
// every renewal and can act on their absence.
func (r *Runner) startHeartbeat(ctx context.Context, raw RawJob, runID string) (stop func()) {
	interval := r.heartbeatInterval()
	// A driver that does not fence its claims has no token to renew against, so
	// there is nothing to hold: renewal would report a lost claim on its first
	// tick and log a warning for every attempt.
	if interval <= 0 || raw.LeaseToken == "" {
		return func() {}
	}

	hbCtx := context.WithoutCancel(ctx)
	stopped := make(chan struct{})
	done := make(chan struct{})

	go func() {
		defer close(done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-stopped:
				return
			case <-ticker.C:
			}
			if !r.renewOnce(hbCtx, raw, runID) {
				return
			}
		}
	}()

	// stop signals through a channel and waits, rather than cancelling the
	// renewal's context.
	//
	// Waiting is what makes "renewal stopped" a fact rather than a race: no
	// renewal is in flight once the attempt is over. Not cancelling is what keeps
	// a single-connection SQLite host alive — database/sql discards a connection
	// whose query was cancelled mid-flight, and against a bare `:memory:` DSN
	// (where the pool must be capped at one, or each connection gets its own
	// private empty database) discarding that connection destroys the database.
	// An in-flight renewal is one short UPDATE, so letting it finish costs far
	// less than interrupting it.
	var once sync.Once
	return func() {
		once.Do(func() { close(stopped) })
		<-done
	}
}

// renewOnce extends the lease once and reports whether renewal should continue.
//
// A failed renewal continues: the lease still has two intervals to run, so the
// next tick is the retry, and giving up on one transient error would strand a
// job whose worker is perfectly healthy. A *lost* claim does not continue —
// there is nothing left to renew, and retrying would only repeat the warning.
func (r *Runner) renewOnce(ctx context.Context, raw RawJob, runID string) bool {
	renewedAt := models.ClockFrom(ctx).Now(ctx)
	// Renew to now+lease rather than leased_until+lease: a heartbeat that stalled
	// and then caught up must not be able to bank a lease far into the future.
	expiresAt := renewedAt.Add(r.cfg.LeaseDuration)

	held, err := r.cfg.Driver.RenewLease(ctx, raw.ID, raw.LeaseToken, expiresAt)
	switch {
	case err != nil:
		r.cfg.Logger.WarnContext(ctx, "jobs: lease renewal failed",
			slog.String("job_id", raw.ID), slog.String("run_id", runID),
			slog.String("kind", raw.Kind), slog.String("error", err.Error()))
		return true
	case !held:
		r.cfg.Logger.WarnContext(ctx, "jobs: lease claim lost; this attempt's result will be discarded",
			slog.String("job_id", raw.ID), slog.String("run_id", runID),
			slog.String("kind", raw.Kind), slog.Int("attempt", raw.Attempt))
		return false
	}

	if r.cfg.OnLeaseRenewed != nil {
		if cbErr := r.cfg.OnLeaseRenewed(ctx, LeaseRenewal{
			JobID: raw.ID, RunID: runID, Kind: raw.Kind, Attempt: raw.Attempt,
			LeaseToken: raw.LeaseToken, RenewedAt: renewedAt, ExpiresAt: expiresAt,
		}); cbErr != nil {
			// The lease is already extended by the time the callback runs, so its
			// error cannot un-extend it. Stopping renewal here would strand a job
			// whose worker is still running, over a failure in the host's bookkeeping.
			r.cfg.Logger.WarnContext(ctx, "jobs: lease-renewed callback failed",
				slog.String("job_id", raw.ID), slog.String("run_id", runID),
				slog.String("error", cbErr.Error()))
		}
	}
	return true
}

// heartbeatInterval resolves the renewal cadence: an explicit positive value as
// given, a negative one as disabled, and zero as LeaseDuration/3 floored at one
// second.
//
// The floor applies only to the derived value, so a test or a short-lease
// deployment can still ask for a faster cadence explicitly. Its consequence is
// worth stating: a lease under three seconds gets less headroom than the divisor
// promises, and one at or under a second gets none at all. Configure
// HeartbeatInterval explicitly there rather than relying on the default.
func (r *Runner) heartbeatInterval() time.Duration {
	switch {
	case r.cfg.HeartbeatInterval < 0:
		return 0
	case r.cfg.HeartbeatInterval > 0:
		return r.cfg.HeartbeatInterval
	}
	if interval := r.cfg.LeaseDuration / heartbeatDivisor; interval > minHeartbeatInterval {
		return interval
	}
	return minHeartbeatInterval
}
