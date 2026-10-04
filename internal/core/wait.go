package core

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"gorm.io/gorm"
)

// Default WaitOpts cadence: the first delay, and the ceiling it doubles to.
const (
	defaultWaitPollInterval    = 500 * time.Millisecond
	defaultWaitMaxPollInterval = 5 * time.Second
)

// WaitOpts configures WaitForJobWithOptions. The zero value reads the job at
// once, then every 500ms doubling to a 5s ceiling, until it reaches a terminal
// state.
type WaitOpts struct {
	// PollInterval is the delay after the first read; each later delay doubles it.
	// Zero or negative selects 500ms. The first read is immediate.
	PollInterval time.Duration
	// MaxPollInterval caps the doubling. Zero or negative selects 5s; a value
	// below PollInterval is raised to it, so the cadence never speeds up.
	MaxPollInterval time.Duration
	// States lists extra states that end the wait — StateRunning, to return once
	// a runner has claimed the job. A terminal state always ends it. A state
	// outside the job state vocabulary is rejected with an ErrValidation error
	// before any read: no job could ever reach it.
	States []JobState
}

// WaitForJob blocks until the job reaches a terminal state and returns its view.
// It is WaitForJobWithOptions with the zero WaitOpts.
//
// It is the waiting half of enqueue-or-join: an Insert that collides returns an
// *AlreadyEnqueuedError naming the in-flight job, and waiting on that id joins it
// instead of running the work twice. Read what a succeeded job produced with
// LatestRun.
func WaitForJob(ctx context.Context, db *gorm.DB, id string) (JobView, error) {
	return WaitForJobWithOptions(ctx, db, id, WaitOpts{})
}

// WaitForJobWithOptions polls the job through db until its state is terminal or
// one of opts.States, and returns the view that ended the wait. The first read is
// immediate; later reads back off from PollInterval to MaxPollInterval, so a long
// wait costs one primary-key read every few seconds. It only reads. An unknown
// state in opts.States is an ErrValidation error, returned before any read.
//
// Wait through the primary, or the handle the job was written through, not a
// replica: a lagging replica reads a job enqueued (or named by a collision) a
// moment ago as missing, and the wait returns ErrJobNotFound for work that exists.
//
// ctx is the only bound. When it ends first, the call returns the last view it
// read and an error wrapping ctx.Err() that names the state the job was still in
// (`flywheel: wait for job "<id>": still running: context deadline exceeded`), so
// errors.Is(err, context.DeadlineExceeded) holds and the view says how far the
// job got. Without a deadline, a paused job, or one whose queue no runner serves,
// is waited on forever.
//
// A job that does not exist returns ErrJobNotFound at once — including one that
// disappears mid-wait (retention, a soft delete) and one written on a transaction
// that has not committed, which db cannot see: wait after the commit. Any other
// read error ends the wait at once, wrapped as `flywheel: wait for job "<id>": …`;
// it is not retried. The returned view is always the last one read, and zero when
// none was.
func WaitForJobWithOptions(ctx context.Context, db *gorm.DB, id string, opts WaitOpts) (JobView, error) {
	for _, s := range opts.States {
		if !s.Valid() {
			return JobView{}, newValidationError("states", fmt.Sprintf("%q is not a job state", s))
		}
	}
	o := opts.resolved()
	var (
		last  JobView
		reads int
	)
	for {
		if err := ctx.Err(); err != nil {
			return last, waitContextError(id, last, reads > 0, err)
		}
		view, err := findJobView(ctx, db, id)
		switch {
		case errors.Is(err, ErrJobNotFound):
			return last, ErrJobNotFound
		case err != nil:
			// A read cut short by ctx fails with a driver error that wraps (or
			// merely follows) the cancellation; report it as the wait ending,
			// with the state last seen, rather than as a database failure.
			if ctxErr := ctx.Err(); ctxErr != nil {
				return last, waitContextError(id, last, reads > 0, ctxErr)
			}
			return last, fmt.Errorf("flywheel: wait for job %q: %w", id, err)
		}
		last = view
		reads++
		if o.stopsAt(view.State) {
			return view, nil
		}

		timer := time.NewTimer(o.delay(reads))
		select {
		case <-ctx.Done():
			timer.Stop()
			return last, waitContextError(id, last, true, ctx.Err())
		case <-timer.C:
		}
	}
}

// resolved applies the defaults to o and raises MaxPollInterval to at least
// PollInterval.
func (o WaitOpts) resolved() WaitOpts {
	if o.PollInterval <= 0 {
		o.PollInterval = defaultWaitPollInterval
	}
	if o.MaxPollInterval <= 0 {
		o.MaxPollInterval = defaultWaitMaxPollInterval
	}
	if o.MaxPollInterval < o.PollInterval {
		o.MaxPollInterval = o.PollInterval
	}
	return o
}

// delay is the wait after the reads-th read: PollInterval doubled once per read
// past the first, capped at MaxPollInterval. It has no jitter: waiters start at
// different moments, so they do not poll in lockstep, and a fixed ladder keeps
// the cost of a wait predictable.
func (o WaitOpts) delay(reads int) time.Duration {
	return expBackoff(o.PollInterval, o.MaxPollInterval, reads)
}

// stopsAt reports whether a job in state ends the wait: a terminal state always
// does, and so does any state the caller listed.
func (o WaitOpts) stopsAt(state string) bool {
	return isTerminalStateString(state) || slices.Contains(o.States, JobState(state))
}

// waitContextError is the error a wait returns when ctx ends it. It names the
// state the job was last seen in, or says none was, and wraps err so errors.Is
// reaches context.DeadlineExceeded or context.Canceled.
func waitContextError(id string, last JobView, observed bool, err error) error {
	if !observed {
		return fmt.Errorf("flywheel: wait for job %q: no state observed: %w", id, err)
	}
	return fmt.Errorf("flywheel: wait for job %q: still %s: %w", id, last.State, err)
}
