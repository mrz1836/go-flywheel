//go:build loadtest

package loadtest

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	flywheel "github.com/mrz1836/go-flywheel"
	"gorm.io/gorm"
)

// ErrGated is what a gated driver returns instead of reaching the database. It
// is the harness's stand-in for an executor that has stopped executing.
var ErrGated = errors.New("loadtest: driver gated by an injected fault")

// faultPollInterval is how often the scheduler checks run progress.
const faultPollInterval = 10 * time.Millisecond

// Fault injects a failure into a run.
//
// Faults are scheduled against run progress — the fraction of jobs drained —
// rather than wall time, so a scenario fires at the same point in the workload
// on a fast machine and a slow one. A fault scheduled at "8 seconds in" is a
// different experiment on every machine it runs on.
type Fault interface {
	// At reports the drain fraction, in (0,1), at which the fault fires.
	At() float64
	// Describe renders the fault for a report. It exists because a Fault cannot
	// be marshaled — it is an interface — and a report that recorded its faults
	// as an empty object would be a report of an experiment nobody can identify.
	Describe() string
	// Validate rejects a configuration the fault cannot meaningfully run against,
	// before anything is provisioned.
	Validate(cfg Config) error
	// Inject applies the fault and returns a function that reverses it, or nil
	// when the fault is permanent.
	Inject(ctx context.Context, h *Harness) (revert func(), err error)
}

// windowedFault is the optional half of Fault: a reversible fault that lasts a
// bounded time rather than until the run ends.
//
// The scheduler asks for it through this interface rather than type-switching on
// the concrete faults, which it used to do. A type switch means every new
// windowed fault silently gets "reverted at end of run" — it compiles, it runs,
// and its window is quietly wrong — whereas an unimplemented interface is the
// author's explicit statement that the fault has no window.
type windowedFault interface {
	// Window reports how long the fault stays injected before it is reverted.
	Window() time.Duration
}

// gate blocks a driver's access to the database.
//
// It is two atomic bools, which matters: a fault fires from its own goroutine
// while every runner is mid-flight, and a gate built on a mutex the drain path
// also takes would perturb the latency it is being injected to measure.
//
// # Why there are two modes rather than one
//
// A plain "block everything" gate cannot reliably produce an orphan, and the
// first version of this file learned that from a flaky test. Closing it while
// the runner happens to be between polls blocks the *next* claim, so the runner
// simply stops working and every job it had already finished stays finished —
// killed=1, crashed=0, and a scenario that asserted nothing.
//
// Kill mode fixes the race by construction. It blocks Finalize immediately but
// lets the runner claim and start one more batch, so a finalize is guaranteed to
// be attempted and guaranteed to be blocked. The first blocked finalize then
// shuts the gate completely. The result is the row shape a SIGKILLed executor
// leaves — claim committed, stub committed, finalize never happened — on every
// run rather than on some of them.
type gate struct {
	// shut blocks every call.
	shut atomic.Bool
	// kill blocks only Finalize, and shuts the gate the first time it does.
	kill atomic.Bool
	// orphaned counts finalizes blocked by kill mode, so a scenario can assert
	// the fault produced what it was injected to produce.
	orphaned atomic.Int64
	// blockedClaims counts Dequeues refused while the gate was shut. It is the
	// report's evidence of a runner's poll cadence during an outage — nothing else
	// measures it, because a gated call deliberately records no latency.
	blockedClaims atomic.Int64
}

// shutGate closes the gate. Every subsequent driver call fails with ErrGated.
func (g *gate) shutGate() { g.shut.Store(true) }

// openGate reopens it, clearing both modes.
func (g *gate) openGate() {
	g.shut.Store(false)
	g.kill.Store(false)
}

// killGate arms kill mode: the runner may claim and start one more batch, and
// the first finalize it attempts is blocked and orphans that batch.
func (g *gate) killGate() { g.kill.Store(true) }

// check reports whether the gate is shut, for every call except Finalize.
func (g *gate) check() error {
	if g.shut.Load() {
		return ErrGated
	}
	return nil
}

// checkFinalize is check plus kill mode. The first finalize it blocks shuts the
// gate for everything else, so the runner claims nothing further.
func (g *gate) checkFinalize() error {
	if g.shut.Load() {
		return ErrGated
	}
	if g.kill.Load() {
		g.orphaned.Add(1)
		g.shut.Store(true)
		return ErrGated
	}
	return nil
}

// gateDriver is the outermost decorator in the chain
// gateDriver → timingDriver → the real driver.
//
// The order is deliberate. Gating outside the timing driver means a gated call
// records no observation at all, rather than recording a near-zero duration that
// would pull every percentile down — a fault that improved the reported latency
// would be worse than no fault.
type gateDriver struct {
	inner flywheel.Driver
	gate  *gate
}

// Dequeue claims unless gated.
func (d *gateDriver) Dequeue(
	ctx context.Context, queues []string, class flywheel.ExecutorClass,
	claimAny bool, limit int, lease time.Duration,
) ([]flywheel.RawJob, error) {
	if err := d.gate.check(); err != nil {
		d.gate.blockedClaims.Add(1)
		return nil, err
	}
	return d.inner.Dequeue(ctx, queues, class, claimAny, limit, lease)
}

// InsertRunStub writes the audit stub unless gated.
func (d *gateDriver) InsertRunStub(
	ctx context.Context, runID string, raw flywheel.RawJob,
	startedAt time.Time, class flywheel.ExecutorClass, execID string,
) error {
	if err := d.gate.check(); err != nil {
		return err
	}
	return d.inner.InsertRunStub(ctx, runID, raw, startedAt, class, execID)
}

// Finalize records the attempt's outcome unless gated.
//
// This is the method the whole gate exists for. A gated Finalize leaves the
// already-committed claim and run stub exactly as they are — job running, lease
// held, stub reading 'started' — which is precisely the row shape a SIGKILLed
// executor leaves behind, and precisely what the lease sweep exists to clean up.
func (d *gateDriver) Finalize(
	ctx context.Context, raw flywheel.RawJob, runID string,
	result flywheel.Result, workErr error, finishedAt time.Time,
) (flywheel.FinalizeOutcome, error) {
	if err := d.gate.checkFinalize(); err != nil {
		return flywheel.FinalizeOutcome{}, err
	}
	return d.inner.Finalize(ctx, raw, runID, result, workErr, finishedAt)
}

// RenewLease extends a claim's lease unless gated.
//
// Gating it is what makes a killed runner's jobs actually expire. A gate that
// blocked finalize but let the heartbeat through would keep the orphan's lease
// alive for as long as the process lived, and the sweep — the thing the fault
// exists to exercise — would never see it.
func (d *gateDriver) RenewLease(
	ctx context.Context, jobID, leaseToken string, until time.Time,
) (bool, error) {
	if err := d.gate.check(); err != nil {
		return false, err
	}
	return d.inner.RenewLease(ctx, jobID, leaseToken, until)
}

// InsertChild delegates. It is unreachable through a decorator — see
// timingDriver.InsertChild — so gating it would gate nothing.
func (d *gateDriver) InsertChild(
	ctx context.Context, tx *gorm.DB, fu flywheel.FollowUp, parentID string,
) error {
	return d.inner.InsertChild(ctx, tx, fu, parentID)
}

// Sweep reclaims expired leases unless gated.
func (d *gateDriver) Sweep(ctx context.Context, now time.Time) (int, error) {
	if err := d.gate.check(); err != nil {
		return 0, err
	}
	return d.inner.Sweep(ctx, now)
}

// --- shipped faults ---------------------------------------------------------

// KillWorker simulates an executor dying mid-attempt: its claims stay claimed,
// its run stubs stay 'started', and nothing it was working on is ever finalized.
//
// # Why it gates the driver rather than cancelling the context
//
// The obvious implementation — cancel the runner's context — asserts nothing,
// and the reason is a deliberate design decision in the runtime.
// baseDriver.Finalize runs its transaction on context.WithoutCancel, so that a
// drain or a shutdown cannot roll back a result the worker already produced. A
// cancelled runner therefore finalizes its in-flight job to retryable and leaves
// no orphan at all. The test would pass, having reproduced nothing.
//
// Gating leaves the claim and the stub committed while the finalize simply never
// happens, which is the row shape a SIGKILLed process actually leaves. See gate
// for why it blocks Finalize first and everything else second — a gate that shut
// all at once produced an orphan only when it happened to fire mid-batch.
type KillWorker struct {
	// Fraction is the drain progress at which the runner dies.
	Fraction float64
	// Runner is the index of the runner to kill. Zero is the first.
	Runner int
}

// At reports when the fault fires.
func (k KillWorker) At() float64 { return k.Fraction }

// Describe renders the fault for a report.
func (k KillWorker) Describe() string {
	return fmt.Sprintf("kill runner %d at %.0f%% drained", k.Runner, 100*k.Fraction)
}

// Validate rejects a run this fault cannot be observed in.
//
// Killing the only runner does not produce an orphan that anything reclaims: it
// produces a stalled run, because there is nothing left to claim the job the
// sweep releases, and the scenario ends at its timeout rather than at an
// assertion.
func (k KillWorker) Validate(cfg Config) error {
	if cfg.Runners < 2 {
		return fmt.Errorf(
			"loadtest: KillWorker needs Runners >= 2, got %d: killing the only runner stalls the drain "+
				"instead of orphaning a job another runner can reclaim: %w",
			cfg.Runners, ErrInvalidConfig,
		)
	}
	if k.Runner < 0 || k.Runner >= cfg.Runners {
		return fmt.Errorf("loadtest: KillWorker.Runner %d is out of range for %d runners: %w",
			k.Runner, cfg.Runners, ErrInvalidConfig)
	}
	return nil
}

// Inject arms kill mode on the runner's gate. The fault is permanent, so revert
// is nil.
//
// The runner's context is deliberately left alone. Cancelling it would not help
// — Finalize runs on context.WithoutCancel, so a cancelled runner finalizes
// cleanly — and it is not needed either: once the gate has blocked a finalize it
// shuts completely, so the runner's next claim fails and it does no further
// work. It spins against a closed gate until the drain ends, which costs one
// failed query per poll interval on a logger that discards.
func (k KillWorker) Inject(_ context.Context, h *Harness) (func(), error) {
	if k.Runner >= len(h.gates) {
		return nil, fmt.Errorf("loadtest: KillWorker.Runner %d has no gate: %w", k.Runner, ErrInvalidConfig)
	}
	h.gates[k.Runner].killGate()
	h.killed.Add(1)
	return nil, nil //nolint:nilnil // a permanent fault has no revert; the interface documents nil
}

// PauseDatabase makes every runner's driver fail for a duration.
//
// # What it does not reproduce
//
// This is a gate, not a network partition, and the difference is worth stating
// plainly rather than leaving a reader to assume the stronger thing. A real
// unreachable database blocks until a TCP timeout, fails transactions in flight,
// exhausts the connection pool, and applies backpressure the client feels as
// latency. A gate fails instantly and cleanly.
//
// Gating was chosen over the alternatives because they are worse, not because it
// is faithful. Closing the pool is irreversible, so a revert function would be a
// lie. Starving the pool blocks rather than fails, which absorbs the entire fault
// window into the latency histograms and reports the outage as slowness. The
// honest position is a fault with a clearly documented shape, rather than an
// unearned number in a report.
//
// The sampler's connection is deliberately ungated: a fault the harness cannot
// see through is a fault the report cannot describe.
type PauseDatabase struct {
	// Fraction is the drain progress at which the pause begins.
	Fraction float64
	// For is how long it lasts.
	For time.Duration
}

// At reports when the fault fires.
func (p PauseDatabase) At() float64 { return p.Fraction }

// Window reports how long the pause lasts, which is what makes this the one
// shipped fault the scheduler reverts before the run ends.
func (p PauseDatabase) Window() time.Duration { return p.For }

// Describe renders the fault for a report.
func (p PauseDatabase) Describe() string {
	return fmt.Sprintf("gate every runner's driver for %s at %.0f%% drained", p.For, 100*p.Fraction)
}

// Validate rejects a zero duration, which would be a fault that never happened.
func (p PauseDatabase) Validate(Config) error {
	if p.For <= 0 {
		return fmt.Errorf("loadtest: PauseDatabase.For must be positive, got %s: %w", p.For, ErrInvalidConfig)
	}
	return nil
}

// Inject gates every runner and returns the function that reopens them.
func (p PauseDatabase) Inject(_ context.Context, h *Harness) (func(), error) {
	for _, g := range h.gates {
		g.shutGate()
	}
	return func() {
		for _, g := range h.gates {
			g.openGate()
		}
	}, nil
}

// MassLeaseExpiry expires every in-flight lease at once, so the next sweep
// reclaims the whole set rather than the one or two a natural expiry produces.
//
// # What it is testing
//
// Not the sweep's throughput — that is the bounded-sweep work, and this fault
// runs against whatever sweep the runtime has. It is testing the fence at scale:
// thousands of jobs are simultaneously reclaimed and re-dispatched under fresh
// tokens while their original attempts are still running, so every one of those
// attempts finalizes into a claim it no longer holds. If the fence has a hole,
// this is the shape that finds it, because it multiplies a rare race by the
// queue depth.
//
// # Why it sweeps rather than waiting to be swept
//
// The first version expired the leases and left the harness's own sweeper to
// notice on its next tick, up to a second later. That reclaimed almost nothing
// and the reason is arithmetic: an attempt is in flight for the length of one
// job, so the chance the sweeper ticks inside that window is the job duration
// divided by the sweep interval. At a 25 ms job and a 1 s sweep it is 2.5%, and
// a fault that fires correctly and then reclaims one row out of thirty-two is
// indistinguishable from one that did not fire.
//
// Injecting the sweep makes the reclaim part of the fault instead of a race
// against a ticker, so what the scenario measures is the fence under a mass
// reclaim rather than the sampling luck of the sweep interval.
//
// # Why it retries rather than firing once
//
// The trigger is anti-correlated with the state the fault needs, which took a
// measurement to notice. Drain progress is counted from OnFinish, and OnFinish
// is exactly the event that empties the running set: at Runners 4 × Workers 8
// the whole in-flight batch finalizes together, so the instant progress crosses
// the threshold is an instant when *nothing* is running. Fired once, this fault
// expired zero leases on three consecutive 50,000-job runs while a concurrent
// poll of the same table showed 32 rows running throughout.
//
// So it retries on the scheduler's own interval until it catches a non-empty
// in-flight set, and reports plainly when it never does. "At 50% drained" is
// approximate either way; a fault that silently expires nothing is not.
type MassLeaseExpiry struct {
	// Fraction is the drain progress at which every held lease is expired.
	Fraction float64
}

// massExpiryAttempts bounds the retry. At faultPollInterval it is a window of
// about a second — long enough to span several claim cycles at any realistic
// work duration, short enough that a run whose queue has genuinely drained does
// not spend meaningful time looking for it.
const massExpiryAttempts = 100

// At reports when the fault fires.
func (m MassLeaseExpiry) At() float64 { return m.Fraction }

// Describe renders the fault for a report.
func (m MassLeaseExpiry) Describe() string {
	return fmt.Sprintf("expire every held lease at %.0f%% drained", 100*m.Fraction)
}

// Validate rejects a run with nothing in flight to expire.
func (m MassLeaseExpiry) Validate(cfg Config) error {
	if cfg.Mix == WorkloadEnqueueOnly {
		return fmt.Errorf(
			"loadtest: MassLeaseExpiry needs a mix that drains, got %q: the enqueue mix starts no runner, "+
				"so there is no lease to expire: %w",
			cfg.Mix, ErrInvalidConfig,
		)
	}
	return nil
}

// Inject expires every held lease and sweeps immediately. The fault is permanent
// — both have already happened by the time it returns — so revert is nil.
//
// The sweep runs on the undecorated driver rather than through a timing one. The
// harness's sweeper owns the sweep histogram's last shard and is running
// concurrently, and a second writer to one shard would race; an untimed sweep
// costs one observation out of a run's worth and keeps the histogram honest.
func (m MassLeaseExpiry) Inject(ctx context.Context, h *Harness) (func(), error) {
	var expired int64
	for attempt := range massExpiryAttempts {
		n, err := h.expireLeases(ctx)
		if err != nil {
			return nil, err
		}
		if expired = n; expired > 0 {
			break
		}
		if attempt == massExpiryAttempts-1 {
			h.notes.add(
				"MassLeaseExpiry found no held lease in %d attempts over %s and expired nothing. "+
					"The run drained before it could catch an in-flight batch.",
				massExpiryAttempts, massExpiryAttempts*faultPollInterval,
			)
			return nil, nil //nolint:nilnil // nothing was injected, so there is nothing to revert
		}
		select {
		case <-ctx.Done():
			return nil, nil //nolint:nilnil // the run ended before the fault could fire
		case <-time.After(faultPollInterval):
		}
	}

	reclaimed, err := h.reclaimExpired(ctx)
	if err != nil {
		return nil, err
	}
	h.notes.add(
		"MassLeaseExpiry expired %d held leases and immediately reclaimed %d. The gap is attempts "+
			"whose heartbeat renewed the lease back, or which finalized, between the two statements.",
		expired, reclaimed,
	)
	return nil, nil //nolint:nilnil // a permanent fault has no revert; the interface documents nil
}

// --- scheduling -------------------------------------------------------------

// runFaultScheduler fires the configured fault when the run reaches its
// declared progress, and reverses it on the way out.
//
// Progress comes from two atomic loads on the harness's own observer counters,
// not from a COUNT(*) against the target. The database answer would be more
// precise and would cost a query against the pool the fault is about to disturb,
// repeatedly, for the whole run — measuring the fault's effect through an
// instrument that is itself part of the load.
func (h *Harness) runFaultScheduler(ctx context.Context, fault Fault) {
	ticker := time.NewTicker(faultPollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		if h.prog.fraction() < fault.At() {
			continue
		}

		revert, err := fault.Inject(ctx, h)
		if err != nil {
			h.errs.add(fmt.Errorf("loadtest: inject %s: %w", fault.Describe(), err))
			return
		}
		h.notes.add("Fault injected at %.1f%% drained: %s.", 100*h.prog.fraction(), fault.Describe())
		if revert == nil {
			return
		}

		// A reversible fault is reversed either when its window closes or when
		// the run ends, whichever comes first. A run that ended without reverting
		// would leave the next assertion looking at a gated harness.
		if windowed, ok := fault.(windowedFault); ok {
			timer := time.NewTimer(windowed.Window())
			defer timer.Stop()
			select {
			case <-timer.C:
			case <-ctx.Done():
			}
		} else {
			<-ctx.Done()
		}
		revert()
		h.notes.add("Fault reverted: %s.", fault.Describe())
		return
	}
}
