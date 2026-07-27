//go:build loadtest

package loadtest

import (
	"context"
	"time"

	flywheel "github.com/mrz1836/go-flywheel"
	"gorm.io/gorm"
)

// timingDriver wraps a Driver and records the wall time of every claim,
// finalize, and sweep into the harness's own histograms.
//
// This is how the harness produces p50 and p99 without depending on the
// observer or metrics packages, and the independence is the point rather than a
// convenience: production latency telemetry is designed for operators, this is
// designed for this project's numbers, and neither should be waiting on the
// other. It also measures the boundary that actually matters — the database
// round trip — rather than the dispatch loop around it.
//
// Each runner gets its own timingDriver bound to its own shard index, handed out
// at construction. That is what makes shard selection free on the hot path: the
// driver already knows which shard it writes to, so there is no lookup, no
// counter, and no goroutine-local anything.
type timingDriver struct {
	inner flywheel.Driver
	claim *recorder
	final *recorder
	sweep *recorder
	// shard is this driver's histogram shard, fixed for the run.
	shard int
}

// newTimingDriver binds a driver to one histogram shard.
func newTimingDriver(inner flywheel.Driver, t *timings, shard int) *timingDriver {
	return &timingDriver{
		inner: inner, claim: t.claim, final: t.finalize, sweep: t.sweep, shard: shard,
	}
}

// timings is the harness's three histograms, shared by every timingDriver.
type timings struct {
	claim, finalize, sweep *recorder
}

// newTimings allocates one shard per runner plus one for the harness's sweeper.
func newTimings(runners int) *timings {
	shards := runners + 1
	return &timings{
		claim:    newRecorder(shards),
		finalize: newRecorder(shards),
		sweep:    newRecorder(shards),
	}
}

// Dequeue times the claim.
//
// An errored claim is still recorded. A claim that failed still cost a round
// trip, and dropping those observations would make a run under a database fault
// report a *better* p99 than a healthy one — the slow attempts would be exactly
// the ones excluded.
func (d *timingDriver) Dequeue(
	ctx context.Context, queues []string, class flywheel.ExecutorClass,
	claimAny bool, limit int, lease time.Duration,
) ([]flywheel.RawJob, error) {
	start := time.Now()
	out, err := d.inner.Dequeue(ctx, queues, class, claimAny, limit, lease)
	d.claim.record(d.shard, time.Since(start))
	return out, err
}

// InsertRunStub delegates without timing.
//
// It is on the dispatch path but it is not one of the three operations the
// report names, and adding a fourth histogram nobody reads would cost an atomic
// per attempt for nothing. Recorded here as a deliberate omission rather than an
// oversight.
func (d *timingDriver) InsertRunStub(
	ctx context.Context, runID string, raw flywheel.RawJob,
	startedAt time.Time, class flywheel.ExecutorClass, execID string,
) error {
	return d.inner.InsertRunStub(ctx, runID, raw, startedAt, class, execID)
}

// Finalize times the finalize transaction.
func (d *timingDriver) Finalize(
	ctx context.Context, raw flywheel.RawJob, runID string,
	result flywheel.Result, workErr error, finishedAt time.Time,
) error {
	start := time.Now()
	err := d.inner.Finalize(ctx, raw, runID, result, workErr, finishedAt)
	d.final.record(d.shard, time.Since(start))
	return err
}

// RenewLease delegates without timing.
//
// It is a single guarded UPDATE off the dispatch path, on a cadence set by the
// lease rather than by the workload, so it is not one of the three operations
// the report names. Its cost shows up where it belongs — in the drain
// throughput, and in the storage sampler's WAL and dead-tuple series, which is
// where an extra UPDATE per job per third-of-lease is actually visible.
func (d *timingDriver) RenewLease(
	ctx context.Context, jobID, leaseToken string, until time.Time,
) (bool, error) {
	return d.inner.RenewLease(ctx, jobID, leaseToken, until)
}

// InsertChild delegates. It is structurally unreachable through this decorator.
//
// The runtime's finalize calls insertFollowUps, which calls InsertChild on its
// own concrete receiver rather than through the Driver interface. A decorator's
// copy is therefore never invoked, and a histogram wired to it would report zero
// observations for a path that ran thousands of times. Documented rather than
// measured, because a zero that looks like a measurement is worse than no
// measurement.
func (d *timingDriver) InsertChild(
	ctx context.Context, tx *gorm.DB, fu flywheel.FollowUp, parentID string,
) error {
	return d.inner.InsertChild(ctx, tx, fu, parentID)
}

// Sweep times the lease sweep.
func (d *timingDriver) Sweep(ctx context.Context, now time.Time) (int, error) {
	start := time.Now()
	n, err := d.inner.Sweep(ctx, now)
	d.sweep.record(d.shard, time.Since(start))
	return n, err
}

// sweepInterval is how often the harness runs its own sweeper.
//
// One second is the cadence a scheduler would plausibly use, and it is what
// makes the steady-state cost of a no-op sweep visible: at 100k rows the sweep
// that reclaims nothing still scans, and that fixed cost is exactly where the
// jobs_running_leased index earns its keep.
const sweepInterval = time.Second

// runSweeper sweeps expired leases until ctx ends.
//
// The harness runs its own sweeper because the Runner does not sweep — nothing
// in the runtime's dispatch loop calls Sweep. Without this goroutine
// Report.Sweep would have no observations at all, and the acceptance criterion
// that the report carry non-zero claim, finalize, and sweep percentiles would be
// unmeetable rather than unmet.
// It sweeps once immediately and then on the interval. The immediate sweep is
// not cosmetic: a run that drains in under a second would otherwise never tick,
// and Report.Sweep would be empty on exactly the short runs a test asserts
// against — a silence indistinguishable from a broken sweeper. It is also what a
// scheduler does, which starts by clearing whatever the last process left
// behind.
func (h *Harness) runSweeper(ctx context.Context, driver flywheel.Driver) {
	ticker := time.NewTicker(sweepInterval)
	defer ticker.Stop()

	h.sweepOnce(ctx, driver)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			h.sweepOnce(ctx, driver)
		}
	}
}

// sweepOnce runs one sweep, recording its duration and any reclaim.
func (h *Harness) sweepOnce(ctx context.Context, driver flywheel.Driver) {
	reclaimed, err := driver.Sweep(ctx, time.Now())
	if err != nil {
		if ctx.Err() == nil {
			h.errs.add(err)
		}
		return
	}
	if reclaimed > 0 {
		h.prog.reclaimed.Add(int64(reclaimed))
	}
}
