//go:build loadtest

package loadtest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	flywheel "github.com/mrz1836/go-flywheel"
	"github.com/mrz1836/go-foundation/models"
)

// outageState is the shared switch the downstream-outage fault flips and the
// worker reads. It carries the simulated instant the outage lifts, so the worker
// fails every attempt until the advanceable clock passes it — which is what makes
// the runner's own backoff ladder, not the worker's NextRetry, govern the retries.
type outageState struct {
	// endNanos is the sim-time UnixNano the outage lifts at; zero means no outage
	// has begun.
	endNanos atomic.Int64
	// failures counts the attempts the outage forced to fail, for the report.
	failures atomic.Int64
}

// begin opens the outage, lifting at the simulated instant end.
func (o *outageState) begin(end time.Time) { o.endNanos.Store(end.UnixNano()) }

// failing reports whether an attempt at simulated instant now falls inside the
// outage window.
func (o *outageState) failing(now time.Time) bool {
	end := o.endNanos.Load()
	return end != 0 && now.UnixNano() < end
}

// end reports the simulated instant the outage lifts, or the zero time if no
// outage has begun. The clock advancer reads it to keep from jumping the boundary.
func (o *outageState) end() time.Time {
	ns := o.endNanos.Load()
	if ns == 0 {
		return time.Time{}
	}
	return time.Unix(0, ns)
}

// errTransientLoad is the error a fail-marked job returns. Left unclassified, the
// runtime treats it as transient — retried until the budget is exhausted, then
// discarded — which is the failure a replay run recovers.
var errTransientLoad = errors.New("loadtest: simulated transient failure")

// loadKind is the job kind every seeded job carries. One kind is enough: the
// harness measures the runtime's paths, and a second kind would only exercise
// the registry's map lookup.
const loadKind = "loadtest.job"

// loadArgs is the seeded job payload.
//
// WorkNanos travels in the row rather than being read from Config at execution
// time, and that is what makes the mixed-speed workload reproducible: the
// per-job duration is decided once, by the generator, from the run seed — so a
// job's cost is a property of the workload, not of when it happened to run or
// which worker picked it up.
//
// Payload pads the row toward a realistic width. A jobs row whose args are `{}`
// is not the row a production database stores, and row width drives page count,
// which drives everything this harness measures about storage.
type loadArgs struct {
	// N is the job's ordinal in the generated workload, so a row can be traced
	// back to the spec that produced it.
	N int `json:"n"`
	// WorkNanos is how long this job's worker sleeps.
	WorkNanos int64 `json:"work_nanos"`
	// Payload is deterministic filler.
	Payload string `json:"payload"`
	// Children, when positive, is how many follow-ups this job enqueues on
	// success. It is zero except in the fan-out and barrier mixes.
	Children int `json:"children,omitempty"`
	// Barrier, when true, declares a fan-in barrier over this job's children. It is
	// set only on the barrier mix's parents; the children and the continuation carry
	// it false.
	Barrier bool `json:"barrier,omitempty"`
	// Fail, when true, makes the worker return a transient error every attempt, so
	// the job exhausts its budget to discarded. It is how a replay run manufactures
	// the failures it then recovers.
	Fail bool `json:"fail,omitempty"`
	// Group is the parent ordinal a fairness-mix child belongs to, so the worker can
	// record which parent each claim served without a second query. It is set only on
	// the fairness mix; every other mix leaves it zero and unread.
	Group int `json:"g,omitempty"`
}

// Kind names the job kind.
func (loadArgs) Kind() string { return loadKind }

// execTracker records which jobs are executing right now, so a job observed in
// two workers at once is counted rather than inferred.
//
// It is the in-process half of the exactly-once check. The other two halves read
// the audit table after the fact — one success row per job, and no two attempts
// on a job overlapping in time — and this one watches it happen. Three
// independent checks because a single one that happened to be wrong would be
// indistinguishable from a guarantee that held.
//
// The map is held only for the duration of an entry or an exit, which is
// microseconds against millisecond-scale database work, so it does not become
// the thing being measured.
type execTracker struct {
	mu      sync.Mutex
	running map[string]bool
	// violations counts entries that found the job already running.
	violations atomic.Int64
}

// enter records a job starting.
func (e *execTracker) enter(jobID string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.running == nil {
		e.running = make(map[string]bool)
	}
	if e.running[jobID] {
		e.violations.Add(1)
		return
	}
	e.running[jobID] = true
}

// exit records a job finishing.
func (e *execTracker) exit(jobID string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	delete(e.running, jobID)
}

// count reports how many concurrent executions were observed.
func (e *execTracker) count() int64 { return e.violations.Load() }

// loadWorker is the harness's only worker.
type loadWorker struct {
	track *execTracker
	// seed and failFraction decide which runtime-fanned children fail, matching the
	// generator's shouldFail so a child's outcome is a property of the workload
	// rather than of which worker claimed it.
	seed         int64
	failFraction float64
	// retryBackoff overrides the runner's retry delay for this worker's transient
	// failures. Zero defers to the runner's own exponential backoff; a replay run
	// sets it small so a fail cohort exhausts in bounded wall time rather than
	// climbing the default ladder to its one-minute cap.
	retryBackoff time.Duration
	// snooze, when set, is the claim-then-snooze baseline: the worker consults it
	// before doing any work and returns Result{Snooze} when the downstream is at
	// budget. It is the mechanism the pre-claim gate is measured against — a snooze
	// spends a full claim cycle and a job_runs row without advancing the job.
	snooze *flywheel.TokenBucket
	// outage, when set, is the downstream-outage switch: while the simulated clock
	// is inside its window the worker fails every attempt transiently, so the
	// runner's own exponential backoff — capped by MaxRetryBackoff — governs the
	// retries. Nil on every run but the outage measurement.
	outage *outageState
	// fairness, when set, records the parent ordinal of each claim in execution
	// order, so the report can measure interleaving and non-starvation. Nil on every
	// run but the fairness mix.
	fairness *fairnessRecorder
}

// Kind names the job kind this worker serves.
func (loadWorker) Kind() string { return loadKind }

// NextRetry overrides the runner's retry backoff with a fixed delay when the run
// configured one. Returning zero defers to the runner's own exponential backoff,
// which is what every non-replay run gets — those runs never fail a job, so this is
// never called for them.
func (w loadWorker) NextRetry(error, int) time.Duration { return w.retryBackoff }

// Work simulates the job's cost and returns.
//
// A zero WorkNanos makes it a no-op, which is the configuration that isolates
// the database path: with no simulated work, the throughput a run reports is the
// runtime's own ceiling rather than a measurement of how long the harness chose
// to sleep.
//
// The sleep honors ctx so an execution timeout or a shutdown is observed
// promptly rather than after the full duration.
func (w loadWorker) Work(ctx context.Context, job *flywheel.Job[loadArgs]) (flywheel.Result, error) {
	if w.track != nil {
		w.track.enter(job.ID)
		defer w.track.exit(job.ID)
	}

	// The claim-then-snooze baseline: consult the downstream's budget first, and
	// defer without doing any work when it is exhausted. The deferral is what the
	// pre-claim gate removes — this attempt already spent a claim, a run stub, and a
	// finalize to discover it could not run.
	if w.snooze != nil {
		if g, _ := w.snooze.Acquire(ctx, "downstream", 1); g.N == 0 {
			delay := g.RetryAfter
			if delay <= 0 {
				delay = time.Millisecond
			}
			return flywheel.Result{Snooze: &delay}, nil
		}
	}

	// The downstream outage: while the simulated clock is inside the window every
	// attempt fails transiently. Returning zero from NextRetry (the outage worker
	// carries no retryBackoff) defers to the runner's own exponential backoff, so
	// the ladder that reschedules the job is the one MaxRetryBackoff caps — the
	// whole point of the measurement.
	if w.outage != nil {
		if now := models.ClockFrom(ctx).Now(ctx); w.outage.failing(now) {
			w.outage.failures.Add(1)
			return flywheel.Result{}, errTransientLoad
		}
	}

	// Record which parent this claim served, in execution order — which tracks claim
	// order closely enough at WorkNanos 0 to show whether the parents interleave or
	// one starves the other.
	if w.fairness != nil {
		w.fairness.record(job.Args.Group)
	}

	if d := time.Duration(job.Args.WorkNanos); d > 0 {
		timer := time.NewTimer(d)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-ctx.Done():
			return flywheel.Result{}, ctx.Err()
		}
	}

	if job.Args.Children <= 0 {
		if job.Args.Fail {
			return flywheel.Result{}, errTransientLoad
		}
		return flywheel.Result{}, nil
	}

	// Fan-out: one parent, many children. The children carry no children of
	// their own, so the shape is one level deep and its total job count is
	// knowable in advance — which is what lets the drain loop know when it is
	// done.
	//
	// The children inherit the parent's queue but not its executor class: a
	// FollowUp with the empty class is claimable by any runner, and the runtime
	// gives a worker no way to read the class it was claimed under. Routing them
	// to the run's own class instead would need the class threaded through the
	// args, which is a heavier promise than this shape needs.
	followUps := make([]flywheel.FollowUp, job.Args.Children)
	for i := range followUps {
		childN := job.Args.N*job.Args.Children + i
		followUps[i] = flywheel.FollowUp{
			Kind:   loadKind,
			Args:   loadArgs{N: childN, WorkNanos: job.Args.WorkNanos, Fail: shouldFail(w.seed, childN, w.failFraction)},
			Queue:  job.Queue,
			Parent: true,
		}
	}
	result := flywheel.Result{FollowUps: followUps}
	if job.Args.Barrier {
		// The barrier mix: declare a continuation over this generation. It is a leaf
		// (Children zero, Barrier false), so it fans out nothing and declares nothing
		// — one continuation per parent, run once the whole generation is terminal.
		result.Barrier = &flywheel.Barrier{
			Kind:  loadKind,
			Args:  loadArgs{N: job.Args.N, WorkNanos: job.Args.WorkNanos},
			Queue: job.Queue,
		}
	}
	return result, nil
}

// marshalArgs renders a loadArgs into the JSON payload the enqueue path takes.
func marshalArgs(args loadArgs) ([]byte, error) {
	payload, err := json.Marshal(args)
	if err != nil {
		return nil, fmt.Errorf("loadtest: marshal args: %w", err)
	}
	return payload, nil
}

// discardLogger returns a logger that writes nothing.
//
// The runtime logs a line per failed poll. During a fault window that is one
// line per runner per poll interval, and the I/O would land on the measurement:
// a harness whose logging changes the number it reports is measuring itself.
func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}
