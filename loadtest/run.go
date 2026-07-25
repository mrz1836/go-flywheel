//go:build loadtest

package loadtest

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	flywheel "github.com/mrz1836/go-flywheel"
)

// completionCheckInterval is how often the drain loop asks the database whether
// any job is still active.
//
// It is the authoritative check — the observer counters are an estimate, and a
// job reclaimed by the sweep fires no event at all — so it has to run, but it is
// a COUNT over the jobs table and must not become a load in its own right. A
// quarter second is short enough that a two-thousand-job scenario does not spend
// its tail waiting for a tick, and long enough that a 100k run pays it a few
// hundred times rather than a few hundred thousand.
const completionCheckInterval = 250 * time.Millisecond

// Run executes one configured run end to end: provision an isolated schema,
// install the production DDL under the configured index condition, seed, drive,
// and tear down.
//
// It is the single entry point behind both the Benchmark functions and the
// scenario mains, and it is deliberately a thin composition of five steps —
// newHarness, seed, drive, collect, Close — rather than one function. The
// benchmarks have to stop the clock around provisioning and seeding, which is
// most of the wall time of a 100k run and none of what they measure, and they
// can only do that if the steps are separable.
//
// A Report is returned even when the run fails, populated with whatever was
// measured before the failure: a run that timed out mid-drain has produced real
// numbers up to that point, and discarding them would make a partial result
// indistinguishable from no result.
func Run(ctx context.Context, cfg Config) (Report, error) {
	cfg, err := cfg.validate()
	if err != nil {
		return Report{}, err
	}

	runCtx, cancel := context.WithTimeout(ctx, cfg.Timeout)
	defer cancel()

	h, err := newHarness(runCtx, cfg)
	if h == nil {
		return Report{}, err
	}
	defer func() {
		// Teardown runs on the caller's context, not the run's: a run that hit its
		// timeout has a cancelled context, and dropping the schema through it
		// would fail and leak the schema.
		if closeErr := h.Close(context.WithoutCancel(ctx)); closeErr != nil {
			h.errs.add(closeErr)
		}
	}()
	if err != nil {
		return Report{}, err
	}

	report := Report{Config: cfg, Schema: h.schema}

	seedElapsed, err := h.seed(runCtx)
	if err != nil {
		return report, err
	}
	if seedElapsed > 0 {
		report.EnqueueThroughput = float64(h.prog.enqueued.Load()) / seedElapsed.Seconds()
	}

	report.StartedAt = time.Now()
	driveErr := h.drive(runCtx)
	report.Duration = time.Since(report.StartedAt)

	if err := h.collect(context.WithoutCancel(ctx), &report); err != nil {
		h.errs.add(err)
	}
	report.Errors = h.errs.errs()
	report.Notes = h.notes.all()

	return report, driveErr
}

// seed loads the target with the run's workload and reports how long it took.
//
// The elapsed time is the enqueue measurement, so it covers the inserts and
// nothing else: schema provisioning happened in newHarness and teardown happens
// after collect, neither of which is in this window.
func (h *Harness) seed(ctx context.Context) (time.Duration, error) {
	if err := h.supportedMix(); err != nil {
		return 0, err
	}

	start := time.Now()
	client := flywheel.NewClient(h.work)
	args := loadArgs{WorkNanos: h.cfg.WorkDuration.Nanoseconds()}

	for i := range h.cfg.Jobs {
		if err := ctx.Err(); err != nil {
			return time.Since(start), fmt.Errorf("loadtest: seed interrupted after %d jobs: %w", i, err)
		}
		args.N = i
		payload, err := marshalArgs(args)
		if err != nil {
			return time.Since(start), err
		}
		if _, err := flywheel.Enqueue(ctx, client, loadKind, payload, flywheel.InsertOpts{
			Queue:         h.cfg.Queue,
			ExecutorClass: flywheel.ExecutorClass(h.cfg.ExecutorClass),
		}); err != nil {
			return time.Since(start), fmt.Errorf("loadtest: seed job %d: %w", i, err)
		}
		h.prog.enqueued.Add(1)
	}
	return time.Since(start), nil
}

// supportedMix reports whether this build can generate the configured mix.
//
// fan-out and mixed-speed need per-job variation — a child count, a bimodal work
// duration — which is the workload generator's job. Until it lands, asking for
// one of them is an error rather than a silently uniform workload: a report
// labelled "mixed-speed" that measured a uniform run is worse than no report.
func (h *Harness) supportedMix() error {
	switch h.cfg.Mix {
	case WorkloadEnqueueOnly, WorkloadDrainOnly, WorkloadSteady:
		return nil
	default:
		return fmt.Errorf(
			"loadtest: mix %q needs the workload generator, which is not in this build: %w",
			h.cfg.Mix, ErrInvalidConfig,
		)
	}
}

// drive runs the workload to completion.
//
// The enqueue-only mix has nothing to drive: its measurement is the seed, and
// starting a runner would drain the very rows whose insert rate is the number.
func (h *Harness) drive(ctx context.Context) error {
	if h.cfg.Mix == WorkloadEnqueueOnly {
		return nil
	}

	if err := h.startRunners(ctx); err != nil {
		return err
	}
	defer h.stopRunners()

	return h.awaitDrain(ctx)
}

// awaitDrain blocks until no job is in a non-terminal state, the run's context
// ends, or the run times out.
//
// The check runs on the probe pool, which never competes with the runners for a
// connection — a completion check that queued behind the work it is waiting for
// would report the drain as slower than it was.
func (h *Harness) awaitDrain(ctx context.Context) error {
	ticker := time.NewTicker(completionCheckInterval)
	defer ticker.Stop()

	for {
		active, err := flywheel.CountActiveJobs(ctx, h.probe)
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			// A transient failure of the check is not a failure of the run: it is
			// collected and the next tick tries again.
			h.errs.add(fmt.Errorf("loadtest: completion check: %w", err))
		} else if active == 0 {
			return nil
		}

		select {
		case <-ticker.C:
		case <-ctx.Done():
			return h.drainTimeout(ctx)
		}
	}
	return h.drainTimeout(ctx)
}

// drainTimeout renders an interrupted drain, distinguishing the harness's own
// timeout from a caller's cancellation and stating how far the run got. "Timed
// out" alone does not distinguish a run that stalled immediately from one that
// missed the deadline on its last few jobs, and those are different findings.
func (h *Harness) drainTimeout(ctx context.Context) error {
	pct := 100 * h.prog.fraction()
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return fmt.Errorf(
			"loadtest: drain reached %.1f%% and did not finish within %s: %w",
			pct, h.cfg.Timeout, ErrRunTimedOut,
		)
	}
	return fmt.Errorf("loadtest: drain interrupted at %.1f%%: %w", pct, ctx.Err())
}

// collect fills in the measured outcome of a run from the database, which is the
// authoritative account — the observer counters are a live estimate for
// scheduling, and a job the sweep reclaimed fires no event at all.
func (h *Harness) collect(ctx context.Context, report *Report) error {
	if report.Duration > 0 {
		report.DrainThroughput = float64(h.prog.terminal()) / report.Duration.Seconds()
	}
	report.Enqueued = h.prog.enqueued.Load()
	report.Retried = h.prog.retried.Load()

	counts, err := h.stateCounts(ctx)
	if err != nil {
		return err
	}
	report.Drained = counts[string(flywheel.StateSucceeded)]
	report.Discarded = counts[string(flywheel.StateDiscarded)] + counts[string(flywheel.StateCancelled)]

	superseded, err := h.supersededCount(ctx)
	if err != nil {
		return err
	}
	// Record what the target actually carried, not what was asked for. The
	// index-condition delta is the point of two of these runs, and a report that
	// only echoed its own Config could not tell a real delta from a schema that
	// silently came up wrong.
	if names, err := installedIndexes(ctx, h.probe); err == nil {
		h.notes.add("Index condition %q installed %d indexes: %s.",
			h.cfg.Indexes, len(names), strings.Join(names, ", "))
	} else {
		h.errs.add(err)
	}

	report.Superseded = superseded
	if superseded > 0 {
		h.notes.add(
			"Superseded (%d) is an inference, not a measurement: the runtime's finalize computes the "+
				"supersede signal and discards it, so it is unobservable through the Driver and the Observer. "+
				"The number counts successful run rows whose job did not end succeeded.", superseded,
		)
	}
	return nil
}

// stateCounts reports how many jobs are in each state.
func (h *Harness) stateCounts(ctx context.Context) (map[string]int64, error) {
	type row struct {
		State string
		N     int64
	}
	var rows []row
	if err := h.probe.WithContext(ctx).
		Raw(`SELECT state, count(*) AS n FROM jobs GROUP BY state`).Scan(&rows).Error; err != nil {
		return nil, fmt.Errorf("loadtest: count jobs by state: %w", err)
	}
	out := make(map[string]int64, len(rows))
	for _, r := range rows {
		out[r.State] = r.N
	}
	return out, nil
}

// supersededCount infers how many finalizations were superseded.
//
// It is an inference and is labelled as one wherever it is reported. The runtime
// computes the signal — a finalize whose state advance matched no row, because a
// cancel or a lease reclaim moved the job out of running underneath it — and
// then returns nil regardless, so neither an Observer nor a Driver decorator can
// see it. What is observable is the residue: a run row that recorded success
// against a job that did not end up succeeded.
func (h *Harness) supersededCount(ctx context.Context) (int64, error) {
	var n int64
	if err := h.probe.WithContext(ctx).Raw(
		`SELECT count(*) FROM job_runs r JOIN jobs j ON j.id = r.job_id
		 WHERE r.outcome = ? AND j.state <> ?`,
		string(flywheel.OutcomeSuccess), string(flywheel.StateSucceeded),
	).Scan(&n).Error; err != nil {
		return 0, fmt.Errorf("loadtest: count superseded finalizations: %w", err)
	}
	return n, nil
}
