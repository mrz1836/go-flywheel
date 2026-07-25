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
	p, specs := h.prepareWorkload()

	var driveErr error
	if cfg.Mix == WorkloadSteady {
		// Enqueue and drain overlap by definition here, so there is no separate
		// seed window to exclude: the whole run is the measured window, and both
		// rates are computed against it.
		report.StartedAt = time.Now()
		driveErr = h.driveSteady(runCtx, p, specs)
		report.Duration = time.Since(report.StartedAt)
		if report.Duration > 0 {
			report.EnqueueThroughput = float64(h.prog.enqueued.Load()) / report.Duration.Seconds()
		}
	} else {
		seedElapsed, seedErr := h.insert(runCtx, p, specs)
		if seedErr != nil {
			report.Errors = h.errs.errs()
			report.Notes = h.notes.all()
			return report, seedErr
		}
		if seedElapsed > 0 {
			report.EnqueueThroughput = float64(h.prog.enqueued.Load()) / seedElapsed.Seconds()
		}

		report.StartedAt = time.Now()
		driveErr = h.drive(runCtx)
		report.Duration = time.Since(report.StartedAt)
	}

	if err := h.collect(context.WithoutCancel(ctx), &report); err != nil {
		h.errs.add(err)
	}
	report.WorkloadDigest = h.digest
	report.Errors = h.errs.errs()
	report.Notes = h.notes.all()

	return report, driveErr
}

// prepareWorkload generates the run's workload and records what it is.
//
// Generation is single-threaded and finishes here, before any runner exists.
// That is what makes "concurrency does not change the workload" a property this
// harness can assert rather than assume: generate at Runners: 1 and generate at
// Runners: 64 return identical slices, and the digest recorded here proves it
// after the fact from a committed report.
func (h *Harness) prepareWorkload() (mixPlan, []jobSpec) {
	p := plan(h.cfg)
	specs := generate(h.cfg)
	h.digest = workloadDigest(specs)

	// A fan-out run inserts parents and creates children as it drains, so the
	// drain denominator is the total job count, not the seeded row count.
	h.prog.target = int64(p.TotalJobs())

	if p.Bulk {
		h.notes.add(
			"Seeding used the bulk path, so EnqueueThroughput is the harness's batch-insert rate " +
				"rather than the runtime's producer API. The enqueue and steady mixes measure the API.",
		)
	}
	return p, specs
}

// insert loads the workload and reports how long it took. The window covers the
// inserts and nothing else: generation happened in prepareWorkload, schema
// provisioning in newHarness, and teardown after collect.
func (h *Harness) insert(ctx context.Context, p mixPlan, specs []jobSpec) (time.Duration, error) {
	start := time.Now()
	if p.Bulk {
		err := seedBulk(ctx, h.work, h.cfg, specs, func(n int) { h.prog.enqueued.Add(int64(n)) })
		return time.Since(start), err
	}
	err := seedAPI(ctx, h.work, h.cfg, specs, func() { h.prog.enqueued.Add(1) })
	return time.Since(start), err
}

// driveSteady runs the enqueue and the drain against each other, which is the
// only mix where queue depth is the difference between two rates rather than a
// starting condition.
//
// The drain wait does not begin until seeding has finished. It has to: the
// completion check asks whether any job is active, and asked one microsecond
// after the runners started it would answer "none" — because none had been
// inserted yet — and declare the run complete before it began.
func (h *Harness) driveSteady(ctx context.Context, p mixPlan, specs []jobSpec) error {
	if err := h.startRunners(ctx); err != nil {
		return err
	}
	defer h.stopRunners()

	seedDone := make(chan error, 1)
	go func() {
		_, err := h.insert(ctx, p, specs)
		seedDone <- err
	}()

	select {
	case err := <-seedDone:
		if err != nil {
			return err
		}
	case <-ctx.Done():
		return h.drainTimeout(ctx)
	}
	return h.awaitDrain(ctx)
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
	// The runners and the sweeper have stopped by now, so the merge sees a
	// settled distribution and has a happens-before edge on every observation.
	report.Claim = h.timings.claim.latency()
	report.Finalize = h.timings.finalize.latency()
	report.Sweep = h.timings.sweep.latency()
	report.Histogram = histogramSpec()
	h.noteHistogramRange()

	// One last reading now the runners have stopped: the periodic series ended
	// wherever the drain did, and PostgreSQL's cumulative statistics only settle
	// once the runners' backends have gone idle.
	h.finalSample(ctx)
	report.Storage = h.samples.all()
	report.PeakRSS = h.peakRSS()
	h.noteSamplingCaveats()

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

// noteHistogramRange discloses when observations fell outside the histogram's
// covered range.
//
// The published relative error applies to quantiles drawn from the in-range
// buckets. A value below 1 µs or above 137 s lands in an unbounded bucket that
// can only report its finite edge, so a report whose quantiles came from one is
// making a weaker claim than its error bar suggests — and has to say so.
func (h *Harness) noteHistogramRange() {
	for name, rec := range map[string]*recorder{
		"claim": h.timings.claim, "finalize": h.timings.finalize, "sweep": h.timings.sweep,
	} {
		m := rec.merge()
		if m.under > 0 {
			h.notes.add(
				"%d %s observations fell below the histogram's 1 µs floor; quantiles drawn from that "+
					"bucket are not covered by the published relative error.", m.under, name,
			)
		}
		if m.over > 0 {
			h.notes.add(
				"%d %s observations exceeded the histogram's 137 s ceiling; quantiles drawn from that "+
					"bucket are not covered by the published relative error.", m.over, name,
			)
		}
	}
}

// peakRSS reports the highest resident set the run reached.
//
// It is the larger of what the sampler saw and what getrusage reports, and each
// term repairs a gap in the other. On linux the sampler reads a genuine current
// RSS but can miss a spike between two ticks, which getrusage's high-water mark
// catches. On darwin the sampler is already reading that high-water mark, so the
// two agree and the second term is the true peak either way.
func (h *Harness) peakRSS() uint64 {
	peak := h.samples.peak()
	if rusage, ok := getrusageMaxRSS(); ok && rusage > peak {
		return rusage
	}
	return peak
}
