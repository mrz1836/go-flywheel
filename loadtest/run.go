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

	// The terminal backlog is fixture, written before the clock starts: it is
	// never claimed, and its insert rate belongs to no number the run reports.
	if err := seedTerminal(runCtx, h.work, cfg, cfg.TerminalSeed, cfg.TerminalSeedAge); err != nil {
		report.Errors = h.errs.errs()
		report.Notes = h.notes.all()
		return report, err
	}

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
	h.startFaults(ctx)

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

	if h.cfg.Duration > 0 {
		return h.holdSteady(ctx, specs)
	}
	return h.awaitDrain(ctx)
}

// holdSteady keeps the live population constant for Config.Duration, then stops.
//
// It is a closed loop: the replenisher tops the queue back up to the seeded
// population, so one job is enqueued for roughly every job that retires. That
// makes the storage question decidable — under a constant row count, table and
// index growth is churn rather than accumulation, and the two are impossible to
// separate when the population is drifting.
//
// It deliberately does not wait for a drain at the end. The run is bounded by
// its clock, and whatever is in flight when the clock expires is left for
// teardown: draining it would add an unbounded, unmeasured tail to a run whose
// whole point was to be time-bounded.
func (h *Harness) holdSteady(ctx context.Context, specs []jobSpec) error {
	deadline := time.After(h.cfg.Duration)
	ticker := time.NewTicker(replenishInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return h.drainTimeout(ctx)
		case <-deadline:
			return nil
		case <-ticker.C:
			if err := h.replenish(ctx, specs); err != nil {
				if ctx.Err() != nil {
					return h.drainTimeout(ctx)
				}
				h.errs.add(err)
			}
		}
	}
}

// replenish enqueues enough jobs to return the live population to the run's
// target, and reports how it failed if it could not.
//
// The deficit is read from the database rather than inferred from the progress
// counters, because the counters are an estimate by construction — a job the
// sweep reclaims fires neither OnFinish nor OnRetry — and an estimate that
// drifts would make the population drift with it, which is the one thing this
// loop exists to prevent.
func (h *Harness) replenish(ctx context.Context, specs []jobSpec) error {
	active, err := flywheel.CountActiveJobs(ctx, h.probe)
	if err != nil {
		return fmt.Errorf("loadtest: replenish: count active: %w", err)
	}
	deficit := int(int64(h.cfg.Jobs) - active)
	if deficit <= 0 {
		return nil
	}
	// Reuse the generated workload cyclically so the replenished jobs have the
	// same shape distribution as the seeded ones. A different distribution would
	// make the second half of the run a different workload from the first.
	offset := int(h.prog.enqueued.Load())
	batch := make([]jobSpec, deficit)
	for i := range batch {
		batch[i] = specs[(offset+i)%len(specs)]
	}
	// Each replenish is its own generation, so the ids it mints cannot collide
	// with the seed's or with a previous replenish's.
	generation := int(h.generation.Add(1))
	return seedBulkFrom(ctx, h.work, h.cfg, batch, generation,
		func(n int) { h.prog.enqueued.Add(int64(n)) })
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
	h.startFaults(ctx)

	return h.awaitDrain(ctx)
}

// startFaults launches the fault scheduler, if this run has one.
//
// It shares the runners' cancellation, so a reversible fault is always reverted
// before collect reads the result: a run that ended mid-fault would leave the
// harness gated and every subsequent assertion looking at a paused system.
func (h *Harness) startFaults(ctx context.Context) {
	if h.cfg.Faults == nil {
		return
	}
	faultCtx, cancel := context.WithCancel(ctx)
	h.cancelFaults = cancel
	h.wg.Go(func() { h.runFaultScheduler(faultCtx, h.cfg.Faults) })
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
	report.PeakXactAge, report.PeakLockWaits, report.LongestLockWait = h.samples.peaks()
	h.noteSamplingCaveats()

	if report.Duration > 0 {
		report.DrainThroughput = float64(h.prog.terminal()) / report.Duration.Seconds()
		// Capacity is slots × wall time: every runner's pool, for the whole measured
		// window. Dividing attempt-time by it is what makes the number comparable
		// across configurations rather than only against itself.
		capacity := float64(h.cfg.Runners) * float64(h.cfg.Workers) * float64(report.Duration.Nanoseconds())
		if capacity > 0 {
			report.SlotUtilization = float64(h.prog.busyNanos.Load()) / capacity
		}
		h.notes.add(
			"SlotUtilization is attempt time over slots × wall time, and it is a floor: an attempt's "+
				"duration runs from its run stub to its finalize, so the claim, stub, and finalize round "+
				"trips around it are capacity the pool was using and this does not count. It read %.1f%% "+
				"over %d runners × %d workers.",
			report.SlotUtilization*100, h.cfg.Runners, h.cfg.Workers,
		)
	}
	report.BlockedClaims = h.blockedClaims()
	if report.BlockedClaims > 0 {
		h.notes.add(
			"A fault's gate refused %d claim attempts. That is the runners' poll cadence through the "+
				"outage: with the poll backoff it follows the exponential ladder, so a count near "+
				"outage ÷ PollInterval would mean the ladder did not engage.",
			report.BlockedClaims,
		)
	}
	report.Enqueued = h.prog.enqueued.Load()
	report.Retried = h.prog.retried.Load()
	report.Reclaimed = h.prog.reclaimed.Load()
	report.ConcurrentExecutions = h.exec.count()

	if report.ConcurrentExecutions > 0 {
		// This is the exactly-once guarantee failing, observed in process. It is a
		// finding, not a statistic, so it is an error as well as a number.
		h.errs.add(fmt.Errorf(
			"loadtest: %d concurrent executions of the same job were observed: %w",
			report.ConcurrentExecutions, ErrExactlyOnceViolated,
		))
	}
	if report.Reclaimed > 0 && h.cfg.Faults == nil {
		h.notes.add(
			"The sweeper reclaimed %d expired leases in a run with no injected fault. That is a finding: "+
				"an attempt outlived its lease, which with renewal enabled means the heartbeat did not keep up.",
			report.Reclaimed,
		)
	}

	counts, err := h.stateCounts(ctx)
	if err != nil {
		return err
	}
	report.Drained = counts[string(flywheel.StateSucceeded)]
	report.Discarded = counts[string(flywheel.StateDiscarded)] + counts[string(flywheel.StateCancelled)]

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

	// The same rule for storage parameters: report what pg_class actually holds,
	// not the condition that was requested.
	if opts, err := installedStorage(ctx, h.probe); err == nil {
		report.StorageParams = opts
		h.notes.add("Storage condition %q installed reloptions: jobs=%q, job_runs=%q.",
			h.cfg.Storage, opts["jobs"], opts["job_runs"])
	} else {
		h.errs.add(err)
	}

	if h.cfg.Duration > 0 {
		h.notes.add(
			"This run is a closed loop: one job was enqueued for every job that retired, so the " +
				"non-terminal working set stayed at Jobs and EnqueueThroughput equals DrainThroughput " +
				"by construction. Neither is a throughput ceiling, and comparing either against an " +
				"open-loop drain baseline is comparing a governor setting against a redline.",
		)
		if h.cfg.RetentionMaxAge <= 0 {
			h.notes.add(
				"The closed loop bounds the working set, not the table: retired jobs stay as terminal " +
					"rows, so total row count climbed at the drain rate for this run. Table growth here " +
					"is churn plus accumulation and the two are not separable. A storage comparison " +
					"wants RetentionMaxAge set as well, short enough to keep the terminal tail bounded.",
			)
		}
	}

	// Superseded is a measurement now: the runtime reports every discarded
	// attempt through the Observer, so the harness counts events rather than
	// inferring from row residue after the fact.
	report.Superseded = h.prog.superseded.Load()
	if err := h.crossCheckSuperseded(ctx, report.Superseded); err != nil {
		return err
	}
	if report.Superseded > 0 {
		note := "%d attempts were superseded: their work ran and its outcome was discarded because the " +
			"claim was lost."
		if h.cfg.Faults == nil {
			note += " No fault was injected, so this is a finding rather than the experiment."
		}
		h.notes.add(note, report.Superseded)
	}
	return nil
}

// crossCheckSuperseded compares the observed supersede count against the row
// residue the old inference used, and notes a disagreement.
//
// The residue query is kept precisely because the event replaced it. The two
// count different things and are expected to differ — the residue misses a
// superseded attempt whose job later succeeded on a retry, and counts a
// successful run row against a job an operator cancelled afterwards — so this is
// a smoke check on the event plumbing, not an assertion. A wildly larger residue
// than event count is the signal that supersedes are happening somewhere the
// Observer does not see.
func (h *Harness) crossCheckSuperseded(ctx context.Context, observed int64) error {
	residue, err := h.supersededCount(ctx)
	if err != nil {
		return err
	}
	if residue != observed {
		h.notes.add(
			"Superseded is %d by observed event and %d by row residue (a successful run row against a job "+
				"that did not end succeeded). The two count different things and are expected to differ; a "+
				"large gap means supersedes are occurring where the Observer does not see them.",
			observed, residue,
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

// supersededCount counts the row residue a supersede leaves behind: a run row
// that recorded success against a job that did not end up succeeded.
//
// This used to be the *only* way to see a supersede — the runtime computed the
// signal and returned nil regardless, so neither an Observer nor a Driver
// decorator could observe it. It is now the cross-check rather than the number,
// and it is kept for exactly that: an independent view, derived from the
// database rather than from the event stream the plumbing under test produces.
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
