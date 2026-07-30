//go:build loadtest

package loadtest

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	flywheel "github.com/mrz1836/go-flywheel"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// Harness is one provisioned run: an isolated schema, three connection pools,
// the runners driving it, and the counters and collectors they feed.
//
// # Concurrency contract
//
// A Harness holds no context.Context. Every field is either immutable after
// newHarness returns or is itself concurrency-safe (an atomic, or a type with
// its own lock), so there is no Harness-level mutex and no field a caller must
// hold one to read. That is a requirement rather than an optimization: a fault
// fires from a different goroutine than the one that built the harness, while
// the runners are mid-flight, and a design in which a fault has to take a lock
// the drain path also takes is a design in which injecting a fault perturbs the
// measurement.
//
// The surface a fault touches is methods, not fields, for the same reason: the
// method can be made safe once, where a field cannot.
type Harness struct {
	cfg    Config
	schema string

	// admin owns CREATE SCHEMA and DROP SCHEMA and is bound to the raw DSN, so
	// teardown does not depend on the schema it is dropping still resolving.
	admin *gorm.DB
	// work is the runners' pool, schema-scoped.
	work *gorm.DB
	// probe is a single schema-scoped connection for the sampler and the
	// authoritative completion check. It is separate so sampling never queues
	// behind the run it is measuring — and, once faults exist, so a fault that
	// gates the work pool leaves the harness able to see through it.
	probe *gorm.DB
	// sched is the scheduler's pool, schema-scoped, sized to its activity count
	// so a maintenance pass never queues behind the runners it is measuring.
	sched *gorm.DB
	// limiterDB is the shared DBLimiter's own pool, opened only for -limiter db so
	// its Acquire never queues behind the work pool. Nil otherwise.
	limiterDB *gorm.DB

	// limiter is the pre-claim gate every runner shares, nil on an ungated run. One
	// instance keys per resource, so sharing it is what makes the whole run's claim
	// rate track a single budget rather than Runners budgets.
	limiter flywheel.Limiter

	// clock is the advanceable simulated clock a clock-driven fault runs on, nil on
	// every other run. It is injected into the runners' context so the runtime's
	// sense of when a retry is due is the harness's to compress.
	clock *harnessClock
	// outage is the downstream-outage switch the worker reads, nil unless a
	// DownstreamOutage fault is configured.
	outage *outageState
	// fairness records the parent ordinal of each claim in order, nil unless the
	// run is the fairness mix.
	fairness *fairnessRecorder

	// inner is the undecorated driver. Each runner wraps it in its own
	// timingDriver bound to its own histogram shard, so shard selection costs
	// nothing on the hot path.
	inner   flywheel.Driver
	timings *timings

	registry *flywheel.Registry

	// digest identifies the generated workload. It is set once, by
	// prepareWorkload, before any runner exists.
	digest string

	// replay records the replay phase's outcome on a -replay run, set by drive
	// while the runners are still up and read by collect. It is nil on a run that
	// did not replay.
	replay *ReplayReport

	prog    *progress
	errs    *errset
	notes   *noteset
	samples *sampleset
	exec    *execTracker

	// gates are the per-runner fault gates, indexed by runner ordinal. A fault
	// reaches a runner's driver through its gate and through nothing else.
	gates []*gate
	// killed counts runners a fault has stopped, so a scenario can assert the
	// fault actually fired rather than assuming it did.
	killed atomic.Int64
	// generation counts bulk-seed generations, so a closed-loop run's repeated
	// top-ups mint disjoint id spaces rather than colliding on the primary key.
	generation atomic.Int64

	runners       []*runnerHandle
	cancelSweeper context.CancelFunc
	cancelFaults  context.CancelFunc
	wg            sync.WaitGroup
}

// progress is the run's live completion state, fed by the harness's own
// Observer.
//
// The counters exist so a fault can be scheduled against how far the run has
// got, cheaply. A naive scheduler would ask the database — a COUNT(*) over a
// 100k-row table, repeatedly, on the pool it is trying not to perturb. Two
// atomic loads cost nothing and cannot contend with the drain.
type progress struct {
	// target is the job count the run is driving toward. Immutable.
	target int64

	enqueued  atomic.Int64
	finished  atomic.Int64
	retried   atomic.Int64
	discarded atomic.Int64
	// reclaimed counts jobs the harness's sweeper returned to available. A
	// reclaimed job runs again, so it is progress undone.
	reclaimed atomic.Int64
	// superseded counts attempts whose outcome the runtime discarded because
	// their claim was lost. It is the direct measurement of double execution,
	// fed by the Observer rather than inferred from row residue after the fact.
	superseded atomic.Int64
	// busyNanos accumulates the duration of every decided attempt, which is what
	// turns the run into a slot-utilization number. Superseded attempts count: the
	// work ran and held a slot for as long as it took, whatever the runtime then
	// did with its outcome.
	busyNanos atomic.Int64
}

// terminal estimates how many jobs have reached a terminal state.
//
// OnFinish fires for every decided attempt, including the ones that will retry,
// and OnRetry fires for exactly that subset — so finished − retried is the count
// of attempts that ended the job. It is an estimate, not a ledger: a job
// reclaimed by the sweep after its runner was gated never fires either event.
// The authoritative number is CountActiveJobs on the probe pool, which the drain
// loop checks on its own ticker; this is the cheap one, for scheduling.
func (p *progress) terminal() int64 {
	return p.finished.Load() - p.retried.Load()
}

// fraction reports drain progress in [0,1], clamped. A zero target yields 0
// rather than a division by zero.
func (p *progress) fraction() float64 {
	if p.target <= 0 {
		return 0
	}
	done := float64(p.terminal()) / float64(p.target)
	switch {
	case done < 0:
		return 0
	case done > 1:
		return 1
	default:
		return done
	}
}

// harnessObserver feeds progress from the runner's lifecycle events. It is the
// harness's own Observer, wired through RunnerConfig like any consumer's would
// be — the harness does not reach into the runtime for this.
//
// Every method must return immediately: the Runner calls them synchronously on
// the dispatch path, so anything slower than an atomic add here would show up in
// the very latency numbers the run exists to produce.
type harnessObserver struct {
	prog *progress
}

// OnClaim is not counted: the claim count is the timing driver's, taken at the
// boundary that matters, and counting it twice invites the two to disagree.
func (harnessObserver) OnClaim(context.Context, flywheel.ClaimEvent) {}

// OnStart is not counted: a started attempt is not progress toward drain.
func (harnessObserver) OnStart(context.Context, flywheel.JobEvent) {}

// OnFinish counts every decided attempt, and separately the ones that ended the
// job permanently unsuccessfully.
func (o harnessObserver) OnFinish(_ context.Context, ev flywheel.FinishEvent) {
	o.prog.finished.Add(1)
	o.prog.busyNanos.Add(ev.Duration.Nanoseconds())
	if ev.Outcome == flywheel.OutcomeError && ev.ErrorClass == flywheel.ErrorPermanent {
		o.prog.discarded.Add(1)
	}
}

// OnRetry counts the subset of finished attempts that will run again, which is
// what makes finished − retried the terminal count.
func (o harnessObserver) OnRetry(context.Context, flywheel.RetryEvent) {
	o.prog.retried.Add(1)
}

// OnSupersede counts attempts whose outcome the runtime discarded.
//
// It is deliberately not counted as progress. A superseded attempt advanced
// nothing, so the job it ran is still in flight and folding it into finished
// would make the drain fraction — which schedules every fault — report a run as
// further along than it is.
func (o harnessObserver) OnSupersede(_ context.Context, ev flywheel.SupersedeEvent) {
	o.prog.superseded.Add(1)
	// It occupied a slot even though it advanced nothing, so it counts toward
	// utilization while staying out of the drain fraction above.
	o.prog.busyNanos.Add(ev.Duration.Nanoseconds())
}

// noteset collects measurement caveats. It is a type rather than a slice because
// notes are appended from the sampler goroutine as well as the run goroutine —
// whether pgstattuple was available is not known until the first sample.
type noteset struct {
	mu    sync.Mutex
	notes []string
	seen  map[string]bool
}

// add records a caveat once. Repeats are dropped: a note appended per sample
// would bury the report it is meant to qualify.
func (n *noteset) add(format string, args ...any) {
	note := fmt.Sprintf(format, args...)

	n.mu.Lock()
	defer n.mu.Unlock()
	if n.seen == nil {
		n.seen = make(map[string]bool)
	}
	if n.seen[note] {
		return
	}
	n.seen[note] = true
	n.notes = append(n.notes, note)
}

// all returns a copy of the collected notes in the order they were added.
func (n *noteset) all() []string {
	n.mu.Lock()
	defer n.mu.Unlock()
	if len(n.notes) == 0 {
		return nil
	}
	return append([]string(nil), n.notes...)
}

// runnerHandle is one running Runner and the controls the harness has over it.
type runnerHandle struct {
	// index is the runner's ordinal, stable for the run. It selects the runner's
	// histogram shard and names it in a fault's report.
	index  int
	runner *flywheel.Runner
	cancel context.CancelFunc
	// done closes when the runner's goroutine has returned.
	done chan struct{}
}

// newHarness provisions a run: the three pools, the isolated schema, the
// production DDL under the configured index condition, the driver, and the
// registry. It does not seed and does not start any runner — that split is what
// lets a benchmark stop its timer around provisioning, which is most of the
// wall clock of a 100k run and none of what the benchmark is measuring.
//
// The caller owns Close, including on the error paths after this returns.
func newHarness(ctx context.Context, cfg Config) (*Harness, error) {
	cfg, err := cfg.validate()
	if err != nil {
		return nil, err
	}

	h := &Harness{
		cfg:     cfg,
		schema:  newSchemaName(),
		errs:    &errset{},
		notes:   &noteset{},
		samples: &sampleset{},
		exec:    &execTracker{},
		prog:    &progress{target: int64(cfg.Jobs)},
	}

	// The admin pool stays tiny and is bound to the raw DSN: it exists to create
	// and drop the schema, and binding it to the schema it drops would make
	// teardown depend on the thing being torn down.
	if h.admin, err = openPool(cfg.DSN, 2); err != nil {
		return nil, fmt.Errorf("loadtest: open admin pool: %w", err)
	}
	if h.admin.Name() != "postgres" {
		return h, fmt.Errorf("loadtest: target dialect is %q: %w", h.admin.Name(), ErrUnsupportedDialect)
	}
	if err = createSchema(ctx, h.admin, h.schema); err != nil {
		return h, err
	}

	scoped := withSearchPath(cfg.DSN, h.schema)

	// The work pool is sized to the run's declared in-flight capacity, and its
	// idle count is pinned to its open count. database/sql defaults MaxIdleConns
	// to 2, so a 32-connection pool would close and reopen 30 TCP connections
	// per burst and the measurement would be dominated by connection setup —
	// which production, with a long-lived pool, never pays.
	if h.work, err = openPool(scoped, cfg.Runners*(cfg.Workers+1)); err != nil {
		return h, fmt.Errorf("loadtest: open work pool: %w", err)
	}
	if h.probe, err = openPool(scoped, 1); err != nil {
		return h, fmt.Errorf("loadtest: open probe pool: %w", err)
	}
	// The scheduler gets its own pool rather than sharing the runners'. Its two
	// activities would otherwise queue behind the work pool whenever the runners
	// saturated it, and the wait would land inside Report.Sweep — which is the
	// number published as the longest maintenance transaction. A sweep latency
	// that is mostly connection acquisition measures the harness's pool sizing,
	// not the runtime.
	if h.sched, err = openPool(scoped, schedulerConnections); err != nil {
		return h, fmt.Errorf("loadtest: open scheduler pool: %w", err)
	}

	if err = installSchema(ctx, h.work, cfg.Indexes, cfg.Storage); err != nil {
		return h, err
	}

	// A clock-driven fault runs on an advanceable clock and needs a lease that
	// survives time compression: the advancer only steps the clock once nothing is
	// running, so a lease longer than any single step cannot expire mid-flight.
	// leaseFor honors an explicit Lease, so this only fills the zero.
	if _, ok := cfg.Faults.(clockDrivenFault); ok {
		h.clock = newHarnessClock()
		h.outage = &outageState{}
		if cfg.Lease == 0 {
			cfg.Lease = outageLease
			h.cfg.Lease = outageLease
		}
	}
	if cfg.Mix == WorkloadFairness {
		h.fairness = &fairnessRecorder{}
	}

	h.inner = flywheel.NewPostgresDriverWithOptions(h.work, workDriverOpts(cfg))
	h.timings = newTimings(cfg.Runners)
	h.registry = flywheel.NewRegistry()
	// The worker carries the seed and fail fraction so a runtime-fanned child's
	// outcome matches the generator's, and a fast retry backoff on a fail run so the
	// cohort exhausts to discarded in bounded wall time rather than climbing the
	// default ladder to its one-minute cap.
	var retryBackoff time.Duration
	if cfg.FailFraction > 0 {
		retryBackoff = replayRetryBackoff
	}
	// The claim-then-snooze baseline: an in-process bucket the worker consults, so a
	// job it cannot run yet spends a full claim and a job_runs row deferring itself.
	var snooze *flywheel.TokenBucket
	if cfg.WorkerSnooze > 0 {
		snooze = flywheel.NewTokenBucket(flywheel.TokenBucketConfig{
			Rate: cfg.WorkerSnooze, Interval: time.Second, Burst: cfg.WorkerSnooze,
		})
	}
	flywheel.Register(h.registry, loadWorker{
		track: h.exec, seed: cfg.Seed, failFraction: cfg.FailFraction,
		retryBackoff: retryBackoff, snooze: snooze, outage: h.outage, fairness: h.fairness,
	})

	if err = h.buildLimiter(scoped); err != nil {
		return h, err
	}

	return h, nil
}

// limiterResource is the single resource every gated harness runner keys on: the
// run protects one simulated downstream, so one resource is the whole budget.
const limiterResource = "loadtest:downstream"

// limiterResourceFor names the resource a runner passes alongside a limiter, and
// the empty string when there is none — NewRunner requires a Resource only when a
// Limiter is set.
func limiterResourceFor(l flywheel.Limiter) string {
	if l == nil {
		return ""
	}
	return limiterResource
}

// buildLimiter constructs the shared pre-claim gate for a gated run, opening the
// DBLimiter its own pool so its Acquire never queues behind the work pool. An
// ungated run leaves h.limiter nil, which the runners read as unlimited.
func (h *Harness) buildLimiter(scopedDSN string) error {
	switch h.cfg.Limiter {
	case LimiterTokenBucket:
		h.limiter = flywheel.NewTokenBucket(flywheel.TokenBucketConfig{
			Rate: h.cfg.Rate, Interval: time.Second, Burst: h.cfg.Burst, MaxConcurrent: h.cfg.MaxConcurrent,
		})
	case LimiterDB:
		db, err := openPool(scopedDSN, h.cfg.limiterConnections())
		if err != nil {
			return fmt.Errorf("loadtest: open limiter pool: %w", err)
		}
		h.limiterDB = db
		holdTTL := time.Duration(0)
		if h.cfg.MaxConcurrent > 0 {
			holdTTL = leaseFor(h.cfg) * 4 // comfortably longer than any job, per HoldTTL's rule
		}
		lim, err := flywheel.NewDBLimiter(db, flywheel.DBLimiterConfig{
			Rate: h.cfg.Rate, Interval: time.Second, Burst: h.cfg.Burst,
			MaxConcurrent: h.cfg.MaxConcurrent, HoldTTL: holdTTL,
		})
		if err != nil {
			return fmt.Errorf("loadtest: build limiter: %w", err)
		}
		h.limiter = lim
	}
	return nil
}

// replayRetryBackoff is the fixed retry delay a fail-fraction run gives its worker,
// so a fail cohort's attempts exhaust quickly and deterministically instead of
// following the runner's exponential ladder up to its one-minute cap.
const replayRetryBackoff = 250 * time.Millisecond

// outageLease is the simulated lease a clock-driven run gives its runners when
// none was set. It is comfortably longer than any single clock advance (which is
// bounded by MaxRetryBackoff), so a claim cannot expire while the advancer steps
// the clock — the advancer only steps once nothing is running, but a generous
// lease removes the race by construction.
const outageLease = time.Hour

// outagePollBackoff caps the runners' poll ladder on a clock-driven run. Between
// advances the queue is quiesced — every retry is scheduled in the simulated
// future — so an ordinary run's ladder would climb toward its 30s ceiling and the
// runner would then sit out the moment the advancer releases the next generation.
// A tight ceiling keeps the runner polling fast enough to claim each released
// batch promptly, which is what keeps the compressed run to seconds.
const outagePollBackoff = 10 * time.Millisecond

// openPool opens one gorm connection pool with a fixed size.
func openPool(dsn string, maxOpen int) (*gorm.DB, error) {
	if maxOpen < 1 {
		maxOpen = 1
	}
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
		// The harness measures the runtime, so it must not add work the runtime
		// would not do. Skipping the default transaction is what a tuned host
		// does; leaving it on would put a BEGIN/COMMIT around every single-row
		// insert and the seeded throughput would be the harness's, not the
		// runtime's.
		SkipDefaultTransaction: true,
	})
	if err != nil {
		return nil, fmt.Errorf("loadtest: open %d-connection pool: %w", maxOpen, err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		return nil, fmt.Errorf("loadtest: resolve sql.DB: %w", err)
	}
	sqlDB.SetMaxOpenConns(maxOpen)
	sqlDB.SetMaxIdleConns(maxOpen)
	sqlDB.SetConnMaxLifetime(0)
	return db, nil
}

// Runners reports how many runner loops the run is driving with. It is a method
// rather than an exported field so a fault cannot reach past it into the slice.
func (h *Harness) Runners() int { return len(h.runners) }

// Note records a measurement caveat on the run's report.
func (h *Harness) Note(format string, args ...any) { h.notes.add(format, args...) }

// Schema returns the isolated schema this run provisioned.
func (h *Harness) Schema() string { return h.schema }

// Close stops every runner, closes every pool, and drops the run's schema. It is
// safe to call on a partially constructed Harness, which is what the error paths
// in newHarness rely on, and it reports the first failure while still attempting
// the rest — a teardown that gave up on its first error would leak a schema per
// failed run.
func (h *Harness) Close(ctx context.Context) error {
	var firstErr error
	fail := func(err error) {
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}

	h.stopRunners()

	for _, db := range []*gorm.DB{h.work, h.probe, h.sched, h.limiterDB} {
		if db == nil {
			continue
		}
		if sqlDB, err := db.DB(); err == nil {
			fail(sqlDB.Close())
		}
	}

	if h.admin != nil {
		if h.schema != "" {
			fail(dropSchema(ctx, h.admin, h.schema))
		}
		if sqlDB, err := h.admin.DB(); err == nil {
			fail(sqlDB.Close())
		}
	}
	return firstErr
}

// stopRunners cancels every runner and the sweeper, then waits for all of them.
//
// Waiting matters beyond tidiness: the histograms are merged after this returns,
// and the wait is what gives that merge a happens-before edge on every recorded
// observation.
func (h *Harness) stopRunners() {
	for _, r := range h.runners {
		r.cancel()
	}
	if h.cancelSweeper != nil {
		h.cancelSweeper()
	}
	if h.cancelFaults != nil {
		h.cancelFaults()
	}
	h.wg.Wait()
	h.runners = nil
	h.cancelSweeper = nil
	h.cancelFaults = nil
}

// startRunners launches cfg.Runners loops against the harness's driver. Each one
// gets its own cancellable context and its own ordinal, so a fault can stop one
// without touching the others.
func (h *Harness) startRunners(ctx context.Context) error {
	h.gates = make([]*gate, h.cfg.Runners)
	for i := range h.gates {
		h.gates[i] = &gate{}
	}

	for i := range h.cfg.Runners {
		runner, err := flywheel.NewRunner(flywheel.RunnerConfig{
			DB: h.work,
			// Runner i writes histogram shard i and sits behind gate i. Both
			// bindings happen here, once, so the hot path never selects either.
			// The chain is gateDriver -> timingDriver -> the real driver: gating
			// outside the timing means a gated call records no observation, rather
			// than a near-zero one that would pull every percentile down.
			Driver:        &gateDriver{inner: newTimingDriver(h.inner, h.timings, i), gate: h.gates[i]},
			Registry:      h.registry,
			Queues:        []string{h.cfg.Queue},
			ExecutorClass: flywheel.ExecutorClass(h.cfg.ExecutorClass),
			Concurrency:   h.cfg.Workers,
			// A short poll keeps an idle runner from sitting out the tail of a
			// drain; it is not on the hot path, which never sleeps while work
			// remains.
			PollInterval:      5 * time.Millisecond,
			MaxPollBackoff:    h.pollBackoffCap(),
			LeaseDuration:     leaseFor(h.cfg),
			HeartbeatInterval: h.cfg.Heartbeat,
			// MaxRetryBackoff caps the runners' own retry ladder. Zero on every run
			// but the outage measurement, where it is the number under test.
			MaxRetryBackoff: h.cfg.MaxRetryBackoff,
			Observer:        harnessObserver{prog: h.prog},
			Logger:          discardLogger(),
			// Every runner shares one limiter keyed on one resource, so the run's
			// combined claim rate tracks a single budget. Nil on an ungated run.
			Limiter:  h.limiter,
			Resource: limiterResourceFor(h.limiter),
		})
		if err != nil {
			return fmt.Errorf("loadtest: build runner %d: %w", i, err)
		}

		runCtx, cancel := context.WithCancel(ctx)
		handle := &runnerHandle{index: i, runner: runner, cancel: cancel, done: make(chan struct{})}
		h.runners = append(h.runners, handle)

		h.wg.Go(func() {
			defer close(handle.done)
			// Run returns its stop reason as an error, and an expected stop —
			// cancellation at the end of a drain — is not a run failure. Only an
			// unexpected one is collected.
			if err := runner.Run(runCtx); err != nil && runCtx.Err() == nil {
				h.errs.add(err)
			}
		})
	}

	// The sweep runs through the runtime's own Scheduler, carrying the harness's
	// timingDriver. Nothing in the dispatch loop calls Sweep, so this is still
	// the only reason Report.Sweep has observations — but the loop above the
	// driver is now the one a deployment runs, not a hand-rolled ticker that
	// resembled it. That is precisely what an injected Driver is for, and it
	// means the sweep numbers describe the scheduler's behavior rather than the
	// harness's imitation of it.
	//
	// The scheduler's driver gets the last histogram shard, past the runners'.
	sweepCtx, cancelSweep := context.WithCancel(ctx)
	h.cancelSweeper = cancelSweep

	inner := flywheel.NewPostgresDriverWithOptions(h.sched, flywheel.DriverOpts{
		SweepBatchSize: h.cfg.SweepBatchSize,
	})
	sweeper := newTimingDriver(inner, h.timings, h.cfg.Runners).
		countingReclaims(&h.prog.reclaimed)
	scheduler, err := flywheel.NewSchedulerWithConfig(flywheel.SchedulerConfig{
		DB:     h.sched,
		Client: flywheel.NewClient(h.sched),
		Driver: sweeper,
		// Only the sweep is wanted. The periodic tick is pushed far out rather
		// than disabled because there is no way to disable it, and a run that
		// installs no periodic definitions has nothing for it to find anyway; the
		// interval keeps it from costing a query per second regardless.
		TickInterval:  time.Hour,
		SweepInterval: sweepInterval,
		// Retention is off unless the run asked for it, exactly as it is for a
		// host: RetentionMaxAge is the switch.
		RetentionMaxAge:    h.cfg.RetentionMaxAge,
		RetentionInterval:  h.cfg.RetentionInterval,
		RetentionBatchSize: h.cfg.RetentionBatchSize,
	})
	if err != nil {
		return fmt.Errorf("loadtest: build scheduler: %w", err)
	}

	// One sweep before the loop starts. A run that drains in under a second
	// would otherwise never tick, and Report.Sweep would be empty on exactly the
	// short runs a test asserts against — a silence indistinguishable from a
	// broken sweeper. A real scheduler's first tick is one interval away, so the
	// harness takes that first sweep itself rather than pretending the scheduler
	// took it.
	if _, err := sweeper.Sweep(sweepCtx, time.Now()); err != nil && sweepCtx.Err() == nil {
		h.errs.add(err)
	}

	h.wg.Go(func() {
		// Run returns its stop reason as an error, and the only way it stops is
		// cancellation at the end of the run, which is not a failure.
		if err := scheduler.Run(sweepCtx); err != nil && sweepCtx.Err() == nil {
			h.errs.add(err)
		}
	})

	// The sampler shares the sweeper's cancellation: both are the harness's own
	// background work, and both must be stopped before collect reads what they
	// produced.
	h.wg.Go(func() { h.runSampler(sweepCtx) })

	return nil
}

// leaseFor picks the lease duration for a run: comfortably longer than the
// simulated work so an ordinary slow job is never reclaimed mid-flight, but
// short enough that a deliberately orphaned job is reclaimed inside the run
// rather than after it.
//
// An explicit Config.Lease wins outright, including when it is shorter than the
// work. That is not a misconfiguration to be corrected — it is the only way to
// put a job past its lease, and therefore the only way to exercise renewal.
func leaseFor(cfg Config) time.Duration {
	if cfg.Lease > 0 {
		return cfg.Lease
	}
	const minLease = 5 * time.Second
	lease := 4 * (cfg.WorkDuration + cfg.WorkJitter)
	if lease < minLease {
		return minLease
	}
	return lease
}

// expireLeases pushes every held lease into the past and reports how many rows
// it touched, so the next sweep sees the whole in-flight set as expired.
//
// It runs on the probe pool, not the work pool, for the same reason the sampler
// does: a fault the harness cannot inject through a gate is a fault that cannot
// be combined with one. It writes the jobs table directly rather than going
// through the Driver because there is no Driver method for "expire a lease" —
// and there should not be: it is a failure to be simulated, not an operation the
// runtime offers.
func (h *Harness) expireLeases(ctx context.Context) (int64, error) {
	res := h.probe.WithContext(ctx).Exec(
		`UPDATE jobs SET leased_until = ? WHERE state = 'running' AND leased_until IS NOT NULL`,
		time.Now().Add(-time.Hour),
	)
	if res.Error != nil {
		return 0, fmt.Errorf("loadtest: expire leases: %w", res.Error)
	}
	return res.RowsAffected, nil
}

// reclaimExpired runs one sweep on the undecorated driver and counts it toward
// the run's reclaim total, for a fault that needs the reclaim to be part of the
// injection rather than a race against the sweeper's ticker.
func (h *Harness) reclaimExpired(ctx context.Context) (int, error) {
	reclaimed, err := h.inner.Sweep(ctx, time.Now())
	if err != nil {
		return 0, fmt.Errorf("loadtest: reclaim expired leases: %w", err)
	}
	h.prog.reclaimed.Add(int64(reclaimed))
	return reclaimed, nil
}

// orphanedByFaults reports how many finalizes a kill fault blocked — how many
// jobs were left claimed, started, and never finished.
//
// It is the difference between "the fault fired" and "the fault did what it was
// injected to do". A scenario that asserts recovery has to check the second one,
// or it is asserting that a healthy run is healthy.
func (h *Harness) orphanedByFaults() int64 {
	var n int64
	for _, g := range h.gates {
		n += g.orphaned.Load()
	}
	return n
}

// clockDriven reports whether the run's fault runs on the advanceable simulated
// clock — the outage measurement's time-compression path.
func (h *Harness) clockDriven() bool { return h.clock != nil }

// pollBackoffCap is the runners' poll-ladder ceiling: tight on a clock-driven run
// so the runner keeps polling fast through the quiesced gaps between advances,
// and the runtime's own default (selected by zero) on every other run.
func (h *Harness) pollBackoffCap() time.Duration {
	if h.clockDriven() {
		return outagePollBackoff
	}
	return 0
}

// advancerInterval is how often the clock advancer checks whether the current
// retry generation has quiesced. It is faster than the runners' poll ladder
// ceiling so an advance never waits on the advancer's own cadence.
const advancerInterval = 20 * time.Millisecond

// runClockAdvancer compresses simulated time so a multi-hour backoff ladder runs
// in seconds. It steps the clock forward only once the current generation has
// quiesced — nothing running and nothing due at the present instant — so every
// scheduled retry is actually claimed before its rung is skipped, which is what
// makes attempt volume deterministic rather than a function of wall-clock luck.
func (h *Harness) runClockAdvancer(ctx context.Context) {
	ticker := time.NewTicker(advancerInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		done, err := h.advanceClockOnce(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			h.errs.add(err)
			continue
		}
		if done {
			return
		}
	}
}

// advanceClockOnce steps the clock forward when the queue has quiesced, and
// reports whether the drain is complete.
//
// It advances only when nothing is claimable at the present instant and nothing
// is running: while a job is actionable the runners own the moment, and advancing
// past it would skip a rung the ladder was meant to climb. When the only
// remaining work is scheduled in the simulated future, it jumps the clock forward
// to release that generation to the runners.
//
// Whole-rung jump vs. fine step. Attempt volume is invariant to how many jobs a
// single advance releases — a job inside the outage fails every attempt whether it
// is claimed alone or with its cohort — so while the whole pending generation sits
// before the outage's end, it jumps to the *latest* pending instant and releases
// the rung at once. That collapses O(jobs) advances into O(rungs). Only when the
// pending set straddles the outage boundary — where the exact instant decides
// success versus failure — does it fall back to the *earliest* instant and step
// finely, so the boundary is resolved at full precision. The measurement sizes the
// outage far past the ladder, so the fast path is the one it runs.
func (h *Harness) advanceClockOnce(ctx context.Context) (done bool, err error) {
	now := h.clock.Now(ctx)

	var row struct {
		Actionable int64
		Pending    int64
		MinAt      *time.Time
		MaxAt      *time.Time
	}
	if err := h.probe.WithContext(ctx).Raw(`
		SELECT
		  count(*) FILTER (
		    WHERE state = 'running'
		       OR (state IN ('available','retryable','scheduled') AND scheduled_at <= ?)
		  ) AS actionable,
		  count(*) FILTER (
		    WHERE state IN ('available','retryable','scheduled') AND scheduled_at > ?
		  ) AS pending,
		  min(scheduled_at) FILTER (
		    WHERE state IN ('available','retryable','scheduled') AND scheduled_at > ?
		  ) AS min_at,
		  max(scheduled_at) FILTER (
		    WHERE state IN ('available','retryable','scheduled') AND scheduled_at > ?
		  ) AS max_at
		FROM jobs`, now, now, now, now,
	).Scan(&row).Error; err != nil {
		return false, fmt.Errorf("loadtest: advance clock: %w", err)
	}

	if row.Actionable > 0 {
		// The runners still have work at the present instant; leave the clock alone.
		return false, nil
	}
	if row.Pending == 0 || row.MinAt == nil || row.MaxAt == nil {
		// Nothing runnable and nothing scheduled: every job is terminal.
		return true, nil
	}

	target := *row.MaxAt
	if end := h.outage.end(); !end.IsZero() && !row.MaxAt.Before(end) {
		// The rung straddles the outage boundary: step to the earliest instant so
		// the success/fail boundary is resolved precisely rather than jumped over.
		target = *row.MinAt
	}
	// The nudge makes the <= comparison at claim time include the target instant.
	h.clock.advanceTo(target.Add(time.Microsecond))
	return false, nil
}

// blockedClaims reports how many claim attempts the gates refused.
//
// It is the only evidence a paused-database run leaves of how hard its runners
// hammered the gate: a gated call records no latency observation by design, so
// without counting it the report cannot distinguish a runner that backed off from
// one that retried at its poll interval throughout.
func (h *Harness) blockedClaims() int64 {
	var n int64
	for _, g := range h.gates {
		n += g.blockedClaims.Load()
	}
	return n
}
