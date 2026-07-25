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

	driver   flywheel.Driver
	registry *flywheel.Registry

	prog  *progress
	errs  *errset
	notes *noteset

	runners []*runnerHandle
	wg      sync.WaitGroup
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
	if ev.Outcome == flywheel.OutcomeError && ev.ErrorClass == flywheel.ErrorPermanent {
		o.prog.discarded.Add(1)
	}
}

// OnRetry counts the subset of finished attempts that will run again, which is
// what makes finished − retried the terminal count.
func (o harnessObserver) OnRetry(context.Context, flywheel.RetryEvent) {
	o.prog.retried.Add(1)
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
		cfg:    cfg,
		schema: newSchemaName(),
		errs:   &errset{},
		notes:  &noteset{},
		prog:   &progress{target: int64(cfg.Jobs)},
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

	if err = installSchema(ctx, h.work, cfg.Indexes); err != nil {
		return h, err
	}

	h.driver = flywheel.NewPostgresDriver(h.work)
	h.registry = flywheel.NewRegistry()
	flywheel.Register(h.registry, loadWorker{})

	return h, nil
}

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

	for _, db := range []*gorm.DB{h.work, h.probe} {
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

// stopRunners cancels every runner and waits for all of them to return.
func (h *Harness) stopRunners() {
	for _, r := range h.runners {
		r.cancel()
	}
	h.wg.Wait()
	h.runners = nil
}

// startRunners launches cfg.Runners loops against the harness's driver. Each one
// gets its own cancellable context and its own ordinal, so a fault can stop one
// without touching the others.
func (h *Harness) startRunners(ctx context.Context) error {
	for i := range h.cfg.Runners {
		runner, err := flywheel.NewRunner(flywheel.RunnerConfig{
			DB:            h.work,
			Driver:        h.driver,
			Registry:      h.registry,
			Queues:        []string{h.cfg.Queue},
			ExecutorClass: flywheel.ExecutorClass(h.cfg.ExecutorClass),
			Concurrency:   h.cfg.Workers,
			// A short poll keeps an idle runner from sitting out the tail of a
			// drain; it is not on the hot path, which never sleeps while work
			// remains.
			PollInterval:  5 * time.Millisecond,
			LeaseDuration: leaseFor(h.cfg),
			Observer:      harnessObserver{prog: h.prog},
			Logger:        discardLogger(),
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
	return nil
}

// leaseFor picks the lease duration for a run: comfortably longer than the
// simulated work so an ordinary slow job is never reclaimed mid-flight, but
// short enough that a deliberately orphaned job is reclaimed inside the run
// rather than after it.
func leaseFor(cfg Config) time.Duration {
	const minLease = 5 * time.Second
	lease := 4 * (cfg.WorkDuration + cfg.WorkJitter)
	if lease < minLease {
		return minLease
	}
	return lease
}
