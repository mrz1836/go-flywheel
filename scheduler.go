package flywheel

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mrz1836/go-foundation/models"
	"github.com/robfig/cron/v3"
	"gorm.io/gorm"
)

// Scheduler defaults.
const (
	defaultBackfillCap       = 10
	defaultTickInterval      = time.Second
	defaultSweepInterval     = 30 * time.Second
	defaultRetentionInterval = time.Hour
)

// Scheduler enqueues jobs from periodic definitions and reclaims stuck jobs.
type Scheduler struct {
	db                   *gorm.DB
	client               *Client
	driver               Driver
	logger               *slog.Logger
	backfillCap          int
	tickInterval         time.Duration
	sweepInterval        time.Duration
	retentionMaxAge      time.Duration
	retentionInterval    time.Duration
	retentionOpts        RetentionOpts
	healthSampleInterval time.Duration
}

// SchedulerConfig configures a Scheduler. DB, Client, and Driver are required;
// the cadence and backfill knobs default when left zero. It is the config form a
// Node composes; NewScheduler is the two-argument shorthand for the common case.
type SchedulerConfig struct {
	// DB is the database the Scheduler reads periodic definitions from and runs
	// the stuck-lease sweep against.
	DB *gorm.DB
	// Client is the producer the Scheduler enqueues periodic jobs through.
	Client *Client
	// Driver is the seam the lease sweep runs through. Required: a Scheduler
	// constructs no Driver of its own.
	//
	// A host that instruments, traces, or fault-injects its Driver must see the
	// sweep like every other database operation, and it only does if the same
	// instance its runners hold is handed here. Passing a freshly constructed
	// Driver satisfies the type and defeats the purpose — the wrapper wraps the
	// instance the host is holding, not the type.
	//
	// It is also the dialect seam. The sweep's reclaim differs by dialect
	// (PostgreSQL takes its batch with FOR UPDATE SKIP LOCKED; SQLite has no such
	// clause and needs none), so a Scheduler that picked its own implementation
	// would be choosing a dialect it was never told.
	Driver Driver
	// Logger logs tick and sweep failures. Optional; defaults to slog.Default().
	Logger *slog.Logger
	// BackfillCap bounds how many missed buckets a single due definition
	// enqueues on catch-up. Optional; defaults to 10.
	BackfillCap int
	// TickInterval is the cadence at which due periodic definitions are checked.
	// Optional; defaults to one second.
	TickInterval time.Duration
	// SweepInterval is the cadence of the stuck-lease reclaim sweep. Optional;
	// defaults to 30 seconds.
	SweepInterval time.Duration
	// RetentionMaxAge enables the retention sweep: terminal jobs (and their
	// job_runs) finalized longer ago than this are hard-deleted. Zero (the
	// default) disables retention entirely — no surprise deletes for an embedded
	// consumer that never asked for them.
	RetentionMaxAge time.Duration
	// RetentionInterval is the cadence of the retention sweep. It applies only
	// when RetentionMaxAge is set; left zero, it defaults to one hour.
	RetentionInterval time.Duration
	// RetentionBatchSize is the number of jobs the retention sweep deletes per
	// transaction. Zero selects the documented default; it is never unbounded.
	RetentionBatchSize int
	// RetentionMaxBatches, when positive, caps how many batches one retention
	// pass runs, bounding the work a single scheduled prune performs. Zero runs
	// each pass until the backlog is exhausted.
	//
	// A pass that ends on this ceiling is logged, so "retention can never keep up
	// with its window" is visible rather than silent.
	RetentionMaxBatches int
	// HealthSampleInterval enables the queue-health heartbeat: when > 0 the
	// Scheduler samples QueueHealth on this cadence and logs a one-line pulse
	// (ready, in-flight, oldest-ready lag, discarded). Zero (the default) disables
	// it — no surprise log output for an embedded consumer that never asked for a
	// heartbeat; a `/metrics` scrape samples fresh regardless.
	HealthSampleInterval time.Duration
}

// NewScheduler returns a Scheduler over db and the producer client with the
// default cadence and backfill cap, selecting the Driver from db's dialect.
//
// It is the shorthand for a host that does not wrap its Driver. A host that
// does — for metrics, tracing, or fault injection — should use
// NewSchedulerWithConfig and pass the same wrapped instance its runners hold,
// because a Driver this constructor builds is one nothing else can see.
//
// An unsupported dialect returns ErrUnsupportedDialect.
func NewScheduler(db *gorm.DB, client *Client) (*Scheduler, error) {
	driver, err := driverFor(db)
	if err != nil {
		return nil, err
	}
	return NewSchedulerWithConfig(SchedulerConfig{DB: db, Client: client, Driver: driver})
}

// driverFor selects the Driver implementation for db's dialect.
func driverFor(db *gorm.DB) (Driver, error) {
	if db == nil {
		return nil, errSchedulerNeedsDB
	}
	return driverForDialect(db.Name(), db)
}

// driverForDialect is the one place the runtime maps a dialect name onto a
// Driver. It takes the name rather than reading it off db so the mapping is
// testable without standing up a database for every dialect it rejects.
//
// It reuses ErrUnsupportedDialect rather than minting a sentinel of its own, so
// a scheduler over an unsupported dialect fails the same way Migrate, IndexSet,
// and InstallIndexes already do for it — the dialect gate has one answer, given
// in one vocabulary.
func driverForDialect(dialect string, db *gorm.DB) (Driver, error) {
	switch dialect {
	case "postgres":
		return NewPostgresDriver(db), nil
	case "sqlite":
		return NewSQLiteDriver(db), nil
	default:
		return nil, fmt.Errorf(
			"flywheel: %w: %q: the scheduler supports postgres or sqlite",
			ErrUnsupportedDialect, dialect,
		)
	}
}

// NewSchedulerWithConfig returns a Scheduler from cfg, applying the cadence and
// backfill defaults for any field left zero.
//
// It is the single authority on SchedulerConfig validity: DB, Client, and
// Driver are all required here rather than being re-checked by each caller, so
// a Node and a directly-constructed Scheduler reject the same configurations
// with the same errors.
func NewSchedulerWithConfig(cfg SchedulerConfig) (*Scheduler, error) {
	switch {
	case cfg.DB == nil:
		return nil, errSchedulerNeedsDB
	case cfg.Client == nil:
		return nil, errSchedulerNeedsClient
	case cfg.Driver == nil:
		return nil, errSchedulerNeedsDriver
	}

	s := &Scheduler{
		db:                cfg.DB,
		client:            cfg.Client,
		driver:            cfg.Driver,
		logger:            cfg.Logger,
		backfillCap:       cfg.BackfillCap,
		tickInterval:      cfg.TickInterval,
		sweepInterval:     cfg.SweepInterval,
		retentionMaxAge:   cfg.RetentionMaxAge,
		retentionInterval: cfg.RetentionInterval,
		retentionOpts: RetentionOpts{
			BatchSize: cfg.RetentionBatchSize, MaxBatches: cfg.RetentionMaxBatches,
		},
		healthSampleInterval: cfg.HealthSampleInterval,
	}
	if s.logger == nil {
		s.logger = slog.Default()
	}
	if s.backfillCap <= 0 {
		s.backfillCap = defaultBackfillCap
	}
	if s.tickInterval <= 0 {
		s.tickInterval = defaultTickInterval
	}
	if s.sweepInterval <= 0 {
		s.sweepInterval = defaultSweepInterval
	}
	// Retention is opt-in via RetentionMaxAge; only then does the interval default.
	if s.retentionMaxAge > 0 && s.retentionInterval <= 0 {
		s.retentionInterval = defaultRetentionInterval
	}
	return s, nil
}

// activity is one maintenance loop: a name, a cadence, the work, and a guard
// that skips a tick whose predecessor is still running.
//
// Each activity owns a goroutine. Before that, Run was a single select over
// four tickers with every case inline, so a slow retention prune blocked the
// lease sweep for its whole duration — and the sweep is the only recovery path
// for work lost to a crashed process. The failure compounds in exactly the
// wrong direction: retention is slow because the database is under load, the
// sweep it blocks is what recovers leases, and the leases it fails to recover
// are what keeps jobs from being re-dispatched.
//
// It is always used through a pointer. An atomic.Bool in a value slice is a
// copylocks vet failure, and a copied guard guards nothing.
type activity struct {
	// name labels the activity in logs.
	name string
	// interval is the cadence at which run fires.
	interval time.Duration
	// run performs one pass. It returns nothing: no caller above this can act on
	// a maintenance failure, and collapsing four specific log messages into one
	// generic error would lose the strings hosts key their alerts on. Each
	// closure logs its own failure in its own words.
	run func(context.Context)
	// running is the self-overlap guard: a tick arriving while a pass is still in
	// flight is skipped rather than queued.
	running atomic.Bool
	// skips counts *consecutive* skipped ticks, reset by every completed pass.
	// One skip is a slow pass; a climbing count is a cadence the deployment has
	// to widen, and only a consecutive count distinguishes the two.
	skips atomic.Int64
}

// loop runs the activity until ctx is cancelled.
func (a *activity) loop(ctx context.Context, logger *slog.Logger) {
	ticker := time.NewTicker(a.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			a.tick(ctx, logger)
		}
	}
}

// tick runs one pass unless the previous one is still going, in which case it
// records and logs the skip.
func (a *activity) tick(ctx context.Context, logger *slog.Logger) {
	if !a.running.CompareAndSwap(false, true) {
		logger.WarnContext(ctx, "jobs: maintenance tick skipped, previous pass still running",
			"activity", a.name,
			"consecutive_skips", a.skips.Add(1),
			"interval", a.interval.String())
		return
	}
	defer a.running.Store(false)
	a.run(ctx)
	a.skips.Store(0)
}

// Run ticks periodic definitions and runs the stuck-lease sweep until ctx is
// cancelled. The sweep runs on a 30-second cadence — frequent enough that a
// crashed executor's jobs come back promptly, cheap enough to be a single
// indexed scan. When retention is enabled (RetentionMaxAge > 0) it also runs a
// retention sweep on its own cadence, and when HealthSampleInterval is set it
// logs a queue-health pulse on that cadence.
//
// Each activity runs on its own goroutine, so none can delay another, and none
// can overlap itself: a tick arriving while its predecessor is still running is
// skipped and logged at warn rather than queued behind it.
//
// Run returns only when ctx is cancelled, and waits for any in-flight pass
// before returning. That wait is bounded by one batch rather than one backlog —
// which is true only because the sweep and the retention prune are batched, and
// is what makes a Node's DrainTimeout a meaningful bound on shutdown.
func (s *Scheduler) Run(ctx context.Context) error {
	s.runActivities(ctx, s.activities())
	return fmt.Errorf("jobs: scheduler stopped: %w", ctx.Err())
}

// runActivities runs each activity on its own goroutine until ctx is cancelled,
// then waits for every in-flight pass to finish.
//
// It is separate from Run so the lifecycle — one goroutine each, cancel, wait —
// is expressed once and can be exercised against a hand-built activity set,
// without a Scheduler configuration that can produce the timing under test.
func (s *Scheduler) runActivities(ctx context.Context, activities []*activity) {
	var wg sync.WaitGroup
	for _, a := range activities {
		wg.Add(1)
		go func() {
			defer wg.Done()
			a.loop(ctx, s.logger)
		}()
	}
	<-ctx.Done()
	wg.Wait()
}

// activities builds the maintenance set for this Scheduler's configuration.
//
// Retention and the health heartbeat stay opt-in structurally rather than by a
// disabled ticker: no interval, no goroutine. A capability that is off should
// cost nothing, not tick into a branch that returns early.
func (s *Scheduler) activities() []*activity {
	acts := []*activity{
		{name: "periodic", interval: s.tickInterval, run: s.tickOnce},
		{name: "sweep", interval: s.sweepInterval, run: s.sweepOnce},
	}
	if s.retentionMaxAge > 0 {
		acts = append(acts, &activity{
			name: "retention", interval: s.retentionInterval, run: s.pruneOnce,
		})
	}
	if s.healthSampleInterval > 0 {
		acts = append(acts, &activity{
			name: "health", interval: s.healthSampleInterval, run: s.logHealth,
		})
	}
	return acts
}

// tickOnce fires every due periodic definition once.
func (s *Scheduler) tickOnce(ctx context.Context) {
	if _, err := s.Tick(ctx); err != nil {
		s.logMaintenanceError(ctx, "jobs: periodic tick failed", err)
	}
}

// sweepOnce reclaims expired leases once.
func (s *Scheduler) sweepOnce(ctx context.Context) {
	if _, err := s.Sweep(ctx); err != nil {
		s.logMaintenanceError(ctx, "jobs: lease sweep failed", err)
	}
}

// pruneOnce runs one retention pass.
func (s *Scheduler) pruneOnce(ctx context.Context) {
	n, err := s.PruneRetention(ctx)
	if err != nil {
		s.logMaintenanceError(ctx, "jobs: retention sweep failed", err)
		return
	}
	if n == 0 {
		return
	}
	s.logger.InfoContext(ctx, "jobs: retention sweep pruned finished jobs", "deleted", n)
	if s.prunedOnItsCeiling(n) {
		// A pass that ends on its ceiling every tick is a retention window the
		// deployment can never catch up to. Saying so is the difference between a
		// knob that is working and one that is silently losing.
		s.logger.InfoContext(ctx, "jobs: retention sweep stopped on its batch ceiling",
			"deleted", n, "max_batches", s.retentionOpts.MaxBatches)
	}
}

// logMaintenanceError logs a maintenance failure, suppressing the ones that
// are just the shutdown arriving.
//
// A cancelled sweep or prune is what a clean drain looks like from inside the
// activity: the batched implementations report partial progress plus the
// context error, which is correct for a caller and noise in a log. Without this
// every ordinary shutdown would gain an error line the unbounded implementation
// never produced — a new alert for a non-event.
func (s *Scheduler) logMaintenanceError(ctx context.Context, msg string, err error) {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return
	}
	s.logger.ErrorContext(ctx, msg, "error", err)
}

// SampleHealth reads a QueueHealth gauge snapshot through the Scheduler's
// database. It parallels Tick, Sweep, and PruneRetention so the heartbeat cadence
// is testable directly, and lets a host reuse the Scheduler's db handle as the
// sampler behind a `/metrics` endpoint.
func (s *Scheduler) SampleHealth(ctx context.Context) (QueueHealth, error) {
	return SampleQueueHealth(ctx, s.db)
}

// logHealth samples queue health and logs a one-line pulse. A sample failure is
// logged and swallowed: a transient read error must not stop the scheduler loop.
func (s *Scheduler) logHealth(ctx context.Context) {
	qh, err := s.SampleHealth(ctx)
	if err != nil {
		s.logger.ErrorContext(ctx, "jobs: queue health sample failed", "error", err)
		return
	}
	s.logger.InfoContext(
		ctx, "jobs: queue health",
		"ready", qh.Ready,
		"inflight", qh.InFlight,
		"scheduled_ahead", qh.ScheduledAhead,
		"oldest_ready", qh.OldestReadyAge.String(),
		"discarded", qh.CountsByState[string(StateDiscarded)],
	)
}

// Tick processes every due, active periodic definition once and reports how
// many jobs were enqueued.
func (s *Scheduler) Tick(ctx context.Context) (int, error) {
	now := models.ClockFrom(ctx).Now(ctx)

	var defs []jobPeriodicRow
	if err := s.db.WithContext(ctx).
		Where("is_active = ? AND next_run_at <= ?", true, now).
		Find(&defs).Error; err != nil {
		return 0, fmt.Errorf("jobs: load due periodics: %w", err)
	}

	// Each definition is fired in isolation: one broken definition (e.g. an
	// invalid cron that slipped past write-time validation) must not starve the
	// healthy definitions scanned after it. Errors are aggregated and still
	// returned so a caller and the scheduler log see every failure.
	enqueued := 0
	var errs []error
	for i := range defs {
		n, err := s.fire(ctx, defs[i], now)
		enqueued += n
		if err != nil {
			errs = append(errs, err)
		}
	}
	return enqueued, errors.Join(errs...)
}

// Sweep reclaims jobs whose lease has expired — state running with leased_until
// in the past, which is what a job looks like when its executor died mid-attempt
// — returning them to available and marking each stale run stub crashed. It is
// the runtime's only recovery path for work lost to a crashed process.
//
// It runs through the configured Driver, so a host that wrapped its Driver
// observes the sweep exactly as it observes a claim or a finalize.
func (s *Scheduler) Sweep(ctx context.Context) (int, error) {
	return s.driver.Sweep(ctx, models.ClockFrom(ctx).Now(ctx))
}

// PruneRetention hard-deletes terminal jobs (and their job_runs) finalized
// longer ago than RetentionMaxAge, in bounded batches, reporting how many jobs
// were removed. It is a no-op returning (0, nil) when retention is disabled, so
// calling it on a retention-less Scheduler can never delete anything.
//
// The returned count is meaningful alongside a non-nil error: committed batches
// are not rolled back by a later batch's failure.
func (s *Scheduler) PruneRetention(ctx context.Context) (int64, error) {
	if s.retentionMaxAge <= 0 {
		return 0, nil
	}
	cutoff := models.ClockFrom(ctx).Now(ctx).Add(-s.retentionMaxAge)
	return DeleteFinishedJobsWithOptions(ctx, s.db, cutoff, s.retentionOpts)
}

// prunedOnItsCeiling reports whether a pass that deleted n rows stopped because
// it ran out of batches rather than out of work.
//
// It is an inference, not a flag the retention pass returns, and the inference
// is exact: MaxBatches batches each deleting a full BatchSize is the only way to
// reach that total without the loop having selected an empty batch.
func (s *Scheduler) prunedOnItsCeiling(n int64) bool {
	max := s.retentionOpts.MaxBatches
	return max > 0 && n == int64(max)*int64(s.retentionOpts.batchSize())
}

// fire enqueues one job per missed bucket of def (capped at backfillCap) and
// advances the definition's next_run_at past now.
//
//nolint:gocognit,gocyclo // schedule selection plus a single enqueue loop
func (s *Scheduler) fire(ctx context.Context, def jobPeriodicRow, now time.Time) (int, error) {
	var (
		buckets []time.Time
		nextRun time.Time
	)
	switch {
	case def.IntervalSeconds != nil && *def.IntervalSeconds > 0:
		buckets, nextRun = intervalBuckets(def.NextRunAt, *def.IntervalSeconds, now, s.backfillCap)
	case def.CronExpr != nil && *def.CronExpr != "":
		var err error
		buckets, nextRun, err = cronBuckets(*def.CronExpr, def.NextRunAt, now, s.backfillCap)
		if err != nil {
			return 0, err
		}
	default:
		return 0, fmt.Errorf("%w: %q", errPeriodicNoSchedule, def.Slug)
	}

	enqueued := 0
	for _, bucket := range buckets {
		ok, err := s.enqueueBucket(ctx, def, bucket)
		if err != nil {
			return enqueued, err
		}
		if ok {
			enqueued++
		}
	}

	upd := map[string]any{"next_run_at": nextRun, "updated_at": now}
	if enqueued > 0 {
		upd["last_enqueued_at"] = now
	}
	if err := s.db.WithContext(ctx).Model(&jobPeriodicRow{}).
		Where("id = ?", def.ID).Updates(upd).Error; err != nil {
		return enqueued, fmt.Errorf("jobs: advance periodic %q: %w", def.Slug, err)
	}
	return enqueued, nil
}

// enqueueBucket enqueues one job for a single time bucket through the Client.
// The bucketed unique_key makes a redundant tick idempotent: a tick that fires
// twice — a restart, a clock adjustment, a backfill overlapping the live
// schedule — computes the same key for the same bucket, so the second insert
// collides and is a successful no-op rather than a duplicate run.
func (s *Scheduler) enqueueBucket(ctx context.Context, def jobPeriodicRow, bucket time.Time) (bool, error) {
	payload := []byte(def.ArgsTemplate)
	if len(payload) == 0 {
		payload = []byte("{}")
	}
	scheduleAt := bucket
	_, err := s.client.insert(ctx, def.Kind, payload, InsertOpts{
		Queue:      def.Queue,
		UniqueKey:  fmt.Sprintf("%s@%d", def.Slug, bucket.Unix()),
		ScheduleAt: &scheduleAt,
	})
	if errors.Is(err, ErrAlreadyEnqueued) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("jobs: enqueue periodic job: %w", err)
	}
	return true, nil
}

// intervalBuckets returns the missed fire times of a fixed-interval schedule
// from start through now (capped at the most recent limit), and the first fire
// time strictly after now.
func intervalBuckets(start time.Time, intervalSeconds int, now time.Time, limit int) ([]time.Time, time.Time) {
	interval := time.Duration(intervalSeconds) * time.Second
	var all []time.Time
	t := start
	for !t.After(now) {
		all = append(all, t)
		t = t.Add(interval)
	}
	return capBuckets(all, limit), t
}

// cronBuckets returns the missed fire times of a cron schedule from start
// through now (capped at the most recent limit), and the first fire time
// strictly after now. start is treated as a valid fire time.
func cronBuckets(expr string, start, now time.Time, limit int) ([]time.Time, time.Time, error) {
	schedule, err := cron.ParseStandard(expr)
	if err != nil {
		return nil, time.Time{}, fmt.Errorf("jobs: parse cron %q: %w", expr, err)
	}
	var all []time.Time
	t := start
	for !t.After(now) {
		all = append(all, t)
		t = schedule.Next(t)
	}
	return capBuckets(all, limit), t, nil
}

// capBuckets keeps only the most recent limit entries.
func capBuckets(all []time.Time, limit int) []time.Time {
	if limit > 0 && len(all) > limit {
		return all[len(all)-limit:]
	}
	return all
}
