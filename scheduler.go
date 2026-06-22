package flywheel

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

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
	db                *gorm.DB
	client            *Client
	logger            *slog.Logger
	backfillCap       int
	tickInterval      time.Duration
	sweepInterval     time.Duration
	retentionMaxAge   time.Duration
	retentionInterval time.Duration
}

// SchedulerConfig configures a Scheduler. Only DB and Client are required; the
// cadence and backfill knobs default when left zero. It is the config form a
// Node composes; NewScheduler is the two-argument shorthand for the common case.
type SchedulerConfig struct {
	// DB is the database the Scheduler reads periodic definitions from and runs
	// the stuck-lease sweep against.
	DB *gorm.DB
	// Client is the producer the Scheduler enqueues periodic jobs through.
	Client *Client
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
}

// NewScheduler returns a Scheduler over db and the producer client with the
// default cadence and backfill cap.
func NewScheduler(db *gorm.DB, client *Client) *Scheduler {
	return NewSchedulerWithConfig(SchedulerConfig{DB: db, Client: client})
}

// NewSchedulerWithConfig returns a Scheduler from cfg, applying the cadence and
// backfill defaults for any field left zero.
func NewSchedulerWithConfig(cfg SchedulerConfig) *Scheduler {
	s := &Scheduler{
		db:                cfg.DB,
		client:            cfg.Client,
		logger:            cfg.Logger,
		backfillCap:       cfg.BackfillCap,
		tickInterval:      cfg.TickInterval,
		sweepInterval:     cfg.SweepInterval,
		retentionMaxAge:   cfg.RetentionMaxAge,
		retentionInterval: cfg.RetentionInterval,
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
	return s
}

// Run ticks periodic definitions and runs the stuck-lease sweep until ctx is
// cancelled. The sweep runs on a 30-second cadence (FR-030). When retention is
// enabled (RetentionMaxAge > 0) it also runs a retention sweep on its own
// cadence; otherwise no retention ticker is armed.
func (s *Scheduler) Run(ctx context.Context) error {
	periodicTicker := time.NewTicker(s.tickInterval)
	defer periodicTicker.Stop()
	sweepTicker := time.NewTicker(s.sweepInterval)
	defer sweepTicker.Stop()

	// A disabled retention sweep gets a stopped ticker with a nil channel, which
	// blocks forever in the select — the same as not having the case at all.
	var retentionC <-chan time.Time
	if s.retentionMaxAge > 0 {
		retentionTicker := time.NewTicker(s.retentionInterval)
		defer retentionTicker.Stop()
		retentionC = retentionTicker.C
	}

	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("jobs: scheduler stopped: %w", ctx.Err())
		case <-periodicTicker.C:
			if _, err := s.Tick(ctx); err != nil {
				s.logger.ErrorContext(ctx, "jobs: periodic tick failed", "error", err)
			}
		case <-sweepTicker.C:
			if _, err := s.Sweep(ctx); err != nil {
				s.logger.ErrorContext(ctx, "jobs: lease sweep failed", "error", err)
			}
		case <-retentionC:
			if n, err := s.PruneRetention(ctx); err != nil {
				s.logger.ErrorContext(ctx, "jobs: retention sweep failed", "error", err)
			} else if n > 0 {
				s.logger.InfoContext(ctx, "jobs: retention sweep pruned finished jobs", "deleted", n)
			}
		}
	}
}

// Tick processes every due, active periodic definition once and reports how
// many jobs were enqueued.
func (s *Scheduler) Tick(ctx context.Context) (int, error) {
	now := ClockFrom(ctx).Now(ctx)

	var defs []jobPeriodicRow
	if err := s.db.WithContext(ctx).
		Where("is_active = ? AND next_run_at <= ?", true, now).
		Find(&defs).Error; err != nil {
		return 0, fmt.Errorf("jobs: load due periodics: %w", err)
	}

	enqueued := 0
	for i := range defs {
		n, err := s.fire(ctx, defs[i], now)
		if err != nil {
			return enqueued, err
		}
		enqueued += n
	}
	return enqueued, nil
}

// Sweep reclaims jobs whose lease has expired (FR-030, FR-031).
func (s *Scheduler) Sweep(ctx context.Context) (int, error) {
	now := ClockFrom(ctx).Now(ctx)
	sweeper := baseDriver{db: s.db}
	return sweeper.Sweep(ctx, now)
}

// PruneRetention hard-deletes terminal jobs (and their job_runs) finalized
// longer ago than RetentionMaxAge, reporting how many jobs were removed. It is a
// no-op returning (0, nil) when retention is disabled, so calling it on a
// retention-less Scheduler can never delete anything.
func (s *Scheduler) PruneRetention(ctx context.Context) (int64, error) {
	if s.retentionMaxAge <= 0 {
		return 0, nil
	}
	cutoff := ClockFrom(ctx).Now(ctx).Add(-s.retentionMaxAge)
	return DeleteFinishedJobs(ctx, s.db, cutoff)
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
// The bucketed unique_key makes a redundant tick idempotent (FR-028): a
// collision is a successful no-op.
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
