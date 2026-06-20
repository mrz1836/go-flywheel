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
	defaultBackfillCap   = 10
	defaultTickInterval  = time.Second
	defaultSweepInterval = 30 * time.Second
)

// Scheduler enqueues jobs from periodic definitions and reclaims stuck jobs.
type Scheduler struct {
	db            *gorm.DB
	client        *Client
	logger        *slog.Logger
	backfillCap   int
	tickInterval  time.Duration
	sweepInterval time.Duration
}

// NewScheduler returns a Scheduler over db and the producer client.
func NewScheduler(db *gorm.DB, client *Client) *Scheduler {
	return &Scheduler{
		db:            db,
		client:        client,
		logger:        slog.Default(),
		backfillCap:   defaultBackfillCap,
		tickInterval:  defaultTickInterval,
		sweepInterval: defaultSweepInterval,
	}
}

// Run ticks periodic definitions and runs the stuck-lease sweep until ctx is
// cancelled. The sweep runs on a 30-second cadence (FR-030).
func (s *Scheduler) Run(ctx context.Context) error {
	periodicTicker := time.NewTicker(s.tickInterval)
	defer periodicTicker.Stop()
	sweepTicker := time.NewTicker(s.sweepInterval)
	defer sweepTicker.Stop()

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
