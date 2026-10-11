package core

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/mrz1836/go-foundation/models"
	"github.com/robfig/cron/v3"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// PausePeriodic holds a periodic definition by slug: from the next tick it
// fires at no due time until ResumePeriodic releases it. It is the operator's
// stop for a schedule that keeps failing or is hammering a downstream service.
//
// A pause is held apart from the definition's Active flag, which is the
// declaration's own switch (see SetPeriodicActive). Nothing that declares a
// definition writes the pause — not UpsertPeriodic, not the Scheduler's
// start-up apply of SchedulerConfig.Periodics, not its reconcile — so a paused
// definition stays paused through every restart that re-declares it, while the
// declaration still updates its kind, queue, args, and cadence. A definition
// fires only when it is active and not paused.
//
// Parameters:
//   - ctx: carries cancellation, and the clock whose now is recorded as the
//     pause's time (models.ClockFrom).
//   - db: the database, or a caller's transaction.
//   - slug: the definition to pause.
//
// Returns ErrPeriodicNotFound when no definition has the slug. Pausing a paused
// definition returns nil and writes nothing, so its first pause's time stands.
//
// Side effects: sets job_periodics.paused_at and updated_at, in a transaction
// of its own (a savepoint in a caller's transaction) that reads the row under a
// row lock on PostgreSQL.
//
// Notes: a fire a tick has already begun when the pause commits completes. The
// row keeps the pause, so DeletePeriodic removes it with the row, and a
// definition the reconcile re-creates after a delete is not paused.
func PausePeriodic(ctx context.Context, db *gorm.DB, slug string) error {
	now := models.ClockFrom(ctx).Now(ctx)
	err := db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		row, err := lockPeriodic(tx, slug)
		if err != nil || row.PausedAt != nil {
			return err
		}
		return tx.Model(&jobPeriodicRow{}).Where("id = ?", row.ID).
			Updates(map[string]any{"paused_at": now, "updated_at": now}).Error
	})
	if err != nil && !errors.Is(err, ErrPeriodicNotFound) {
		return fmt.Errorf("flywheel: pause periodic %q: %w", slug, err)
	}
	return err
}

// ResumePeriodic releases a definition PausePeriodic held, by slug. It fires
// next at its first fire time after now, so the fires the pause skipped are
// never enqueued — a long pause does not end in a burst of backfilled jobs.
//
// This function performs the following steps:
//   - Reads the definition under a row lock on PostgreSQL; none is
//     ErrPeriodicNotFound.
//   - Returns nil, writing nothing, when the definition is not paused.
//   - Clears the pause and, when its next_run_at is not after now, moves it to
//     the first fire after now on the definition's own schedule: an interval
//     keeps its phase (next_run_at plus the smallest whole number of intervals
//     that passes now), and a cron takes its next match after now. A
//     next_run_at still in the future is kept.
//
// Parameters:
//   - ctx: carries cancellation, and the clock whose now the next fire is
//     computed from (models.ClockFrom).
//   - db: the database, or a caller's transaction.
//   - slug: the definition to resume.
//
// Returns ErrPeriodicNotFound when no definition has the slug, or the parse
// error of a stored cron expression that no longer parses.
//
// Side effects: clears job_periodics.paused_at and writes next_run_at and
// updated_at, in a transaction of its own (a savepoint in a caller's
// transaction).
//
// Notes: resuming leaves the Active flag alone, so a definition that is also
// inactive stays off; and a cron with no CRON_TZ= zone takes its next match in
// the clock's zone, as on its creation (see PeriodicSpec.Cron).
func ResumePeriodic(ctx context.Context, db *gorm.DB, slug string) error {
	now := models.ClockFrom(ctx).Now(ctx)
	err := db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		row, err := lockPeriodic(tx, slug)
		if err != nil || row.PausedAt == nil {
			return err
		}
		nextRun, err := resumedNextRun(row, now)
		if err != nil {
			return err
		}
		return tx.Model(&jobPeriodicRow{}).Where("id = ?", row.ID).
			Updates(map[string]any{"paused_at": nil, "next_run_at": nextRun, "updated_at": now}).Error
	})
	if err != nil && !errors.Is(err, ErrPeriodicNotFound) {
		return fmt.Errorf("flywheel: resume periodic %q: %w", slug, err)
	}
	return err
}

// lockPeriodic reads slug's definition inside tx, under FOR UPDATE on
// PostgreSQL so a concurrent pause, resume, or fire waits for this one. SQLite
// has no row lock and needs none: its writers are already serialized, the same
// idiom lockBucket uses. A missing definition is ErrPeriodicNotFound.
func lockPeriodic(tx *gorm.DB, slug string) (jobPeriodicRow, error) {
	q := quietMissing(tx).Where("slug = ?", slug)
	if tx.Name() == "postgres" {
		q = q.Clauses(clause.Locking{Strength: "UPDATE"})
	}
	var row jobPeriodicRow
	if err := q.First(&row).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return jobPeriodicRow{}, ErrPeriodicNotFound
		}
		return jobPeriodicRow{}, err
	}
	return row, nil
}

// resumedNextRun is the next_run_at a resume writes for row at now: row's own
// next_run_at when it is still after now, else the first fire strictly after
// now on row's schedule.
func resumedNextRun(row jobPeriodicRow, now time.Time) (time.Time, error) {
	if row.NextRunAt.After(now) {
		return row.NextRunAt, nil
	}
	switch {
	case row.IntervalSeconds != nil && *row.IntervalSeconds > 0:
		interval := time.Duration(*row.IntervalSeconds) * time.Second
		passed := int64(now.Sub(row.NextRunAt) / interval)
		return row.NextRunAt.Add(time.Duration(passed+1) * interval), nil
	case row.CronExpr != nil && *row.CronExpr != "":
		schedule, err := cron.ParseStandard(*row.CronExpr)
		if err != nil {
			return time.Time{}, fmt.Errorf("parse cron %q: %w", *row.CronExpr, err)
		}
		return schedule.Next(now), nil
	default:
		return time.Time{}, fmt.Errorf("%w: %q", errPeriodicNoSchedule, row.Slug)
	}
}
