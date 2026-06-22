package flywheel

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/robfig/cron/v3"
	"gorm.io/datatypes"
	"gorm.io/gorm"
)

// PeriodicSpec declares a periodic (cron or fixed-interval) job. It is the
// exported, host-facing form of a job_periodics row: UpsertPeriodic reconciles it
// by slug, and the Scheduler fires the matching kind on each due tick. Exactly one
// of Cron or Every must be set.
type PeriodicSpec struct {
	// Slug is the stable identity of the schedule; re-upserting the same slug
	// updates the existing definition rather than creating a duplicate.
	Slug string
	// Kind is the worker kind enqueued on each fire.
	Kind string
	// Queue is the queue the enqueued jobs land on. Empty defaults to "periodic".
	Queue string
	// ArgsTemplate is the JSON args payload for each enqueued job. Empty defaults
	// to an empty object.
	ArgsTemplate []byte
	// Cron is a standard 5-field cron expression. Mutually exclusive with Every.
	Cron string
	// Every is a fixed interval between fires. Mutually exclusive with Cron.
	Every time.Duration
	// Active toggles the definition. An inactive definition is preserved but never
	// fires.
	Active bool
}

// validate checks the required fields and the exactly-one-of-schedule rule,
// parsing a cron expression to reject a malformed one up front.
func (s PeriodicSpec) validate() error {
	if s.Slug == "" {
		return newValidationError("slug", "is required")
	}
	if s.Kind == "" {
		return newValidationError("kind", "is required")
	}
	hasCron := s.Cron != ""
	hasEvery := s.Every > 0
	if hasCron == hasEvery {
		return newValidationError("schedule", "exactly one of Cron or Every is required")
	}
	if hasEvery && s.Every < time.Second {
		// interval_seconds has second granularity; a sub-second interval would
		// round to zero and produce a scheduleless row.
		return newValidationError("every", "must be at least 1 second")
	}
	if hasCron {
		if _, err := cron.ParseStandard(s.Cron); err != nil {
			return fmt.Errorf("flywheel: parse cron %q: %w", s.Cron, err)
		}
	}
	return nil
}

// nextFireAfter returns the first fire time strictly after now for spec.
func nextFireAfter(spec PeriodicSpec, now time.Time) (time.Time, error) {
	if spec.Every > 0 {
		return now.Add(spec.Every), nil
	}
	schedule, err := cron.ParseStandard(spec.Cron)
	if err != nil {
		return time.Time{}, fmt.Errorf("flywheel: parse cron %q: %w", spec.Cron, err)
	}
	return schedule.Next(now), nil
}

// UpsertPeriodic inserts or updates a periodic definition by slug. On insert it
// seeds next_run_at to the next fire after now (so a fresh schedule does not fire
// immediately). On update it preserves the existing next_run_at cursor unless the
// schedule itself changed, so reconciling an unchanged config on restart does not
// reset the cadence. It is the exported writer for job_periodics, which the CLI
// and a host's startup reconciliation use to declare schedules in code.
func UpsertPeriodic(ctx context.Context, db *gorm.DB, spec PeriodicSpec) error {
	if err := spec.validate(); err != nil {
		return err
	}
	now := ClockFrom(ctx).Now(ctx)

	args := spec.ArgsTemplate
	if len(args) == 0 {
		args = []byte("{}")
	}
	queue := spec.Queue
	if queue == "" {
		queue = defaultPeriodicQueue
	}

	var existing jobPeriodicRow
	err := db.WithContext(ctx).Where("slug = ?", spec.Slug).First(&existing).Error
	switch {
	case errors.Is(err, gorm.ErrRecordNotFound):
		return insertPeriodic(ctx, db, spec, queue, args, now)
	case err != nil:
		return fmt.Errorf("flywheel: load periodic %q: %w", spec.Slug, err)
	default:
		return updatePeriodic(ctx, db, existing, spec, queue, args, now)
	}
}

// insertPeriodic creates a new periodic row with a freshly computed next_run_at.
func insertPeriodic(ctx context.Context, db *gorm.DB, spec PeriodicSpec, queue string, args []byte, now time.Time) error {
	nextRun, err := nextFireAfter(spec, now)
	if err != nil {
		return err
	}
	row := jobPeriodicRow{
		Slug:         spec.Slug,
		Kind:         spec.Kind,
		Queue:        queue,
		ArgsTemplate: datatypes.JSON(args),
		NextRunAt:    nextRun,
		IsActive:     spec.Active,
	}
	if spec.Every > 0 {
		secs := int(spec.Every.Seconds())
		row.IntervalSeconds = &secs
	} else {
		cronExpr := spec.Cron
		row.CronExpr = &cronExpr
	}
	if err := db.WithContext(ctx).Create(&row).Error; err != nil {
		return fmt.Errorf("flywheel: insert periodic %q: %w", spec.Slug, WrapDBError(err))
	}
	return nil
}

// updatePeriodic reconciles an existing periodic row in place.
func updatePeriodic(
	ctx context.Context, db *gorm.DB, existing jobPeriodicRow, spec PeriodicSpec, queue string, args []byte, now time.Time,
) error {
	updates := map[string]any{
		"kind":             spec.Kind,
		"queue":            queue,
		"args_template":    datatypes.JSON(args),
		"is_active":        spec.Active,
		"updated_at":       now,
		"cron_expr":        nil,
		"interval_seconds": nil,
	}
	if spec.Every > 0 {
		updates["interval_seconds"] = int(spec.Every.Seconds())
	} else {
		updates["cron_expr"] = spec.Cron
	}
	if scheduleChanged(existing, spec) {
		nextRun, err := nextFireAfter(spec, now)
		if err != nil {
			return err
		}
		updates["next_run_at"] = nextRun
	}
	if err := db.WithContext(ctx).Model(&jobPeriodicRow{}).
		Where("id = ?", existing.ID).Updates(updates).Error; err != nil {
		return fmt.Errorf("flywheel: update periodic %q: %w", spec.Slug, WrapDBError(err))
	}
	return nil
}

// scheduleChanged reports whether spec's schedule differs from the existing row,
// so the next_run_at cursor is reset only on a real cadence change.
func scheduleChanged(existing jobPeriodicRow, spec PeriodicSpec) bool {
	if spec.Every > 0 {
		return existing.IntervalSeconds == nil || *existing.IntervalSeconds != int(spec.Every.Seconds())
	}
	return existing.CronExpr == nil || *existing.CronExpr != spec.Cron
}

// SetPeriodicActive toggles a periodic definition's active flag by slug without
// touching its schedule or next_run_at cursor. Deactivating preserves the row —
// it stays inspectable — but stops it firing; reactivating resumes it on the
// existing cadence. It is the writer behind a declarative reconcile's
// orphan-disable and the CLI's enable/disable, and returns ErrPeriodicNotFound
// when no definition has the slug.
func SetPeriodicActive(ctx context.Context, db *gorm.DB, slug string, active bool) error {
	now := ClockFrom(ctx).Now(ctx)
	res := db.WithContext(ctx).Model(&jobPeriodicRow{}).Where("slug = ?", slug).Updates(map[string]any{
		"is_active":  active,
		"updated_at": now,
	})
	if res.Error != nil {
		return fmt.Errorf("flywheel: set periodic %q active: %w", slug, res.Error)
	}
	if res.RowsAffected == 0 {
		return ErrPeriodicNotFound
	}
	return nil
}

// DeletePeriodic removes a periodic definition by slug. Jobs it already enqueued
// are untouched — only the schedule that would produce new ones is removed. It is
// the writer behind `flywheel schedule rm`, idiomatic with UpsertPeriodic, and
// returns ErrPeriodicNotFound when no definition has the slug.
func DeletePeriodic(ctx context.Context, db *gorm.DB, slug string) error {
	res := db.WithContext(ctx).Where("slug = ?", slug).Delete(&jobPeriodicRow{})
	if res.Error != nil {
		return fmt.Errorf("flywheel: delete periodic %q: %w", slug, res.Error)
	}
	if res.RowsAffected == 0 {
		return ErrPeriodicNotFound
	}
	return nil
}

// PeriodicView is the public read projection of a periodic definition.
type PeriodicView struct {
	Slug            string     `json:"slug"`
	Kind            string     `json:"kind"`
	Queue           string     `json:"queue"`
	Cron            string     `json:"cron,omitempty"`
	IntervalSeconds int        `json:"interval_seconds,omitempty"`
	NextRunAt       time.Time  `json:"next_run_at"`
	LastEnqueuedAt  *time.Time `json:"last_enqueued_at,omitempty"`
	Active          bool       `json:"active"`
}

// ListPeriodics returns every periodic definition, ordered by slug.
func ListPeriodics(ctx context.Context, db *gorm.DB) ([]PeriodicView, error) {
	var rows []jobPeriodicRow
	if err := db.WithContext(ctx).Order("slug").Find(&rows).Error; err != nil {
		return nil, fmt.Errorf("flywheel: list periodics: %w", err)
	}
	views := make([]PeriodicView, len(rows))
	for i := range rows {
		views[i] = periodicViewFromRow(rows[i])
	}
	return views, nil
}

// periodicViewFromRow projects an unexported jobPeriodicRow into PeriodicView.
func periodicViewFromRow(r jobPeriodicRow) PeriodicView {
	v := PeriodicView{
		Slug:           r.Slug,
		Kind:           r.Kind,
		Queue:          r.Queue,
		NextRunAt:      r.NextRunAt,
		LastEnqueuedAt: r.LastEnqueuedAt,
		Active:         r.IsActive,
	}
	if r.CronExpr != nil {
		v.Cron = *r.CronExpr
	}
	if r.IntervalSeconds != nil {
		v.IntervalSeconds = *r.IntervalSeconds
	}
	return v
}

// RetryJob forces a job back to available so a runner re-claims it on the next
// poll, clearing any lease and finalization. It is the operator action behind a
// "retry now" — it works on a terminal (discarded/cancelled/succeeded) job as well
// as a stuck one. It returns ErrJobNotFound when no live job has the id.
func RetryJob(ctx context.Context, db *gorm.DB, id string) error {
	now := ClockFrom(ctx).Now(ctx)
	res := db.WithContext(ctx).Model(&jobRow{}).Where("id = ?", id).Updates(map[string]any{
		"state":        string(StateAvailable),
		"leased_until": nil,
		"finalized_at": nil,
		"scheduled_at": now,
		"updated_at":   now,
	})
	if res.Error != nil {
		return fmt.Errorf("flywheel: retry job %q: %w", id, res.Error)
	}
	if res.RowsAffected == 0 {
		return ErrJobNotFound
	}
	return nil
}

// CancelJob moves a job to the terminal cancelled state. An attempt already in
// flight is not interrupted, but the job will not be retried or re-claimed. It
// returns ErrJobNotFound when no live job has the id.
func CancelJob(ctx context.Context, db *gorm.DB, id string) error {
	now := ClockFrom(ctx).Now(ctx)
	res := db.WithContext(ctx).Model(&jobRow{}).Where("id = ?", id).Updates(map[string]any{
		"state":        string(StateCancelled),
		"leased_until": nil,
		"finalized_at": now,
		"updated_at":   now,
	})
	if res.Error != nil {
		return fmt.Errorf("flywheel: cancel job %q: %w", id, res.Error)
	}
	if res.RowsAffected == 0 {
		return ErrJobNotFound
	}
	return nil
}
