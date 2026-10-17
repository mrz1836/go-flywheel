package core

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/mrz1836/go-foundation/models"
	"github.com/robfig/cron/v3"
	"gorm.io/datatypes"
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
// It records who paused the definition and why: the type, id, and reason of
// the context's actor (WithActor; the system when there is none) on the row,
// where PeriodicView.PausedBy reports them, and one AuditPaused entry for the
// slug (AuditTargetPeriodic) in the control history, written in the pause's
// transaction, which also runs the context's audit hook.
//
// Parameters:
//   - ctx: carries cancellation, the actor, and the clock whose now is recorded
//     as the pause's time (models.ClockFrom).
//   - db: the database, or a caller's transaction.
//   - slug: the definition to pause.
//
// Returns ErrPeriodicNotFound when no definition has the slug, and a
// *ValidationError naming its field for an actor the history can't record,
// before anything is read. Pausing a paused definition returns nil and writes
// nothing, so its first pause's time and actor stand and no entry is recorded.
//
// Side effects: sets job_periodics.paused_at, paused_by_type, paused_by_id,
// paused_reason, and updated_at, and writes the entry, in a transaction of its
// own (a savepoint in a caller's transaction) that reads the row under a row
// lock on PostgreSQL.
//
// Notes: a fire a tick has already begun when the pause commits completes. The
// row keeps the pause, so DeletePeriodic removes it with the row, and a
// definition the reconcile re-creates after a delete is not paused.
func PausePeriodic(ctx context.Context, db *gorm.DB, slug string) error {
	actor, err := resolveActor(ctx)
	if err != nil {
		return err
	}
	now := models.ClockFrom(ctx).Now(ctx)
	err = db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		row, err := lockPeriodic(tx, slug)
		if err != nil || row.PausedAt != nil {
			return err
		}
		if err := tx.Model(&jobPeriodicRow{}).Where("id = ?", row.ID).Updates(map[string]any{
			"paused_at":      now,
			"paused_by_type": string(actor.Type),
			"paused_by_id":   actor.ID,
			"paused_reason":  actor.Reason,
			"updated_at":     now,
		}).Error; err != nil {
			return err
		}
		return recordAudit(ctx, tx, actor, now,
			[]auditChange{{target: AuditTargetPeriodic, targetID: slug, action: AuditPaused}})
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
//   - Resolves the context's actor (WithActor; the system when there is none),
//     refusing one the history can't record with a *ValidationError naming its
//     field, before anything is read.
//   - Reads the definition under a row lock on PostgreSQL; none is
//     ErrPeriodicNotFound.
//   - Returns nil, writing nothing, when the definition is not paused.
//   - Clears the pause and who paused it and, when its next_run_at is not after
//     now, moves it to the first fire after now on the definition's own
//     schedule: an interval keeps its phase (next_run_at plus the smallest
//     whole number of intervals that passes now), and a cron takes its next
//     match after now. A next_run_at still in the future is kept.
//   - Writes one AuditResumed entry for the slug, attributed to the actor, and
//     runs the context's audit hook.
//
// Parameters:
//   - ctx: carries cancellation, the actor, and the clock whose now the next
//     fire is computed from (models.ClockFrom).
//   - db: the database, or a caller's transaction.
//   - slug: the definition to resume.
//
// Returns ErrPeriodicNotFound when no definition has the slug, or the parse
// error of a stored cron expression that no longer parses.
//
// Side effects: clears job_periodics.paused_at and the paused_by columns,
// writes next_run_at and updated_at, and writes the entry, in a transaction of
// its own (a savepoint in a caller's transaction).
//
// Notes: resuming leaves the Active flag alone, so a definition that is also
// inactive stays off; and a cron with no CRON_TZ= zone takes its next match in
// the clock's zone, as on its creation (see PeriodicSpec.Cron).
func ResumePeriodic(ctx context.Context, db *gorm.DB, slug string) error {
	actor, err := resolveActor(ctx)
	if err != nil {
		return err
	}
	now := models.ClockFrom(ctx).Now(ctx)
	err = db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		row, err := lockPeriodic(tx, slug)
		if err != nil || row.PausedAt == nil {
			return err
		}
		nextRun, err := resumedNextRun(row, now)
		if err != nil {
			return err
		}
		if err := tx.Model(&jobPeriodicRow{}).Where("id = ?", row.ID).Updates(map[string]any{
			"paused_at":      nil,
			"paused_by_type": "",
			"paused_by_id":   "",
			"paused_reason":  "",
			"next_run_at":    nextRun,
			"updated_at":     now,
		}).Error; err != nil {
			return err
		}
		return recordAudit(ctx, tx, actor, now,
			[]auditChange{{target: AuditTargetPeriodic, targetID: slug, action: AuditResumed}})
	})
	if err != nil && !errors.Is(err, ErrPeriodicNotFound) {
		return fmt.Errorf("flywheel: resume periodic %q: %w", slug, err)
	}
	return err
}

// TriggerPeriodic runs slug's definition now: it enqueues one job of the
// definition's kind, args template, and queue, due now and with no unique key,
// and returns the job's id. It runs a paused or an inactive definition too — a
// manual run is how an operator re-runs a fire the schedule missed or skipped —
// and leaves its pause, its Active flag, and its next fire as they are.
//
// The insert goes through c, so c's ClientOpts apply: its Registry's Defaults
// fill what the definition leaves unset, and its InsertHook sees the insert as
// InsertSourcePeriodic, with the slug, as it sees the Scheduler's fires. In the
// same transaction, on c's write connection, the run is remembered in the
// definition's history as a manual fire (see PeriodicHistory),
// last_enqueued_at moves to now, and one AuditTriggered entry for the slug,
// its details naming the job, is written to the control history under the
// context's actor (WithActor; the system when there is none), with the
// context's audit hook. The job, its history entry, and its audit entry commit
// or roll back together, so a failing hook enqueues nothing.
//
// It reads the definition under a row lock on PostgreSQL, so a concurrent fire
// records its history after this one. An unknown slug is ErrPeriodicNotFound,
// and an actor the history can't record is a *ValidationError naming its field,
// each with nothing written; a nil c is an error.
func TriggerPeriodic(ctx context.Context, c *Client, slug string) (string, error) {
	if c == nil {
		return "", fmt.Errorf("flywheel: TriggerPeriodic: client is nil")
	}
	actor, err := resolveActor(ctx)
	if err != nil {
		return "", err
	}
	var id string
	err = c.WriteDB().WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		row, err := lockPeriodic(tx, slug)
		if err != nil {
			return err
		}
		payload := []byte(row.ArgsTemplate)
		if len(payload) == 0 {
			payload = []byte("{}")
		}
		now := models.ClockFrom(ctx).Now(ctx)
		jobID, collided, err := c.insert(ctx, row.Kind, payload, InsertOpts{Tx: tx, Queue: row.Queue, ScheduleAt: &now},
			insertOrigin{source: InsertSourcePeriodic, periodicSlug: slug})
		if err != nil {
			return err
		}
		if collided {
			return errors.New("the insert collided with a job already enqueued")
		}
		history, err := prependFires(row.RecentJobs, []periodicFireEntry{{JobID: jobID, FiredAt: now, Manual: true}})
		if err != nil {
			return err
		}
		if err := tx.Model(&jobPeriodicRow{}).Where("id = ?", row.ID).Updates(map[string]any{
			"last_enqueued_at": now,
			"updated_at":       now,
			"recent_jobs":      history,
		}).Error; err != nil {
			return err
		}
		id = jobID
		return recordAudit(ctx, tx, actor, now, []auditChange{{
			target: AuditTargetPeriodic, targetID: slug, action: AuditTriggered, details: map[string]any{"job_id": jobID},
		}})
	})
	switch {
	case errors.Is(err, ErrPeriodicNotFound):
		return "", err
	case err != nil:
		return "", fmt.Errorf("flywheel: trigger periodic %q: %w", slug, err)
	default:
		return id, nil
	}
}

// lockPeriodic reads slug's definition inside tx, under FOR UPDATE on
// PostgreSQL so a concurrent pause, resume, or fire waits for this one. SQLite
// has no row lock and needs none: its writers are already serialized, the same
// idiom lockBucket uses. A missing definition is ErrPeriodicNotFound.
func lockPeriodic(tx *gorm.DB, slug string) (jobPeriodicRow, error) {
	row, found, err := lockPeriodicWhere(tx, "slug = ?", slug)
	switch {
	case err != nil:
		return jobPeriodicRow{}, err
	case !found:
		return jobPeriodicRow{}, ErrPeriodicNotFound
	default:
		return row, nil
	}
}

// lockPeriodicWhere reads the definition query and arg select inside tx, locked
// as lockPeriodic locks it, reporting a missing one as found == false.
func lockPeriodicWhere(tx *gorm.DB, query string, arg any) (jobPeriodicRow, bool, error) {
	q := quietMissing(tx).Where(query, arg)
	if tx.Name() == "postgres" {
		q = q.Clauses(clause.Locking{Strength: "UPDATE"})
	}
	var row jobPeriodicRow
	if err := q.First(&row).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return jobPeriodicRow{}, false, nil
		}
		return jobPeriodicRow{}, false, err
	}
	return row, true, nil
}

// PeriodicHistorySize is how many of its latest fires a periodic definition
// remembers, and so the most PeriodicHistory returns.
const PeriodicHistorySize = 20

// PeriodicFire is one remembered fire of a periodic definition, as
// PeriodicHistory reports it, with what became of the job it enqueued.
type PeriodicFire struct {
	// JobID is the job the fire enqueued.
	JobID string `json:"job_id"`
	// FiredAt is the fire's time, in UTC: a scheduled fire's due time (its
	// bucket, which a backfill enqueues late), or when TriggerPeriodic ran.
	FiredAt time.Time `json:"fired_at"`
	// Manual reports a fire TriggerPeriodic made rather than the Scheduler.
	Manual bool `json:"manual"`
	// State is the job's state; empty once the job is gone, as retention removes
	// a finished one. A soft-deleted job keeps its state.
	State JobState `json:"state,omitempty"`
	// Outcome is the outcome of the job's latest run; empty while it has none.
	Outcome string `json:"outcome,omitempty"`
	// FinishedAt is when the job's latest run finished, in UTC; nil while it has
	// none or it is running.
	FinishedAt *time.Time `json:"finished_at,omitempty"`
}

// periodicFireEntry is one entry of a definition's history as job_periodics'
// recent_jobs stores it.
type periodicFireEntry struct {
	JobID   string    `json:"job_id"`
	FiredAt time.Time `json:"fired_at"`
	Manual  bool      `json:"manual"`
}

// decodeFires returns the history stored, newest first. A value that does not
// decode is an empty history, so a hand-edited row cannot fail a fire or a
// read.
func decodeFires(stored datatypes.JSON) []periodicFireEntry {
	var ring []periodicFireEntry
	if len(stored) == 0 || json.Unmarshal(stored, &ring) != nil {
		return nil
	}
	return ring
}

// prependFires returns the history stored with fires recorded on top of it.
// fires are in the order they were made, oldest first, and go ahead of the
// stored entries newest first, with their times in UTC; the result keeps the
// newest PeriodicHistorySize. A stored value that does not decode starts the
// history afresh.
func prependFires(stored datatypes.JSON, fires []periodicFireEntry) (datatypes.JSON, error) {
	ring := make([]periodicFireEntry, 0, PeriodicHistorySize)
	for i := len(fires) - 1; i >= 0 && len(ring) < PeriodicHistorySize; i-- {
		fire := fires[i]
		fire.FiredAt = fire.FiredAt.UTC()
		ring = append(ring, fire)
	}
	for _, entry := range decodeFires(stored) {
		if len(ring) == PeriodicHistorySize {
			break
		}
		ring = append(ring, entry)
	}
	raw, err := json.Marshal(ring)
	if err != nil {
		return nil, fmt.Errorf("encode the periodic history: %w", err)
	}
	return datatypes.JSON(raw), nil
}

// PeriodicHistory returns slug's latest fires, newest first — at most
// PeriodicHistorySize, scheduled and manual alike — each with its job's state,
// its latest run's outcome, and that run's finish time. A fire whose job is gone
// keeps its id and time, with an empty state.
//
// Every fire that enqueues a job is remembered, in the transaction that records
// it on the definition's row: a scheduled fire at its bucket's time, each bucket
// of a backfill on its own, and a TriggerPeriodic run at its time, marked
// Manual. A bucket that collided with a job already enqueued for it is not
// remembered again. The history lives on the row, so DeletePeriodic removes it.
//
// It costs three bounded reads: the definition by its slug, the fired jobs by
// primary key (soft-deleted ones included), and their latest runs through
// job_runs_job_attempt. An unknown slug is ErrPeriodicNotFound.
func PeriodicHistory(ctx context.Context, db *gorm.DB, slug string) ([]PeriodicFire, error) {
	if db == nil {
		return nil, fmt.Errorf("flywheel: PeriodicHistory: db is nil")
	}
	row, found, err := loadPeriodic(ctx, db, slug)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, ErrPeriodicNotFound
	}
	ring := decodeFires(row.RecentJobs)
	history := make([]PeriodicFire, len(ring))
	ids := make([]string, len(ring))
	for i, entry := range ring {
		history[i] = PeriodicFire{JobID: entry.JobID, FiredAt: entry.FiredAt.UTC(), Manual: entry.Manual}
		ids[i] = entry.JobID
	}
	if len(ids) == 0 {
		return history, nil
	}

	var jobs []jobRow
	if err := db.WithContext(ctx).Unscoped().Model(&jobRow{}).Select("id", "state", "attempt").
		Where("id IN ?", ids).Find(&jobs).Error; err != nil {
		return nil, fmt.Errorf("flywheel: periodic history %q: read jobs: %w", slug, err)
	}
	byID := make(map[string]jobRow, len(jobs))
	latest := make([][]any, 0, len(jobs))
	for _, job := range jobs {
		byID[job.ID] = job
		if job.Attempt > 0 {
			latest = append(latest, []any{job.ID, job.Attempt})
		}
	}
	runs := make(map[string]jobRunRow, len(latest))
	if len(latest) > 0 {
		var rows []jobRunRow
		if err := db.WithContext(ctx).Model(&jobRunRow{}).Select("job_id", "attempt", "outcome", "finished_at").
			Where("(job_id, attempt) IN ?", latest).Find(&rows).Error; err != nil {
			return nil, fmt.Errorf("flywheel: periodic history %q: read runs: %w", slug, err)
		}
		for _, run := range rows {
			runs[run.JobID] = run
		}
	}

	for i := range history {
		job, ok := byID[history[i].JobID]
		if !ok {
			continue
		}
		history[i].State = JobState(job.State)
		if run, ok := runs[job.ID]; ok {
			history[i].Outcome = run.Outcome
			if run.FinishedAt != nil {
				at := run.FinishedAt.UTC()
				history[i].FinishedAt = &at
			}
		}
	}
	return history, nil
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
