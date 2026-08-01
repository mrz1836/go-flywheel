package flywheel

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/mrz1836/go-foundation/models"
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
	now := models.ClockFrom(ctx).Now(ctx)

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
		return fmt.Errorf("flywheel: insert periodic %q: %w", spec.Slug, models.WrapDBError(err))
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
		return fmt.Errorf("flywheel: update periodic %q: %w", spec.Slug, models.WrapDBError(err))
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
	now := models.ClockFrom(ctx).Now(ctx)
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

// RetryOpts configures a RetryJobWithOptions call. The zero value is a plain,
// guarded retry: a terminal job is refused with ErrJobTerminal, no retry budget is
// restored, and the re-run is immediate — exactly what RetryJob issues.
type RetryOpts struct {
	// Force retries a job that has already reached a terminal state — including a
	// succeeded one. Without it a terminal job is refused with ErrJobTerminal, so an
	// operator "retry now" can never silently re-run finished work. Re-running a
	// completed job is a legitimate action; it just has to be deliberate.
	Force bool
	// ResetAttempts restores the job's retry budget by raising max_attempts so the
	// job has Budget attempts remaining. It never lowers attempt: the attempt counter
	// is the audit key for job_runs and stays strictly monotonic, so the budget is
	// expressed as headroom rather than as a rewind — the same mechanism a snooze uses
	// to stay free. Without it a retry grants no new headroom, so a job discarded at
	// attempt == max_attempts is re-claimed exactly once and discarded again on that
	// attempt, which is rarely what "replay the failures" intends.
	ResetAttempts bool
	// Budget is the number of attempts to grant when ResetAttempts is set. Zero (or
	// negative) restores the job's original max_attempts as headroom — max_attempts
	// becomes attempt + max_attempts — while a positive value sets max_attempts to
	// attempt + Budget. It is ignored when ResetAttempts is false.
	Budget int
	// Delay, when > 0, schedules the job that far in the future instead of making it
	// immediately available. The claim gates on scheduled_at <= now in both drivers,
	// so a future scheduled_at defers the re-run without any state change.
	Delay time.Duration
}

// RetryJob forces a job back to available so a runner re-claims it on the next
// poll, clearing any lease and finalization. It is the operator action behind a
// "retry now".
//
// It is state-guarded to the same standard as CancelJob: a job that has already
// reached a terminal state (succeeded, cancelled, discarded) is left exactly as it
// is and ErrJobTerminal is returned, so a retry can never silently re-run finished
// work and clear the outcome that recorded it. Use RetryJobWithOptions with Force
// to re-run a terminal job deliberately. It returns ErrJobNotFound when no live job
// has the id.
//
// This is a change from the earlier unguarded behavior, which returned any job —
// including a succeeded one — to available. The guard is the point.
func RetryJob(ctx context.Context, db *gorm.DB, id string) error {
	return RetryJobWithOptions(ctx, db, id, RetryOpts{})
}

// RetryJobWithOptions is RetryJob with explicit options. Force retries a job that
// has already reached a terminal state; without it a terminal job returns
// ErrJobTerminal. ResetAttempts restores the job's retry budget by raising
// max_attempts (see RetryOpts.Budget) rather than rewinding attempt, so a job
// discarded at attempt == max_attempts gets real headroom and the job_runs audit
// sequence stays continuous. Delay schedules the re-run into the future instead of
// making the job immediately available. It returns ErrJobNotFound when no live job
// has the id. See RetryOpts.
func RetryJobWithOptions(ctx context.Context, db *gorm.DB, id string, opts RetryOpts) error {
	now := models.ClockFrom(ctx).Now(ctx)
	q := db.WithContext(ctx).Model(&jobRow{}).Where("id = ?", id)
	if !opts.Force {
		// Scope the write to the states a job can still be retried from, naming the
		// allowed states rather than excluding the terminal ones — the same guard
		// CancelJob uses, and for the same reason: a row in a state this runtime does
		// not recognize is refused rather than clobbered, so the guard can never be
		// defeated by a state added to the vocabulary but missed here.
		q = q.Where("state IN ?", nonTerminalStateStrings())
	}
	upd := map[string]any{
		"state":        string(StateAvailable),
		"leased_until": nil,
		// An attempt may still be running against this job. Clearing its token is
		// what stops that attempt finalizing over the re-run the operator just
		// asked for.
		"lease_token":  nil,
		"finalized_at": nil,
		// Delay 0 makes the job immediately claimable (scheduled_at == now); a
		// positive Delay defers the re-run, gated by the claim's scheduled_at <= now.
		"scheduled_at": now.Add(opts.Delay),
		"updated_at":   now,
	}
	applyRetryBudget(upd, opts)
	res := q.Updates(upd)
	if res.Error != nil {
		return fmt.Errorf("flywheel: retry job %q: %w", id, res.Error)
	}
	if res.RowsAffected == 0 {
		return classifyRetryMiss(ctx, db, id)
	}
	return nil
}

// applyRetryBudget adds the max_attempts reset to a retry's column map when
// opts.ResetAttempts is set. The budget is restored as headroom above the current
// attempt — max_attempts = attempt + Budget, or attempt + max_attempts (the
// original budget) when Budget is non-positive — applied in-SQL per row, mirroring
// the free-snooze path's gorm.Expr in jobFinalizeUpdate. attempt itself is never
// written, so it stays strictly monotonic as the job_runs(job_id, attempt) audit
// key and a replayed job's next run continues the sequence rather than colliding
// with it. It is shared by RetryJobWithOptions and the bulk replay engine, so the
// two cannot drift on the arithmetic that makes a replay obviously correct.
func applyRetryBudget(upd map[string]any, opts RetryOpts) {
	if !opts.ResetAttempts {
		return
	}
	if opts.Budget <= 0 {
		upd["max_attempts"] = gorm.Expr("attempt + max_attempts")
	} else {
		upd["max_attempts"] = gorm.Expr("attempt + ?", opts.Budget)
	}
}

// classifyRetryMiss explains why RetryJobWithOptions's UPDATE matched no row,
// exactly as classifyCancelMiss does for CancelJob: the job does not exist
// (ErrJobNotFound), or it exists but is terminal and Force was not set
// (ErrJobTerminal). Under Force the UPDATE carries no state guard, so a miss can
// only be a missing (or soft-deleted) job — the count is zero and this returns
// ErrJobNotFound, which is the right answer.
//
// The count is scoped exactly like the UPDATE — Model(&jobRow{}) applies gorm's
// deleted_at IS NULL — so a soft-deleted job reads as missing rather than terminal.
// It runs outside a transaction for the same reason classifyCancelMiss does: the
// miss path writes nothing, and serializing an operator-scale diagnostic read would
// cost a write lock on SQLite for no correctness gain.
func classifyRetryMiss(ctx context.Context, db *gorm.DB, id string) error {
	var count int64
	if err := db.WithContext(ctx).Model(&jobRow{}).Where("id = ?", id).Count(&count).Error; err != nil {
		return fmt.Errorf("flywheel: retry job %q: %w", id, err)
	}
	if count == 0 {
		return ErrJobNotFound
	}
	return ErrJobTerminal
}

// CancelJob moves a still-in-flight job to the terminal cancelled state. An
// attempt already running is not interrupted, but the job will not be retried or
// re-claimed.
//
// A job that has already reached a terminal state (succeeded, cancelled, or
// discarded) is left exactly as it is and ErrJobTerminal is returned: cancelling
// must never overwrite a recorded outcome or its finalized_at stamp. It returns
// ErrJobNotFound when no live job has the id.
func CancelJob(ctx context.Context, db *gorm.DB, id string) error {
	now := models.ClockFrom(ctx).Now(ctx)
	// Scope the write to the states a job can still be cancelled from. Naming the
	// allowed states rather than excluding the terminal ones is deliberate: a row
	// in a state this runtime does not recognize is refused rather than clobbered,
	// so the guard can never be defeated by a state added to the vocabulary but
	// missed here.
	res := db.WithContext(ctx).Model(&jobRow{}).
		Where("id = ? AND state IN ?", id, nonTerminalStateStrings()).
		Updates(map[string]any{
			"state":        string(StateCancelled),
			"leased_until": nil,
			// The running attempt is not interrupted, but its claim is: clearing
			// the token is what keeps its finalize from overwriting the cancel.
			"lease_token":  nil,
			"finalized_at": now,
			"updated_at":   now,
		})
	if res.Error != nil {
		return fmt.Errorf("flywheel: cancel job %q: %w", id, res.Error)
	}
	if res.RowsAffected == 0 {
		return classifyCancelMiss(ctx, db, id)
	}
	return nil
}

// classifyCancelMiss explains why CancelJob's guarded UPDATE matched no row: the
// job does not exist (ErrJobNotFound), or it exists but is no longer cancellable
// (ErrJobTerminal). The count is scoped exactly like the UPDATE — Model(&jobRow{})
// applies gorm's deleted_at IS NULL — so a soft-deleted job reads as missing
// rather than terminal.
//
// It runs outside a transaction on purpose. The miss path writes nothing, and the
// only interleaving it can misread is a concurrent RetryJob resurrecting the job
// between the two statements, which reports ErrJobTerminal for a job that just
// became available again — a stale diagnosis the operator resolves by re-running
// the cancel. A deletion in the same window yields ErrJobNotFound, which is the
// right answer regardless. Serializing an operator-scale diagnostic read would
// cost a write lock on SQLite for no correctness gain.
func classifyCancelMiss(ctx context.Context, db *gorm.DB, id string) error {
	var count int64
	if err := db.WithContext(ctx).Model(&jobRow{}).Where("id = ?", id).Count(&count).Error; err != nil {
		return fmt.Errorf("flywheel: cancel job %q: %w", id, err)
	}
	if count == 0 {
		return ErrJobNotFound
	}
	return ErrJobTerminal
}

// defaultScopeBatchSize bounds a parent-scoped operation's per-transaction batch
// when ScopeOpts leaves BatchSize zero. Like defaultSweepBatchSize it is a bound,
// not a knob to switch off: no value of BatchSize, zero or negative, produces an
// unbounded transaction.
const defaultScopeBatchSize = 1000

// ScopeResult reports what a parent-scoped operation did. The skipped counts are
// broken out by reason so a caller distinguishes "nothing to do" from "refused".
type ScopeResult struct {
	// Changed is the number of children whose state the operation advanced.
	Changed int64
	// SkippedTerminal is the number of children left alone because they had already
	// reached a terminal state — a succeeded child is never clobbered by a batch
	// cancel. It is counted before the operation runs, so a cancel's own
	// freshly-cancelled rows are never miscounted as pre-existing terminal ones.
	SkippedTerminal int64
	// SkippedRunning is the number of children left in flight. None of the three
	// operations interrupts a running attempt: it finalizes normally, and the pause
	// or cancel applies to its next claim.
	SkippedRunning int64
	// Batches is the number of transactions that changed rows.
	Batches int
}

// ScopeOpts bounds a parent-scoped operation.
type ScopeOpts struct {
	// BatchSize is the number of children updated per transaction. Zero or negative
	// selects defaultScopeBatchSize; it is never unbounded.
	BatchSize int
}

// batchSize resolves the configured batch size, applying the default for a
// non-positive value.
func (o ScopeOpts) batchSize() int {
	if o.BatchSize <= 0 {
		return defaultScopeBatchSize
	}
	return o.BatchSize
}

// scopeTransition is one parent-scoped operation's state machine: the states it
// moves children out of, and the columns it writes. The three operations differ
// only in these two things and share scopeByParent's batched, guarded loop.
type scopeTransition struct {
	op      string
	source  []string
	updates func(now time.Time) map[string]any
}

// PauseByParent holds every claimable child of a parent — available, retryable,
// or scheduled — in the paused state, so no further child is claimed. It is
// state-guarded: a terminal child is never touched (counted in SkippedTerminal),
// and a running child is left to finalize (counted in SkippedRunning). A running
// attempt is not interrupted; the pause takes effect on its next claim.
//
// The work is bounded — one transaction per batch, BatchSize children each —
// exactly like the lease sweep. A held child leaves the claimable scope, so the
// loop terminates on a short batch with no cursor. Cancellation is checked between
// batches and committed batches are kept, so a cancelled pause is partial progress,
// returned alongside the wrapped context error.
func PauseByParent(ctx context.Context, db *gorm.DB, parentJobID string, opts ScopeOpts) (ScopeResult, error) {
	return scopeByParent(ctx, db, parentJobID, opts, scopeTransition{
		op:     "pause",
		source: []string{string(StateAvailable), string(StateRetryable), string(StateScheduled)},
		updates: func(now time.Time) map[string]any {
			return map[string]any{"state": string(StatePaused), "updated_at": now}
		},
	})
}

// ResumeByParent returns every paused child of a parent to available. It resumes
// only jobs paused by PauseByParent — its source state is paused alone — so a child
// deferred to a future time by its own backoff is not disturbed. A resumed child
// keeps its scheduled_at, so one paused while deferred stays deferred rather than
// becoming claimable early.
//
// It is bounded and cancellable on the same terms as PauseByParent.
func ResumeByParent(ctx context.Context, db *gorm.DB, parentJobID string, opts ScopeOpts) (ScopeResult, error) {
	return scopeByParent(ctx, db, parentJobID, opts, scopeTransition{
		op:     "resume",
		source: []string{string(StatePaused)},
		updates: func(now time.Time) map[string]any {
			return map[string]any{"state": string(StateAvailable), "updated_at": now}
		},
	})
}

// CancelByParent moves every non-running, non-terminal child of a parent — paused,
// available, retryable, or scheduled — to the terminal cancelled state, stamping
// finalized_at and releasing any lease. A running child is left to finalize
// (SkippedRunning) and a terminal child is left exactly as it is (SkippedTerminal),
// so a succeeded child's state and finalized_at are never overwritten.
//
// It is bounded and cancellable on the same terms as PauseByParent.
func CancelByParent(ctx context.Context, db *gorm.DB, parentJobID string, opts ScopeOpts) (ScopeResult, error) {
	return scopeByParent(ctx, db, parentJobID, opts, scopeTransition{
		op: "cancel",
		source: []string{
			string(StatePaused), string(StateAvailable), string(StateRetryable), string(StateScheduled),
		},
		updates: func(now time.Time) map[string]any {
			return map[string]any{
				"state": string(StateCancelled), "finalized_at": now,
				"leased_until": nil, "lease_token": nil, "updated_at": now,
			}
		},
	})
}

// scopeByParent runs one parent-scoped transition: it counts what the operation
// leaves alone, then moves the source-state children to the target in bounded,
// per-transaction batches. It is the shared engine behind Pause/Resume/CancelByParent.
func scopeByParent(
	ctx context.Context, db *gorm.DB, parentJobID string, opts ScopeOpts, t scopeTransition,
) (ScopeResult, error) {
	if db == nil {
		return ScopeResult{}, fmt.Errorf("flywheel: %s by parent: db is nil", t.op)
	}
	batchSize := opts.batchSize()
	now := models.ClockFrom(ctx).Now(ctx)
	var result ScopeResult

	// A dead context on entry does no work at all — not even the counting reads —
	// and reports the progress made in the loop's own vocabulary, matching the sweep.
	if err := ctx.Err(); err != nil {
		return ScopeResult{}, fmt.Errorf("flywheel: %s by parent cancelled after 0 changed: %w", t.op, err)
	}

	// Count what the operation deliberately leaves alone, before it runs. Counting
	// terminal children first is what keeps a cancel's own freshly-cancelled rows out
	// of SkippedTerminal: after the loop those rows are terminal too, and a post-hoc
	// count could not tell them from ones that were terminal all along.
	if err := db.WithContext(ctx).Model(&jobRow{}).
		Where("parent_job_id = ? AND state = ?", parentJobID, string(StateRunning)).
		Count(&result.SkippedRunning).Error; err != nil {
		return ScopeResult{}, fmt.Errorf("flywheel: %s by parent: count running: %w", t.op, err)
	}
	if err := db.WithContext(ctx).Model(&jobRow{}).
		Where("parent_job_id = ? AND state IN ?", parentJobID, terminalStateStrings()).
		Count(&result.SkippedTerminal).Error; err != nil {
		return ScopeResult{}, fmt.Errorf("flywheel: %s by parent: count terminal: %w", t.op, err)
	}

	// Move the source-state children to the target in bounded batches, one
	// transaction each — the sweep's shape. A moved child leaves the source scope, so
	// termination is a short SELECT, no cursor. Cancellation is checked between
	// batches; the batch in flight when a cancel arrives rolls back whole and the
	// committed ones are kept, so a cancelled operation is partial progress.
	for {
		if err := ctx.Err(); err != nil {
			return result, fmt.Errorf(
				"flywheel: %s by parent cancelled after %d changed: %w", t.op, result.Changed, err,
			)
		}
		selected, changed, err := scopeBatch(ctx, db, parentJobID, t, batchSize, now)
		if err != nil {
			return result, fmt.Errorf("flywheel: %s by parent: %w", t.op, err)
		}
		result.Changed += changed
		if selected > 0 {
			result.Batches++
		}
		if selected < batchSize {
			return result, nil
		}
	}
}

// scopeBatch moves one bounded batch of a parent's source-state children to the
// target inside a single transaction. It returns how many rows it selected (which
// drives the loop's termination) and how many it actually changed.
//
// The UPDATE re-guards on the source states — WHERE id IN ? AND state IN ? — rather
// than trusting the ids the SELECT found: without SKIP LOCKED a concurrent operation
// could move one of them between the SELECT and the UPDATE, and an unguarded UPDATE
// would resurrect it out of the state that concurrent operation left it in. The
// re-guard makes the batch move only children still eligible, so Changed counts real
// transitions and no terminal row is ever revived.
func scopeBatch(
	ctx context.Context, db *gorm.DB, parentJobID string, t scopeTransition, batchSize int, now time.Time,
) (selected int, changed int64, err error) {
	err = db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var ids []string
		if e := tx.Model(&jobRow{}).
			Where("parent_job_id = ? AND state IN ?", parentJobID, t.source).
			Limit(batchSize).Pluck("id", &ids).Error; e != nil {
			return e
		}
		selected = len(ids)
		if selected == 0 {
			return nil
		}
		res := tx.Model(&jobRow{}).
			Where("id IN ? AND state IN ?", ids, t.source).
			Updates(t.updates(now))
		if res.Error != nil {
			return res.Error
		}
		changed = res.RowsAffected
		return nil
	})
	return selected, changed, err
}
