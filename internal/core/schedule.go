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

// PeriodicSpec declares a periodic (cron or fixed-interval) job. It is the
// exported, host-facing form of a job_periodics row: UpsertPeriodic reconciles it
// by slug, SchedulerConfig.Periodics declares a host's set of them, and the
// Scheduler fires the matching kind on each due tick. Exactly one of Cron or Every
// must be set.
type PeriodicSpec struct {
	// Slug is the stable identity of the schedule; re-upserting the same slug
	// updates the existing definition rather than creating a duplicate.
	Slug string
	// Kind is the worker kind enqueued on each fire.
	Kind string
	// Queue is the queue the enqueued jobs land on. Empty defaults to "periodic".
	Queue string
	// ArgsTemplate is the JSON args payload for each enqueued job. Empty defaults
	// to an empty object; anything else must be valid JSON.
	ArgsTemplate []byte
	// Cron is a standard 5-field cron expression. Mutually exclusive with Every.
	//
	// An expression that names no zone runs in the zone of the time it is
	// evaluated against: the Scheduler clock's now when the schedule is created
	// or changed (the default clock returns time.Now(), the process's local zone),
	// and on every fire after that the stored next_run_at, as the database driver
	// returns it. Whether "0 9 * * *" fires at 09:00 UTC or 09:00 local time, and
	// whether it follows daylight saving, then depends on the host's zone and the
	// driver. Prefix the expression with CRON_TZ=<IANA zone> to pin it —
	// "CRON_TZ=UTC 0 9 * * *", or "CRON_TZ=America/New_York 0 9 * * *" for 09:00
	// New York time across daylight-saving changes — and the schedule runs in that
	// zone wherever it is evaluated. An unknown zone is refused like any malformed
	// expression.
	Cron string
	// Every is a fixed interval between fires. Mutually exclusive with Cron.
	Every time.Duration
	// Active toggles the definition. An inactive definition is preserved but never
	// fires. The zero value is inactive, so a definition meant to fire sets Active
	// true. It is the declaration's switch, applied on every upsert; an operator's
	// PausePeriodic is held apart from it and never written by an upsert.
	Active bool
}

// validate checks the required fields and the exactly-one-of-schedule rule,
// parsing a cron expression to reject a malformed one up front, and rejects an
// args template that is not JSON. Every failure wraps ErrValidation.
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
			return &cronSpecError{err: fmt.Errorf("flywheel: parse cron %q: %w", s.Cron, err)}
		}
	}
	if len(s.ArgsTemplate) > 0 && !json.Valid(s.ArgsTemplate) {
		// Checked here because neither dialect does it usefully: SQLite stores
		// any text, so every fire would enqueue args no worker can decode, and
		// PostgreSQL's jsonb refuses it only at the insert, as a database error.
		return newValidationError("args_template", "must be valid JSON")
	}
	return nil
}

// cronSpecError is a malformed cron expression PeriodicSpec.validate rejects. Its
// text is the parse error's, unchanged — operators search logs for it — and it
// unwraps to both ErrValidation, which every other validation failure wraps, and
// the parser's own error.
//
// It is a type rather than a second %w because fmt.Errorf prints every operand:
// wrapping ErrValidation that way would append the sentinel's text to the message.
type cronSpecError struct{ err error }

// Error returns the parse error's text.
func (e *cronSpecError) Error() string { return e.err.Error() }

// Unwrap exposes ErrValidation and the parse error for errors.Is and errors.As.
func (e *cronSpecError) Unwrap() []error { return []error{ErrValidation, e.err} }

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
// reset the cadence. Active is written on both paths: a definition upserted
// inactive is stored inactive.
//
// It never writes the pause. A definition paused with PausePeriodic stays paused
// through an upsert, which still applies the spec's kind, queue, args, cadence,
// and Active to it; only ResumePeriodic lifts the pause.
//
// It is safe to call concurrently for one slug — two hosts declaring the same new
// schedule at once. The call whose insert loses adopts the row that landed and
// updates it, so concurrent upserts end exactly where the same calls made one
// after the other would: the last writer's spec wins.
//
// It is the exported writer for job_periodics, behind `flywheel schedule add`. A
// host declaring its schedules in code will usually rather set
// SchedulerConfig.Periodics, which applies them this way when the Scheduler starts
// and re-creates any that go missing while it runs.
func UpsertPeriodic(ctx context.Context, db *gorm.DB, spec PeriodicSpec) error {
	_, err := upsertPeriodic(ctx, db, spec)
	return err
}

// upsertPeriodic is UpsertPeriodic, also returning the paused_at of the row it
// updated: nil for a row it inserted or one that is not paused. The Scheduler's
// start-up apply reads it to log a declaration it finds paused.
func upsertPeriodic(ctx context.Context, db *gorm.DB, spec PeriodicSpec) (*time.Time, error) {
	if err := spec.validate(); err != nil {
		return nil, err
	}
	now := models.ClockFrom(ctx).Now(ctx)

	existing, found, err := loadPeriodic(ctx, db, spec.Slug)
	if err != nil {
		return nil, err
	}
	if !found {
		_, inserted, ierr := insertPeriodic(ctx, db, spec, now)
		if ierr != nil || inserted {
			return nil, ierr
		}
		// A concurrent writer inserted the slug between the lookup and the insert.
		// Treating that as success could leave its spec in place rather than this
		// one — a rolling deploy that changes a cron would keep the old one — so
		// adopt the row that landed and apply this spec to it.
		if existing, found, err = loadPeriodic(ctx, db, spec.Slug); err != nil {
			return nil, err
		}
		if !found {
			return nil, fmt.Errorf("flywheel: upsert periodic %q: inserted and removed concurrently", spec.Slug)
		}
	}
	return existing.PausedAt, updatePeriodic(ctx, db, existing, spec, now)
}

// loadPeriodic reads the periodic definition for slug, reporting a missing one as
// found == false rather than as an error. The lookup is quiet about the miss, so
// declaring a new schedule logs no "record not found".
func loadPeriodic(ctx context.Context, db *gorm.DB, slug string) (jobPeriodicRow, bool, error) {
	var row jobPeriodicRow
	err := quietMissing(db).WithContext(ctx).Where("slug = ?", slug).First(&row).Error
	switch {
	case errors.Is(err, gorm.ErrRecordNotFound):
		return jobPeriodicRow{}, false, nil
	case err != nil:
		return jobPeriodicRow{}, false, fmt.Errorf("flywheel: load periodic %q: %w", slug, err)
	default:
		return row, true, nil
	}
}

// periodicQueueAndArgs resolves spec's queue and args template, applying the
// defaults an empty value selects.
func periodicQueueAndArgs(spec PeriodicSpec) (string, datatypes.JSON) {
	queue := spec.Queue
	if queue == "" {
		queue = defaultPeriodicQueue
	}
	args := spec.ArgsTemplate
	if len(args) == 0 {
		args = []byte("{}")
	}
	return queue, datatypes.JSON(args)
}

// periodicRow builds the job_periodics row spec declares, with next_run_at at its
// first fire time after now.
func periodicRow(spec PeriodicSpec, now time.Time) (jobPeriodicRow, error) {
	nextRun, err := nextFireAfter(spec, now)
	if err != nil {
		return jobPeriodicRow{}, err
	}
	queue, args := periodicQueueAndArgs(spec)
	row := jobPeriodicRow{
		Slug:         spec.Slug,
		Kind:         spec.Kind,
		Queue:        queue,
		ArgsTemplate: args,
		NextRunAt:    nextRun,
		IsActive:     new(spec.Active),
	}
	if spec.Every > 0 {
		secs := int(spec.Every.Seconds())
		row.IntervalSeconds = &secs
	} else {
		cronExpr := spec.Cron
		row.CronExpr = &cronExpr
	}
	return row, nil
}

// insertPeriodic inserts spec as a new periodic row, with next_run_at at its first
// fire time after now, and reports whether the insert landed, returning the row
// it wrote when it did. The insert does nothing when the slug already exists: a
// concurrent writer that inserted it first is (jobPeriodicRow{}, false, nil), not
// an error, and the caller decides what losing means.
//
// The conflict clause is targetless, as conflictInsertChunk's is. It covers
// idx_job_periodics_slug without naming it, so a host-owned install that lacks the
// index gets a plain insert rather than PostgreSQL's "no unique or exclusion
// constraint matching the ON CONFLICT specification". A targetless DO NOTHING is
// also what keeps a caller's transaction usable: a unique violation on PostgreSQL
// would abort it.
//
// RowsAffected is exact here, unlike conflictInsertChunk's: a single-struct create
// counts the rows its RETURNING clause yields, and a skipped insert yields none.
// TestInsertPeriodicReportsWhetherItLanded pins it on both dialects.
func insertPeriodic(ctx context.Context, db *gorm.DB, spec PeriodicSpec, now time.Time) (jobPeriodicRow, bool, error) {
	row, err := periodicRow(spec, now)
	if err != nil {
		return jobPeriodicRow{}, false, err
	}
	res := db.WithContext(ctx).Clauses(clause.OnConflict{DoNothing: true}).Create(&row)
	if res.Error != nil {
		return jobPeriodicRow{}, false, fmt.Errorf(
			"flywheel: insert periodic %q: %w", spec.Slug, models.WrapDBError(res.Error),
		)
	}
	if res.RowsAffected != 1 {
		return jobPeriodicRow{}, false, nil
	}
	return row, true, nil
}

// updatePeriodic reconciles an existing periodic row in place. It writes the
// spec's definition and Active, and never paused_at: the pause belongs to
// PausePeriodic and ResumePeriodic alone.
func updatePeriodic(ctx context.Context, db *gorm.DB, existing jobPeriodicRow, spec PeriodicSpec, now time.Time) error {
	queue, args := periodicQueueAndArgs(spec)
	updates := map[string]any{
		"kind":             spec.Kind,
		"queue":            queue,
		"args_template":    args,
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
//
// The active flag is the declaration's own switch: PeriodicSpec.Active writes
// it too, so a host that declares the definition in SchedulerConfig.Periodics
// re-applies its declared Active on its next start, undoing this call. To stop
// a declared definition at run time, through every restart, use PausePeriodic.
// The two are separate, and the definition fires only when it is active and not
// paused; this call neither pauses nor resumes it.
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

// PeriodicView is the public read projection of a periodic definition. A
// definition fires only when it is Active and not Paused.
type PeriodicView struct {
	Slug            string     `json:"slug"`
	Kind            string     `json:"kind"`
	Queue           string     `json:"queue"`
	Cron            string     `json:"cron,omitempty"`
	IntervalSeconds int        `json:"interval_seconds,omitempty"`
	NextRunAt       time.Time  `json:"next_run_at"`
	LastEnqueuedAt  *time.Time `json:"last_enqueued_at,omitempty"`
	Active          bool       `json:"active"`
	// Paused reports an operator's PausePeriodic, which holds through every
	// restart that re-declares the definition until ResumePeriodic.
	Paused bool `json:"paused"`
	// PausedAt is when the definition was paused; nil while it is not.
	PausedAt *time.Time `json:"paused_at,omitempty"`
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
		Active:         r.IsActive != nil && *r.IsActive,
		Paused:         r.PausedAt != nil,
		PausedAt:       r.PausedAt,
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
// guarded retry: a running job is refused with ErrJobRunning and a terminal job
// with ErrJobTerminal, no retry budget is restored, and the re-run is immediate —
// exactly what RetryJob issues.
type RetryOpts struct {
	// Force also retries a job that has already reached a terminal state —
	// including a succeeded one. Without it a terminal job is refused with
	// ErrJobTerminal, so an operator "retry now" can never silently re-run finished
	// work. Re-running a completed job is a legitimate action; it just has to be
	// deliberate.
	//
	// Force never retries a running job, nor a cancelled one whose attempt is still
	// running: a cancel fences a running attempt without awaiting it, so the
	// attempt's run stays started until its worker returns, and re-queueing the job
	// would start a second attempt beside the first. Both are refused with
	// ErrJobRunning unless AllowRunning is set too.
	Force bool
	// AllowRunning also re-queues a job whose attempt is still running: a running
	// job, or, with Force, a cancelled one whose attempt has not returned. That
	// attempt is not stopped. Its claim is cleared, so a runner can claim the job
	// again while it still runs, and its result is discarded when it returns (a
	// supersede): the job's work may run twice, at the same time. Set it when the
	// attempt's process is known to be gone — a cancelled job whose process died
	// before its attempt returned keeps a started run that nothing else clears — or
	// when a second, overlapping run is acceptable. Replay ignores it.
	AllowRunning bool
	// ResetAttempts restores the job's retry budget by raising max_attempts so the
	// job has Budget attempts remaining. It never lowers attempt: the attempt counter
	// is the audit key for job_runs and stays strictly monotonic, so the budget is
	// expressed as headroom rather than as a rewind — the same mechanism a snooze uses
	// to stay free. Without it a retry grants no new headroom, so a job discarded at
	// attempt == max_attempts is re-claimed exactly once and discarded again on that
	// attempt, which is rarely what "replay the failures" intends.
	ResetAttempts bool
	// Budget is the number of attempts to grant when ResetAttempts is set. A
	// positive value sets max_attempts to attempt + Budget. Zero (or negative)
	// grants the job's current max_attempts as headroom — max_attempts becomes
	// attempt + max_attempts — which is its original budget only for a job that
	// never snoozed: every snooze raised max_attempts by one, so a job that
	// snoozed n times gets n more attempts than it was enqueued with. Set Budget
	// to grant an exact number. It is ignored when ResetAttempts is false.
	Budget int
	// Delay, when > 0, schedules the job that far in the future instead of making it
	// immediately available. The claim gates on scheduled_at <= now in both drivers,
	// so a future scheduled_at defers the re-run without any state change.
	Delay time.Duration
}

// RetryJob returns a waiting job — available, retryable, scheduled, or paused — to
// available so a runner claims it on the next poll, clearing any lease and
// finalization. It is the operator action behind a "retry now".
//
// It is state-guarded to the same standard as CancelJob: a job that has already
// reached a terminal state (succeeded, cancelled, discarded) is left exactly as it
// is and ErrJobTerminal is returned, so a retry can never silently re-run finished
// work and clear the outcome that recorded it. A running job is left exactly as it
// is too, with its lease, and ErrJobRunning is returned: re-queueing it would let a
// runner start a second attempt while the first still runs, so its work would run
// twice at once. Use RetryJobWithOptions with Force to re-run a terminal job
// deliberately, and with AllowRunning to re-queue a running one anyway. It returns
// ErrJobNotFound when no live job has the id.
//
// This is a change from the earlier unguarded behavior, which returned any job —
// including a succeeded one — to available. The guard is the point.
func RetryJob(ctx context.Context, db *gorm.DB, id string) error {
	return RetryJobWithOptions(ctx, db, id, RetryOpts{})
}

// RetryJobWithOptions is RetryJob with explicit options. Force also retries a job
// that has already reached a terminal state; without it a terminal job returns
// ErrJobTerminal. A running job returns ErrJobRunning, and so, with Force, does a
// cancelled job whose attempt is still running, unless AllowRunning is set.
// ResetAttempts restores the job's retry budget by raising max_attempts (see
// RetryOpts.Budget) rather than rewinding attempt, so a job discarded at attempt
// == max_attempts gets real headroom and the job_runs audit sequence stays
// continuous. Delay schedules the re-run into the future instead of making the job
// immediately available. It returns ErrJobNotFound when no live job has the id.
// See RetryOpts.
//
// Under Force, a terminal job whose UniqueActiveKey a newer live job now holds
// cannot return to a live state: the index allows one live holder per key. The
// call then returns an error wrapping an *AlreadyEnqueuedError that names that
// job, and leaves the retried job as it was. The UPDATE runs in a transaction of
// its own — a savepoint when db is a caller's transaction, unless the host turned
// off GORM's nested transactions — so that refusal leaves a caller's PostgreSQL
// transaction usable, for the read that names the holder and for the caller's
// own writes after it.
func RetryJobWithOptions(ctx context.Context, db *gorm.DB, id string, opts RetryOpts) error {
	now := models.ClockFrom(ctx).Now(ctx)
	upd := map[string]any{
		"state":        string(StateAvailable),
		"leased_until": nil,
		// Under AllowRunning an attempt may still be running against this job.
		// Clearing its token is what stops that attempt finalizing over the re-run
		// the operator just asked for.
		"lease_token":  nil,
		"finalized_at": nil,
		// Delay 0 makes the job immediately claimable (scheduled_at == now); a
		// positive Delay defers the re-run, gated by the claim's scheduled_at <= now.
		"scheduled_at": now.Add(opts.Delay),
		"updated_at":   now,
	}
	applyRetryBudget(upd, opts)

	// A refused UPDATE aborts the PostgreSQL transaction it runs in. Running it in
	// a transaction of its own — GORM makes that a savepoint on a caller's
	// transaction handle — confines the abort to the UPDATE, so the holder read
	// below and the caller's own work after this call still run. Retry is an
	// operator action, so the extra statements cost nothing that matters.
	var affected int64
	err := db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// Scope the write to the states the options allow, naming them rather than
		// excluding the ones refused — the same guard CancelJob uses, and for the
		// same reason: a row in a state this runtime does not recognize is refused
		// rather than clobbered, so the guard can never be defeated by a state added
		// to the vocabulary but missed here.
		q := tx.Model(&jobRow{}).Where("id = ? AND state IN ?", id, retryStateStrings(opts))
		if opts.Force && !opts.AllowRunning {
			// A cancelled job is terminal by state while its attempt may still be
			// running; re-queueing it then would run its work twice at once.
			q = q.Where("NOT " + cancelledAttemptStillRuns("jobs"))
		}
		res := q.Updates(upd)
		affected = res.RowsAffected
		return res.Error
	})
	if err != nil {
		if isDuplicateKey(err) {
			return fmt.Errorf("flywheel: retry job %q: %w", id, retryHolder(ctx, db, id))
		}
		return fmt.Errorf("flywheel: retry job %q: %w", id, err)
	}
	if affected == 0 {
		return classifyRetryMiss(ctx, db, id, opts)
	}
	return nil
}

// retryStateStrings returns the states a retry with opts may move a job out of:
// the waiting states (available, retryable, scheduled, paused) always, the
// terminal states with Force, and running with AllowRunning.
func retryStateStrings(opts RetryOpts) []string {
	states := []JobState{StateAvailable, StateRetryable, StateScheduled, StatePaused}
	if opts.Force {
		states = append(states, TerminalStates()...)
	}
	if opts.AllowRunning {
		states = append(states, StateRunning)
	}
	return stateStrings(states)
}

// cancelledAttemptStillRuns is a correlated SQL predicate over the jobs row the
// query names outer: true when the job is cancelled while the attempt it was
// cancelled under is still running. A cancel fences a running attempt without
// awaiting it, and the attempt's run is recorded only when its worker returns, so
// until then the run at the job's attempt still reads started. It is one probe of
// job_runs_job_attempt. The retry under Force and the replay share it, so neither
// can start a second attempt beside one still running. The states are the
// runtime's own constants, so inlining them carries no injection risk.
func cancelledAttemptStillRuns(outer string) string {
	return "(" + outer + ".state = '" + string(StateCancelled) + "' AND EXISTS (SELECT 1 FROM job_runs AS r" +
		" WHERE r.job_id = " + outer + ".id AND r.attempt = " + outer + ".attempt" +
		" AND r.outcome = '" + string(OutcomeStarted) + "'))"
}

// retryHolder builds the collision error for a retry whose UPDATE hit a unique
// index. Only jobs_unique_active_key can reject it — the retry writes no
// unique_key — and only when the job was terminal and a newer live job holds its
// UniqueActiveKey, so the holder is read through the same predicate an insert's
// collision uses; the retried job, still terminal, cannot match it. Either read
// failing leaves the holder unnamed rather than masking the collision.
func retryHolder(ctx context.Context, db *gorm.DB, id string) *AlreadyEnqueuedError {
	var rows []jobRow
	err := db.WithContext(ctx).Model(&jobRow{}).Select("unique_active_key").
		Where("id = ?", id).Limit(1).Find(&rows).Error
	if err != nil || len(rows) == 0 || rows[0].UniqueActiveKey == nil {
		return &AlreadyEnqueuedError{}
	}
	return alreadyEnqueued(ctx, db, "", *rows[0].UniqueActiveKey)
}

// applyRetryBudget adds the max_attempts reset to a retry's column map when
// opts.ResetAttempts is set. The budget is restored as headroom above the current
// attempt — max_attempts = attempt + Budget, or attempt + max_attempts (the
// current max_attempts, which every snooze raised by one) when Budget is
// non-positive — applied in-SQL per row, mirroring the free-snooze path's
// gorm.Expr in jobFinalizeUpdate. attempt itself is never written, so it stays
// strictly monotonic as the job_runs(job_id, attempt) audit key and a replayed
// job's next run continues the sequence rather than colliding with it. It is shared by RetryJobWithOptions and the bulk replay engine, so the
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

// classifyRetryMiss explains why RetryJobWithOptions's UPDATE matched no row, as
// classifyCancelMiss does for CancelJob: the job does not exist (ErrJobNotFound);
// its attempt is still running — it is running, or, under Force without
// AllowRunning, it is cancelled with its attempt's run still started
// (ErrJobRunning); or it is in a state the options do not allow, which is a
// terminal one without Force (ErrJobTerminal).
//
// The read is scoped exactly like the UPDATE — Model(&jobRow{}) applies gorm's
// deleted_at IS NULL — so a soft-deleted job reads as missing rather than terminal.
// It runs outside a transaction for the same reason classifyCancelMiss does: the
// miss path writes nothing, and serializing an operator-scale diagnostic read would
// cost a write lock on SQLite for no correctness gain.
func classifyRetryMiss(ctx context.Context, db *gorm.DB, id string, opts RetryOpts) error {
	var rows []jobRow
	if err := db.WithContext(ctx).Model(&jobRow{}).Select("state", "attempt").
		Where("id = ?", id).Limit(1).Find(&rows).Error; err != nil {
		return fmt.Errorf("flywheel: retry job %q: %w", id, err)
	}
	if len(rows) == 0 {
		return ErrJobNotFound
	}
	switch JobState(rows[0].State) {
	case StateRunning:
		return ErrJobRunning
	case StateCancelled:
		if !opts.Force || opts.AllowRunning {
			break
		}
		var started int64
		if err := db.WithContext(ctx).Model(&jobRunRow{}).
			Where("job_id = ? AND attempt = ? AND outcome = ?", id, rows[0].Attempt, string(OutcomeStarted)).
			Count(&started).Error; err != nil {
			return fmt.Errorf("flywheel: retry job %q: %w", id, err)
		}
		if started > 0 {
			return ErrJobRunning
		}
	}
	return ErrJobTerminal
}

// CancelJob moves a still-in-flight job to the terminal cancelled state. The job
// will not be retried or re-claimed, and an attempt already running is fenced:
// its result is discarded when it returns. That attempt is not interrupted
// unless its Runner sets RunnerConfig.CooperativeCancel, in which case its
// context is cancelled with the cause ErrJobCancelled within one heartbeat, for
// its worker to stop on. Until the attempt returns, the job reads cancelled
// (FindJob) while its run at the job's attempt still reads outcome started
// (LatestRun, ListRuns): that is a cancel still waiting on a running attempt.
//
// When the job is a child whose parent declared a Barrier and it was the last of
// its generation still pending, the cancel enqueues the barrier's continuation, in
// the same transaction, as a finalize that ended the child would have. A running
// child's cancel fires it at once: the child is terminal, and its attempt's late
// finalize is superseded and fires nothing. The continuation can therefore start
// while the cancelled attempt's worker is still running, since a cancel fences an
// attempt's result and does not wait for its worker to stop.
//
// A job that has already reached a terminal state (succeeded, cancelled, or
// discarded) is left exactly as it is and ErrJobTerminal is returned: cancelling
// must never overwrite a recorded outcome or its finalized_at stamp. It returns
// ErrJobNotFound when no live job has the id.
//
// The update and the barrier check run in a transaction of their own — a
// savepoint when db is a caller's transaction, unless the host turned off GORM's
// nested transactions.
func CancelJob(ctx context.Context, db *gorm.DB, id string) error {
	now := models.ClockFrom(ctx).Now(ctx)
	var affected int64
	err := db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// Scope the write to the states a job can still be cancelled from. Naming
		// the allowed states rather than excluding the terminal ones is deliberate:
		// a row in a state this runtime does not recognize is refused rather than
		// clobbered, so the guard can never be defeated by a state added to the
		// vocabulary but missed here.
		res := tx.Model(&jobRow{}).
			Where("id = ? AND state IN ?", id, nonTerminalStateStrings()).
			Updates(map[string]any{
				"state":        string(StateCancelled),
				"leased_until": nil,
				// The running attempt is not interrupted here, but its claim is:
				// clearing the token is what keeps its finalize from overwriting the
				// cancel, and what a cooperative Runner's heartbeat notices.
				"lease_token":  nil,
				"finalized_at": now,
				"updated_at":   now,
			})
		if res.Error != nil {
			return res.Error
		}
		affected = res.RowsAffected
		if affected == 0 {
			return nil
		}
		var row jobRow
		if err := tx.Model(&jobRow{}).Select("parent_job_id").Where("id = ?", id).First(&row).Error; err != nil {
			return fmt.Errorf("read parent: %w", err)
		}
		if row.ParentJobID == nil {
			return nil
		}
		_, err := fireBarrierIfComplete(ctx, tx, *row.ParentJobID)
		return err
	})
	if err != nil {
		return fmt.Errorf("flywheel: cancel job %q: %w", id, err)
	}
	if affected == 0 {
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
//
// For a replay, Changed + SkippedTerminal + SkippedRunning + SkippedActiveKey is
// every in-scope job that was terminal or running when the replay began, unless a
// concurrent operation moved one mid-replay. A FailedSince window leaves the
// targeted jobs finalized before it out of the sum.
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
	// or cancel applies to its next claim. A replay that targets cancelled jobs
	// also counts here each one whose attempt is still running, which it leaves
	// cancelled.
	SkippedRunning int64
	// SkippedActiveKey is set by a replay alone; the batch controls leave it zero. It
	// is the number of targeted jobs left terminal because another job held their
	// UniqueActiveKey live when their batch ran: a job outside the cohort, or one the
	// replay itself returned to available, earlier in the same batch or in an
	// earlier one. The index allows one live job per key at a time, so of the
	// targeted jobs in a batch that share a key, the first in id order is the one
	// replayed.
	SkippedActiveKey int64
	// Batches is the number of transactions that changed rows. A batch that changed
	// none is not counted: one whose rows a concurrent operation moved first, or a
	// replay batch whose jobs were all left terminal for their key.
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
// moves children out of, the columns it writes, and whether the move can end the
// parent's generation. The three operations differ only in these and share
// scopeByParent's batched, guarded loop.
type scopeTransition struct {
	op      string
	source  []string
	updates func(now time.Time) map[string]any
	// fireBarrier runs the parent's barrier check after each batch that changed a
	// child. Only a transition into a terminal state sets it: the cancel.
	fireBarrier bool
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
// When the parent declared a Barrier and a batch leaves no child pending, that
// batch enqueues the barrier's continuation in its own transaction, as a finalize
// that ended the last child would have, and the call returns: the continuation is
// now the parent's one pending child, and the call never cancels the
// continuation it enqueued. With a child still running, the continuation fires
// when that child finishes. A later CancelByParent on the same parent cancels a
// still-pending continuation like any other pending child.
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
		fireBarrier: true,
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
		selected, changed, fired, err := scopeBatch(ctx, db, parentJobID, t, batchSize, now)
		if err != nil {
			return result, fmt.Errorf("flywheel: %s by parent: %w", t.op, err)
		}
		result.Changed += changed
		if changed > 0 {
			result.Batches++
		}
		// A batch that fired the parent's barrier left no child pending but the
		// continuation it enqueued, an available child of this parent. Another
		// batch, which a full batch would otherwise run, would select and cancel it.
		if fired || selected < batchSize {
			return result, nil
		}
	}
}

// scopeBatch moves one bounded batch of a parent's source-state children to the
// target inside a single transaction. It returns how many rows it selected (which
// drives the loop's termination), how many it actually changed, and, for a
// transition that runs the barrier check, whether the batch fired the parent's
// barrier.
//
// The UPDATE re-guards on the source states — WHERE id IN ? AND state IN ? — rather
// than trusting the ids the SELECT found: without SKIP LOCKED a concurrent operation
// could move one of them between the SELECT and the UPDATE, and an unguarded UPDATE
// would resurrect it out of the state that concurrent operation left it in. The
// re-guard makes the batch move only children still eligible, so Changed counts real
// transitions and no terminal row is ever revived.
func scopeBatch(
	ctx context.Context, db *gorm.DB, parentJobID string, t scopeTransition, batchSize int, now time.Time,
) (selected int, changed int64, fired bool, err error) {
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
		if !t.fireBarrier || changed == 0 {
			return nil
		}
		var e error
		fired, e = fireBarrierIfComplete(ctx, tx, parentJobID)
		return e
	})
	return selected, changed, fired, err
}
