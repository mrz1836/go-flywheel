package flywheel

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"gorm.io/datatypes"
	"gorm.io/gorm"
)

// maxErrorMessage is the byte cap on a stored JobRun.error_message.
const maxErrorMessage = 4096

// Driver is the database seam the Runner and Scheduler reach through. It has
// two implementations — driver_postgres.go (FOR UPDATE SKIP LOCKED) and
// driver_sqlite.go (BEGIN IMMEDIATE + serialized claim) — so the runtime code
// above it never sees the SQL dialect.
type Driver interface {
	// Dequeue atomically claims up to limit ready jobs from the given queues
	// for an executor of the given kind, leasing each for the lease duration.
	// A claimed job has its state advanced to running and its attempt
	// incremented.
	Dequeue(ctx context.Context, queues []string, kind ExecutorKind, limit int, lease time.Duration) ([]RawJob, error)

	// InsertRunStub commits a job_runs row with outcome started before the
	// worker runs, so a side-effect FK to runID resolves through a crash.
	InsertRunStub(ctx context.Context, runID string, raw RawJob, startedAt time.Time, kind ExecutorKind, execID string) error

	// Finalize runs, in one transaction, the run-row outcome update, the
	// jobs.state advance, and any follow-up inserts. A follow-up colliding with
	// an existing unique_key is skipped, not fatal.
	Finalize(ctx context.Context, raw RawJob, runID string, result Result, workErr error, finishedAt time.Time) error

	// InsertChild writes one follow-up job on tx, skipping a unique_key
	// collision without error.
	InsertChild(ctx context.Context, tx *gorm.DB, fu FollowUp, parentID string) error

	// Sweep reclaims jobs whose lease has expired (state running, leased_until
	// in the past), returning them to available and marking each stale run stub
	// crashed. It reports how many jobs were reclaimed.
	Sweep(ctx context.Context, now time.Time) (reclaimed int, err error)
}

// classifiedError is how the Runner hands the driver its verdict on a failed
// attempt without widening the Driver.Finalize signature: the Runner wraps the
// worker error with the error class it computed and the retry delay it chose
// (worker NextRetry override or config-driven backoff).
type classifiedError struct {
	cause      error
	class      ErrorClass
	retryDelay time.Duration
}

// Error returns the underlying cause's message.
func (e *classifiedError) Error() string { return e.cause.Error() }

// Unwrap exposes the cause for errors.Is/As.
func (e *classifiedError) Unwrap() error { return e.cause }

// finalizeOutcome is the state-machine decision for one finalization.
type finalizeOutcome struct {
	jobState         JobState
	runOutcome       RunOutcome
	scheduledAt      *time.Time
	finalizedAt      *time.Time
	maxAttemptsDelta int
	errorClass       *ErrorClass
	followUps        bool
}

// planFinalize maps an attempt's result and error onto the job state machine.
// Cancellation and snooze take precedence over an error.
//
// A snooze is free (research §6): it must never advance the job toward
// discarded. It is made free by raising max_attempts by one rather than by
// decrementing attempt — attempt is the dequeue counter and also the JobRun
// audit key (the job_runs(job_id, attempt) unique index, FR-020), so it must
// stay strictly monotonic. Raising max_attempts preserves the retry headroom
// (max_attempts - attempt) exactly, which is the observable guarantee.
//
//nolint:gocognit // one switch over the four mutually exclusive outcomes
func planFinalize(raw RawJob, result Result, workErr error, finishedAt time.Time) finalizeOutcome {
	switch {
	case result.Cancel:
		return finalizeOutcome{
			jobState: StateCancelled, runOutcome: OutcomeCancelled, finalizedAt: &finishedAt,
		}
	case result.Snooze != nil:
		when := finishedAt.Add(*result.Snooze)
		return finalizeOutcome{
			jobState: StateScheduled, runOutcome: OutcomeSnooze,
			scheduledAt: &when, maxAttemptsDelta: 1,
		}
	case workErr != nil:
		class := ErrorTransient
		delay := defaultBackoff(raw.Attempt)
		var ce *classifiedError
		if errors.As(workErr, &ce) {
			if ce.class != "" {
				class = ce.class
			}
			if ce.retryDelay > 0 {
				delay = ce.retryDelay
			}
		}
		out := finalizeOutcome{runOutcome: OutcomeError, errorClass: &class}
		permanent := class == ErrorPermanent || class == ErrorValidation
		if permanent || raw.Attempt >= raw.MaxAttempts {
			out.jobState = StateDiscarded
			out.finalizedAt = &finishedAt
		} else {
			when := finishedAt.Add(delay)
			out.jobState = StateRetryable
			out.scheduledAt = &when
		}
		return out
	default:
		return finalizeOutcome{
			jobState: StateSucceeded, runOutcome: OutcomeSuccess,
			finalizedAt: &finishedAt, followUps: true,
		}
	}
}

// defaultBackoff is the fallback retry delay when the Runner supplied none.
func defaultBackoff(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	d := time.Second
	for range attempt - 1 {
		d *= 2
		if d >= time.Minute {
			return time.Minute
		}
	}
	return d
}

// runOnValues is the set of run_on values an executor of the given kind may
// claim. A local executor is unrestricted (nil).
func runOnValues(kind ExecutorKind) []string {
	switch kind {
	case ExecutorLambda:
		return []string{string(RunOnLambda), string(RunOnEither)}
	case ExecutorECS:
		return []string{string(RunOnECS), string(RunOnEither)}
	case ExecutorLocal:
		return nil
	default:
		return nil
	}
}

// claimableStates are the job states Dequeue may claim from.
var claimableStates = []string{ //nolint:gochecknoglobals // intentional shared constant slice
	string(StateAvailable), string(StateRetryable), string(StateScheduled),
}

// rawFromRow converts a claimed jobs row into a RawJob with the given attempt.
func rawFromRow(r jobRow, attempt int) (RawJob, error) {
	var tags []string
	if len(r.Tags) > 0 {
		if err := json.Unmarshal(r.Tags, &tags); err != nil {
			return RawJob{}, fmt.Errorf("jobs: decode tags: %w", err)
		}
	}
	return RawJob{
		ID:          r.ID,
		Kind:        r.Kind,
		Queue:       r.Queue,
		Args:        []byte(r.Args),
		Attempt:     attempt,
		MaxAttempts: r.MaxAttempts,
		ParentJobID: r.ParentJobID,
		Tags:        tags,
		ScheduledAt: r.ScheduledAt,
		Metadata:    []byte(r.Metadata),
	}, nil
}

// truncate caps s at n bytes.
func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}

// baseDriver holds the Driver methods shared by the Postgres and SQLite
// implementations — only Dequeue differs by dialect.
type baseDriver struct {
	db *gorm.DB
}

// InsertRunStub commits a job_runs row with outcome started before the worker
// runs (research §8).
func (d *baseDriver) InsertRunStub(
	ctx context.Context, runID string, raw RawJob, startedAt time.Time, kind ExecutorKind, execID string,
) error {
	row := jobRunRow{
		ID:           runID,
		JobID:        raw.ID,
		Attempt:      raw.Attempt,
		ExecutorKind: string(kind),
		ExecutorID:   execID,
		StartedAt:    startedAt,
		Outcome:      string(OutcomeStarted),
		CreatedAt:    startedAt,
	}
	if err := d.db.WithContext(ctx).Create(&row).Error; err != nil {
		return fmt.Errorf("jobs: insert run stub: %w", WrapDBError(err))
	}
	return nil
}

// jobFinalizeUpdate is the jobs-row column set for a finalization.
func jobFinalizeUpdate(plan finalizeOutcome, finishedAt time.Time) map[string]any {
	upd := map[string]any{
		"state":        string(plan.jobState),
		"updated_at":   finishedAt,
		"leased_until": nil,
	}
	if plan.scheduledAt != nil {
		upd["scheduled_at"] = *plan.scheduledAt
	}
	if plan.finalizedAt != nil {
		upd["finalized_at"] = *plan.finalizedAt
	}
	if plan.maxAttemptsDelta != 0 {
		upd["max_attempts"] = gorm.Expr("max_attempts + ?", plan.maxAttemptsDelta)
	}
	return upd
}

// runFinalizeUpdate is the job_runs-row column set for a finalization.
func runFinalizeUpdate(
	plan finalizeOutcome, result Result, workErr error, finishedAt time.Time, durationMs, enqueued int,
) (map[string]any, error) {
	upd := map[string]any{
		"outcome":           string(plan.runOutcome),
		"finished_at":       finishedAt,
		"duration_ms":       durationMs,
		"cost_micros":       result.CostMicros,
		"enqueued_children": enqueued,
	}
	if plan.errorClass != nil {
		upd["error_class"] = string(*plan.errorClass)
	}
	if workErr != nil {
		message := truncate(workErr.Error(), maxErrorMessage)
		upd["error_message"] = message
		payload, err := json.Marshal(map[string]string{"message": message})
		if err != nil {
			return nil, fmt.Errorf("jobs: marshal error payload: %w", err)
		}
		upd["error_payload"] = datatypes.JSON(payload)
	}
	if result.Output != nil {
		out, err := json.Marshal(result.Output)
		if err != nil {
			return nil, fmt.Errorf("jobs: marshal output: %w", err)
		}
		upd["output"] = datatypes.JSON(out)
	}
	return upd, nil
}

// Finalize applies one attempt's outcome — the run-row update, the jobs.state
// advance, and any follow-up inserts — in a single transaction (FR-015).
//
//nolint:gocognit // one transaction closure with three cohesive steps
func (d *baseDriver) Finalize(
	ctx context.Context, raw RawJob, runID string, result Result, workErr error, finishedAt time.Time,
) error {
	plan := planFinalize(raw, result, workErr, finishedAt)

	err := d.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var stub jobRunRow
		if err := tx.Model(&jobRunRow{}).
			Select("started_at").Where("id = ?", runID).First(&stub).Error; err != nil {
			return fmt.Errorf("jobs: load run stub: %w", err)
		}
		durationMs := int(finishedAt.Sub(stub.StartedAt).Milliseconds())

		if err := tx.Model(&jobRow{}).Where("id = ?", raw.ID).
			Updates(jobFinalizeUpdate(plan, finishedAt)).Error; err != nil {
			return fmt.Errorf("jobs: advance job state: %w", err)
		}

		enqueued := 0
		if plan.followUps {
			var followErr error
			if enqueued, followErr = d.insertFollowUps(ctx, tx, result.FollowUps, raw.ID); followErr != nil {
				return followErr
			}
		}

		runUpd, err := runFinalizeUpdate(plan, result, workErr, finishedAt, durationMs, enqueued)
		if err != nil {
			return err
		}
		if err := tx.Model(&jobRunRow{}).Where("id = ?", runID).Updates(runUpd).Error; err != nil {
			return fmt.Errorf("jobs: update run row: %w", err)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("jobs: finalize: %w", err)
	}
	return nil
}

// InsertChild writes one follow-up job on tx. A unique_key collision is
// surfaced as ErrAlreadyEnqueued so Finalize can skip it without aborting.
func (d *baseDriver) InsertChild(
	ctx context.Context, tx *gorm.DB, fu FollowUp, parentID string,
) error {
	payload, err := json.Marshal(fu.Args)
	if err != nil {
		return fmt.Errorf("jobs: marshal child args: %w", err)
	}
	now := ClockFrom(ctx).Now(ctx)
	row := jobRow{
		ID:          NewID(),
		CreatedAt:   now,
		UpdatedAt:   now,
		Metadata:    datatypes.JSON(metadataWithRequestID(nil, RequestIDFrom(ctx))),
		Kind:        fu.Kind,
		Queue:       orString(fu.Queue, defaultQueue),
		Args:        datatypes.JSON(payload),
		Priority:    orInt(fu.Priority, defaultPriority),
		State:       string(StateAvailable),
		MaxAttempts: defaultMaxAttempts,
		ScheduledAt: now,
		RunOn:       string(RunOnEither),
		Tags:        datatypes.JSON("[]"),
	}
	if fu.ScheduleAt != nil {
		row.ScheduledAt = *fu.ScheduleAt
	}
	if fu.UniqueKey != "" {
		uk := fu.UniqueKey
		row.UniqueKey = &uk
	}
	if fu.Parent {
		pid := parentID
		row.ParentJobID = &pid
	}
	if createErr := tx.WithContext(ctx).Create(&row).Error; createErr != nil {
		wrapped := WrapDBError(createErr)
		if errors.Is(wrapped, ErrDuplicateKey) {
			return ErrAlreadyEnqueued
		}
		return fmt.Errorf("jobs: insert child: %w", wrapped)
	}
	return nil
}

// Sweep reclaims expired-lease jobs and marks their stale run stubs crashed.
func (d *baseDriver) Sweep(ctx context.Context, now time.Time) (int, error) {
	reclaimed := 0
	err := d.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var ids []string
		if err := tx.Model(&jobRow{}).
			Where("state = ? AND leased_until < ?", string(StateRunning), now).
			Pluck("id", &ids).Error; err != nil {
			return fmt.Errorf("jobs: find expired leases: %w", err)
		}
		if len(ids) == 0 {
			return nil
		}
		if err := tx.Model(&jobRow{}).Where("id IN ?", ids).Updates(map[string]any{
			"state":        string(StateAvailable),
			"leased_until": nil,
			"updated_at":   now,
		}).Error; err != nil {
			return fmt.Errorf("jobs: reclaim jobs: %w", err)
		}
		if err := tx.Model(&jobRunRow{}).
			Where("job_id IN ? AND outcome = ?", ids, string(OutcomeStarted)).
			Updates(map[string]any{
				"outcome":     string(OutcomeCrashed),
				"finished_at": now,
			}).Error; err != nil {
			return fmt.Errorf("jobs: crash stale stubs: %w", err)
		}
		reclaimed = len(ids)
		return nil
	})
	if err != nil {
		return 0, fmt.Errorf("jobs: sweep: %w", err)
	}
	return reclaimed, nil
}

// insertFollowUps inserts a worker's follow-up jobs on tx, skipping unique_key
// collisions (FR-016), and reports how many were enqueued.
func (d *baseDriver) insertFollowUps(
	ctx context.Context, tx *gorm.DB, followUps []FollowUp, parentID string,
) (int, error) {
	enqueued := 0
	for _, fu := range followUps {
		err := d.InsertChild(ctx, tx, fu, parentID)
		if errors.Is(err, ErrAlreadyEnqueued) {
			continue
		}
		if err != nil {
			return enqueued, err
		}
		enqueued++
	}
	return enqueued, nil
}
