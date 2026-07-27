package flywheel

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"
	"unicode/utf8"

	"github.com/mrz1836/go-foundation/ctxutil"
	"github.com/mrz1836/go-foundation/models"
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
	// Dequeue atomically claims up to limit ready jobs from the given queues for
	// the given executor class, leasing each for the lease duration. Unless
	// claimAny is set, it claims only jobs whose executor_class equals class or is
	// the empty wildcard. A claimed job has its state advanced to running and its
	// attempt incremented.
	Dequeue(ctx context.Context, queues []string, class ExecutorClass, claimAny bool, limit int, lease time.Duration) ([]RawJob, error)

	// InsertRunStub commits a job_runs row with outcome started before the
	// worker runs, so a side-effect FK to runID resolves through a crash. class
	// is recorded as the run's executor_class.
	InsertRunStub(ctx context.Context, runID string, raw RawJob, startedAt time.Time, class ExecutorClass, execID string) error

	// Finalize runs, in one transaction, the run-row outcome update, the
	// jobs.state advance, and any follow-up inserts. A follow-up colliding with
	// an existing unique_key is skipped, not fatal.
	Finalize(ctx context.Context, raw RawJob, runID string, result Result, workErr error, finishedAt time.Time) error

	// RenewLease extends a claimed job's lease, scoped to the claim that holds
	// it. It reports whether the claim is still held: false means the claim was
	// superseded — swept, cancelled, or otherwise released — and the caller must
	// stop renewing.
	//
	// It never returns an error for a lost claim. Losing a claim is an outcome,
	// not a failure: it is the ordinary consequence of a lease expiring, and a
	// caller that had to distinguish it from a database error by inspecting one
	// would get it wrong.
	RenewLease(ctx context.Context, jobID, leaseToken string, until time.Time) (held bool, err error)

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
// A snooze is free: it must never advance the job toward discarded, because a
// worker that defers its own work has not failed. It is made free by raising
// max_attempts by one rather than by decrementing attempt — attempt is the
// dequeue counter and also the JobRun audit key (the job_runs(job_id, attempt)
// unique index), so it must stay strictly monotonic. Raising max_attempts
// preserves the retry headroom (max_attempts - attempt) exactly, which is the
// observable guarantee.
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
		outcome := OutcomeError
		if class == ErrorTimeout {
			outcome = OutcomeTimeout
		}
		out := finalizeOutcome{runOutcome: outcome, errorClass: &class}
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

// expBackoff is the exponential retry ladder shared by the Runner's
// configurable backoff and the driver's fallback: base, doubling once per
// attempt past the first, capped at maxDelay.
func expBackoff(base, maxDelay time.Duration, attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	delay := base
	for range attempt - 1 {
		delay *= 2
		if delay >= maxDelay {
			return maxDelay
		}
	}
	return delay
}

// defaultBackoff is the fallback retry delay when the Runner supplied none.
func defaultBackoff(attempt int) time.Duration {
	return expBackoff(time.Second, time.Minute, attempt)
}

// claimableStates are the job states Dequeue may claim from.
var claimableStates = []string{ //nolint:gochecknoglobals // intentional shared constant slice
	string(StateAvailable), string(StateRetryable), string(StateScheduled),
}

// rawFromRow converts a claimed jobs row into a RawJob with the given attempt
// and lease token.
//
// Both are parameters rather than fields read off r because neither dialect
// hands them back: SQLite converts the row it selected *before* the claim
// updated it, and the Postgres claim deliberately does not name lease_token in
// its RETURNING list — the caller minted the token, so returning it would be
// dead weight in the one statement on the hot path.
func rawFromRow(r jobRow, attempt int, leaseToken string) (RawJob, error) {
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
		TimeoutMs:   r.TimeoutMs,
		LeaseToken:  leaseToken,
		ParentJobID: r.ParentJobID,
		Tags:        tags,
		ScheduledAt: r.ScheduledAt,
		Metadata:    []byte(r.Metadata),
	}, nil
}

// truncate caps s at n bytes, cutting on a rune boundary so a multi-byte rune is
// never split. A raw byte slice would split a straddling rune and leave invalid
// UTF-8 in the stored error message (corrupting the audit trail, and failing the
// text insert outright on Postgres); backing the cut off to the previous rune
// start keeps the stored prefix valid.
func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	for n > 0 && !utf8.RuneStart(s[n]) {
		n--
	}
	return s[:n]
}

// baseDriver holds the Driver methods shared by the Postgres and SQLite
// implementations — only Dequeue differs by dialect.
type baseDriver struct {
	db *gorm.DB
}

// InsertRunStub commits a job_runs row with outcome started before the worker
// runs. Committing first is what makes job_runs.id a usable foreign-key target:
// a side-effect row the worker writes during the attempt can reference it, and
// the reference survives a crash — the sweep marks the stub crashed rather than
// deleting it.
func (d *baseDriver) InsertRunStub(
	ctx context.Context, runID string, raw RawJob, startedAt time.Time, class ExecutorClass, execID string,
) error {
	row := jobRunRow{
		ID:            runID,
		JobID:         raw.ID,
		Attempt:       raw.Attempt,
		ExecutorClass: string(class),
		ExecutorID:    execID,
		StartedAt:     startedAt,
		Outcome:       string(OutcomeStarted),
		CreatedAt:     startedAt,
	}
	if err := d.db.WithContext(ctx).Create(&row).Error; err != nil {
		return fmt.Errorf("jobs: insert run stub: %w", models.WrapDBError(err))
	}
	return nil
}

// jobFinalizeUpdate is the jobs-row column set for a finalization.
//
// The lease token is cleared alongside leased_until on every transition, because
// every transition a finalization can make leaves the running state — even a
// snooze or a retry, which return the job to the pool for a fresh claim to take
// under a fresh token.
func jobFinalizeUpdate(plan finalizeOutcome, finishedAt time.Time) map[string]any {
	upd := map[string]any{
		"state":        string(plan.jobState),
		"updated_at":   finishedAt,
		"leased_until": nil,
		"lease_token":  nil,
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
		message, payload, err := runErrorFields(workErr.Error())
		if err != nil {
			return nil, err
		}
		upd["error_message"] = message
		upd["error_payload"] = payload
	}
	if result.Output != nil {
		out, err := marshalRunOutput(result.Output)
		if err != nil {
			return nil, err
		}
		upd["output"] = out
	}
	return upd, nil
}

// runErrorFields renders a raw error message into the pair of job_runs columns
// that record it: the truncated error_message and its error_payload JSON. It is
// shared by the runtime's own finalize path and SeedRun, so a seeded row's error
// is byte-identical in shape to one a real failed attempt produced — same cap,
// same rune-safe cut, same payload key.
func runErrorFields(message string) (string, datatypes.JSON, error) {
	message = truncate(message, maxErrorMessage)
	payload, err := json.Marshal(map[string]string{"message": message})
	if err != nil {
		return "", nil, fmt.Errorf("jobs: marshal error payload: %w", err)
	}
	return message, datatypes.JSON(payload), nil
}

// marshalRunOutput renders a worker's structured output into the job_runs.output
// payload. A nil output stores no value, leaving the column NULL. It is shared
// by the finalize path and SeedRun so both land the same bytes in the column
// ListRuns reads back.
func marshalRunOutput(output any) (datatypes.JSON, error) {
	if output == nil {
		return nil, nil
	}
	out, err := json.Marshal(output)
	if err != nil {
		return nil, fmt.Errorf("jobs: marshal output: %w", err)
	}
	return datatypes.JSON(out), nil
}

// Finalize applies one attempt's outcome — the run-row update, the jobs.state
// advance, and any follow-up inserts — in a single transaction, so a crash
// between the three cannot leave a job succeeded with its children unenqueued or
// its audit row still reading started.
//
//nolint:gocognit // one transaction closure with three cohesive steps
func (d *baseDriver) Finalize(
	ctx context.Context, raw RawJob, runID string, result Result, workErr error, finishedAt time.Time,
) error {
	plan := planFinalize(raw, result, workErr, finishedAt)

	// A recorded outcome must survive shutdown: a drain or cancel that cancels ctx
	// mid-finalize would otherwise roll back the worker's result. WithoutCancel
	// detaches the finalize transaction from ctx's cancellation while preserving
	// its values, so the clock and request id InsertChild reads still resolve.
	txCtx := context.WithoutCancel(ctx)

	err := d.db.WithContext(txCtx).Transaction(func(tx *gorm.DB) error {
		var stub jobRunRow
		if err := tx.Model(&jobRunRow{}).
			Select("started_at").Where("id = ?", runID).First(&stub).Error; err != nil {
			return fmt.Errorf("jobs: load run stub: %w", err)
		}
		durationMs := int(finishedAt.Sub(stub.StartedAt).Milliseconds())

		// Scope the state advance to the claim this attempt still holds. A
		// concurrent CancelJob or a lease-sweep reclaim moves the job out of
		// running and supersedes this finalize, so the UPDATE matches no row. We
		// honor that — no state advance, no follow-up enqueue — but still write the
		// job_runs audit row so the attempt is not lost, then return nil.
		//
		// The token is what makes the guard mean "this job is running under my
		// claim" rather than "this job is running". Without it a reclaimed job is
		// running again under a *different* attempt, the original attempt's
		// finalize still matches, and whichever attempt finishes first wins
		// regardless of which one holds the lease.
		res := tx.Model(&jobRow{}).
			Where("id = ? AND state = ? AND lease_token = ?", raw.ID, string(StateRunning), raw.LeaseToken).
			Updates(jobFinalizeUpdate(plan, finishedAt))
		if res.Error != nil {
			return fmt.Errorf("jobs: advance job state: %w", res.Error)
		}
		superseded := res.RowsAffected == 0

		enqueued := 0
		if plan.followUps && !superseded {
			var followErr error
			if enqueued, followErr = d.insertFollowUps(txCtx, tx, result.FollowUps, raw.ID); followErr != nil {
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

// RenewLease extends a claimed job's lease for as long as the claim still holds
// it. There is no dialect split: it is a single guarded UPDATE, and both drivers
// express it identically.
//
// The guard is the same one Finalize uses, for the same reason — an attempt that
// no longer holds the claim must not extend a lease the next claim is relying
// on. That makes a lost claim indistinguishable from a lost race here, which is
// correct: both mean "stop renewing".
//
// until is an absolute expiry rather than an extension, so a caller that renews
// to now+lease cannot bank an ever-growing lease out of a stalled heartbeat.
func (d *baseDriver) RenewLease(
	ctx context.Context, jobID, leaseToken string, until time.Time,
) (bool, error) {
	res := d.db.WithContext(ctx).Model(&jobRow{}).
		Where("id = ? AND state = ? AND lease_token = ?", jobID, string(StateRunning), leaseToken).
		Updates(map[string]any{
			"leased_until": until,
			"updated_at":   models.ClockFrom(ctx).Now(ctx),
		})
	if res.Error != nil {
		return false, fmt.Errorf("jobs: renew lease: %w", res.Error)
	}
	return res.RowsAffected == 1, nil
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
	now := models.ClockFrom(ctx).Now(ctx)
	row := jobRow{
		ID:            models.NewID(),
		CreatedAt:     now,
		UpdatedAt:     now,
		Metadata:      datatypes.JSON(ctxutil.RequestIDToMetadata(nil, ctxutil.RequestIDFrom(ctx))),
		Kind:          fu.Kind,
		Queue:         orString(fu.Queue, defaultQueue),
		Args:          datatypes.JSON(payload),
		Priority:      orInt(fu.Priority, defaultPriority),
		State:         string(StateAvailable),
		MaxAttempts:   defaultMaxAttempts,
		ScheduledAt:   now,
		ExecutorClass: string(fu.ExecutorClass),
		Tags:          datatypes.JSON("[]"),
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
		wrapped := models.WrapDBError(createErr)
		if errors.Is(wrapped, models.ErrDuplicateKey) {
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
			// Clearing the token is what makes the reclaim final: the attempt whose
			// lease just expired can no longer finalize over the next claim, and it
			// cannot renew the lease it no longer holds either.
			"lease_token": nil,
			"updated_at":  now,
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

// insertFollowUps inserts a worker's follow-up jobs on tx and reports how many
// were enqueued. A unique_key collision is skipped rather than fatal: the child
// is already enqueued, so the parent's finalization has nothing to correct and
// must not be rolled back over it.
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
