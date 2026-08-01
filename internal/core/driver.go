package core

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"
	"unicode/utf8"

	"github.com/mrz1836/go-foundation/backoff"
	"github.com/mrz1836/go-foundation/ctxutil"
	"github.com/mrz1836/go-foundation/models"
	"gorm.io/datatypes"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
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
	//
	// It reports what it actually persisted, which is not always what the
	// attempt's result implied: an attempt whose claim was superseded has its
	// audit row written and nothing else. The caller is expected to report the
	// returned outcome rather than re-deriving one from the result.
	Finalize(
		ctx context.Context, raw RawJob, runID string, result Result, workErr error, finishedAt time.Time,
	) (FinalizeOutcome, error)

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
	// crashed, in bounded batches. It reports how many jobs were reclaimed in
	// total across every batch.
	//
	// Each batch is its own transaction, so a deep backlog never becomes one
	// long lock-holding statement — the case that arises exactly when the system
	// is already in trouble, because a backlog that deep is what an executor
	// pool leaves when it dies holding its whole lease set.
	//
	// It loops until the backlog is drained or ctx is cancelled, and reports the
	// count reclaimed so far alongside the context error. Completed batches are
	// committed and are not rolled back, so a cancelled sweep is partial
	// progress rather than none. The batch in flight when the cancel arrives
	// rolls back whole — its jobs stay running and the next sweep collects them,
	// which makes an interrupted sweep work redone rather than work lost.
	//
	// One Sweep does not guarantee a drained backlog. On PostgreSQL a batch is
	// taken with FOR UPDATE SKIP LOCKED, so a short batch may mean a concurrent
	// sweeper holds the rest rather than that none remain. The next sweep takes
	// them.
	Sweep(ctx context.Context, now time.Time) (reclaimed int, err error)
}

// defaultSweepBatchSize is the number of expired leases reclaimed per
// transaction when DriverOpts leaves SweepBatchSize zero.
//
// It is a bound, not a tuning knob to switch off: there is no value of
// SweepBatchSize, zero or negative, that produces an unbounded transaction.
const defaultSweepBatchSize = 1000

// Follow-up fan-out bounds applied when DriverOpts leaves them zero. A worker's
// Result.FollowUps is inserted inside the finalize transaction, so an unbounded
// fan-out is an unbounded, lock-holding transaction: the insert is chunked, and
// the total is capped.
const (
	// defaultFollowUpChunk is the number of child rows per INSERT during finalize.
	// Like defaultSweepBatchSize it is a bound, not a knob to switch off.
	defaultFollowUpChunk = 500
	// defaultFollowUpLimit is the maximum children one attempt may enqueue. A
	// larger fan-out is a design error: spawn a fan-out coordinator job instead of
	// returning N children from one attempt.
	defaultFollowUpLimit = 10000
	// defaultBarrierMaxChildren caps a barrier-bearing generation. It shares
	// defaultFollowUpLimit's value but is a separate bound: a barrier adds an
	// O(children) completion count per child finalize on top of the cheap chunked
	// insert FollowUpLimit bounds, so an operator may want to cap it independently.
	defaultBarrierMaxChildren = 10000
)

// DriverOpts configures a Driver's batching behavior. The zero value selects the
// documented defaults, and no field is ever unbounded.
type DriverOpts struct {
	// SweepBatchSize is the number of expired leases reclaimed per transaction.
	// Zero or negative selects defaultSweepBatchSize.
	//
	// It bounds transaction duration and lock-hold time, and on PostgreSQL it
	// also bounds the statement's bind-parameter count — the reclaim binds one
	// parameter per row, and the extended protocol rejects a statement carrying
	// more than 65,535 of them outright.
	SweepBatchSize int

	// FollowUpChunkSize is the number of child rows per INSERT when a finalize
	// enqueues a worker's Result.FollowUps. Zero or negative selects
	// defaultFollowUpChunk. It bounds how long the finalize transaction holds its
	// row lock while fanning out.
	FollowUpChunkSize int

	// FollowUpLimit is the maximum number of children one attempt may enqueue.
	// Zero or negative selects defaultFollowUpLimit. A worker returning more than
	// this fails the finalize with ErrFollowUpLimit rather than silently
	// truncating — the fan-out is removed as a way to make the runtime hold an
	// unbounded lock.
	FollowUpLimit int

	// BarrierMaxChildren caps the children of a generation that carries a
	// Result.Barrier. Zero or negative selects defaultBarrierMaxChildren. A wider
	// barrier fails the finalize with ErrBarrierTooWide. It is a separate, and often
	// tighter, bound than FollowUpLimit: the barrier's per-child completion count
	// makes a barrier-bearing generation cost O(children²), where a plain fan-out is
	// O(children).
	BarrierMaxChildren int
}

// sweepBatchSize resolves the configured batch size, applying the default for a
// non-positive value.
func (o DriverOpts) sweepBatchSize() int {
	if o.SweepBatchSize <= 0 {
		return defaultSweepBatchSize
	}
	return o.SweepBatchSize
}

// followUpChunkSize resolves the follow-up insert chunk size, applying the
// default for a non-positive value.
func (o DriverOpts) followUpChunkSize() int {
	if o.FollowUpChunkSize <= 0 {
		return defaultFollowUpChunk
	}
	return o.FollowUpChunkSize
}

// followUpLimit resolves the per-attempt follow-up ceiling, applying the default
// for a non-positive value.
func (o DriverOpts) followUpLimit() int {
	if o.FollowUpLimit <= 0 {
		return defaultFollowUpLimit
	}
	return o.FollowUpLimit
}

// barrierMaxChildren resolves the per-generation barrier ceiling, applying the
// default for a non-positive value.
func (o DriverOpts) barrierMaxChildren() int {
	if o.BarrierMaxChildren <= 0 {
		return defaultBarrierMaxChildren
	}
	return o.BarrierMaxChildren
}

// reclaimFunc is one dialect's expired-lease batch: it selects up to limit jobs
// whose lease expired before now, returns them to available with their fence
// token cleared, and reports the ids it reclaimed.
//
// The dialect passes this *up* into the shared loop rather than the loop
// reaching *down* into the dialect, because Go embedding promotes methods
// without dispatching them: a loop on baseDriver calling d.reclaimExpired would
// bind to baseDriver's own copy, never the outer type's, and the SKIP LOCKED
// path would silently never run.
type reclaimFunc func(ctx context.Context, tx *gorm.DB, now time.Time, limit int) ([]string, error)

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

// FinalizeOutcome reports what a finalization actually persisted.
//
// It is deliberately not the same type as the internal finalizePlan: the plan is
// what the attempt's result implied, this is what the database ended up holding,
// and a superseded finalize is precisely the case where the two differ.
type FinalizeOutcome struct {
	// Superseded is true when the attempt no longer held the claim: the audit row
	// was written and the job's state was left untouched.
	Superseded bool
	// State is the job's state after finalization. When Superseded it is the
	// state the superseding claim left, read back rather than planned — which is
	// what distinguishes "cancelled underneath the attempt" from "reclaimed and
	// running again". It is empty only when the job no longer exists.
	State JobState
	// RunOutcome is what was written to the audit row. It is the attempt's real
	// outcome even when Superseded: the attempt happened, and losing the claim
	// discards its effect on the job, not the record of the work.
	RunOutcome RunOutcome
	// ErrorClass is the classification written to the audit row, empty when the
	// attempt carried no error.
	ErrorClass ErrorClass
	// ScheduledAt is when the job next becomes claimable — set only for a retry
	// or a snooze, nil for a terminal state and when Superseded.
	ScheduledAt *time.Time
	// EnqueuedChildren is the number of follow-ups enqueued; always zero when
	// Superseded.
	EnqueuedChildren int
}

// finalizePlan is the state-machine decision for one finalization.
type finalizePlan struct {
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
func planFinalize(raw RawJob, result Result, workErr error, finishedAt time.Time) finalizePlan {
	switch {
	case result.Cancel:
		return finalizePlan{
			jobState: StateCancelled, runOutcome: OutcomeCancelled, finalizedAt: &finishedAt,
		}
	case result.Snooze != nil:
		when := finishedAt.Add(*result.Snooze)
		return finalizePlan{
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
		out := finalizePlan{runOutcome: outcome, errorClass: &class}
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
		return finalizePlan{
			jobState: StateSucceeded, runOutcome: OutcomeSuccess,
			finalizedAt: &finishedAt, followUps: true,
		}
	}
}

// expBackoff is the exponential retry ladder shared by the Runner's
// configurable backoff and the driver's fallback: base, doubling once per
// attempt past the first, capped at maxDelay. The canonical implementation lives
// in go-foundation/backoff; this thin wrapper keeps the call sites and their
// tests unchanged.
func expBackoff(base, maxDelay time.Duration, attempt int) time.Duration {
	return backoff.Exponential(base, maxDelay, attempt)
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
// implementations — Dequeue and the sweep's reclaim step differ by dialect.
//
// It deliberately has no Sweep method. A method whose correct implementation is
// dialect-specific must not have a dialect-neutral default here: embedding
// hands that default to any dialect that forgets to override it, silently and
// forever, which is how the Scheduler came to run a non-SKIP-LOCKED sweep
// against PostgreSQL. Each dialect declares Sweep and drives the shared loop
// below, so the compiler is the enforcement.
type baseDriver struct {
	db   *gorm.DB
	opts DriverOpts
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
func jobFinalizeUpdate(plan finalizePlan, finishedAt time.Time) map[string]any {
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
	plan finalizePlan, result Result, workErr error, finishedAt time.Time, durationMs, enqueued int,
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
// It returns what it persisted rather than what it planned. The two diverge
// whenever the attempt's claim was superseded, and the caller has no other way
// to tell: re-deriving the plan on the far side would produce the outcome the
// attempt *would* have had, which is exactly the wrong answer.
//
//nolint:gocognit // one transaction closure with three cohesive steps
func (d *baseDriver) Finalize(
	ctx context.Context, raw RawJob, runID string, result Result, workErr error, finishedAt time.Time,
) (FinalizeOutcome, error) {
	plan := planFinalize(raw, result, workErr, finishedAt)
	out := FinalizeOutcome{
		State:       plan.jobState,
		RunOutcome:  plan.runOutcome,
		ScheduledAt: plan.scheduledAt,
	}
	if plan.errorClass != nil {
		out.ErrorClass = *plan.errorClass
	}

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
		// A barrier is declared on the spawning job's own row, folded into the state
		// advance so it is written atomically under the fence and never on a superseded
		// finalize. It is validated first, so a too-wide or childless barrier fails the
		// whole finalize rather than half-declaring itself.
		upd := jobFinalizeUpdate(plan, finishedAt)
		if plan.followUps && result.Barrier != nil {
			if err := validateBarrier(result, d.opts.barrierMaxChildren()); err != nil {
				return err
			}
			kind, specJSON, err := resolveBarrierColumns(tx, raw, result.Barrier)
			if err != nil {
				return err
			}
			upd["barrier_kind"] = kind
			upd["barrier_spec"] = specJSON
		}
		res := tx.Model(&jobRow{}).
			Where("id = ? AND state = ? AND lease_token = ?", raw.ID, string(StateRunning), raw.LeaseToken).
			Updates(upd)
		if res.Error != nil {
			return fmt.Errorf("jobs: advance job state: %w", res.Error)
		}
		out.Superseded = res.RowsAffected == 0
		if out.Superseded {
			// Nothing was scheduled, because nothing was written to the job at all.
			out.ScheduledAt = nil
			// Read back the state the superseding claim left, inside the same
			// transaction. Reporting the plan's state would name a state nothing
			// wrote, and the real one is what tells a caller *how* the claim was
			// lost — cancelled underneath the attempt, or reclaimed and re-running.
			// It is one extra SELECT on a path that should be rare.
			var current jobRow
			switch err := tx.Model(&jobRow{}).
				Select("state").Where("id = ?", raw.ID).First(&current).Error; {
			case err == nil:
				out.State = JobState(current.State)
			case errors.Is(err, gorm.ErrRecordNotFound):
				// Deleted out from under the attempt. There is no state to report,
				// and the zero value says exactly that.
				out.State = ""
			default:
				return fmt.Errorf("jobs: read superseded job state: %w", err)
			}
		}

		enqueued := 0
		if plan.followUps && !out.Superseded {
			var followErr error
			if enqueued, followErr = d.insertFollowUps(txCtx, tx, result.FollowUps, raw.ID); followErr != nil {
				return followErr
			}
		}
		out.EnqueuedChildren = enqueued

		// The barrier completion check: when this finalize moves a child to a terminal
		// state, its parent's barrier — if it declared one — may now be complete. It
		// runs for any terminal transition (success, failure, or cancel), so a
		// half-failed generation still gets its finalizer, but never for a retry or
		// snooze (the child is still pending) or a superseded finalize.
		if !out.Superseded && raw.ParentJobID != nil && isTerminalStateString(string(plan.jobState)) {
			if err := d.fireBarrierIfComplete(txCtx, tx, *raw.ParentJobID); err != nil {
				return err
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
		// The transaction rolled back, so nothing was persisted and there is no
		// outcome to report. The zero value says so.
		return FinalizeOutcome{}, fmt.Errorf("jobs: finalize: %w", err)
	}
	return out, nil
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

// buildChildRow constructs the jobs row for one follow-up from fu and the
// spawning job's id.
//
// It preserves the child's asymmetry with a top-level Insert: a child may carry a
// UniqueKey and, when fu.Parent, a parent_job_id, but never a UniqueActiveKey. It
// is shared by InsertChild (one child) and insertFollowUps (the chunked fan-out),
// so the two cannot drift on a child's row shape.
func buildChildRow(ctx context.Context, fu FollowUp, parentID string) (jobRow, error) {
	payload, err := json.Marshal(fu.Args)
	if err != nil {
		return jobRow{}, fmt.Errorf("jobs: marshal child args: %w", err)
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
	return row, nil
}

// InsertChild writes one follow-up job on tx. A unique_key collision is
// surfaced as ErrAlreadyEnqueued so Finalize can skip it without aborting.
//
// It remains on the Driver interface as a single-row public seam, but production
// no longer routes through it: the finalize fan-out (insertFollowUps) uses the
// shared chunk primitive. Its single-row behavior here is unchanged.
func (d *baseDriver) InsertChild(
	ctx context.Context, tx *gorm.DB, fu FollowUp, parentID string,
) error {
	row, err := buildChildRow(ctx, fu, parentID)
	if err != nil {
		return err
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

// sweep runs reclaim in bounded batches until the backlog is drained or ctx is
// cancelled, reporting the total reclaimed across every batch.
//
// It is the shared half of every dialect's Sweep: the loop, the bound, and the
// cancellation contract are dialect-independent, and only the batch's reclaim
// statement is not.
//
// There is deliberately no ceiling on the number of batches, where retention
// has one. An unreclaimed lease is stalled work — a job no runner will pick up
// until the sweep releases it — while an unpruned row is only storage. Stopping
// a sweep early to bound its duty cycle would trade a recovery guarantee for
// tidiness.
func (d *baseDriver) sweep(ctx context.Context, now time.Time, reclaim reclaimFunc) (int, error) {
	batchSize := d.opts.sweepBatchSize()
	reclaimed := 0
	for {
		// Cancellation is checked between batches only. Interrupting a batch
		// mid-transaction would roll it back and lose work already done, and a
		// batch is bounded by construction, so the wait is bounded too.
		if err := ctx.Err(); err != nil {
			return reclaimed, fmt.Errorf("jobs: sweep cancelled after %d reclaimed: %w", reclaimed, err)
		}
		n, err := d.sweepBatch(ctx, now, batchSize, reclaim)
		reclaimed += n
		if err != nil {
			return reclaimed, err
		}
		// A short batch means the backlog is drained — not that it is empty. Under
		// SKIP LOCKED it may instead mean a concurrent sweeper holds the rest,
		// which the next sweep collects.
		if n < batchSize {
			return reclaimed, nil
		}
	}
}

// sweepBatch reclaims one bounded batch and crashes its stale run stubs inside a
// single transaction, so a batch can never leave a job available with its audit
// row still reading started.
func (d *baseDriver) sweepBatch(
	ctx context.Context, now time.Time, limit int, reclaim reclaimFunc,
) (int, error) {
	reclaimed := 0
	err := d.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		ids, err := reclaim(ctx, tx, now, limit)
		if err != nil {
			return err
		}
		if len(ids) == 0 {
			return nil
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

// reclaimUpdate is the jobs-row column set for a lease reclaim, shared by both
// dialects so neither can drift from the other on the columns that matter.
//
// Clearing the token is what makes the reclaim final: the attempt whose lease
// just expired can no longer finalize over the next claim, and it cannot renew
// the lease it no longer holds either. Dropping it would silently reopen the
// double-execution window the fence exists to close.
func reclaimUpdate(now time.Time) map[string]any {
	return map[string]any{
		"state":        string(StateAvailable),
		"leased_until": nil,
		"lease_token":  nil,
		"updated_at":   now,
	}
}

// insertFollowUps inserts a worker's follow-up jobs on tx and reports how many
// were enqueued. A unique_key collision is skipped rather than fatal: the child
// is already enqueued, so the parent's finalization has nothing to correct and
// must not be rolled back over it.
//
// The fan-out is bounded on both axes. A count over the configured limit fails
// with ErrFollowUpLimit before anything is written, so the finalize transaction
// rolls back and enqueues nothing — a loud failure in place of a silently
// truncated set. Within the limit the children are inserted in chunks — one
// bounded multi-row INSERT each, on the finalize tx directly (no sub-transaction,
// so the fan-out stays atomic with the state advance) — instead of one statement
// per child holding the row lock across the whole set. The count is the number of
// rows that actually landed, read back per chunk by the shared primitive: a
// colliding child is skipped by ON CONFLICT DO NOTHING and does not count, exactly
// as it did not under the old per-child ErrAlreadyEnqueued swallow.
func (d *baseDriver) insertFollowUps(
	ctx context.Context, tx *gorm.DB, followUps []FollowUp, parentID string,
) (int, error) {
	if len(followUps) == 0 {
		return 0, nil
	}
	if len(followUps) > d.opts.followUpLimit() {
		return 0, ErrFollowUpLimit
	}

	rows := make([]jobRow, len(followUps))
	for i, fu := range followUps {
		row, err := buildChildRow(ctx, fu, parentID)
		if err != nil {
			return 0, err
		}
		rows[i] = row
	}

	chunkSize := d.opts.followUpChunkSize()
	enqueued := 0
	for start := 0; start < len(rows); start += chunkSize {
		end := min(start+chunkSize, len(rows))
		landed, err := conflictInsertChunk(ctx, tx, rows[start:end])
		if err != nil {
			return enqueued, err
		}
		enqueued += len(landed)
	}
	return enqueued, nil
}

// barrierSpec is the fully-resolved continuation stored in jobs.barrier_spec: the
// defaulted routing plus the marshaled args, so the child that fires the barrier
// builds the continuation without re-reading the parent's columns.
type barrierSpec struct {
	Args          json.RawMessage `json:"args,omitempty"`
	Queue         string          `json:"queue"`
	ExecutorClass string          `json:"executor_class"`
	Priority      int             `json:"priority"`
}

// validateBarrier rejects a barrier that could never fire or would cost too much:
// it must name a continuation kind, cover at least one child, and stay within the
// configured ceiling. It runs before any row is written, so a bad barrier fails the
// whole finalize rather than half-declaring itself.
func validateBarrier(result Result, maxChildren int) error {
	if result.Barrier.Kind == "" {
		return newValidationError("barrier kind", "is required")
	}
	children := 0
	for i := range result.FollowUps {
		if result.FollowUps[i].Parent {
			children++
		}
	}
	if children == 0 {
		return ErrBarrierNoChildren
	}
	if children > maxChildren {
		return ErrBarrierTooWide
	}
	return nil
}

// resolveBarrierColumns builds the barrier_kind and barrier_spec column values for
// the spawning job's row, resolving the continuation's routing against the parent's
// own columns so the later fire needs no defaulting. queue is on the RawJob;
// executor_class and priority are read from the parent row, which is the rare
// declaration path rather than a per-finalize cost.
func resolveBarrierColumns(tx *gorm.DB, raw RawJob, b *Barrier) (string, datatypes.JSON, error) {
	var parent jobRow
	if err := tx.Model(&jobRow{}).Select("executor_class, priority").
		Where("id = ?", raw.ID).First(&parent).Error; err != nil {
		return "", nil, fmt.Errorf("jobs: read barrier parent routing: %w", err)
	}
	args, err := json.Marshal(b.Args)
	if err != nil {
		return "", nil, fmt.Errorf("jobs: marshal barrier args: %w", err)
	}
	spec := barrierSpec{
		Args:          args,
		Queue:         orString(b.Queue, raw.Queue),
		ExecutorClass: orString(string(b.ExecutorClass), parent.ExecutorClass),
		Priority:      orInt(b.Priority, parent.Priority),
	}
	specJSON, err := json.Marshal(spec)
	if err != nil {
		return "", nil, fmt.Errorf("jobs: marshal barrier spec: %w", err)
	}
	return b.Kind, datatypes.JSON(specJSON), nil
}

// fireBarrierIfComplete enqueues a parent's barrier continuation when the child
// whose finalize is running is the last of the generation to reach a terminal
// state. It runs inside that child's finalize transaction, so the enqueue is atomic
// with the child's own state advance and inherits its supersede-idempotency.
//
// The overwhelmingly common case — a parent that declared no barrier — returns at
// the fast gate, having taken no lock and issued no completion count. When a barrier
// is declared it serializes concurrent completion checks on the parent row: without
// that, two children finalizing at once could each see the other as still pending
// under READ COMMITTED and neither fire. On SQLite the single-writer model already
// serializes finalizes, so the row lock is taken on PostgreSQL alone.
func (d *baseDriver) fireBarrierIfComplete(ctx context.Context, tx *gorm.DB, parentID string) error {
	// Fast gate: a plain read of the parent's barrier_kind by primary key. A parent
	// with no barrier — every parent that did not declare one — returns here, having
	// touched nothing else.
	var gate jobRow
	switch err := tx.WithContext(ctx).Model(&jobRow{}).
		Select("barrier_kind").Where("id = ?", parentID).First(&gate).Error; {
	case errors.Is(err, gorm.ErrRecordNotFound):
		return nil // the parent row is gone (pruned); nothing to fire
	case err != nil:
		return fmt.Errorf("jobs: read barrier gate: %w", err)
	}
	if gate.BarrierKind == nil {
		return nil
	}

	// A barrier is declared. Lock the parent row so concurrent completion checks
	// serialize on it, then re-read under the lock: a concurrent finalize may have
	// fired and cleared the barrier between the gate read and the lock.
	var parent jobRow
	locked := tx.WithContext(ctx).Model(&jobRow{}).
		Select("barrier_kind, barrier_spec").Where("id = ?", parentID)
	if tx.Name() == "postgres" {
		locked = locked.Clauses(clause.Locking{Strength: "UPDATE"})
	}
	switch err := locked.First(&parent).Error; {
	case errors.Is(err, gorm.ErrRecordNotFound):
		return nil
	case err != nil:
		return fmt.Errorf("jobs: lock barrier parent: %w", err)
	}
	if parent.BarrierKind == nil {
		return nil // a concurrent finalize already fired and cleared it
	}

	// Count the parent's still-pending children, index-only via jobs_parent_state
	// (Model adds deleted_at IS NULL, which the partial index requires). This child's
	// own terminal state is already applied in this transaction, so a zero count means
	// it is the last of the generation.
	var remaining int64
	if err := tx.WithContext(ctx).Model(&jobRow{}).
		Where("parent_job_id = ? AND state NOT IN ?", parentID, terminalStateStrings()).
		Count(&remaining).Error; err != nil {
		return fmt.Errorf("jobs: count barrier siblings: %w", err)
	}
	if remaining > 0 {
		return nil
	}

	// Last child: enqueue the continuation, keyed so a torn retry can never enqueue it
	// twice, and clear the barrier so the continuation's own finalize skips the check.
	fu, err := followUpFromBarrierSpec(*parent.BarrierKind, parent.BarrierSpec, parentID)
	if err != nil {
		return err
	}
	row, err := buildChildRow(ctx, fu, parentID)
	if err != nil {
		return err
	}
	if _, err := conflictInsertChunk(ctx, tx, []jobRow{row}); err != nil {
		return fmt.Errorf("jobs: enqueue barrier continuation: %w", err)
	}
	if err := tx.WithContext(ctx).Model(&jobRow{}).Where("id = ?", parentID).
		Updates(map[string]any{"barrier_kind": nil, "barrier_spec": nil}).Error; err != nil {
		return fmt.Errorf("jobs: clear fired barrier: %w", err)
	}
	return nil
}

// followUpFromBarrierSpec reconstructs the continuation FollowUp from a parent's
// stored barrier columns. Parent is set so the continuation joins the same
// generation (and shows up in the parent's own Progress rollup), and the unique key
// is the parent id, so the barrier enqueues at most one continuation ever — even
// under a torn retry, where ON CONFLICT DO NOTHING absorbs the second insert.
func followUpFromBarrierSpec(kind string, specJSON datatypes.JSON, parentID string) (FollowUp, error) {
	var spec barrierSpec
	if err := json.Unmarshal(specJSON, &spec); err != nil {
		return FollowUp{}, fmt.Errorf("jobs: unmarshal barrier spec: %w", err)
	}
	return FollowUp{
		Kind:          kind,
		Args:          spec.Args,
		Queue:         spec.Queue,
		ExecutorClass: ExecutorClass(spec.ExecutorClass),
		Priority:      spec.Priority,
		Parent:        true,
		UniqueKey:     "barrier:" + parentID,
	}, nil
}
