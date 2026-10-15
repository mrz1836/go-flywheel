package core

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/mrz1836/go-foundation/models"
	"gorm.io/gorm"
)

// RescheduleJob moves a waiting job — available, retryable, scheduled, or
// paused — to another time, keeping its state: it sets scheduled_at to at, as
// given (as InsertOpts.ScheduleAt is), and updated_at. A time in the past makes
// an available job claimable at once; a paused job stays paused until
// ResumeJob. A zero at is refused with a *ValidationError naming "at".
//
// It never touches a running job: a running job is refused with ErrJobRunning,
// and a job that has reached a terminal state with ErrJobTerminal, each left
// exactly as it was. It returns ErrJobNotFound when no live job has the id.
//
// It runs in a transaction of its own — a savepoint when db is a caller's
// transaction — in which it reads the job under a row lock (FOR UPDATE on
// PostgreSQL), refuses from the state it read, and writes one AuditRescheduled
// entry to the control history: attributed to the context's actor (WithActor;
// the system when there is none), with the state the job was in (from_state)
// and its old and new scheduled_at (from, to) in UTC. A refused call writes
// nothing, and a caller's rollback removes the entry with the change. An actor
// the history can't record is refused with a *ValidationError before anything
// is read.
func RescheduleJob(ctx context.Context, db *gorm.DB, id string, at time.Time) error {
	actor, err := resolveActor(ctx)
	if err != nil {
		return err
	}
	if at.IsZero() {
		return newValidationError("at", "is required")
	}
	return controlJob(ctx, db, actor, "reschedule", AuditRescheduled, id, waitingStates(),
		func(tx *gorm.DB, job jobRow, now time.Time) (map[string]any, bool, error) {
			if err := updateLockedJob(tx, job, map[string]any{"scheduled_at": at, "updated_at": now}); err != nil {
				return nil, false, err
			}
			return map[string]any{
				"from_state": job.State,
				"from":       job.ScheduledAt.UTC().Format(time.RFC3339Nano),
				"to":         at.UTC().Format(time.RFC3339Nano),
			}, true, nil
		})
}

// SetJobPriority gives a waiting job — available, retryable, scheduled, or
// paused — another priority, keeping its state: it sets priority and
// updated_at. A runner claims the lowest priority first, as it does for
// InsertOpts.Priority. A priority below 1 is refused with a *ValidationError
// naming "priority". Setting the priority the job already has changes nothing,
// updated_at included, and records nothing.
//
// It never touches a running job: a running job is refused with ErrJobRunning,
// and a job that has reached a terminal state with ErrJobTerminal, each left
// exactly as it was. It returns ErrJobNotFound when no live job has the id.
//
// It reads the job under a row lock in a transaction of its own, as
// RescheduleJob does, and writes one AuditReprioritized entry, with the state
// the job was in (from_state) and its old and new priority (from, to).
func SetJobPriority(ctx context.Context, db *gorm.DB, id string, priority int) error {
	actor, err := resolveActor(ctx)
	if err != nil {
		return err
	}
	if priority < 1 {
		return newValidationError("priority", "must be at least 1")
	}
	return controlJob(ctx, db, actor, "reprioritize", AuditReprioritized, id, waitingStates(),
		func(tx *gorm.DB, job jobRow, now time.Time) (map[string]any, bool, error) {
			if job.Priority == priority {
				return nil, false, nil
			}
			if err := updateLockedJob(tx, job, map[string]any{"priority": priority, "updated_at": now}); err != nil {
				return nil, false, err
			}
			return map[string]any{"from_state": job.State, "from": job.Priority, "to": priority}, true, nil
		})
}

// PauseJob holds one claimable job — available, retryable, or scheduled — in
// the paused state, so no runner claims it until ResumeJob: it sets state and
// updated_at, and keeps scheduled_at. Pausing a job that is already paused
// changes nothing, updated_at included, and records nothing. It is the
// single-job twin of PauseByParent.
//
// It never touches a running job: a running job is refused with ErrJobRunning,
// and a job that has reached a terminal state with ErrJobTerminal, each left
// exactly as it was. It returns ErrJobNotFound when no live job has the id.
//
// It reads the job under a row lock in a transaction of its own, as
// RescheduleJob does, and writes one AuditPaused entry, with the state the job
// was in (from_state).
func PauseJob(ctx context.Context, db *gorm.DB, id string) error {
	actor, err := resolveActor(ctx)
	if err != nil {
		return err
	}
	return controlJob(ctx, db, actor, "pause", AuditPaused, id, waitingStates(),
		func(tx *gorm.DB, job jobRow, now time.Time) (map[string]any, bool, error) {
			if JobState(job.State) == StatePaused {
				return nil, false, nil
			}
			if err := updateLockedJob(tx, job, map[string]any{"state": string(StatePaused), "updated_at": now}); err != nil {
				return nil, false, err
			}
			return map[string]any{"from_state": job.State}, true, nil
		})
}

// ResumeJob returns a paused job to available: it sets state and updated_at,
// and keeps scheduled_at, so a job paused while deferred stays deferred rather
// than becoming claimable early. Resuming a job that is available, retryable,
// or scheduled changes nothing, updated_at included, and records nothing. It is
// the single-job twin of ResumeByParent.
//
// It never touches a running job: a running job is refused with ErrJobRunning,
// and a job that has reached a terminal state with ErrJobTerminal, each left
// exactly as it was. It returns ErrJobNotFound when no live job has the id.
//
// It reads the job under a row lock in a transaction of its own, as
// RescheduleJob does, and writes one AuditResumed entry, with the state the job
// was in (from_state).
func ResumeJob(ctx context.Context, db *gorm.DB, id string) error {
	actor, err := resolveActor(ctx)
	if err != nil {
		return err
	}
	return controlJob(ctx, db, actor, "resume", AuditResumed, id, waitingStates(),
		func(tx *gorm.DB, job jobRow, now time.Time) (map[string]any, bool, error) {
			if JobState(job.State) != StatePaused {
				return nil, false, nil
			}
			if err := updateLockedJob(tx, job, map[string]any{"state": string(StateAvailable), "updated_at": now}); err != nil {
				return nil, false, err
			}
			return map[string]any{"from_state": job.State}, true, nil
		})
}

// DeleteJob hard-deletes one finished job — succeeded, cancelled, or
// discarded — with its runs and their finish-log entries, in the order
// DeleteFinishedJobs deletes them: the runs' job_run_finishes entries, then the
// job_runs rows, then the jobs row. Deleting a job frees its
// InsertOpts.UniqueKey, as retention does.
//
// It deletes only a finished job, and never touches a running one: a running
// job is refused with ErrJobRunning, and a job that is available, retryable,
// scheduled, or paused with ErrJobNotTerminal, each left exactly as it was. It
// returns ErrJobNotFound when no live job has the id; a soft-deleted job is
// left as it is.
//
// The job's history stays: its earlier entries are kept, and the delete adds
// one AuditDeleted entry, with the state the job was in (from_state and state)
// and its kind, written as RescheduleJob writes its own, under a row lock in a
// transaction of its own. Only the job's own rows go. Its children keep their
// parent_job_id, and a barrier it declared and had not fired goes with its row,
// so that barrier never fires.
func DeleteJob(ctx context.Context, db *gorm.DB, id string) error {
	actor, err := resolveActor(ctx)
	if err != nil {
		return err
	}
	return controlJob(ctx, db, actor, "delete", AuditDeleted, id, TerminalStates(),
		func(tx *gorm.DB, job jobRow, _ time.Time) (map[string]any, bool, error) {
			deleted, err := deleteJobRows(tx, []string{job.ID})
			if err != nil {
				return nil, false, err
			}
			if deleted != 1 {
				return nil, false, fmt.Errorf("flywheel: job %q changed while it was locked", job.ID)
			}
			return map[string]any{"from_state": job.State, "kind": job.Kind, "state": job.State}, true, nil
		})
}

// waitingStates are the states a job waits to be claimed in, the ones
// RescheduleJob, SetJobPriority, PauseJob, and ResumeJob act on.
func waitingStates() []JobState {
	return []JobState{StateAvailable, StateRetryable, StateScheduled, StatePaused}
}

// jobChange makes a single-job control's change to job, on tx, at now, the
// clock reading the control took after its lock. It returns the details the
// change's entry records, or false for a no-op that wrote nothing.
type jobChange func(tx *gorm.DB, job jobRow, now time.Time) (details map[string]any, changed bool, err error)

// controlJob runs a single-job control in a transaction of its own (a
// savepoint when db is a caller's transaction): it reads the job under a row
// lock, refuses a state outside acts through controlMiss, reads the clock, and
// runs change; when change changed the job, it records one action entry for
// it under actor, stamped with that reading, in the same transaction. A
// refusal returns its sentinel as it is, and any other failure names op and
// the job.
func controlJob(
	ctx context.Context, db *gorm.DB, actor Actor, op string, action AuditAction, id string, acts []JobState,
	change jobChange,
) error {
	err := db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		job, err := lockJob(ctx, tx, id)
		if err != nil {
			return err
		}
		// The states a control acts on are named rather than the refused ones
		// excluded, as the retry and the cancel do: a row in a state this
		// runtime does not recognize is refused rather than changed.
		if !slices.Contains(acts, JobState(job.State)) {
			return controlMiss(JobState(job.State))
		}
		now := models.ClockFrom(ctx).Now(ctx)
		details, changed, err := change(tx, job, now)
		if err != nil || !changed {
			return err
		}
		return recordAudit(ctx, tx, actor, now, []auditChange{{
			target: AuditTargetJob, targetID: id, action: action, details: details,
		}})
	})
	switch {
	case err == nil:
		return nil
	case errors.Is(err, ErrJobNotFound), errors.Is(err, ErrJobRunning), errors.Is(err, ErrJobTerminal),
		errors.Is(err, ErrJobNotTerminal):
		return err
	default:
		return fmt.Errorf("flywheel: %s job %q: %w", op, id, err)
	}
}
