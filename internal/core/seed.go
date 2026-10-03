package core

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/mrz1836/go-foundation/models"
	"gorm.io/gorm"
)

// RunSeed describes one job_runs row to write directly. It is the audit-side
// counterpart to InsertOpts: a host uses it to stand up realistic run history
// for tests, backfills, or an import, without reaching into the runtime's row
// structs.
//
// A seeded run is an ordinary audit row. It is not dispatched, does not advance
// the job's state, and does not consume an attempt.
type RunSeed struct {
	// JobID is the job this attempt belongs to. Required.
	JobID string
	// Attempt is the attempt number. It must be unique per job (job_runs carries
	// a unique index on (job_id, attempt)); a collision returns
	// ErrRunAlreadyRecorded.
	Attempt int
	// ExecutorClass and ExecutorID identify the executor that ran the attempt.
	// ExecutorID is required: job_runs is an audit table and the row's own
	// lifecycle hook treats the executor's identity as mandatory, so a seeded row
	// names one rather than carrying an invented default into the audit trail.
	ExecutorClass ExecutorClass
	ExecutorID    string
	// Outcome is the recorded outcome. Defaults to OutcomeStarted, matching the
	// stub the runtime writes before a worker body runs.
	Outcome RunOutcome
	// StartedAt defaults to the context clock's now.
	StartedAt time.Time
	// FinishedAt, when set, also fills duration_ms.
	FinishedAt *time.Time
	// Output is the structured result payload, stored on job_runs.output — the
	// same column Result.Output lands in, readable back through ListRuns.
	Output any
	// ErrorMessage, when non-empty, is truncated and stored with its JSON
	// payload, exactly as a failed attempt's error is.
	ErrorMessage string
	// Tx, when set, writes the row on the caller's transaction.
	Tx *gorm.DB
}

// SeedRun writes one job_runs row and returns its id.
//
// The returned id is a stable foreign-key target. The row is committed before
// SeedRun returns — or, with Tx set, when the caller's transaction commits — so
// a host provenance row may reference it immediately, from a later statement or
// a separate transaction.
//
// JobID and ExecutorID are required and return a ValidationError (wrapping
// ErrValidation) when empty. Everything else defaults: Outcome to
// OutcomeStarted, the stub the runtime writes before a worker body runs, and
// StartedAt to the context clock's now.
//
// It is the audit-side twin of Enqueue: same shape, same guarantees, and the
// same mapping of a database rejection onto a sentinel. A (job_id, attempt)
// collision returns ErrRunAlreadyRecorded rather than a raw driver error. That
// mapping depends on the job_runs_job_attempt unique index — an IndexCorrectness
// entry of IndexSet — being installed; without it the duplicate is accepted
// silently.
//
// The seeded row is byte-identical in shape to a runtime-written one: the same
// marshaling and the same error truncation, reached through the same helpers the
// finalize path uses, the same kind and queue copied from its job, and the same
// UTC timestamps, and — for a finished run — the same job_run_finishes entry a
// finalize writes, in the same transaction. What a seed cannot know it leaves
// NULL — the queue wait and the job state the attempt applied — so a seeded run
// is counted by outcome and duration, never as a retry or a discard.
//
// A seeded run that finishes in an hour the stats rollup has already closed is
// not in that hour's rollup until RebuildStats covers it.
func SeedRun(ctx context.Context, db *gorm.DB, seed RunSeed) (string, error) {
	if seed.JobID == "" {
		return "", newValidationError("job_id", "is required")
	}
	if seed.ExecutorID == "" {
		return "", newValidationError("executor_id", "is required")
	}

	target := db
	if seed.Tx != nil {
		target = seed.Tx
	}
	if target == nil {
		return "", fmt.Errorf("flywheel: SeedRun: db is nil")
	}

	startedAt := seed.StartedAt
	if startedAt.IsZero() {
		startedAt = models.ClockFrom(ctx).Now(ctx)
	}
	outcome := seed.Outcome
	if outcome == "" {
		outcome = OutcomeStarted
	}

	// The run's kind and queue are its job's, read on the same handle the row is
	// written on — the caller's transaction when Tx is set, so a job inserted
	// earlier in that transaction is visible. Unscoped, because a soft-deleted job's
	// history is still history. A job that does not exist leaves both empty, which
	// the stats aggregate resolves through jobs exactly as it does a row an older
	// binary wrote.
	var job jobRow
	if err := target.WithContext(ctx).Unscoped().Model(&jobRow{}).
		Select("kind, queue").Where("id = ?", seed.JobID).Limit(1).Find(&job).Error; err != nil {
		return "", fmt.Errorf("flywheel: SeedRun: read job: %w", err)
	}

	row := jobRunRow{
		ID:            models.NewID(),
		JobID:         seed.JobID,
		Attempt:       seed.Attempt,
		Kind:          job.Kind,
		Queue:         job.Queue,
		ExecutorClass: string(seed.ExecutorClass),
		ExecutorID:    seed.ExecutorID,
		StartedAt:     startedAt,
		Outcome:       string(outcome),
		CreatedAt:     startedAt,
	}
	if seed.FinishedAt != nil {
		finishedAt := *seed.FinishedAt
		row.FinishedAt = &finishedAt
		durationMs := int(finishedAt.Sub(startedAt).Milliseconds())
		row.DurationMs = &durationMs
	}
	if seed.ErrorMessage != "" {
		message, payload, err := runErrorFields(seed.ErrorMessage)
		if err != nil {
			return "", fmt.Errorf("flywheel: SeedRun: %w", err)
		}
		row.ErrorMessage = &message
		row.ErrorPayload = payload
	}
	output, marshalErr := marshalRunOutput(seed.Output)
	if marshalErr != nil {
		return "", fmt.Errorf("flywheel: SeedRun: %w", marshalErr)
	}
	row.Output = output

	// The run and its finish-log entry are written together, as a finalize writes
	// them: on the caller's transaction when Tx is set, or in one of their own.
	write := func(tx *gorm.DB) error {
		if createErr := tx.WithContext(ctx).Create(&row).Error; createErr != nil {
			wrapped := models.WrapDBError(createErr)
			if errors.Is(wrapped, models.ErrDuplicateKey) {
				return ErrRunAlreadyRecorded
			}
			return fmt.Errorf("flywheel: SeedRun: %w", wrapped)
		}
		if row.FinishedAt == nil {
			return nil
		}
		if err := tx.WithContext(ctx).Exec(fmt.Sprintf(finishLogInsert, "job_runs", " AND id = ?"), row.ID).Error; err != nil {
			return fmt.Errorf("flywheel: SeedRun: log finish: %w", err)
		}
		return nil
	}
	var err error
	if seed.Tx != nil {
		err = write(seed.Tx)
	} else {
		err = target.WithContext(ctx).Transaction(write)
	}
	if err != nil {
		return "", err
	}
	return row.ID, nil
}
