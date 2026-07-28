package flywheel

import (
	"context"
	"fmt"
	"time"

	"gorm.io/gorm"
)

// defaultRetentionBatchSize is the number of jobs deleted per transaction when
// RetentionOpts leaves BatchSize zero.
//
// It is a bound, not a knob that can be switched off: no value of BatchSize,
// zero or negative, produces an unbounded transaction.
const defaultRetentionBatchSize = 1000

// RetentionOpts configures a retention pass. The zero value selects the
// documented defaults and runs until the backlog is exhausted.
type RetentionOpts struct {
	// BatchSize is the number of jobs deleted per transaction. Zero or negative
	// selects defaultRetentionBatchSize.
	//
	// It bounds transaction duration and lock-hold time, and on PostgreSQL it
	// also bounds the statement's bind-parameter count — the deletes bind one
	// parameter per row, and the extended protocol rejects a statement carrying
	// more than 65,535 of them outright.
	BatchSize int
	// MaxBatches, when positive, caps how many batches one pass runs. Zero runs
	// until the backlog is exhausted.
	//
	// It exists so a scheduled prune has a predictable duty cycle: a first pass
	// against a long-lived database has months of history to remove, and a host
	// may prefer that spread across several ticks rather than taken in one. The
	// sweep has no equivalent ceiling, deliberately — an unpruned row is only
	// storage, while an unreclaimed lease is stalled work.
	MaxBatches int
}

// batchSize resolves the configured batch size, applying the default for a
// non-positive value.
func (o RetentionOpts) batchSize() int {
	if o.BatchSize <= 0 {
		return defaultRetentionBatchSize
	}
	return o.BatchSize
}

// DeleteFinishedJobs hard-deletes terminal jobs (succeeded, cancelled,
// discarded) finalized before olderThan, together with their job_runs audit
// rows, and reports how many jobs were removed. It is DeleteFinishedJobsWithOptions
// with default options.
//
// It is the retention primitive behind `flywheel prune` and the Scheduler's
// optional retention sweep: a forever-running daemon needs a way to keep jobs
// and job_runs from growing unbounded.
func DeleteFinishedJobs(ctx context.Context, db *gorm.DB, olderThan time.Time) (int64, error) {
	return DeleteFinishedJobsWithOptions(ctx, db, olderThan, RetentionOpts{})
}

// DeleteFinishedJobsWithOptions hard-deletes terminal jobs and their audit rows
// in bounded batches, each its own transaction, and reports the total deleted
// across every batch.
//
// The delete is hard, not soft: jobs is soft-deletable, but retention reclaims
// storage, so it bypasses the soft-delete scope (Unscoped). Soft-deleted
// terminal jobs are purged too, so their audit rows do not orphan.
//
// # Delete order is contractual
//
// Within each batch the job_runs rows are deleted before their jobs rows. The
// library declares no foreign key between the two tables, so on its own schema
// the order is unobservable — but a host may declare one, and under an
// ON DELETE CASCADE the order is visible in the reported counts: reversed, the
// runs delete would match nothing and the cascade would do the work silently.
// It is part of the contract rather than an implementation detail.
//
// # Batching is keyset pagination, not an ordering by age
//
// Batches advance a cursor over the primary key rather than ordering by
// finalized_at. There is no index on finalized_at, so ordering by it cannot
// terminate early under a LIMIT and every batch would be a full scan plus a
// top-N sort — measured at 500k rows and batch 1000, that spelling touches 4.7×
// the heap blocks this one does.
//
// The practical consequence: because job ids are UUIDv7, whose leading bits are
// a millisecond timestamp, batches proceed in approximately oldest-created
// order. Approximately, not exactly, and not by finalization: a retry ladder
// reorders completion relative to creation. A pass stopped early by MaxBatches
// or a cancelled context therefore leaves a coherent tail by id, not strictly
// by age.
//
// # A partial pass reports what it committed
//
// The returned count is meaningful alongside a non-nil error: batch k failing
// returns the rows committed by batches 1..k-1, which are not rolled back. A
// caller that treats a non-nil error as "nothing happened" would be wrong.
func DeleteFinishedJobsWithOptions(
	ctx context.Context, db *gorm.DB, olderThan time.Time, opts RetentionOpts,
) (int64, error) {
	batchSize := opts.batchSize()
	var (
		deleted int64
		cursor  string
	)
	for batch := 0; opts.MaxBatches <= 0 || batch < opts.MaxBatches; batch++ {
		// Cancellation is checked between batches only. Interrupting a batch
		// mid-transaction would roll it back and lose work already done, and a
		// batch is bounded by construction, so the wait is bounded too.
		if err := ctx.Err(); err != nil {
			return deleted, fmt.Errorf(
				"flywheel: delete finished jobs cancelled after %d deleted: %w", deleted, err,
			)
		}
		n, last, err := deleteFinishedBatch(ctx, db, olderThan, cursor, batchSize)
		deleted += n
		if err != nil {
			return deleted, err
		}
		if last == "" {
			// The batch selected nothing: the backlog is exhausted.
			return deleted, nil
		}
		cursor = last
	}
	return deleted, nil
}

// deleteFinishedBatch removes one bounded batch inside a single transaction,
// reporting how many jobs it deleted and the last id it saw — the cursor for the
// next batch.
//
// An empty last id means the batch selected no rows, which is how the caller
// detects exhaustion. It is distinct from a zero count: a batch can select rows
// whose jobs delete affects fewer rows than the runs delete did.
//
// The cursor predicate is added only once there is a cursor, rather than seeded
// with an empty string that every id sorts after. An empty string is a valid
// value for a text id and an *invalid literal* for a uuid one, and a host is
// free to declare jobs.id as uuid — the runtime's own installer happens to
// create it as text, so a sentinel comparison would work everywhere the library
// tests itself and fail on a schema it never builds.
func deleteFinishedBatch(
	ctx context.Context, db *gorm.DB, olderThan time.Time, cursor string, limit int,
) (int64, string, error) {
	var (
		deleted int64
		last    string
	)
	err := db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		query := tx.Unscoped().Model(&jobRow{}).
			Where(
				"state IN ? AND finalized_at IS NOT NULL AND finalized_at < ?",
				terminalStateStrings(), olderThan,
			)
		if cursor != "" {
			query = query.Where("id > ?", cursor)
		}

		var ids []string
		if err := query.Order("id").Limit(limit).
			Pluck("id", &ids).Error; err != nil {
			return fmt.Errorf("flywheel: find finished jobs: %w", err)
		}
		if len(ids) == 0 {
			return nil
		}
		// Runs before jobs, per the contract in the exported doc comment.
		if err := tx.Where("job_id IN ?", ids).Delete(&jobRunRow{}).Error; err != nil {
			return fmt.Errorf("flywheel: delete finished job runs: %w", err)
		}
		res := tx.Unscoped().Where("id IN ?", ids).Delete(&jobRow{})
		if res.Error != nil {
			return fmt.Errorf("flywheel: delete finished jobs: %w", res.Error)
		}
		deleted = res.RowsAffected
		// Pluck ordered by id, so the last element is the batch's high-water mark.
		last = ids[len(ids)-1]
		return nil
	})
	if err != nil {
		return 0, "", fmt.Errorf("flywheel: delete finished jobs: %w", err)
	}
	return deleted, last, nil
}
