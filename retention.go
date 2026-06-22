package flywheel

import (
	"context"
	"fmt"
	"time"

	"gorm.io/gorm"
)

// DeleteFinishedJobs hard-deletes terminal jobs (succeeded, cancelled, discarded)
// finalized before olderThan, together with their job_runs audit rows, and
// reports how many jobs were removed. It is the retention primitive behind
// `flywheel prune` and the Scheduler's optional retention sweep: a
// forever-running daemon needs a way to keep jobs and job_runs from growing
// unbounded.
//
// The delete is hard, not soft: jobs is soft-deletable, but retention reclaims
// storage, so it bypasses the soft-delete scope (Unscoped). flywheel has no
// foreign-key cascade between jobs and job_runs, so the runs are deleted by
// job_id first and the jobs second, both inside one transaction so a failure
// leaves neither table half-pruned. Soft-deleted terminal jobs are purged too,
// so their audit rows do not orphan.
func DeleteFinishedJobs(ctx context.Context, db *gorm.DB, olderThan time.Time) (int64, error) {
	var deleted int64
	err := db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var ids []string
		if err := tx.Unscoped().Model(&jobRow{}).
			Where("state IN ? AND finalized_at IS NOT NULL AND finalized_at < ?", terminalStateStrings(), olderThan).
			Pluck("id", &ids).Error; err != nil {
			return fmt.Errorf("flywheel: find finished jobs: %w", err)
		}
		if len(ids) == 0 {
			return nil
		}
		if err := tx.Where("job_id IN ?", ids).Delete(&jobRunRow{}).Error; err != nil {
			return fmt.Errorf("flywheel: delete finished job runs: %w", err)
		}
		res := tx.Unscoped().Where("id IN ?", ids).Delete(&jobRow{})
		if res.Error != nil {
			return fmt.Errorf("flywheel: delete finished jobs: %w", res.Error)
		}
		deleted = res.RowsAffected
		return nil
	})
	if err != nil {
		return 0, fmt.Errorf("flywheel: delete finished jobs: %w", err)
	}
	return deleted, nil
}
