package flywheel

import (
	"context"
	"fmt"
	"time"

	"github.com/mrz1836/go-foundation/models"
	"gorm.io/gorm"
)

// postgresDriver is the PostgreSQL Driver implementation. It claims jobs with a
// single CTE statement using FOR UPDATE SKIP LOCKED, so N executors poll the
// same table with zero blocking and no double-execution: a claimant skips rows
// another transaction already holds instead of queueing behind them.
type postgresDriver struct {
	baseDriver
}

// NewPostgresDriver returns a Driver backed by a PostgreSQL connection.
func NewPostgresDriver(db *gorm.DB) Driver {
	return &postgresDriver{baseDriver{db: db}}
}

// Dequeue claims up to limit ready jobs in one round trip: the CTE selects and
// row-locks the highest-priority rows with SKIP LOCKED, then the UPDATE claims
// and leases them atomically, RETURNING the claimed rows.
func (d *postgresDriver) Dequeue(
	ctx context.Context, queues []string, class ExecutorClass, claimAny bool, limit int, lease time.Duration,
) ([]RawJob, error) {
	if limit <= 0 || len(queues) == 0 {
		return nil, nil
	}
	now := models.ClockFrom(ctx).Now(ctx)
	// One token per claim call, stamped on every row in the batch. See
	// RawJob.LeaseToken for why per-row generation would buy nothing.
	token := models.NewID()

	classFilter := ""
	args := []any{now, queues}
	if !claimAny {
		classFilter = "AND (executor_class = ? OR executor_class = '')"
		args = append(args, string(class))
	}
	args = append(args, limit, now.Add(lease), token, now)

	sql := fmt.Sprintf(`
WITH claimed AS (
    SELECT id FROM jobs
    WHERE state IN ('available', 'retryable', 'scheduled')
      AND deleted_at IS NULL
      AND scheduled_at <= ?
      AND queue IN ?
      %s
    ORDER BY priority, scheduled_at
    LIMIT ?
    FOR UPDATE SKIP LOCKED
)
UPDATE jobs
SET state = 'running', attempt = attempt + 1, leased_until = ?, lease_token = ?, updated_at = ?
FROM claimed
WHERE jobs.id = claimed.id
RETURNING jobs.id, jobs.kind, jobs.queue, jobs.args, jobs.attempt, jobs.max_attempts,
    jobs.timeout_ms, jobs.parent_job_id, jobs.tags, jobs.scheduled_at, jobs.metadata`, classFilter)

	var rows []jobRow
	if err := d.db.WithContext(ctx).Raw(sql, args...).Scan(&rows).Error; err != nil {
		return nil, fmt.Errorf("jobs: postgres dequeue: %w", err)
	}

	claimed := make([]RawJob, 0, len(rows))
	for _, r := range rows {
		// The RETURNING attempt is already incremented by the UPDATE. lease_token
		// is deliberately absent from RETURNING: this call minted it.
		rj, err := rawFromRow(r, r.Attempt, token)
		if err != nil {
			return nil, err
		}
		claimed = append(claimed, rj)
	}
	return claimed, nil
}
