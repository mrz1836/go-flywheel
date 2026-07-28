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

// NewPostgresDriver returns a Driver backed by a PostgreSQL connection, with the
// default batching options.
func NewPostgresDriver(db *gorm.DB) Driver {
	return NewPostgresDriverWithOptions(db, DriverOpts{})
}

// NewPostgresDriverWithOptions returns a Driver backed by a PostgreSQL
// connection, with opts controlling its batching. A zero opts is exactly
// NewPostgresDriver.
func NewPostgresDriverWithOptions(db *gorm.DB, opts DriverOpts) Driver {
	return &postgresDriver{baseDriver{db: db, opts: opts}}
}

// Sweep reclaims expired leases in bounded batches, each taken with
// FOR UPDATE SKIP LOCKED.
//
// SKIP LOCKED is what makes two concurrent sweeps safe: each takes a disjoint
// batch instead of one blocking on the other's row locks for a whole
// transaction. It does not make concurrent sweeps correct *as a deployment* —
// the scheduler is a singleton by design — it makes an accidental second one a
// throughput cost rather than an outage.
func (d *postgresDriver) Sweep(ctx context.Context, now time.Time) (int, error) {
	return d.sweep(ctx, now, d.reclaimExpired)
}

// reclaimExpired takes one batch of expired leases and returns them to
// available, reporting the ids it reclaimed.
//
// It is one statement: the CTE selects and row-locks up to limit expired rows
// with SKIP LOCKED, and the UPDATE reclaims them, RETURNING the ids the caller
// needs for the run-stub update. Splitting it into a SELECT and an UPDATE would
// hold the locks across a round trip for no benefit.
//
// The predicate carries deleted_at IS NULL explicitly. A raw statement gets no
// GORM soft-delete scope, and without it this dialect would reclaim
// soft-deleted running jobs while SQLite's Model-scoped path does not — a
// dialect divergence in a reclaim path, introduced by the rewrite that exists to
// make the dialects explicit.
func (d *postgresDriver) reclaimExpired(
	ctx context.Context, tx *gorm.DB, now time.Time, limit int,
) ([]string, error) {
	const sql = `
WITH expired AS (
    SELECT id FROM jobs
    WHERE state = 'running' AND leased_until < ? AND deleted_at IS NULL
    ORDER BY leased_until
    LIMIT ?
    FOR UPDATE SKIP LOCKED
)
UPDATE jobs
SET state = 'available', leased_until = NULL, lease_token = NULL, updated_at = ?
FROM expired
WHERE jobs.id = expired.id
RETURNING jobs.id`

	var ids []string
	if err := tx.WithContext(ctx).Raw(sql, now, limit, now).Scan(&ids).Error; err != nil {
		return nil, fmt.Errorf("jobs: reclaim jobs: %w", err)
	}
	return ids, nil
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
