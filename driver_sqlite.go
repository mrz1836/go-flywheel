package flywheel

import (
	"context"
	"fmt"
	"time"

	"github.com/mrz1836/go-foundation/models"
	"gorm.io/gorm"
)

// sqliteDriver is the SQLite Driver implementation. It claims jobs with a
// serialized SELECT-then-UPDATE inside a BEGIN IMMEDIATE transaction. SQLite has
// no SKIP LOCKED, so nothing stops a second claimant from selecting the same
// row: it is correct only at Concurrency 1, which NewRunner enforces with
// ErrSQLiteConcurrency.
type sqliteDriver struct {
	baseDriver
}

// NewSQLiteDriver returns a Driver backed by a SQLite connection, with the
// default batching options. The connection should be opened with the
// _txlock=immediate DSN parameter so the write lock is taken up front: a
// transaction that starts deferred and upgrades to a write mid-claim can fail
// with SQLITE_BUSY even against a single writer.
func NewSQLiteDriver(db *gorm.DB) Driver {
	return NewSQLiteDriverWithOptions(db, DriverOpts{})
}

// NewSQLiteDriverWithOptions returns a Driver backed by a SQLite connection,
// with opts controlling its batching. A zero opts is exactly NewSQLiteDriver.
func NewSQLiteDriverWithOptions(db *gorm.DB, opts DriverOpts) Driver {
	return &sqliteDriver{baseDriver{db: db, opts: opts}}
}

// Sweep reclaims expired leases in bounded batches.
//
// SQLite has no SKIP LOCKED, and needs none: it serializes writers, so there is
// never a second transaction holding rows this one would want to skip. Batching
// still earns its place here — it bounds how long the single writer is held, and
// on a busy database that is the whole contention story.
func (d *sqliteDriver) Sweep(ctx context.Context, now time.Time) (int, error) {
	return d.sweep(ctx, now, d.reclaimExpired)
}

// reclaimExpired takes one batch of expired leases and returns them to
// available, reporting the ids it reclaimed.
//
// It is a SELECT of the oldest expired leases followed by an UPDATE of their
// ids, both inside the caller's transaction — structurally the same shape as
// this dialect's Dequeue, for the same reason: without a RETURNING-capable
// UPDATE the ids have to be read before they are written.
//
// It runs through Model(&jobRow{}), so GORM's soft-delete scope applies and a
// soft-deleted running job is left alone. The PostgreSQL path spells that same
// condition out by hand, because a raw statement inherits no scope.
func (d *sqliteDriver) reclaimExpired(
	ctx context.Context, tx *gorm.DB, now time.Time, limit int,
) ([]string, error) {
	var ids []string
	if err := tx.WithContext(ctx).Model(&jobRow{}).
		Where("state = ? AND leased_until < ?", string(StateRunning), now).
		Order("leased_until").
		Limit(limit).
		Pluck("id", &ids).Error; err != nil {
		return nil, fmt.Errorf("jobs: find expired leases: %w", err)
	}
	if len(ids) == 0 {
		return nil, nil
	}
	if err := tx.WithContext(ctx).Model(&jobRow{}).
		Where("id IN ?", ids).Updates(reclaimUpdate(now)).Error; err != nil {
		return nil, fmt.Errorf("jobs: reclaim jobs: %w", err)
	}
	return ids, nil
}

// Dequeue claims up to limit ready jobs. SQLite has no SKIP LOCKED, so the
// claim is a SELECT of the highest-priority rows followed by an UPDATE of their
// ids, both inside one transaction.
//
//nolint:gocognit,gocyclo // the select-then-claim transaction is one cohesive unit
func (d *sqliteDriver) Dequeue(
	ctx context.Context, queues []string, class ExecutorClass, claimAny bool, limit int, lease time.Duration,
) ([]RawJob, error) {
	if limit <= 0 || len(queues) == 0 {
		return nil, nil
	}

	var claimed []RawJob
	err := d.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		now := models.ClockFrom(ctx).Now(ctx)
		// One token per claim call, stamped on every row in the batch. See
		// RawJob.LeaseToken for why per-row generation would buy nothing.
		token := models.NewID()

		query := tx.Model(&jobRow{}).
			Where("state IN ?", claimableStates).
			Where("scheduled_at <= ?", now).
			Where("queue IN ?", queues).
			Order("priority, scheduled_at").
			Limit(limit)
		if !claimAny {
			query = query.Where("executor_class IN ?", []string{string(class), ""})
		}

		var rows []jobRow
		if err := query.Find(&rows).Error; err != nil {
			return fmt.Errorf("select claimable jobs: %w", err)
		}
		if len(rows) == 0 {
			return nil
		}

		ids := make([]string, len(rows))
		for i := range rows {
			ids[i] = rows[i].ID
		}
		if err := tx.Model(&jobRow{}).Where("id IN ?", ids).Updates(map[string]any{
			"state":        string(StateRunning),
			"attempt":      gorm.Expr("attempt + 1"),
			"leased_until": now.Add(lease),
			"lease_token":  token,
			"updated_at":   now,
		}).Error; err != nil {
			return fmt.Errorf("claim jobs: %w", err)
		}

		claimed = make([]RawJob, 0, len(rows))
		for _, r := range rows {
			// rows were selected before the claim, so the attempt and the token
			// both come from this call rather than from the row.
			rj, convErr := rawFromRow(r, r.Attempt+1, token)
			if convErr != nil {
				return convErr
			}
			claimed = append(claimed, rj)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("jobs: sqlite dequeue: %w", err)
	}
	return claimed, nil
}
