package flywheel

import (
	"context"
	"fmt"
	"time"

	"gorm.io/gorm"
)

// sqliteDriver is the SQLite Driver implementation. It claims jobs with a
// serialized SELECT-then-UPDATE inside a BEGIN IMMEDIATE transaction; it is
// correct only at Concurrency 1, which NewRunner enforces (FR-039).
type sqliteDriver struct {
	baseDriver
}

// NewSQLiteDriver returns a Driver backed by a SQLite connection. The
// connection should be opened with the _txlock=immediate DSN parameter so the
// write lock is taken up front (research §3).
func NewSQLiteDriver(db *gorm.DB) Driver {
	return &sqliteDriver{baseDriver{db: db}}
}

// Dequeue claims up to limit ready jobs. SQLite has no SKIP LOCKED, so the
// claim is a SELECT of the highest-priority rows followed by an UPDATE of their
// ids, both inside one transaction.
//
//nolint:gocognit,gocyclo // the select-then-claim transaction is one cohesive unit
func (d *sqliteDriver) Dequeue(
	ctx context.Context, queues []string, kind ExecutorKind, limit int, lease time.Duration,
) ([]RawJob, error) {
	if limit <= 0 || len(queues) == 0 {
		return nil, nil
	}

	var claimed []RawJob
	err := d.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		now := ClockFrom(ctx).Now(ctx)

		query := tx.Model(&jobRow{}).
			Where("state IN ?", claimableStates).
			Where("scheduled_at <= ?", now).
			Where("queue IN ?", queues).
			Order("priority, scheduled_at").
			Limit(limit)
		if rv := runOnValues(kind); rv != nil {
			query = query.Where("run_on IN ?", rv)
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
			"updated_at":   now,
		}).Error; err != nil {
			return fmt.Errorf("claim jobs: %w", err)
		}

		claimed = make([]RawJob, 0, len(rows))
		for _, r := range rows {
			rj, convErr := rawFromRow(r, r.Attempt+1)
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
