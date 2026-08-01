package core

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/glebarez/sqlite"
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

// SQLiteOpts configures SQLite driver construction. It embeds the batching
// DriverOpts so a host can set the batch sizes and the pragma-check policy in one
// value. The zero value takes the default batch sizes and runs the pragma check.
type SQLiteOpts struct {
	DriverOpts
	// SkipPragmaCheck disables the connection verification. Use it only when the
	// connection is configured by other means and the check's queries are
	// unwelcome — a shared pool the host has already hardened, say.
	SkipPragmaCheck bool
}

// NewSQLiteDriver returns a Driver backed by a SQLite connection, with the
// default batching options. It verifies the connection's pragmas and logs any
// failure through slog.Default(), returning the driver regardless so its
// one-argument, error-free signature is preserved.
//
// The connection should be opened with the _txlock=immediate DSN parameter so the
// write lock is taken up front: a transaction that starts deferred and upgrades
// to a write mid-claim can fail with SQLITE_BUSY even against a single writer.
// _txlock is a DSN parameter PRAGMA cannot report, so the check warns about it
// rather than failing. A host that wants a missing pragma to be fatal should call
// NewSQLiteDriverWithOptions.
func NewSQLiteDriver(db *gorm.DB) Driver {
	if err := checkSQLitePragmas(db); err != nil {
		slog.Default().Error(
			"flywheel: sqlite connection check failed; the serialized claim can deadlock under load — "+
				"use NewSQLiteDriverWithOptions to make this fatal, or see the embedder checklist in docs/RUNBOOK.md",
			"error", err,
		)
	}
	return &sqliteDriver{baseDriver{db: db, opts: DriverOpts{}}}
}

// NewSQLiteDriverWithOptions returns a Driver backed by a SQLite connection, with
// opts controlling batching and the pragma-check policy. Unless
// opts.SkipPragmaCheck is set it verifies the connection is configured for
// concurrent job claiming, returning ErrSQLitePragma (naming the offending
// pragma) on a hard failure.
//
// The runtime's SQLite claim is a serialized SELECT-then-UPDATE that must take
// the write lock up front; without BEGIN IMMEDIATE semantics a concurrent reader
// can hold a shared lock the claim then cannot upgrade, deadlocking under exactly
// the load the runtime is for. The check reports a misconfigured connection at
// construction rather than as an intermittent failure later.
func NewSQLiteDriverWithOptions(db *gorm.DB, opts SQLiteOpts) (Driver, error) {
	if !opts.SkipPragmaCheck {
		if err := checkSQLitePragmas(db); err != nil {
			return nil, err
		}
	}
	return &sqliteDriver{baseDriver{db: db, opts: opts.DriverOpts}}, nil
}

// checkSQLitePragmas verifies the connection pragmas the serialized claim relies
// on, returning ErrSQLitePragma (naming the pragma) on a hard failure. A file
// database must be in WAL so readers do not block the claim's writer; an
// in-memory database — which cannot use WAL — is exempt from that one requirement
// but, like any database, still needs a positive busy_timeout to absorb the brief
// claim lock and a safe synchronous level so a committed outcome survives a crash.
// foreign_keys and _txlock are warned about (file databases only), never fatal.
func checkSQLitePragmas(db *gorm.DB) error {
	journalMode, err := scanPragmaString(db, "journal_mode")
	if err != nil {
		return fmt.Errorf("%w: reading journal_mode: %w", ErrSQLitePragma, err)
	}
	inMemory := strings.EqualFold(journalMode, "memory")
	if !inMemory && !strings.EqualFold(journalMode, "wal") {
		return fmt.Errorf(
			"%w: journal_mode is %q, want wal — open with the _pragma=journal_mode(WAL) DSN parameter",
			ErrSQLitePragma, journalMode,
		)
	}

	busyTimeout, err := scanPragmaInt(db, "busy_timeout")
	if err != nil {
		return fmt.Errorf("%w: reading busy_timeout: %w", ErrSQLitePragma, err)
	}
	if busyTimeout <= 0 {
		return fmt.Errorf(
			"%w: busy_timeout is %d, want > 0 — open with the _pragma=busy_timeout(5000) DSN parameter",
			ErrSQLitePragma, busyTimeout,
		)
	}

	synchronous, err := scanPragmaInt(db, "synchronous")
	if err != nil {
		return fmt.Errorf("%w: reading synchronous: %w", ErrSQLitePragma, err)
	}
	// 1 = NORMAL, 2 = FULL are safe; 0 = OFF risks losing a committed outcome on
	// power loss, which for a durable job runtime is exactly the failure it exists
	// to prevent.
	if synchronous != 1 && synchronous != 2 {
		return fmt.Errorf(
			"%w: synchronous is %d, want NORMAL(1) or FULL(2), not OFF(0)",
			ErrSQLitePragma, synchronous,
		)
	}

	if !inMemory {
		warnSQLitePragmas(db)
	}
	return nil
}

// warnSQLitePragmas logs the non-fatal connection concerns for a file database:
// foreign_keys off (a host FK onto job_runs would be unenforced; the runtime
// declares none) and a DSN missing _txlock=immediate (the claim's write lock is
// taken lazily and can hit SQLITE_BUSY under load). Neither refuses the
// connection — a false negative that rejects a working connection is a worse
// failure than a documented requirement — and neither fires for an in-memory
// database, where a single writer makes both moot.
func warnSQLitePragmas(db *gorm.DB) {
	if foreignKeys, err := scanPragmaInt(db, "foreign_keys"); err == nil && foreignKeys == 0 {
		slog.Default().Warn(
			"flywheel: sqlite foreign_keys is off; a host FK onto job_runs will be unenforced (the runtime declares none)",
		)
	}

	// _txlock is a DSN parameter PRAGMA cannot report, so it is read off the
	// dialector when that is the glebarez driver and skipped otherwise.
	if d, ok := db.Dialector.(*sqlite.Dialector); ok {
		if !strings.Contains(strings.ToLower(d.DSN), "_txlock=immediate") {
			slog.Default().Warn(
				"flywheel: sqlite DSN lacks _txlock=immediate; the claim's write lock is taken lazily and can " +
					"hit SQLITE_BUSY under load — see the embedder checklist in docs/RUNBOOK.md",
			)
		}
	}
}

// scanPragmaString reads a text-valued PRAGMA from db.
func scanPragmaString(db *gorm.DB, pragma string) (string, error) {
	var v string
	err := db.Raw("PRAGMA " + pragma).Row().Scan(&v)
	return v, err
}

// scanPragmaInt reads an integer-valued PRAGMA from db.
func scanPragmaInt(db *gorm.DB, pragma string) (int, error) {
	var v int
	err := db.Raw("PRAGMA " + pragma).Row().Scan(&v)
	return v, err
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
