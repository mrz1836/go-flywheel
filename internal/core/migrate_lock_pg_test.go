//go:build integration

package core

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// reopenSchema opens a second, independent pool on the schema db is bound to —
// what another process migrating the same database looks like.
func reopenSchema(t *testing.T, db *gorm.DB) *gorm.DB {
	t.Helper()
	var schema string
	require.NoError(t, db.Raw(`SELECT current_schema()`).Scan(&schema).Error)
	other, err := gorm.Open(postgres.Open(withSearchPath(requirePostgresDSN(t), schema)),
		&gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	require.NoError(t, err)
	t.Cleanup(func() {
		if sqlDB, dbErr := other.DB(); dbErr == nil {
			_ = sqlDB.Close()
		}
	})
	return other
}

// migrationLockFree reports whether another session could take db's schema
// migration lock right now. One statement takes and releases it, so the probe
// never leaves it held.
func migrationLockFree(t *testing.T, db *gorm.DB) bool {
	t.Helper()
	var free bool
	require.NoError(t, db.Raw(`SELECT CASE WHEN pg_try_advisory_lock(?, hashtext(current_schema()))
		THEN pg_advisory_unlock(?, hashtext(current_schema())) ELSE false END`,
		migrationLockClass, migrationLockClass).Scan(&free).Error)
	return free
}

// migrateWithin runs MigrateWithOptions and fails the test if it has not
// returned within limit — a deadlock surfaces as a failure, not a hung suite.
func migrateWithin(t *testing.T, db *gorm.DB, opts MigrateOpts, limit time.Duration) error {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- MigrateWithOptions(db, opts) }()
	select {
	case err := <-done:
		return err
	case <-time.After(limit):
		t.Fatalf("MigrateWithOptions did not return within %s", limit)
		return nil
	}
}

// TestMigrateLockTimeoutHoldsNoLockPastItsOwnStepPostgres is finding 11's
// regression test. With LockTimeout set, every step used to run in one
// transaction, so the ACCESS EXCLUSIVE lock the job_runs ADD COLUMNs take was
// held until the whole migration committed — including while a later statement
// waited, up to the timeout, for another table. Here that later statement is the
// storage-parameter ALTER on jobs, queued behind a SHARE UPDATE EXCLUSIVE lock
// (what a running VACUUM holds). While it waits, job_runs must already be
// committed and readable by a session that will not wait for it.
func TestMigrateLockTimeoutHoldsNoLockPastItsOwnStepPostgres(t *testing.T) {
	t.Parallel()
	db := newBarePostgres(t)
	installLegacySchema(t, db)

	holder := db.Begin()
	require.NoError(t, holder.Error)
	t.Cleanup(func() { holder.Rollback() })
	require.NoError(t, holder.Exec(`LOCK TABLE jobs IN SHARE UPDATE EXCLUSIVE MODE`).Error)

	done := make(chan error, 1)
	go func() { done <- MigrateWithOptions(db, MigrateOpts{LockTimeout: 3 * time.Second}) }()

	require.Eventually(t, func() bool {
		var waiting int64
		_ = db.Raw(`SELECT count(*) FROM pg_locks WHERE relation = to_regclass('jobs') AND NOT granted`).
			Scan(&waiting).Error
		return waiting > 0
	}, 5*time.Second, 10*time.Millisecond, "the migrate reaches the storage step and queues behind the holder on jobs")

	var kindColumns int64
	require.NoError(t, db.Raw(`SELECT count(*) FROM information_schema.columns
		WHERE table_schema = current_schema() AND table_name = 'job_runs' AND column_name = 'kind'`).
		Scan(&kindColumns).Error)
	assert.EqualValues(t, 1, kindColumns, "job_runs' columns committed with their own step, before the wait")
	require.NoError(t, db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Exec(`SET LOCAL lock_timeout = '200ms'`).Error; err != nil {
			return err
		}
		var n int64
		return tx.Raw(`SELECT count(*) FROM job_runs`).Scan(&n).Error
	}), "job_runs is not locked while the migrate waits on another table")

	err := <-done
	require.Error(t, err, "the storage step could not take its lock within the timeout")
	assert.Contains(t, err.Error(), "SQLSTATE 55P03", "a lock_not_available failure")

	require.NoError(t, holder.Rollback().Error)
	require.NoError(t, MigrateWithOptions(db, MigrateOpts{LockTimeout: 3 * time.Second}),
		"the retry completes what the failed run left undone")
	gaps, err := InspectSchema(context.Background(), db)
	require.NoError(t, err)
	assert.Empty(t, gaps)
	assert.True(t, migrationLockFree(t, db), "the failed and the retried migration both released the lock")
}

// TestConcurrentMigrationsOfOneSchemaBothSucceedPostgres starts two migrations
// of one fresh schema at the same instant from two pools — two `flywheel serve`
// processes starting together. Unserialized, both race the same CREATE TABLE
// and one fails on a duplicate object; under the migration lock the second runs
// after the first and finds nothing to do. One pool is capped at a single
// connection, so the migration provably runs entirely on the connection that
// holds the lock.
func TestConcurrentMigrationsOfOneSchemaBothSucceedPostgres(t *testing.T) {
	t.Parallel()
	for round := range 3 {
		first := newBarePostgres(t)
		second := reopenSchema(t, first)
		sqlDB, err := second.DB()
		require.NoError(t, err)
		sqlDB.SetMaxOpenConns(1)

		start := make(chan struct{})
		errs := make(chan error, 2)
		for _, db := range []*gorm.DB{first, second} {
			go func() {
				<-start
				errs <- MigrateWithOptions(db, MigrateOpts{Concurrently: true})
			}()
		}
		close(start)
		for range 2 {
			select {
			case err := <-errs:
				require.NoError(t, err, "round %d", round)
			case <-time.After(time.Minute):
				t.Fatalf("round %d: concurrent migrations did not finish", round)
			}
		}

		gaps, err := InspectSchema(context.Background(), first)
		require.NoError(t, err)
		assert.Empty(t, gaps, "round %d", round)
		drift, err := InspectIndexes(context.Background(), first)
		require.NoError(t, err)
		assert.Empty(t, drift, "round %d", round)
		assert.True(t, migrationLockFree(t, first), "round %d: the lock was released", round)
	}
}

// TestMigrationWaitsForTheLockHolderPostgres proves the serialization directly:
// while another session holds the schema's migration lock, a Migrate does not
// proceed, and once the holder releases it the Migrate completes.
func TestMigrationWaitsForTheLockHolderPostgres(t *testing.T) {
	t.Parallel()
	db := newBarePostgres(t)
	sqlDB, err := db.DB()
	require.NoError(t, err)
	ctx := context.Background()
	holder, err := sqlDB.Conn(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = holder.Close() })
	_, err = holder.ExecContext(ctx, `SELECT pg_advisory_lock($1, hashtext(current_schema()))`, migrationLockClass)
	require.NoError(t, err)

	done := make(chan error, 1)
	go func() { done <- MigrateWithOptions(db, MigrateOpts{}) }()
	select {
	case err := <-done:
		t.Fatalf("Migrate ran while another session held the migration lock: %v", err)
	case <-time.After(500 * time.Millisecond):
	}
	assert.False(t, db.Migrator().HasTable("jobs"), "nothing was created while it waited")

	_, err = holder.ExecContext(ctx, `SELECT pg_advisory_unlock($1, hashtext(current_schema()))`, migrationLockClass)
	require.NoError(t, err)
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(time.Minute):
		t.Fatal("Migrate did not proceed after the lock was released")
	}
	assert.True(t, db.Migrator().HasTable("jobs"))
}

// TestMigrationLockIsReleasedWhenTheMigrationFailsPostgres covers the error path:
// a migration that fails inside the lock — here on an invalid correctness index,
// which it reports rather than rebuilds — still releases it, so the next
// migration is not blocked behind a pooled connection that kept it.
func TestMigrationLockIsReleasedWhenTheMigrationFailsPostgres(t *testing.T) {
	t.Parallel()
	db := NewPostgresIsolatedDB(t)
	assert.True(t, migrationLockFree(t, db), "a successful migration released the lock")
	require.NoError(t, db.Exec(`UPDATE pg_index SET indisvalid = false
		WHERE indexrelid = (SELECT c.oid FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
		                    WHERE n.nspname = current_schema() AND c.relname = 'jobs_unique_key')`).Error)

	err := MigrateWithOptions(db, MigrateOpts{Concurrently: true})
	require.ErrorIs(t, err, ErrIndexDrift)
	assert.True(t, migrationLockFree(t, db), "a failed migration released the lock")
}

// TestMigrateConcurrentlyRepairsAnInvalidIndexPostgres is the path `flywheel
// serve` and `flywheel doctor` now take: a Migrate with Concurrently rebuilds a
// performance index an interrupted concurrent build left invalid, where a plain
// Migrate fails the start with an IndexDriftError naming the fix.
func TestMigrateConcurrentlyRepairsAnInvalidIndexPostgres(t *testing.T) {
	t.Parallel()
	db := NewPostgresIsolatedDB(t)
	require.NoError(t, db.Exec(`UPDATE pg_index SET indisvalid = false
		WHERE indexrelid = (SELECT c.oid FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
		                    WHERE n.nspname = current_schema() AND c.relname = 'jobs_finished')`).Error)

	err := Migrate(db)
	var de *IndexDriftError
	require.True(t, errors.As(err, &de), "a plain Migrate reports the invalid index")
	assert.Contains(t, err.Error(), "invalid")
	assert.Contains(t, err.Error(), "flywheel migrate --concurrently")

	require.NoError(t, MigrateWithOptions(db, MigrateOpts{Concurrently: true}))
	drift, err := InspectIndexes(context.Background(), db)
	require.NoError(t, err)
	assert.Empty(t, drift, "the concurrent migrate rebuilt it valid")
}

// TestMigrateOfACurrentSchemaRunsNoDDLPostgres pins that an up-to-date schema is
// left alone: no spurious ALTER COLUMN ... SET DEFAULT on the jsonb columns, no
// storage ALTER whose values already hold.
func TestMigrateOfACurrentSchemaRunsNoDDLPostgres(t *testing.T) {
	t.Parallel()
	assertMigrateOfACurrentSchemaRunsNoDDL(t, NewPostgresIsolatedDB(t))
}

// TestMigrateRunsOnAPoolOfOneConnectionPostgres runs the full live-database
// upgrade — concurrent builds and per-step lock-timeout transactions — through a
// pool capped at one connection. Every statement must use the connection that
// holds the migration lock; one that reached for the pool would wait forever.
func TestMigrateRunsOnAPoolOfOneConnectionPostgres(t *testing.T) {
	t.Parallel()
	db := newBarePostgres(t)
	installLegacySchema(t, db)
	sqlDB, err := db.DB()
	require.NoError(t, err)
	sqlDB.SetMaxOpenConns(1)

	require.NoError(t, migrateWithin(t, db,
		MigrateOpts{Concurrently: true, LockTimeout: 5 * time.Second}, time.Minute))
	gaps, err := InspectSchema(context.Background(), db)
	require.NoError(t, err)
	assert.Empty(t, gaps)
	drift, err := InspectIndexes(context.Background(), db)
	require.NoError(t, err)
	assert.Empty(t, drift)
}
