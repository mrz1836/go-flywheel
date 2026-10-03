//go:build integration

package core

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// TestMigrateUpgradesALegacySchemaInPlacePostgres is the library-owned upgrade
// on PostgreSQL, run the way the runbook says to run it against a live
// database: concurrent index builds, so no writer is blocked, and a lock timeout,
// so the column additions cannot queue every query behind a long transaction.
func TestMigrateUpgradesALegacySchemaInPlacePostgres(t *testing.T) {
	t.Parallel()
	upgradeSuite(t, newBarePostgres, func(db *gorm.DB) error {
		return MigrateWithOptions(db, MigrateOpts{Concurrently: true, LockTimeout: 5 * time.Second})
	})
}

// TestHostOwnedUpgradePostgres is the host-owned upgrade — the path both real
// hosts take: their migration tool applies the new columns and table (here,
// AutoMigrate over Models stands in for the generated migration), then the
// deploy step installs indexes and storage parameters through the runtime's
// helpers.
func TestHostOwnedUpgradePostgres(t *testing.T) {
	t.Parallel()
	upgradeSuite(t, newBarePostgres, func(db *gorm.DB) error {
		ctx := context.Background()
		if err := db.AutoMigrate(Models()...); err != nil {
			return err
		}
		if err := InstallIndexesWithOptions(ctx, db, IndexOpts{Concurrently: true, LockTimeout: 5 * time.Second}); err != nil {
			return err
		}
		return InstallStorageParameters(ctx, db)
	})
}

// TestInspectSchemaReportsWhatAnOlderSchemaLacksPostgres mirrors the SQLite
// parity check through information_schema's dialect.
func TestInspectSchemaReportsWhatAnOlderSchemaLacksPostgres(t *testing.T) {
	t.Parallel()
	db := newBarePostgres(t)
	installLegacySchema(t, db)
	drift, err := InspectSchema(context.Background(), db)
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{
		"job_runs.kind", "job_runs.queue", "job_runs.queue_wait_ms", "job_runs.job_state",
		"job_runs.superseded", "job_run_finishes (table)", "job_stats_hourly (table)",
	}, schemaDriftNames(drift))

	current, err := InspectSchema(context.Background(), NewPostgresIsolatedDB(t))
	require.NoError(t, err)
	assert.Empty(t, current)
}

// TestRunnerFailsFastOnAnOutdatedSchemaPostgres is the deploy-order guard on
// PostgreSQL.
func TestRunnerFailsFastOnAnOutdatedSchemaPostgres(t *testing.T) {
	t.Parallel()
	db := newBarePostgres(t)
	installLegacySchema(t, db)
	r, err := NewRunner(RunnerConfig{
		DB: db, Driver: NewPostgresDriver(db), Registry: NewRegistry(), Queues: []string{defaultQueue},
	})
	require.NoError(t, err)
	err = r.RunUntilIdle(context.Background())
	require.ErrorIs(t, err, ErrSchemaOutdated)
	assert.Contains(t, err.Error(), "job_runs.superseded")
}

// TestAHandRunConcurrentIndexPassesInspectIndexesPostgres proves the definition
// a host gets from running CREATE INDEX CONCURRENTLY by hand — the runbook's
// path for a large live table — is accepted as parity, not reported as drift.
func TestAHandRunConcurrentIndexPassesInspectIndexesPostgres(t *testing.T) {
	t.Parallel()
	db := NewPostgresIsolatedDB(t)
	ctx := context.Background()
	set, err := IndexSet("postgres")
	require.NoError(t, err)
	for _, idx := range set {
		if idx.Name != "jobs_finished" {
			continue
		}
		require.NoError(t, db.Exec(`DROP INDEX `+idx.Name).Error)
		require.NoError(t, db.Exec(concurrentIndexDDL(idx.DDL)).Error, "the CONCURRENTLY form runs outside a transaction")
	}
	drift, err := InspectIndexes(ctx, db)
	require.NoError(t, err)
	assert.Empty(t, drift, "an index built concurrently has the same definition")
}

// TestAnInvalidIndexIsDriftAndConcurrentlyRepairsItPostgres covers the leftover
// of a failed concurrent build: present, definition matching, but invalid. It
// must be reported — a check comparing definitions alone would pass it — and a
// re-install with Concurrently must replace it without blocking writers.
func TestAnInvalidIndexIsDriftAndConcurrentlyRepairsItPostgres(t *testing.T) {
	t.Parallel()
	db := NewPostgresIsolatedDB(t)
	ctx := context.Background()

	// Mark the index invalid the way a failed concurrent build leaves it. The
	// catalog flag is the whole difference, so flipping it is a faithful fixture.
	require.NoError(t, db.Exec(`UPDATE pg_index SET indisvalid = false
		WHERE indexrelid = (SELECT c.oid FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
		                    WHERE n.nspname = current_schema() AND c.relname = 'jobs_finished')`).Error)

	drift, err := InspectIndexes(ctx, db)
	require.NoError(t, err)
	require.Len(t, drift, 1)
	assert.Equal(t, "jobs_finished", drift[0].Name)
	assert.True(t, drift[0].Invalid)

	err = InstallIndexes(ctx, db)
	var de *IndexDriftError
	require.True(t, errors.As(err, &de), "without Concurrently an invalid index is reported, not touched")

	require.NoError(t, InstallIndexesWithOptions(ctx, db, IndexOpts{Concurrently: true}))
	drift, err = InspectIndexes(ctx, db)
	require.NoError(t, err)
	assert.Empty(t, drift, "the concurrent re-install rebuilt it valid")
}

// TestAnInvalidCorrectnessIndexIsNotRepairedUninvitedPostgres pins the
// boundary: dropping a unique index, even an invalid one, gives up a guarantee,
// so Concurrently alone reports it and only Reconcile rebuilds it.
func TestAnInvalidCorrectnessIndexIsNotRepairedUninvitedPostgres(t *testing.T) {
	t.Parallel()
	db := NewPostgresIsolatedDB(t)
	ctx := context.Background()
	require.NoError(t, db.Exec(`UPDATE pg_index SET indisvalid = false
		WHERE indexrelid = (SELECT c.oid FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
		                    WHERE n.nspname = current_schema() AND c.relname = 'jobs_unique_key')`).Error)

	err := InstallIndexesWithOptions(ctx, db, IndexOpts{Concurrently: true})
	require.ErrorIs(t, err, ErrIndexDrift)
	require.NoError(t, InstallIndexesWithOptions(ctx, db, IndexOpts{Reconcile: true}))
	drift, err := InspectIndexes(ctx, db)
	require.NoError(t, err)
	assert.Empty(t, drift)
}

// TestLockTimeoutBoundsTheWaitPostgres proves LockTimeout does what the runbook
// relies on: a schema statement that cannot get its lock fails fast instead of
// queueing every later query behind it.
func TestLockTimeoutBoundsTheWaitPostgres(t *testing.T) {
	t.Parallel()
	db := NewPostgresIsolatedDB(t)
	ctx := context.Background()
	require.NoError(t, db.Exec(`DROP INDEX jobs_finished`).Error)

	// Hold a lock that conflicts with CREATE INDEX's SHARE lock — ROW EXCLUSIVE,
	// what any open writing transaction holds — in another transaction. Not
	// ACCESS EXCLUSIVE: that would also block the catalog read InspectIndexes
	// makes first, and the test would measure the wrong wait.
	holder := db.Begin()
	require.NoError(t, holder.Error)
	t.Cleanup(func() { holder.Rollback() })
	require.NoError(t, holder.Exec(`LOCK TABLE jobs IN ROW EXCLUSIVE MODE`).Error)

	start := time.Now()
	err := InstallIndexesWithOptions(ctx, db, IndexOpts{LockTimeout: 200 * time.Millisecond})
	require.Error(t, err, "the build cannot take its lock while another transaction holds the table")
	assert.Contains(t, err.Error(), "lock timeout")
	assert.Less(t, time.Since(start), 5*time.Second, "and it gave up promptly")
}
