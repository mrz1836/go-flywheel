package flywheel

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// hostOwnedSchemaWithDriftedStateIndex builds a host-owned schema (tables from
// Models, no indexes) and then installs jobs_state under its runtime name with a
// deliberately wrong definition: the runtime declares it partial
// (WHERE deleted_at IS NULL) and this installs it non-partial. The installer then
// sees exactly one drifted index among eight absent ones.
//
// It deliberately uses a synthetic drift rather than a historical one — the
// pre-v0.7.1 reproductions live in the definition-level suite — so this file
// exercises the opts mechanism, not a particular incident.
func hostOwnedSchemaWithDriftedStateIndex(t *testing.T) *gorm.DB {
	t.Helper()
	db := newBareSQLite(t)
	require.NoError(t, db.AutoMigrate(Models()...), "the host's loader owns the tables")
	require.NoError(t, db.Exec(`CREATE INDEX jobs_state ON jobs (state)`).Error,
		"a non-partial jobs_state drifts from the runtime's partial declaration")
	return db
}

// TestInstallIndexesReportsDriftByDefault proves the default does not silently
// succeed over a drifted index: it creates what is absent, leaves the drifted
// index untouched, and returns an IndexDriftError naming it, what is installed,
// and what was expected.
func TestInstallIndexesReportsDriftByDefault(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db := hostOwnedSchemaWithDriftedStateIndex(t)

	err := InstallIndexes(ctx, db)
	require.Error(t, err, "a drifted index must not read as a successful install")
	require.ErrorIs(t, err, ErrIndexDrift)

	var drift *IndexDriftError
	require.ErrorAs(t, err, &drift)
	require.Len(t, drift.Drift, 1)
	assert.Equal(t, "jobs_state", drift.Drift[0].Name)
	assert.NotEmpty(t, drift.Drift[0].Installed, "the report carries the installed definition")
	assert.NotEmpty(t, drift.Drift[0].Expected, "the report carries the expected definition")

	// The message names the index and both definitions, so a host acts without
	// reading library source.
	msg := err.Error()
	assert.Contains(t, msg, "jobs_state")
	assert.Contains(t, msg, "installed:")
	assert.Contains(t, msg, "expected:")

	// The absent indexes were still created: the default makes progress on what it
	// can safely touch and reports only what it cannot.
	for _, name := range migrateIndexes {
		if name == "jobs_state" {
			continue
		}
		assert.True(t, sqliteHasIndex(t, db, name),
			"absent index %q must be created even when another has drifted", name)
	}

	// The drifted index was left exactly as it was — the default never drops.
	remaining, err := InspectIndexes(ctx, db)
	require.NoError(t, err)
	require.Len(t, remaining, 1, "only the drifted index remains unreconciled")
	assert.Equal(t, "jobs_state", remaining[0].Name)
}

// TestInstallIndexesWithReconcileFixesDrift proves the opt-in path rebuilds the
// drifted index in place and reaches parity, and that it is idempotent.
func TestInstallIndexesWithReconcileFixesDrift(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db := hostOwnedSchemaWithDriftedStateIndex(t)

	// The default reports drift — established so the reconcile below is fixing a
	// real one, not passing vacuously.
	require.ErrorIs(t, InstallIndexes(ctx, db), ErrIndexDrift)

	require.NoError(t, InstallIndexesWithOptions(ctx, db, IndexOpts{Reconcile: true}),
		"reconcile drops and recreates the drifted index rather than failing")

	drift, err := InspectIndexes(ctx, db)
	require.NoError(t, err)
	assert.Empty(t, drift, "reconcile brings every index to the runtime's definition")

	require.NoError(t, InstallIndexesWithOptions(ctx, db, IndexOpts{Reconcile: true}),
		"a second reconcile run finds nothing to do")
}

// TestInstallIndexesWithReconcileOnAFreshSchema proves reconcile is not a
// different install: on a schema with no indexes it just creates them all, with
// no drift to rebuild.
func TestInstallIndexesWithReconcileOnAFreshSchema(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db := newBareSQLite(t)
	require.NoError(t, db.AutoMigrate(Models()...))

	require.NoError(t, InstallIndexesWithOptions(ctx, db, IndexOpts{Reconcile: true}))
	for _, name := range migrateIndexes {
		assert.True(t, sqliteHasIndex(t, db, name), "reconcile must still create the absent index %q", name)
	}
	drift, err := InspectIndexes(ctx, db)
	require.NoError(t, err)
	assert.Empty(t, drift)
}

// TestMigrateReportsIndexDriftByDefault proves the fail-loud default is uniform:
// Migrate, not only InstallIndexes, refuses to report success over a drifted
// index, and MigrateOpts.Reconcile heals it on the same terms.
func TestMigrateReportsIndexDriftByDefault(t *testing.T) {
	t.Parallel()
	db := hostOwnedSchemaWithDriftedStateIndex(t)

	err := Migrate(db)
	require.ErrorIs(t, err, ErrIndexDrift, "Migrate fails loudly on drift by default, like InstallIndexes")

	require.NoError(t, MigrateWithOptions(db, MigrateOpts{Reconcile: true}),
		"MigrateOpts.Reconcile rebuilds the drifted index")
	drift, err := InspectIndexes(context.Background(), db)
	require.NoError(t, err)
	assert.Empty(t, drift)
}

// TestInstallIndexesWithOptionsRejectsANilDB guards the precondition on the
// options entry point too.
func TestInstallIndexesWithOptionsRejectsANilDB(t *testing.T) {
	t.Parallel()
	require.Error(t, InstallIndexesWithOptions(context.Background(), nil, IndexOpts{Reconcile: true}))
}

// TestReconcileIndexSurfacesDropAndCreateErrors covers the two failure points of a
// single in-place rebuild: the DROP must find its index, and the CREATE must
// succeed. The transaction rolls back on either, so a correctness-bearing index is
// never left absent.
func TestReconcileIndexSurfacesDropAndCreateErrors(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	t.Run("a failed drop is surfaced", func(t *testing.T) {
		t.Parallel()
		db := newBareSQLite(t)
		require.NoError(t, db.AutoMigrate(Models()...))
		// No index named "ghost" exists, so the unguarded DROP INDEX fails.
		err := reconcileIndex(ctx, db, IndexDrift{Name: "ghost", Expected: `CREATE INDEX ghost ON jobs (state)`})
		require.ErrorContains(t, err, "drop")
	})

	t.Run("a failed create is surfaced", func(t *testing.T) {
		t.Parallel()
		db := newBareSQLite(t)
		require.NoError(t, db.AutoMigrate(Models()...))
		require.NoError(t, db.Exec(`CREATE INDEX tmp_idx ON jobs (state)`).Error)
		// The drop succeeds; the create fails on a table that does not exist.
		err := reconcileIndex(ctx, db, IndexDrift{Name: "tmp_idx", Expected: `CREATE INDEX tmp_idx ON no_such_table (col)`})
		require.ErrorContains(t, err, "create")
	})
}

// TestApplyIndexesSurfacesErrors covers applyIndexes' three failure exits: the
// initial inspection, the create of an absent index, and a propagated reconcile
// failure.
func TestApplyIndexesSurfacesErrors(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	t.Run("a failed inspection aborts the install", func(t *testing.T) {
		t.Parallel()
		db := newDB(t)
		closeDB(t, db)
		require.Error(t, InstallIndexes(ctx, db), "a catalog read failure aborts before any create")
	})

	t.Run("a failed create of an absent index is surfaced", func(t *testing.T) {
		t.Parallel()
		db := newBareSQLite(t)
		require.NoError(t, db.AutoMigrate(Models()...)) // tables, no indexes
		sqlDB, err := db.DB()
		require.NoError(t, err)
		sqlDB.SetMaxOpenConns(1)
		require.NoError(t, db.Exec(`PRAGMA query_only = ON`).Error)

		require.ErrorContains(t, InstallIndexes(ctx, db), "create index",
			"an absent index whose CREATE fails aborts the install")
	})

	t.Run("a failed reconcile propagates", func(t *testing.T) {
		t.Parallel()
		db := newDB(t) // full schema: every runtime index present
		// Re-create jobs_state non-partial so it is the single drifted index, with
		// every other index already present, so applyIndexes reaches only the reconcile.
		require.NoError(t, db.Exec(`DROP INDEX jobs_state`).Error)
		require.NoError(t, db.Exec(`CREATE INDEX jobs_state ON jobs (state)`).Error)

		sqlDB, err := db.DB()
		require.NoError(t, err)
		sqlDB.SetMaxOpenConns(1)
		require.NoError(t, db.Exec(`PRAGMA query_only = ON`).Error)

		require.Error(t, InstallIndexesWithOptions(ctx, db, IndexOpts{Reconcile: true}),
			"a reconcile that cannot rebuild the drifted index surfaces the failure")
	})
}
