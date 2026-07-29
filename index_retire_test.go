package flywheel

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// migratedSchemaWithRetiredJobsParent stands up the full runtime schema and then
// re-creates jobs_parent — the index jobs_parent_state supersedes — so the
// installer and the parity check face the exact straggler a database upgraded
// across the retirement still carries. jobs_parent is intentionally absent from
// IndexSet, so it cannot be built from there: reconstructing the pre-upgrade state
// is the one case a test in this package spells an index the runtime no longer
// declares, and it is the historical definition, captured, not invented.
func migratedSchemaWithRetiredJobsParent(t *testing.T) *gorm.DB {
	t.Helper()
	db := newDB(t)
	require.NoError(t, db.Exec(
		`CREATE INDEX IF NOT EXISTS jobs_parent ON jobs (parent_job_id) WHERE parent_job_id IS NOT NULL`,
	).Error, "reconstruct the retired index a pre-upgrade database still carries")
	return db
}

// TestInstallIndexesDropsTheRetiredJobsParent proves the auto-drop: a database
// still carrying jobs_parent has it removed on the next InstallIndexes, with no
// opt-in and without disturbing the covering jobs_parent_state.
func TestInstallIndexesDropsTheRetiredJobsParent(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db := migratedSchemaWithRetiredJobsParent(t)
	require.True(t, sqliteHasIndex(t, db, "jobs_parent"), "precondition: the retired index is present")

	require.NoError(t, InstallIndexes(ctx, db), "a retired straggler is dropped, not reported as an error")

	assert.False(t, sqliteHasIndex(t, db, "jobs_parent"), "the retired index is dropped")
	assert.True(t, sqliteHasIndex(t, db, "jobs_parent_state"), "the covering index survives")

	// Idempotent: a second run has nothing to drop and still succeeds.
	require.NoError(t, InstallIndexes(ctx, db))
}

// TestMigrateDropsTheRetiredJobsParent proves the drop is uniform across the two
// install paths: Migrate removes the straggler too, not only InstallIndexes.
func TestMigrateDropsTheRetiredJobsParent(t *testing.T) {
	t.Parallel()
	db := migratedSchemaWithRetiredJobsParent(t)

	require.NoError(t, Migrate(db))
	assert.False(t, sqliteHasIndex(t, db, "jobs_parent"), "Migrate drops the retired index")
	assert.True(t, sqliteHasIndex(t, db, "jobs_parent_state"))
}

// TestInspectIndexesSurfacesTheRetiredStraggler proves the read-only parity check
// reports a surviving retired index as an informational straggler — Retired set,
// Expected empty — so a host that hand-applies DDL and never calls InstallIndexes
// still sees it, even once the auto-drop is gone. It is not counted as drift: it
// carries no expected definition and never becomes an IndexDriftError.
func TestInspectIndexesSurfacesTheRetiredStraggler(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db := migratedSchemaWithRetiredJobsParent(t)

	drift, err := InspectIndexes(ctx, db)
	require.NoError(t, err)
	require.Len(t, drift, 1, "the schema is at parity except for the retired straggler")
	assert.Equal(t, "jobs_parent", drift[0].Name)
	assert.True(t, drift[0].Retired, "a superseded index is reported as retired, not drift")
	assert.NotEmpty(t, drift[0].Installed, "the straggler's installed definition is reported")
	assert.Empty(t, drift[0].Expected, "a retired index has no expected definition")
}

// TestInspectIndexesIsCleanOnAFreshInstallWithNoStraggler is the companion: a
// freshly migrated schema — which never had jobs_parent — reports no straggler,
// so the retired check adds no noise to the common case.
func TestInspectIndexesIsCleanOnAFreshInstallWithNoStraggler(t *testing.T) {
	t.Parallel()
	drift, err := InspectIndexes(context.Background(), newDB(t))
	require.NoError(t, err)
	assert.Empty(t, drift, "a fresh install carries no retired straggler")
}
