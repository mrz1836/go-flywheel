package core

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// schemaWithForeignJobsIndex stands up the full runtime schema and then creates
// jobs_parent on jobs — an index the runtime declares in neither IndexSet nor
// retiredIndexNames. It stands in for any index a host adds on a runtime table
// for its own purposes: the installer must leave it strictly alone. jobs_parent
// is a convenient stand-in because it is spelled from a real historical
// definition, but nothing about it is special now that retiredIndexNames is
// empty — it is just a name the runtime does not own.
func schemaWithForeignJobsIndex(t *testing.T) *gorm.DB {
	t.Helper()
	db := newDB(t)
	require.NoError(t, db.Exec(
		`CREATE INDEX IF NOT EXISTS jobs_parent ON jobs (parent_job_id) WHERE parent_job_id IS NOT NULL`,
	).Error, "create a foreign index the runtime neither declares nor retires")
	return db
}

// TestInstallIndexesLeavesAForeignIndexAlone proves the safety property behind an
// empty retiredIndexNames: an index the runtime declares in neither IndexSet nor
// retiredIndexNames is never dropped. The installer touches only what it owns, so
// a host's own index on a runtime table survives every InstallIndexes.
func TestInstallIndexesLeavesAForeignIndexAlone(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db := schemaWithForeignJobsIndex(t)
	require.True(t, sqliteHasIndex(t, db, "jobs_parent"), "precondition: the foreign index is present")

	require.NoError(t, InstallIndexes(ctx, db), "a foreign index is left alone, not an error")

	assert.True(t, sqliteHasIndex(t, db, "jobs_parent"), "the foreign index survives — the runtime does not own it")
	assert.True(t, sqliteHasIndex(t, db, "jobs_parent_state"), "the runtime's own index is present")

	// Idempotent: a second run still leaves the foreign index alone.
	require.NoError(t, InstallIndexes(ctx, db))
	assert.True(t, sqliteHasIndex(t, db, "jobs_parent"))
}

// TestMigrateLeavesAForeignIndexAlone proves the same across the other install
// path: Migrate does not drop a foreign index either.
func TestMigrateLeavesAForeignIndexAlone(t *testing.T) {
	t.Parallel()
	db := schemaWithForeignJobsIndex(t)

	require.NoError(t, Migrate(db))
	assert.True(t, sqliteHasIndex(t, db, "jobs_parent"), "Migrate leaves a foreign index alone")
	assert.True(t, sqliteHasIndex(t, db, "jobs_parent_state"))
}

// TestInspectIndexesIgnoresAForeignIndex proves the read-only parity check reports
// nothing for a foreign index: it is not in the desired set, so it is not drift,
// and retiredIndexNames is empty, so it is not a straggler. An empty result means
// the runtime's own schema is at parity, and a host's extra index is invisible to
// the check rather than a false positive.
func TestInspectIndexesIgnoresAForeignIndex(t *testing.T) {
	t.Parallel()
	drift, err := InspectIndexes(context.Background(), schemaWithForeignJobsIndex(t))
	require.NoError(t, err)
	assert.Empty(t, drift, "a foreign index is neither drift nor a retired straggler")
}

// TestInspectIndexesIsCleanOnAFreshInstall is the companion: a freshly migrated
// schema reports no drift and no straggler, so the parity check is quiet in the
// common case.
func TestInspectIndexesIsCleanOnAFreshInstall(t *testing.T) {
	t.Parallel()
	drift, err := InspectIndexes(context.Background(), newDB(t))
	require.NoError(t, err)
	assert.Empty(t, drift, "a fresh install is at parity")
}
