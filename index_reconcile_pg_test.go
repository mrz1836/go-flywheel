//go:build integration

package flywheel

import (
	"context"
	"testing"

	"github.com/mrz1836/go-foundation/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// driftRuntimeIndexPG drops a runtime index and recreates it under the same name
// with a wrong definition, so InspectIndexes sees exactly one drift on an
// otherwise-migrated schema.
func driftRuntimeIndexPG(t *testing.T, db *gorm.DB, name, wrongDDL string) {
	t.Helper()
	require.NoError(t, db.Exec(`DROP INDEX `+name).Error, "drop the correct %q", name)
	require.NoError(t, db.Exec(wrongDDL).Error, "install a drifted %q", name)
}

// TestInstallIndexesReportsDriftByDefaultPostgres is the PostgreSQL half of the
// fail-loud default: a drifted index is reported, not silently kept, on the
// dialect whose catalog rewrites the definition most.
func TestInstallIndexesReportsDriftByDefaultPostgres(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db := NewPostgresIsolatedDB(t)
	driftRuntimeIndexPG(t, db, "jobs_state", `CREATE INDEX jobs_state ON jobs (state)`)

	err := InstallIndexes(ctx, db)
	require.ErrorIs(t, err, ErrIndexDrift)

	var drift *IndexDriftError
	require.ErrorAs(t, err, &drift)
	require.Len(t, drift.Drift, 1)
	assert.Equal(t, "jobs_state", drift.Drift[0].Name)
	assert.Contains(t, err.Error(), "jobs_state")
}

// TestInstallIndexesReconcilesDriftPostgres proves the opt-in rebuild reaches
// parity on PostgreSQL, where the drop and recreate run under a real
// ACCESS EXCLUSIVE lock inside the reconcile transaction.
func TestInstallIndexesReconcilesDriftPostgres(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db := NewPostgresIsolatedDB(t)
	driftRuntimeIndexPG(t, db, "jobs_state", `CREATE INDEX jobs_state ON jobs (state)`)

	require.ErrorIs(t, InstallIndexes(ctx, db), ErrIndexDrift)
	require.NoError(t, InstallIndexesWithOptions(ctx, db, IndexOpts{Reconcile: true}))

	drift, err := InspectIndexes(ctx, db)
	require.NoError(t, err)
	assert.Empty(t, drift, "reconcile brings jobs_state back to the runtime's partial definition")
}

// TestReconcileKeepsAUniqueIndexEnforcingPostgres is the correctness half of the
// reconcile design: a drifted unique index is rebuilt without ever ceasing to
// enforce. The drop and recreate share one transaction, so the ACCESS EXCLUSIVE
// lock is held across the window and no duplicate can slip in between them; after
// reconcile the uniqueness the runtime depends on is intact.
func TestReconcileKeepsAUniqueIndexEnforcingPostgres(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db := NewPostgresIsolatedDB(t)

	// Drift jobs_unique_key by widening it to a non-partial unique index (the
	// runtime declares it partial on unique_key IS NOT NULL).
	driftRuntimeIndexPG(t, db, "jobs_unique_key",
		`CREATE UNIQUE INDEX jobs_unique_key ON jobs (unique_key) WHERE unique_key IS NOT NULL AND state IS NOT NULL`)
	require.ErrorIs(t, InstallIndexes(ctx, db), ErrIndexDrift)

	require.NoError(t, InstallIndexesWithOptions(ctx, db, IndexOpts{Reconcile: true}))
	drift, err := InspectIndexes(ctx, db)
	require.NoError(t, err)
	assert.Empty(t, drift)

	// The rebuilt index still rejects a duplicate unique_key.
	uk := "reconcile-idempotency-probe"
	first := newJobRowWithUniqueKey(models.NewID(), &uk)
	require.NoError(t, db.Create(&first).Error, "the first enqueue always succeeds")
	dup := uk
	second := newJobRowWithUniqueKey(models.NewID(), &dup)
	require.Error(t, db.Create(&second).Error,
		"the reconciled jobs_unique_key must still reject a duplicate")
}
