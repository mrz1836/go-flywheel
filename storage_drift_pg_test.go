//go:build integration

package flywheel

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// driftByParam indexes a storage-parameter drift slice by parameter name.
func driftByParam(drift []StorageParameterDrift) map[string]StorageParameterDrift {
	out := make(map[string]StorageParameterDrift, len(drift))
	for _, d := range drift {
		out[d.Parameter] = d
	}
	return out
}

// TestInspectStorageParametersReportsNoDriftOnAFreshInstallPostgres proves the
// parity check is clean against a schema the runtime just installed — the same
// promise InspectIndexes makes for indexes.
func TestInspectStorageParametersReportsNoDriftOnAFreshInstallPostgres(t *testing.T) {
	t.Parallel()
	db := NewPostgresIsolatedDB(t)

	drift, err := InspectStorageParameters(context.Background(), db)
	require.NoError(t, err)
	assert.Empty(t, drift, "a freshly migrated jobs table carries exactly the runtime's parameters")
}

// TestInspectStorageParametersReportsDriftPostgres proves the detection: a jobs
// table with a wrong fillfactor and no autovacuum tuning reports each parameter,
// naming the table, the parameter, the installed value, and the expected one.
// This is the check a host with a hand-written ALTER runs in CI.
func TestInspectStorageParametersReportsDriftPostgres(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db := NewPostgresIsolatedDB(t)

	// A host-style table: a different fillfactor and none of the autovacuum tuning.
	require.NoError(t, db.Exec(
		`ALTER TABLE jobs RESET (autovacuum_vacuum_scale_factor, autovacuum_vacuum_cost_limit)`,
	).Error)
	require.NoError(t, db.Exec(`ALTER TABLE jobs SET (fillfactor = 100)`).Error)

	drift, err := InspectStorageParameters(ctx, db)
	require.NoError(t, err)

	byParam := driftByParam(drift)

	ff, ok := byParam["fillfactor"]
	require.True(t, ok, "a wrong fillfactor must be reported")
	assert.Equal(t, "jobs", ff.Table)
	assert.Equal(t, "100", ff.Installed)
	assert.Equal(t, "80", ff.Expected)

	scale, ok := byParam["autovacuum_vacuum_scale_factor"]
	require.True(t, ok, "an unset autovacuum parameter must be reported")
	assert.Empty(t, scale.Installed, "an absent parameter reports an empty Installed")
	assert.Equal(t, "0.02", scale.Expected)

	_, ok = byParam["autovacuum_vacuum_cost_limit"]
	assert.True(t, ok, "the second unset autovacuum parameter must be reported too")
}

// TestInstallStorageParametersConvergesFillfactorPostgres is the corrected A7: a
// storage parameter does not silently drift the way an index does, because the
// install unconditionally SETs it. A jobs table left at fillfactor 100 converges
// to 80 on the next InstallStorageParameters, with no error and no opt-in — so
// there is no fail-loud default to add, only the detection above.
func TestInstallStorageParametersConvergesFillfactorPostgres(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db := NewPostgresIsolatedDB(t)

	require.NoError(t, db.Exec(`ALTER TABLE jobs SET (fillfactor = 100)`).Error)
	requireFillfactor(t, db, "100")

	// The plain install self-heals: no reconcile flag, no error.
	require.NoError(t, InstallStorageParameters(ctx, db))
	requireFillfactor(t, db, "80")

	drift, err := InspectStorageParameters(ctx, db)
	require.NoError(t, err)
	assert.Empty(t, drift, "after the convergent install the table is at parity")
}

// requireFillfactor asserts jobs currently carries the given fillfactor.
func requireFillfactor(t *testing.T, db *gorm.DB, want string) {
	t.Helper()
	opts, err := readReloptions(context.Background(), db, "jobs")
	require.NoError(t, err)
	assert.Equal(t, want, opts["fillfactor"])
}
