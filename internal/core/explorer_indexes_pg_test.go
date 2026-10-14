//go:build integration

package core

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestInstalledExplorerIndexesInspectCleanPostgres proves the four explorer
// indexes, the GIN tags index included, install concurrently and inspect clean
// on PostgreSQL; that an absent one is drift only to a host that claims it; that
// a drifted one is reported and rebuilt, and an invalid one repaired, by
// IndexSet's rules; and that an extra index is inspected whatever table it is on.
func TestInstalledExplorerIndexesInspectCleanPostgres(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db := NewPostgresIsolatedDB(t)
	set := explorerSet(t, "postgres")
	require.Len(t, set, 4)

	require.NoError(t, InstallIndexesWithOptions(ctx, db, IndexOpts{ExtraIndexes: set, Concurrently: true}))
	for _, idx := range set {
		assert.True(t, pgHasIndex(t, db, idx.Name), "%s is created", idx.Name)
	}
	drift, err := InspectIndexesWithOptions(ctx, db, InspectIndexOpts{ExtraIndexes: set})
	require.NoError(t, err)
	assert.Empty(t, drift, "the catalog's rendering of each, the GIN index's included, matches its DDL")
	drift, err = InspectIndexes(ctx, db)
	require.NoError(t, err)
	assert.Empty(t, drift, "InspectIndexes says nothing about a host's explorer indexes")

	require.NoError(t, db.Exec(`DROP INDEX jobs_explore_tags`).Error)
	drift, err = InspectIndexes(ctx, db)
	require.NoError(t, err)
	assert.Empty(t, drift, "an entry a host did not install is never drift to InspectIndexes")
	drift, err = InspectIndexesWithOptions(ctx, db, InspectIndexOpts{ExtraIndexes: set})
	require.NoError(t, err)
	require.Len(t, drift, 1)
	assert.Equal(t, "jobs_explore_tags", drift[0].Name)
	assert.Empty(t, drift[0].Installed, "a claimed entry that is absent is reported")
	require.NoError(t, MigrateWithOptions(db, MigrateOpts{ExtraIndexes: set, Concurrently: true}))
	assert.True(t, pgHasIndex(t, db, "jobs_explore_tags"), "MigrateOpts.ExtraIndexes creates it")

	require.NoError(t, db.Exec(`DROP INDEX jobs_explore_kind`).Error)
	require.NoError(t, db.Exec(`CREATE INDEX jobs_explore_kind ON jobs (kind)`).Error)
	drift, err = InspectIndexesWithOptions(ctx, db, InspectIndexOpts{ExtraIndexes: set})
	require.NoError(t, err)
	require.Len(t, drift, 1)
	assert.Equal(t, "jobs_explore_kind", drift[0].Name)
	assert.NotEmpty(t, drift[0].Installed)
	err = InstallIndexesWithOptions(ctx, db, IndexOpts{ExtraIndexes: set})
	require.ErrorIs(t, err, ErrIndexDrift, "a drifted extra fails the install unless reconciled")
	require.NoError(t, InstallIndexesWithOptions(ctx, db, IndexOpts{ExtraIndexes: set, Reconcile: true}))

	require.NoError(t, db.Exec(`UPDATE pg_index SET indisvalid = false
		WHERE indexrelid = (SELECT c.oid FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
		                    WHERE n.nspname = current_schema() AND c.relname = 'jobs_explore_queue')`).Error)
	drift, err = InspectIndexesWithOptions(ctx, db, InspectIndexOpts{ExtraIndexes: set})
	require.NoError(t, err)
	require.Len(t, drift, 1)
	assert.True(t, drift[0].Invalid, "an invalid extra is reported as such")
	require.NoError(t, InstallIndexesWithOptions(ctx, db, IndexOpts{ExtraIndexes: set, Concurrently: true}),
		"Concurrently rebuilds an invalid performance extra")
	drift, err = InspectIndexesWithOptions(ctx, db, InspectIndexOpts{ExtraIndexes: set})
	require.NoError(t, err)
	assert.Empty(t, drift)

	// The catalog read finds an extra index by its name, so a host's index on a
	// table of its own is installed and inspected like the runtime's.
	require.NoError(t, db.Exec(`CREATE TABLE host_things (id text)`).Error)
	hostIndex := Index{
		Name: "host_things_id", Kind: IndexPerformance, Table: "host_things",
		DDL: `CREATE INDEX IF NOT EXISTS host_things_id ON host_things (id)`,
	}
	require.NoError(t, InstallIndexesWithOptions(ctx, db, IndexOpts{ExtraIndexes: []Index{hostIndex}}))
	drift, err = InspectIndexesWithOptions(ctx, db, InspectIndexOpts{ExtraIndexes: []Index{hostIndex}})
	require.NoError(t, err)
	assert.Empty(t, drift, "an extra index on another table is read back by name")
}
