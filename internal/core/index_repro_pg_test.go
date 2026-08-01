//go:build integration

package core

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestInstallerCatchesTheHistoricalDriftPostgres is the PostgreSQL half of the
// reproduction. It matters most here: the incidents were on PostgreSQL databases,
// and this is the dialect whose catalog rewrites the definition on the way in, so
// a comparison that reduced too far would miss the drift exactly where it
// happened. The name check is shown green on both stale indexes, and the
// definition check is shown to catch them and the default install to fail.
func TestInstallerCatchesTheHistoricalDriftPostgres(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db := newBarePostgres(t)
	seedSchemaWithHistoricalDrift(t, db)

	assert.True(t, pgHasIndex(t, db, "jobs_ready"),
		"the stale jobs_ready exists by name — a presence check passes against it")
	assert.True(t, pgHasIndex(t, db, "idx_jobs_deleted_at"),
		"the stale idx_jobs_deleted_at exists by name — a presence check passes against it")

	drift, err := InspectIndexes(ctx, db)
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"jobs_ready", "idx_jobs_deleted_at"}, driftNames(drift),
		"the definition check reports exactly the two historically-drifted indexes and no others")

	err = InstallIndexes(ctx, db)
	require.ErrorIs(t, err, ErrIndexDrift,
		"the installer no longer reports success on the historical drift")
	assert.Contains(t, err.Error(), "jobs_ready")
	assert.Contains(t, err.Error(), "idx_jobs_deleted_at")

	require.NoError(t, InstallIndexesWithOptions(ctx, db, IndexOpts{Reconcile: true}))
	after, err := InspectIndexes(ctx, db)
	require.NoError(t, err)
	assert.Empty(t, after, "reconcile brings the historical drift to the runtime's definitions")
}
