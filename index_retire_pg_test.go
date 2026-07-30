//go:build integration

package flywheel

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestInstallIndexesDropsTheRetiredJobsParentPostgres proves the drop resolves and
// executes on PostgreSQL, where DROP INDEX IF EXISTS runs against the connection's
// search-path schema rather than a bare in-memory database.
func TestInstallIndexesDropsTheRetiredJobsParentPostgres(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db := NewPostgresIsolatedDB(t)

	// Reconstruct the pre-upgrade straggler: jobs_parent is retired and absent from
	// IndexSet, so it is spelled here, from its historical definition.
	require.NoError(t, db.Exec(
		`CREATE INDEX IF NOT EXISTS jobs_parent ON jobs (parent_job_id) WHERE parent_job_id IS NOT NULL`,
	).Error)

	var before int64
	require.NoError(t, db.Raw(
		`SELECT count(*) FROM pg_indexes WHERE schemaname = current_schema() AND indexname = 'jobs_parent'`,
	).Scan(&before).Error)
	require.EqualValues(t, 1, before, "precondition: the retired index is present")

	require.NoError(t, InstallIndexes(ctx, db), "the retired straggler is dropped, not an error")

	var after, covering int64
	require.NoError(t, db.Raw(
		`SELECT count(*) FROM pg_indexes WHERE schemaname = current_schema() AND indexname = 'jobs_parent'`,
	).Scan(&after).Error)
	require.NoError(t, db.Raw(
		`SELECT count(*) FROM pg_indexes WHERE schemaname = current_schema() AND indexname = 'jobs_parent_state'`,
	).Scan(&covering).Error)
	assert.EqualValues(t, 0, after, "the retired index is dropped on PostgreSQL")
	assert.EqualValues(t, 1, covering, "the covering jobs_parent_state survives")

	drift, err := InspectIndexes(ctx, db)
	require.NoError(t, err)
	assert.Empty(t, drift, "the drop leaves the schema at parity with no straggler")
}
