//go:build integration

package flywheel

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestInstallIndexesLeavesAForeignIndexAlonePostgres proves the leave-alone
// property resolves and holds on PostgreSQL, where the catalog read runs against
// the connection's search-path schema rather than a bare in-memory database. An
// index the runtime declares in neither IndexSet nor retiredIndexNames survives
// InstallIndexes and is never reported as drift.
func TestInstallIndexesLeavesAForeignIndexAlonePostgres(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	db := NewPostgresIsolatedDB(t)

	// A foreign index on a runtime table: the runtime does not own it, so the
	// installer must leave it strictly alone.
	require.NoError(t, db.Exec(
		`CREATE INDEX IF NOT EXISTS jobs_parent ON jobs (parent_job_id) WHERE parent_job_id IS NOT NULL`,
	).Error)

	var before int64
	require.NoError(t, db.Raw(
		`SELECT count(*) FROM pg_indexes WHERE schemaname = current_schema() AND indexname = 'jobs_parent'`,
	).Scan(&before).Error)
	require.EqualValues(t, 1, before, "precondition: the foreign index is present")

	require.NoError(t, InstallIndexes(ctx, db), "a foreign index is left alone, not an error")

	var after, covering int64
	require.NoError(t, db.Raw(
		`SELECT count(*) FROM pg_indexes WHERE schemaname = current_schema() AND indexname = 'jobs_parent'`,
	).Scan(&after).Error)
	require.NoError(t, db.Raw(
		`SELECT count(*) FROM pg_indexes WHERE schemaname = current_schema() AND indexname = 'jobs_parent_state'`,
	).Scan(&covering).Error)
	assert.EqualValues(t, 1, after, "the foreign index survives on PostgreSQL")
	assert.EqualValues(t, 1, covering, "the runtime's own jobs_parent_state is present")

	drift, err := InspectIndexes(ctx, db)
	require.NoError(t, err)
	assert.Empty(t, drift, "a foreign index is neither drift nor a straggler")
}
