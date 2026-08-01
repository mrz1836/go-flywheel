//go:build integration

package core

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// reloptionsFor reads a table's storage parameters back from the catalog, which
// is the only authority on what was actually installed.
func reloptionsFor(t *testing.T, db *gorm.DB, table string) string {
	t.Helper()
	var opts string
	require.NoError(t, db.Raw(`
		SELECT coalesce(array_to_string(c.reloptions, ','), '')
		FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = current_schema() AND c.relname = ?`, table).Scan(&opts).Error)
	return opts
}

// TestMigrateInstallsTheStorageParametersPostgres proves Migrate's new step
// reaches the database, read back from pg_class rather than inferred from the
// DDL constant this package also owns.
func TestMigrateInstallsTheStorageParametersPostgres(t *testing.T) {
	t.Parallel()
	db := NewPostgresIsolatedDB(t)

	// NewPostgresIsolatedDB migrates, so the parameters are already in place.
	opts := strings.ToLower(reloptionsFor(t, db, "jobs"))
	assert.Contains(t, opts, "autovacuum_vacuum_scale_factor=0.02",
		"the scale factor is what turns 5 autovacuum cycles into 29 at a 1M working set")
	assert.Contains(t, opts, "autovacuum_vacuum_cost_limit=1000")
	assert.Contains(t, opts, "fillfactor=80")
}

// TestStorageParametersLeaveTheAppendOnlyTablesAlonePostgres pins the negative
// half. An append-only table gains nothing from either setting, and a lower
// fillfactor on one is pure waste — free space reserved on every page for
// updates that never come.
func TestStorageParametersLeaveTheAppendOnlyTablesAlonePostgres(t *testing.T) {
	t.Parallel()
	db := NewPostgresIsolatedDB(t)

	assert.Empty(t, reloptionsFor(t, db, "job_runs"),
		"job_runs is append-only and must carry no storage parameters")
	assert.Empty(t, reloptionsFor(t, db, "job_periodics"),
		"job_periodics holds one row per schedule and must carry no storage parameters")
}

// TestInstallStorageParametersIsIdempotentPostgres proves the host-owned path is
// safe on every deploy, which is the same promise InstallIndexes makes.
func TestInstallStorageParametersIsIdempotentPostgres(t *testing.T) {
	t.Parallel()
	db := NewPostgresIsolatedDB(t)
	ctx := context.Background()

	before := reloptionsFor(t, db, "jobs")
	require.NoError(t, InstallStorageParameters(ctx, db))
	require.NoError(t, InstallStorageParameters(ctx, db))

	assert.Equal(t, before, reloptionsFor(t, db, "jobs"),
		"re-applying the parameters leaves the same reloptions")
}

// TestInstallStorageParametersReachesAHostOwnedSchemaPostgres covers the path
// the consumers actually use.
//
// Both real hosts own their schema: their migration tool creates the tables and
// they call the runtime's install helpers. Without this entry point the tuning
// would only ever reach a database installed by Migrate — which neither of them
// uses — so the measured benefit would apply to nobody.
func TestInstallStorageParametersReachesAHostOwnedSchemaPostgres(t *testing.T) {
	t.Parallel()
	db := NewPostgresIsolatedDB(t)
	ctx := context.Background()

	// Strip the parameters to simulate a host-created table, then install them
	// the way a host-owned schema would.
	require.NoError(t, db.Exec(
		`ALTER TABLE jobs RESET (autovacuum_vacuum_scale_factor, autovacuum_vacuum_cost_limit, fillfactor)`,
	).Error)
	require.Empty(t, reloptionsFor(t, db, "jobs"), "the reset left a bare table")

	require.NoError(t, InstallStorageParameters(ctx, db))

	opts := strings.ToLower(reloptionsFor(t, db, "jobs"))
	assert.Contains(t, opts, "fillfactor=80")
	assert.Contains(t, opts, "autovacuum_vacuum_scale_factor=0.02")
}
