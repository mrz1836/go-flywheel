package core

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestStorageParameterSetTargetsOnlyTheChurningTable pins which tables get
// parameters and which deliberately do not.
//
// jobs is the only table with update churn: every job transitions
// available → running → terminal, plus a pair per retry. job_runs is
// append-only and job_periodics holds one row per schedule, so a lower
// fillfactor on either would reserve free space on every page for updates that
// never arrive.
func TestStorageParameterSetTargetsOnlyTheChurningTable(t *testing.T) {
	t.Parallel()

	set, err := StorageParameterSet("postgres")
	require.NoError(t, err)
	require.NotEmpty(t, set)

	for _, p := range set {
		assert.Equal(t, "jobs", p.Table, "only jobs has the update churn these settings act on")
		assert.Contains(t, strings.ToLower(p.DDL), "alter table jobs set (")
	}
}

// TestStorageParameterSetCarriesTheMeasuredSettings pins the two settings the
// benchmark measured, so a change to either is deliberate and re-measured
// rather than incidental.
func TestStorageParameterSetCarriesTheMeasuredSettings(t *testing.T) {
	t.Parallel()

	ddl, err := StorageParameters("postgres")
	require.NoError(t, err)
	joined := strings.ToLower(strings.Join(ddl, "\n"))

	assert.Contains(t, joined, "autovacuum_vacuum_scale_factor = 0.02",
		"the scale factor is the load-bearing half: it is what turns 5 autovacuum cycles into 29")
	assert.Contains(t, joined, "autovacuum_vacuum_cost_limit = 1000",
		"firing more often helps nothing if each pass is throttled to the default budget")
	assert.Contains(t, joined, "fillfactor = 80")
}

// TestStorageParameterSetIsEmptyOnSQLite is the documented SQLite behavior for
// a PostgreSQL-only feature: explicitly nothing, not a silent gap and not an
// error a caller has to special-case.
func TestStorageParameterSetIsEmptyOnSQLite(t *testing.T) {
	t.Parallel()

	set, err := StorageParameterSet("sqlite")
	require.NoError(t, err, "SQLite has no equivalent and needs none; that is not an error")
	assert.Empty(t, set)

	ddl, err := StorageParameters("sqlite")
	require.NoError(t, err)
	assert.Empty(t, ddl)
}

// TestStorageParameterSetRejectsAnUnsupportedDialect proves the gate reuses the
// runtime's existing sentinel rather than inventing a second vocabulary for the
// same question.
func TestStorageParameterSetRejectsAnUnsupportedDialect(t *testing.T) {
	t.Parallel()

	_, err := StorageParameterSet("mysql")
	require.ErrorIs(t, err, ErrUnsupportedDialect)
	assert.Contains(t, err.Error(), "mysql")

	_, err = StorageParameters("mysql")
	require.ErrorIs(t, err, ErrUnsupportedDialect, "StorageParameters fails identically to the set")
}

// TestInstallStorageParametersIsANoOpOnSQLite proves the install path is safe to
// call unconditionally, which is what lets Migrate call it without a dialect
// branch of its own.
func TestInstallStorageParametersIsANoOpOnSQLite(t *testing.T) {
	t.Parallel()
	db := newDB(t)

	require.NoError(t, InstallStorageParameters(context.Background(), db))
	// Idempotent, and still a no-op the second time.
	require.NoError(t, InstallStorageParameters(context.Background(), db))
}

// TestInstallStorageParametersRejectsANilDB matches InstallIndexes' guard.
func TestInstallStorageParametersRejectsANilDB(t *testing.T) {
	t.Parallel()

	require.Error(t, InstallStorageParameters(context.Background(), nil))
}

// TestMigrateSucceedsOnSQLiteWithStorageParameters guards the integration
// point: Migrate now has a storage step, and SQLite must pass straight through
// it rather than failing on DDL it cannot express.
func TestMigrateSucceedsOnSQLiteWithStorageParameters(t *testing.T) {
	t.Parallel()
	db := newBareSQLite(t)

	require.NoError(t, Migrate(db))
	require.NoError(t, Migrate(db), "Migrate stays idempotent with the storage step in it")
}
