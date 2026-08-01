//go:build integration

package core

import (
	"testing"

	"github.com/mrz1836/go-foundation/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestPostgresIsolatedDBInstallsEveryProductionIndex makes the Postgres fixture
// itself the subject, against the same independent oracle the SQLite half uses.
// It matters more here than there: this is the only dialect that runs the FOR
// UPDATE SKIP LOCKED claim, so a reduced schema under this fixture means the
// exactly-once concurrency tests exercise a plan production never uses.
func TestPostgresIsolatedDBInstallsEveryProductionIndex(t *testing.T) {
	db := NewPostgresIsolatedDB(t)

	for _, table := range migrateTables {
		assert.True(t, db.Migrator().HasTable(table),
			"NewPostgresIsolatedDB must create %q", table)
	}
	for _, name := range migrateIndexes {
		assert.True(t, pgHasIndex(t, db, name),
			"NewPostgresIsolatedDB must install %q — the claim plan is only real with it", name)
	}
}

// TestPostgresIsolatedDBEnforcesOneRunPerAttempt proves job_runs_job_attempt
// rejects on Postgres, not merely that it exists. The two rows carry different
// primary keys, so the unique index on (job_id, attempt) is the only constraint
// that can reject the second insert.
func TestPostgresIsolatedDBEnforcesOneRunPerAttempt(t *testing.T) {
	db := NewPostgresIsolatedDB(t)

	first := newJobRunRow(models.NewID(), "job-1", 1)
	require.NoError(t, db.Create(&first).Error)

	second := newJobRunRow(models.NewID(), "job-1", 1)
	require.Error(t, db.Create(&second).Error,
		"a second audit row for the same (job_id, attempt) must be rejected")

	next := newJobRunRow(models.NewID(), "job-1", 2)
	assert.NoError(t, db.Create(&next).Error, "attempt 2 on the same job is a distinct audit row")
}

// TestPostgresIsolatedDBEnforcesOnePeriodicPerSlug proves idx_job_periodics_slug
// rejects on Postgres.
func TestPostgresIsolatedDBEnforcesOnePeriodicPerSlug(t *testing.T) {
	db := NewPostgresIsolatedDB(t)

	first := newJobPeriodicRow(models.NewID(), "nightly-rollup")
	require.NoError(t, db.Create(&first).Error)

	second := newJobPeriodicRow(models.NewID(), "nightly-rollup")
	require.Error(t, db.Create(&second).Error, "a second schedule for the same slug must be rejected")

	other := newJobPeriodicRow(models.NewID(), "hourly-sweep")
	assert.NoError(t, db.Create(&other).Error, "a different slug is a different schedule")
}
