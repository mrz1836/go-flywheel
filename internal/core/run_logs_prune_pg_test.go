//go:build integration

package core

import (
	"testing"

	"gorm.io/gorm"
)

// TestRunLogsArePrunedWithTheirRunsPostgres runs the prune suite on PostgreSQL
// with a host's foreign key from job_run_logs.run_id to job_runs.id, no
// cascade: every delete path removes the lines before the runs they reference.
func TestRunLogsArePrunedWithTheirRunsPostgres(t *testing.T) {
	t.Parallel()
	runLogsPruneSuite(t, func(t *testing.T) (*gorm.DB, Driver) {
		t.Helper()
		db := NewPostgresIsolatedDB(t)
		return db, NewPostgresDriver(db)
	}, true)
}
