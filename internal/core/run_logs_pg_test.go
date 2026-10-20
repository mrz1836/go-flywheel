//go:build integration

package core

import (
	"testing"

	"gorm.io/gorm"
)

// TestRunLogsCaptureTheWorkersLinesAndAttrsPostgres runs the capture suite on
// PostgreSQL, where the attributes are stored as jsonb.
func TestRunLogsCaptureTheWorkersLinesAndAttrsPostgres(t *testing.T) {
	t.Parallel()
	runLogsCaptureSuite(t, func(t *testing.T) (*gorm.DB, Driver) {
		t.Helper()
		db := NewPostgresIsolatedDB(t)
		return db, NewPostgresDriver(db)
	})
}
