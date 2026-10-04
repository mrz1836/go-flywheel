//go:build integration

package core

import (
	"testing"

	"gorm.io/gorm"
)

// postgresWritePath is the PostgreSQL opener for the write-path suite.
func postgresWritePath(t *testing.T) (*gorm.DB, Driver) {
	t.Helper()
	db := NewPostgresIsolatedDB(t)
	return db, NewPostgresDriver(db)
}

// TestRunRowsStampAnalyticsColumnsPostgres is the PostgreSQL half of the
// write-path suite: the same outcomes, the same columns, through the SKIP
// LOCKED claim and the PostgreSQL sweep.
func TestRunRowsStampAnalyticsColumnsPostgres(t *testing.T) {
	t.Parallel()
	writePathSuite(t, postgresWritePath)
}
