//go:build integration

package core

import (
	"testing"

	"gorm.io/gorm"
)

// TestSweepDiscardsAJobWhoseCrashedAttemptWasItsLastPostgres runs the discard
// through PostgreSQL's single reclaim statement, which decides each row's state
// in place.
func TestSweepDiscardsAJobWhoseCrashedAttemptWasItsLastPostgres(t *testing.T) {
	t.Parallel()
	sweepDiscardSuite(t, NewPostgresIsolatedDB(t))
}

// TestCrashedStubRecordsTheStateTheSweepAppliedPostgres runs the stub update
// through PostgreSQL's data-modifying statement.
func TestCrashedStubRecordsTheStateTheSweepAppliedPostgres(t *testing.T) {
	t.Parallel()
	crashedStubSuite(t, NewPostgresIsolatedDB(t))
}

// TestSweepDiscardsAJobOnItsMaxCrashesCrashPostgres runs the crash budget through
// PostgreSQL's reclaim statement, which counts a job's earlier crashes in place.
func TestSweepDiscardsAJobOnItsMaxCrashesCrashPostgres(t *testing.T) {
	t.Parallel()
	maxCrashesSuite(t,
		func(t *testing.T) *gorm.DB { return NewPostgresIsolatedDB(t) },
		func(_ *testing.T, db *gorm.DB, opts DriverOpts) Driver { return NewPostgresDriverWithOptions(db, opts) })
}
