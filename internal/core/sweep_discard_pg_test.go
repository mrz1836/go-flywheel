//go:build integration

package core

import "testing"

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
