//go:build integration

package core

import "testing"

// TestSweepCrashesOnlyTheRunOfTheReclaimedAttemptPostgres runs the reclaimed-run
// rule through PostgreSQL's data-modifying stub update.
func TestSweepCrashesOnlyTheRunOfTheReclaimedAttemptPostgres(t *testing.T) {
	t.Parallel()

	t.Run("the older attempt's run is left to it", func(t *testing.T) {
		t.Parallel()
		reclaimedRunSuite(t, NewPostgresIsolatedDB(t))
	})
	t.Run("a reclaimed attempt with no run names none", func(t *testing.T) {
		t.Parallel()
		reclaimedRunWithoutAStubSuite(t, NewPostgresIsolatedDB(t))
	})
}
