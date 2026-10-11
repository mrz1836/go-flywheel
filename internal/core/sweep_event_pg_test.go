//go:build integration

package core

import "testing"

// TestSweepEventNamesEachReclaimedJobPostgres reads the per-job report off
// PostgreSQL's reclaim statement, whose rows come back in no particular order.
func TestSweepEventNamesEachReclaimedJobPostgres(t *testing.T) {
	t.Parallel()
	sweepEventSuite(t, NewPostgresIsolatedDB(t), func(d Driver) Driver { return &countingDriver{Driver: d} })
}
