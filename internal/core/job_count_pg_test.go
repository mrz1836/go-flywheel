//go:build integration

package core

import "testing"

// TestCountJobsIsExactToTheCapThenEstimatesPostgres runs the count suite on
// PostgreSQL, where a capped count carries the planner's estimate.
func TestCountJobsIsExactToTheCapThenEstimatesPostgres(t *testing.T) {
	t.Parallel()
	countSuite(t, postgresOpener)
}
