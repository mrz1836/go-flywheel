//go:build integration

package core

import "testing"

// TestAPausedQueueIsNotClaimedWithinOneRefreshPostgres runs the suite on
// PostgreSQL, with two Runners of two slots each claiming concurrently.
func TestAPausedQueueIsNotClaimedWithinOneRefreshPostgres(t *testing.T) {
	t.Parallel()
	pausedQueueSuite(t, NewPostgresIsolatedDB(t), 2)
}
