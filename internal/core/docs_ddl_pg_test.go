//go:build integration

package core

import "testing"

// TestDocumentedUpgradeDDLPostgres runs the guide's PostgreSQL upgrade block.
func TestDocumentedUpgradeDDLPostgres(t *testing.T) {
	t.Parallel()
	documentedUpgradeSuite(t, newBarePostgres(t), "postgres")
}

// TestDocumentedStatsUpgradeDDLPostgres runs the guide's PostgreSQL upgrade
// from a release before the stats tables.
func TestDocumentedStatsUpgradeDDLPostgres(t *testing.T) {
	t.Parallel()
	documentedStatsUpgradeSuite(t, newBarePostgres(t), "postgres")
}
