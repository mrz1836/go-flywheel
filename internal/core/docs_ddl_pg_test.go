//go:build integration

package core

import "testing"

// TestDocumentedUpgradeDDLPostgres runs the guide's PostgreSQL upgrade block.
func TestDocumentedUpgradeDDLPostgres(t *testing.T) {
	t.Parallel()
	documentedUpgradeSuite(t, newBarePostgres(t), "postgres")
}
