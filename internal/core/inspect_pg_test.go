//go:build integration

package core

import "testing"

// TestInspectSuitePostgres is the PostgreSQL half of the live-read suite.
func TestInspectSuitePostgres(t *testing.T) {
	t.Parallel()
	inspectSuite(t, postgresOpener)
}
