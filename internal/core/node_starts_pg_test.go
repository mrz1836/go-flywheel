//go:build integration

package core

import "testing"

// TestNodeStartsPageInStartOrderWithoutGapsOrRepeatsPostgres runs the walk on
// PostgreSQL, where started_at is a timestamptz.
func TestNodeStartsPageInStartOrderWithoutGapsOrRepeatsPostgres(t *testing.T) {
	t.Parallel()
	nodeStartsSuite(t, NewPostgresIsolatedDB(t))
}
