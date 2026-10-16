//go:build integration

package core

import "testing"

// TestFinalizeFingerprintsOnlyErrorsAndTimeoutsPostgres runs the suite through
// PostgreSQL's finalize, which writes the run and its finish in one statement.
func TestFinalizeFingerprintsOnlyErrorsAndTimeoutsPostgres(t *testing.T) {
	t.Parallel()
	finalizeFingerprintsSuite(t, NewPostgresIsolatedDB(t))
}
