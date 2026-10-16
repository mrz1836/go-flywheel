//go:build integration

package core

import "testing"

// TestEnqueuedCountsEachJobInItsInsertHourPostgres runs the suite on
// PostgreSQL, where an hour's jobs are a range of text ids under the
// database's own collation.
func TestEnqueuedCountsEachJobInItsInsertHourPostgres(t *testing.T) {
	t.Parallel()
	enqueuedCountsSuite(t, postgresOpener)
}
