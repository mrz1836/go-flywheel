//go:build integration

package core

import "testing"

// TestSetQueueConcurrencyRecordsOneEntryPerChangePostgres runs the suite on
// PostgreSQL, where the control locks the queue's row to change it.
func TestSetQueueConcurrencyRecordsOneEntryPerChangePostgres(t *testing.T) {
	t.Parallel()
	queueConcurrencySuite(t, NewPostgresIsolatedDB(t))
}
