//go:build integration

package core

import "testing"

// TestQueueSamplesAreWrittenAndPrunedPostgres runs the sampling suite on
// PostgreSQL, where sampled_at is a timestamptz and the repeat is absorbed by
// ON CONFLICT DO NOTHING on the same key.
func TestQueueSamplesAreWrittenAndPrunedPostgres(t *testing.T) {
	t.Parallel()
	queueSamplesSuite(t, postgresOpener, newBarePostgres)
}
