//go:build integration

package flywheel

import "testing"

// TestSampleQueueHealthSnapshotPostgres proves the queue-health gauge — counts,
// ready, in-flight, scheduled-ahead, and the oldest-ready lag — reads correctly
// on Postgres, not only SQLite (where MIN(scheduled_at)'s lost datetime affinity
// forced the ordered-read design).
func TestSampleQueueHealthSnapshotPostgres(t *testing.T) {
	t.Parallel()
	assertQueueHealthSnapshot(t, NewPostgresIsolatedDB(t))
}

// TestSampleQueueHealthEmptyPostgres proves the empty-queue zero-value snapshot
// on Postgres.
func TestSampleQueueHealthEmptyPostgres(t *testing.T) {
	t.Parallel()
	assertQueueHealthEmpty(t, NewPostgresIsolatedDB(t))
}

// TestRecentFailuresPostgres proves the recent-failures diagnosis read on
// Postgres.
func TestRecentFailuresPostgres(t *testing.T) {
	t.Parallel()
	assertRecentFailures(t, NewPostgresIsolatedDB(t))
}
