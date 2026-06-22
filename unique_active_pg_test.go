//go:build integration

package flywheel

import "testing"

// TestUniqueActiveKeyEnforcedPostgres proves the jobs_unique_active_key partial
// unique index is enforced on Postgres, not only SQLite.
func TestUniqueActiveKeyEnforcedPostgres(t *testing.T) {
	t.Parallel()
	assertUniqueActiveKeyEnforced(t, NewPostgresIsolatedDB(t))
}
