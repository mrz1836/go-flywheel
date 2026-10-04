//go:build integration

package core

import "testing"

// TestJobViewProjectionIsCompletePostgres proves the narrowed JobView reads lose
// nothing on PostgreSQL, where jsonb and timestamptz scan differently from
// SQLite's text columns.
func TestJobViewProjectionIsCompletePostgres(t *testing.T) {
	t.Parallel()
	assertJobViewProjectionIsComplete(t, NewPostgresIsolatedDB(t))
}
