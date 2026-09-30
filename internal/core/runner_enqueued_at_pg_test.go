//go:build integration

package core

import (
	"testing"
)

// TestRunnerJobEnqueuedAtIsTheInsertTimePostgres is the Postgres mirror: the
// claim's RETURNING list must carry created_at for the worker to see it.
func TestRunnerJobEnqueuedAtIsTheInsertTimePostgres(t *testing.T) {
	t.Parallel()
	for _, tc := range enqueuedAtCases() {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			db := NewPostgresIsolatedDB(t)
			assertEnqueuedAtIsInsertTime(t, db, tc, func(reg *Registry) *Runner {
				return newPostgresRunner(t, db, reg, 1)
			})
		})
	}
}
