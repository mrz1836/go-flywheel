//go:build integration

package core

import "testing"

// TestALimiterOverrideAppliesOnTheNextAcquirePostgres runs the override suite
// on PostgreSQL, where Acquire locks the bucket row it reads the overrides from
// and SetLimiterBudget locks it to change them.
func TestALimiterOverrideAppliesOnTheNextAcquirePostgres(t *testing.T) {
	t.Parallel()
	limiterOverrideSuite(t, NewPostgresIsolatedDB(t))
}
