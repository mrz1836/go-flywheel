//go:build integration

package core

import "testing"

// TestListLimitersReportsLiveHoldsAndTheNextExpiryPostgres reads limiter state
// on PostgreSQL, where the soonest expiry comes back as a timestamptz.
func TestListLimitersReportsLiveHoldsAndTheNextExpiryPostgres(t *testing.T) {
	t.Parallel()
	limiterStateSuite(t, NewPostgresIsolatedDB(t))
}
