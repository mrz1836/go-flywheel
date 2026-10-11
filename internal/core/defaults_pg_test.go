//go:build integration

package core

import "testing"

// TestDefaultsSeedEveryInsertUnderTheCallersOptionsPostgres runs every insert
// path on PostgreSQL, where InsertMany writes through the conflict-insert
// statement and an insert on the caller's transaction shares its connection.
func TestDefaultsSeedEveryInsertUnderTheCallersOptionsPostgres(t *testing.T) {
	t.Parallel()
	defaultsInsertSuite(t, postgresOpener)
}

// TestDefaultsReachAPeriodicFirePostgres fires the periodic on PostgreSQL.
func TestDefaultsReachAPeriodicFirePostgres(t *testing.T) {
	t.Parallel()
	defaultsPeriodicSuite(t, postgresOpener)
}

// TestDefaultsReachFollowUpsPostgres inserts the follow-ups in a PostgreSQL
// finalize.
func TestDefaultsReachFollowUpsPostgres(t *testing.T) {
	t.Parallel()
	defaultsFollowUpSuite(t, NewPostgresIsolatedDB(t))
}

// TestDefaultsReachBarrierContinuationsOnEveryFiringPathPostgres stores the
// barrier spec in PostgreSQL's jsonb column and fires it on every path.
func TestDefaultsReachBarrierContinuationsOnEveryFiringPathPostgres(t *testing.T) {
	t.Parallel()
	defaultsBarrierSuite(t, postgresOpener)
}

// TestABarrierSpecStoredWithoutDefaultsBuildsItsOldRowPostgres fires a spec
// stored without attempts or a timeout on PostgreSQL.
func TestABarrierSpecStoredWithoutDefaultsBuildsItsOldRowPostgres(t *testing.T) {
	t.Parallel()
	legacyBarrierSuite(t, postgresOpener)
}
