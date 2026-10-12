//go:build integration

package core

import "testing"

func TestOversizedOutputIsStoredAsATruncationMarkerPostgres(t *testing.T) {
	t.Parallel()
	db := NewPostgresIsolatedDB(t)
	assertOversizedOutputIsStoredAsATruncationMarker(t, db, NewPostgresDriver(db))
}

func TestLongErrorIsFlaggedTruncatedPostgres(t *testing.T) {
	t.Parallel()
	db := NewPostgresIsolatedDB(t)
	assertLongErrorIsFlaggedTruncated(t, db, NewPostgresDriver(db))
}

func TestRecoveredPanicKeepsItsStackPostgres(t *testing.T) {
	t.Parallel()
	db := NewPostgresIsolatedDB(t)
	assertRecoveredPanicKeepsItsStack(t, db, NewPostgresDriver(db))
}

func TestJobRunViewCarriesTheStoredErrorPayloadPostgres(t *testing.T) {
	t.Parallel()
	db := NewPostgresIsolatedDB(t)
	assertJobRunViewCarriesTheStoredErrorPayload(t, db, NewPostgresDriver(db))
}

func TestMaxOutputBytesSetsTheCapPostgres(t *testing.T) {
	t.Parallel()
	db := NewPostgresIsolatedDB(t)
	assertMaxOutputBytesSetsTheCap(t, db, func(o DriverOpts) Driver { return NewPostgresDriverWithOptions(db, o) })
}
