//go:build integration

package core

import "testing"

// TestUndecodableArgsDiscardOnTheFirstAttemptWhateverTheClassifierSaysPostgres
// runs the decode failure through PostgreSQL's claim and finalize.
func TestUndecodableArgsDiscardOnTheFirstAttemptWhateverTheClassifierSaysPostgres(t *testing.T) {
	t.Parallel()
	undecodableArgsSuite(t, NewPostgresIsolatedDB(t))
}
