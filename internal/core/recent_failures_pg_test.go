//go:build integration

package core

import "testing"

func TestRecentFailuresLoadsNoArgsOrOutputPostgres(t *testing.T) {
	t.Parallel()
	assertRecentFailuresLoadsNoArgsOrOutput(t, NewPostgresIsolatedDB(t))
}
