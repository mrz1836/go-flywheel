//go:build integration

package core

import "testing"

func TestEveryListReadReturnsAtMost200Postgres(t *testing.T) {
	t.Parallel()
	assertEveryListReadReturnsAtMost200(t, NewPostgresIsolatedDB(t))
}

func TestListRunsWithoutALimitReadsAtMost200Postgres(t *testing.T) {
	t.Parallel()
	assertListRunsWithoutALimitReadsAtMost200(t, NewPostgresIsolatedDB(t))
}
