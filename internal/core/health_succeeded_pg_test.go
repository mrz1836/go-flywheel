//go:build integration

package core

import "testing"

func TestQueueHealthDoesNotCountSucceededJobsPostgres(t *testing.T) {
	t.Parallel()
	assertQueueHealthDoesNotCountSucceededJobs(t, NewPostgresIsolatedDB(t))
}
