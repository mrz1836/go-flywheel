//go:build integration

package core

import "testing"

// TestSingleJobControlsRefuseARunningJobPostgres mirrors the single-job
// controls' refusals on PostgreSQL, where each control reads the job under a
// row lock.
func TestSingleJobControlsRefuseARunningJobPostgres(t *testing.T) {
	t.Parallel()
	assertSingleJobControlsRefuseARunningJob(t, NewPostgresIsolatedDB(t))
}
