//go:build integration

package core

import (
	"testing"

	"gorm.io/gorm"
)

// TestRetryRefusesARunningJobPostgres runs the retry guard on PostgreSQL.
func TestRetryRefusesARunningJobPostgres(t *testing.T) {
	t.Parallel()
	retryRefusesRunningSuite(t, NewPostgresIsolatedDB(t))
}

// TestForceRetriesATerminalJobButNotARunningOnePostgres runs Force's cases on
// PostgreSQL, including the job_runs probe for a cancelled job whose attempt
// still runs.
func TestForceRetriesATerminalJobButNotARunningOnePostgres(t *testing.T) {
	t.Parallel()
	forceRetrySuite(t, func(t *testing.T) *gorm.DB { return NewPostgresIsolatedDB(t) })
}
