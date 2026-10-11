//go:build integration

package core

import (
	"testing"

	"gorm.io/gorm"
)

// TestReplayNeverRequeuesAJobWhoseAttemptStillRunsPostgres runs the replay's
// running-attempt rules on PostgreSQL, including the job_runs probe a cancelled
// job's replay makes.
func TestReplayNeverRequeuesAJobWhoseAttemptStillRunsPostgres(t *testing.T) {
	t.Parallel()
	replayRunningSuite(t, func(t *testing.T) *gorm.DB { return NewPostgresIsolatedDB(t) })
}
