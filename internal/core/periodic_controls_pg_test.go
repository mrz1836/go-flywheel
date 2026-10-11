//go:build integration

package core

import "testing"

// TestAPausedScheduleStaysPausedWhenRedeclaredPostgres runs the re-declaring
// restarts against PostgreSQL, where the pause reads and writes its row under a
// row lock.
func TestAPausedScheduleStaysPausedWhenRedeclaredPostgres(t *testing.T) {
	t.Parallel()
	pausedRedeclaredSuite(t, NewPostgresIsolatedDB(t))
}

// TestAPausedScheduleDoesNotFirePostgres runs the tick's filter on PostgreSQL.
func TestAPausedScheduleDoesNotFirePostgres(t *testing.T) {
	t.Parallel()
	pausedDoesNotFireSuite(t, NewPostgresIsolatedDB(t))
}

// TestResumeDoesNotBackfillSkippedFiresPostgres runs the resume on PostgreSQL,
// whose timestamptz columns return the cursor in the session's zone.
func TestResumeDoesNotBackfillSkippedFiresPostgres(t *testing.T) {
	t.Parallel()
	resumeNoBackfillSuite(t, NewPostgresIsolatedDB(t))
}

// TestPausingAndResumingAreIdempotentPostgres runs the no-op cases and the
// unknown slug on PostgreSQL.
func TestPausingAndResumingAreIdempotentPostgres(t *testing.T) {
	t.Parallel()
	pauseIdempotentSuite(t, NewPostgresIsolatedDB(t))
}
