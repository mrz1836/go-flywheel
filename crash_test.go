package flywheel

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSweepReclaimsExpiredLeaseAndCrashesStub simulates an executor that
// claimed a job, then died. The next Sweep tick must move the job from
// running back to available and mark its in-flight job_runs row crashed
// rather than leaving it stuck on "started" forever.
func TestSweepReclaimsExpiredLeaseAndCrashesStub(t *testing.T) {
	t.Parallel()
	db := newDB(t)

	// Insert a job in running state with an expired lease and a stub run row.
	now := time.Now().UTC()
	pastLease := now.Add(-time.Minute)
	jobID := "00000000-0000-7000-8000-00000000aaaa"
	runID := "00000000-0000-7000-8000-00000000bbbb"

	require.NoError(t, db.Exec(
		`INSERT INTO jobs(id, kind, queue, args, priority, state, attempt, max_attempts, scheduled_at, executor_class, leased_until, tags, created_at, updated_at, metadata)
		 VALUES (?,'test.k','default','{}',100,?,1,25,?,?,?,'[]',?,?,?)`,
		jobID, string(StateRunning), now, string(AnyClass), pastLease, now, now, "{}",
	).Error)
	require.NoError(t, db.Exec(
		`INSERT INTO job_runs(id, job_id, attempt, executor_class, executor_id, started_at, outcome, created_at)
		 VALUES (?,?,1,?,'h1',?,?,?)`,
		runID, jobID, "local", now.Add(-time.Hour), string(OutcomeStarted), now.Add(-time.Hour),
	).Error)

	sweeper := baseDriver{db: db}
	n, err := sweeper.Sweep(context.Background(), now)
	require.NoError(t, err)
	assert.Equal(t, 1, n, "exactly one job had an expired lease")

	// The job is back to available; the stub run row is crashed.
	var state string
	require.NoError(t, db.Table("jobs").Select("state").Where("id = ?", jobID).Scan(&state).Error)
	assert.Equal(t, string(StateAvailable), state)

	var outcome string
	require.NoError(t, db.Table("job_runs").Select("outcome").Where("id = ?", runID).Scan(&outcome).Error)
	assert.Equal(t, string(OutcomeCrashed), outcome, "the orphaned stub must be marked crashed, not left started")
}
