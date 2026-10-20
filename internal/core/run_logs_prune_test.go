package core

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// runLogsPruneSuite proves a job's captured lines leave with its runs, through
// the database open returns: retention, DeleteJob, and a bulk delete each
// remove one of two finished jobs, and that job's lines are gone while the
// other's remain. With hostKey it first declares the foreign key a host may
// add from the lines to their runs, with no cascade, so a delete that removed
// the runs before their lines would be refused.
func runLogsPruneSuite(t *testing.T, open func(t *testing.T) (*gorm.DB, Driver), hostKey bool) {
	t.Helper()

	// seed runs two jobs that each log two lines, and returns the database and
	// the jobs' ids.
	seed := func(t *testing.T) (db *gorm.DB, gone, kept string) {
		t.Helper()
		db, d := open(t)
		if hostKey {
			require.NoError(t, db.Exec(`ALTER TABLE job_run_logs
				ADD CONSTRAINT fk_job_run_logs_run FOREIGN KEY (run_id) REFERENCES job_runs(id)`).Error)
		}
		ctx := context.Background()
		var err error
		gone, err = Insert(ctx, NewClient(db), runLogArgs{V: "gone"}, InsertOpts{})
		require.NoError(t, err)
		kept, err = Insert(ctx, NewClient(db), runLogArgs{V: "kept"}, InsertOpts{})
		require.NoError(t, err)
		require.NoError(t, runLogRunner(t, db, d, logging(func(l *slog.Logger) {
			l.Info("one")
			l.Warn("two")
		}), nil).RunUntilIdle(ctx))
		require.EqualValues(t, 2, runLogCount(t, db, gone))
		require.EqualValues(t, 2, runLogCount(t, db, kept))
		return db, gone, kept
	}
	assertPruned := func(t *testing.T, db *gorm.DB, gone, kept string) {
		t.Helper()
		var jobs int64
		require.NoError(t, db.Unscoped().Model(&jobRow{}).Where("id = ?", gone).Count(&jobs).Error)
		assert.Zero(t, jobs, "the job is deleted")
		assert.Zero(t, runLogCount(t, db, gone), "and its lines with it")
		assert.EqualValues(t, 2, runLogCount(t, db, kept), "the other job keeps its lines")
	}

	t.Run("retention", func(t *testing.T) {
		t.Parallel()
		db, gone, kept := seed(t)
		now := time.Now()
		require.NoError(t, db.Model(&jobRow{}).Where("id = ?", gone).
			Update("finalized_at", now.Add(-40*24*time.Hour)).Error)
		n, err := DeleteFinishedJobs(context.Background(), db, now.Add(-30*24*time.Hour))
		require.NoError(t, err)
		assert.EqualValues(t, 1, n)
		assertPruned(t, db, gone, kept)
	})

	t.Run("DeleteJob", func(t *testing.T) {
		t.Parallel()
		db, gone, kept := seed(t)
		require.NoError(t, DeleteJob(context.Background(), db, gone))
		assertPruned(t, db, gone, kept)
	})

	t.Run("a bulk delete", func(t *testing.T) {
		t.Parallel()
		db, gone, kept := seed(t)
		_, err := BulkJobs(context.Background(), db, BulkSpec{IDs: []string{gone}, Action: BulkDelete})
		require.NoError(t, err)
		assertPruned(t, db, gone, kept)
	})
}

// runLogCount counts the captured lines of jobID's runs.
func runLogCount(t *testing.T, db *gorm.DB, jobID string) int64 {
	t.Helper()
	var n int64
	require.NoError(t, db.Model(&jobRunLogRow{}).Where("job_id = ?", jobID).Count(&n).Error)
	return n
}

// TestRunLogsArePrunedWithTheirRuns runs the prune suite on SQLite.
func TestRunLogsArePrunedWithTheirRuns(t *testing.T) {
	t.Parallel()
	runLogsPruneSuite(t, func(t *testing.T) (*gorm.DB, Driver) {
		t.Helper()
		db := newDB(t)
		return db, NewSQLiteDriver(db)
	}, false)
}
