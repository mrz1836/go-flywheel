package core

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRunLogsRequireTheirTable is the deploy-order guard for job_run_logs: a
// Runner that captures run logs writes the table, and a Scheduler with
// retention on deletes from it, so each refuses to start without it, naming it
// and the fix; a Runner without capture and a Scheduler without retention touch
// neither and start.
func TestRunLogsRequireTheirTable(t *testing.T) {
	t.Parallel()

	t.Run("a migrated schema carries the table and its index", func(t *testing.T) {
		t.Parallel()
		db := newDB(t)
		assert.True(t, db.Migrator().HasTable("job_run_logs"))
		assert.True(t, sqliteHasIndex(t, db, "job_run_logs_job"))
		set, err := IndexSet(db.Name())
		require.NoError(t, err)
		require.NotEmpty(t, set)
		last := set[len(set)-1]
		assert.Equal(t, "job_run_logs_job", last.Name, "the index joins the set last")
		assert.Equal(t, IndexPerformance, last.Kind, "it serves the delete of a job's lines, and guards nothing")
		assertNoSchemaOrIndexDrift(t, db)
	})

	t.Run("a Runner capturing logs needs the table", func(t *testing.T) {
		t.Parallel()
		db := newDB(t)
		require.NoError(t, db.Exec("DROP TABLE job_run_logs").Error)
		ctx := context.Background()
		id, err := Enqueue(ctx, NewClient(db), "stats.write", []byte(`{}`), InsertOpts{})
		require.NoError(t, err)

		err = rwRunner(t, db, NewRegistry(), func(c *RunnerConfig) {
			c.CaptureLogs = &RunLogCapture{}
		}).RunUntilIdle(ctx)
		require.ErrorIs(t, err, ErrSchemaOutdated)
		assert.Contains(t, err.Error(), "job_run_logs (table)", "the error names the missing table")
		assert.Contains(t, err.Error(), "flywheel migrate", "and how to fix it")
		assert.Equal(t, string(StateAvailable), jobState(t, db, id), "nothing was claimed")
	})

	t.Run("a Scheduler with retention needs the table", func(t *testing.T) {
		t.Parallel()
		db := newDB(t)
		require.NoError(t, db.Exec("DROP TABLE job_run_logs").Error)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		err := newSchedulerCfg(t, SchedulerConfig{
			DB: db, Client: NewClient(db), RetentionMaxAge: 30 * 24 * time.Hour,
		}).Run(ctx)
		require.ErrorIs(t, err, ErrSchemaOutdated)
		assert.Contains(t, err.Error(), "job_run_logs (table)", "its retention deletes a job's lines with its runs")
		assert.NoError(t, ctx.Err(), "it stopped at once, before any activity")
	})

	t.Run("a Runner without capture and a Scheduler without retention start", func(t *testing.T) {
		t.Parallel()
		db := newDB(t)
		require.NoError(t, db.Exec("DROP TABLE job_run_logs").Error)
		reg := NewRegistry()
		Register[statsWriteArgs](reg, statsWriteWorker{})
		ctx := context.Background()
		id, err := Insert(ctx, NewClient(db), statsWriteArgs{}, InsertOpts{})
		require.NoError(t, err)

		require.NoError(t, rwRunner(t, db, reg).RunUntilIdle(ctx), "no capture: the table is never written")
		assert.Equal(t, string(StateSucceeded), jobState(t, db, id))
		s := newSchedulerCfg(t, SchedulerConfig{DB: db, Client: NewClient(db), AuditRetention: time.Hour})
		require.NoError(t, s.checkSchema(ctx), "no RetentionMaxAge: no job is deleted, so no line is either")
	})
}
