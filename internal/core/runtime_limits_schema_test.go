package core

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// progressAndLimitColumns are the columns that carry an attempt's progress, a
// limiter's overrides, and a queue's concurrency cap.
//
//nolint:gochecknoglobals // shared expectation fixture
var progressAndLimitColumns = []SchemaDrift{
	{Table: "job_runs", Column: "progress"},
	{Table: "job_runs", Column: "progress_at"},
	{Table: "limiter_buckets", Column: "max_concurrent_override"},
	{Table: "limiter_buckets", Column: "rate_override"},
	{Table: "limiter_buckets", Column: "burst_override"},
	{Table: "job_queues", Column: "max_concurrency"},
}

// dropColumns drops each named column from a schema Migrate installed, as a
// database the previous release installed lacks them.
func dropColumns(t *testing.T, db *gorm.DB, cols ...SchemaDrift) {
	t.Helper()
	for _, col := range cols {
		require.NoError(t, db.Exec("ALTER TABLE "+col.Table+" DROP COLUMN "+col.Column).Error, "drop %s", col)
	}
}

// TestTheRuntimeRefusesASchemaWithoutTheProgressAndLimitColumns is the
// deploy-order guard for the progress and limit columns: each part of the
// runtime that writes or reads one refuses to start without it, naming it and
// the fix, and a part that touches none of them starts.
func TestTheRuntimeRefusesASchemaWithoutTheProgressAndLimitColumns(t *testing.T) {
	t.Parallel()

	t.Run("a migrated schema carries every column", func(t *testing.T) {
		t.Parallel()
		db := newDB(t)
		drift, err := InspectSchema(context.Background(), db)
		require.NoError(t, err)
		assert.Empty(t, drift)
		for _, col := range progressAndLimitColumns {
			assert.True(t, db.Migrator().HasColumn(col.Table, col.Column), "%s exists", col)
		}
	})

	t.Run("a Runner needs the progress column", func(t *testing.T) {
		t.Parallel()
		db := newDB(t)
		dropColumns(t, db, SchemaDrift{Table: "job_runs", Column: "progress"})
		ctx := context.Background()
		id, err := Enqueue(ctx, NewClient(db), "stats.write", []byte(`{}`), InsertOpts{})
		require.NoError(t, err)

		err = rwRunner(t, db, NewRegistry(), func(c *RunnerConfig) { c.QueueStateRefresh = -1 }).RunUntilIdle(ctx)
		require.ErrorIs(t, err, ErrSchemaOutdated)
		assert.Contains(t, err.Error(), "job_runs.progress", "the error names the missing column")
		assert.Contains(t, err.Error(), "flywheel migrate", "and how to fix it")
		assert.Equal(t, string(StateAvailable), jobState(t, db, id), "nothing was claimed")
	})

	t.Run("a Scheduler needs the progress time", func(t *testing.T) {
		t.Parallel()
		db := newDB(t)
		dropColumns(t, db, SchemaDrift{Table: "job_runs", Column: "progress_at"})
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		err := newSchedulerCfg(t, SchedulerConfig{DB: db, Client: NewClient(db)}).Run(ctx)
		require.ErrorIs(t, err, ErrSchemaOutdated)
		assert.Contains(t, err.Error(), "job_runs.progress_at", "its lease sweep writes job_runs")
		assert.NoError(t, ctx.Err(), "it stopped at once, before any activity")
	})

	t.Run("a Runner reading queue state needs the cap", func(t *testing.T) {
		t.Parallel()
		db := newDB(t)
		dropColumns(t, db, SchemaDrift{Table: "job_queues", Column: "max_concurrency"})

		err := rwRunner(t, db, NewRegistry()).RunUntilIdle(context.Background())
		require.ErrorIs(t, err, ErrSchemaOutdated)
		assert.Contains(t, err.Error(), "job_queues.max_concurrency")
	})

	t.Run("a Runner with a DBLimiter needs the overrides", func(t *testing.T) {
		t.Parallel()
		db := newDB(t)
		dropColumns(t, db, SchemaDrift{Table: "limiter_buckets", Column: "max_concurrent_override"})
		limiter, err := NewDBLimiter(db, DBLimiterConfig{MaxConcurrent: 2, HoldTTL: time.Minute})
		require.NoError(t, err)

		err = rwRunner(t, db, NewRegistry(), func(c *RunnerConfig) {
			c.QueueStateRefresh = -1
			c.Limiter = limiter
			c.Resource = "vendor"
		}).RunUntilIdle(context.Background())
		require.ErrorIs(t, err, ErrSchemaOutdated)
		assert.Contains(t, err.Error(), "limiter_buckets.max_concurrent_override")
	})

	t.Run("a Runner with neither starts without those columns", func(t *testing.T) {
		t.Parallel()
		db := newDB(t)
		dropColumns(t, db, progressAndLimitColumns[2:]...)
		reg := NewRegistry()
		Register[statsWriteArgs](reg, statsWriteWorker{})
		ctx := context.Background()
		_, err := Insert(ctx, NewClient(db), statsWriteArgs{}, InsertOpts{})
		require.NoError(t, err)

		r := rwRunner(t, db, reg, func(c *RunnerConfig) { c.QueueStateRefresh = -1 })
		require.NoError(t, r.RunUntilIdle(ctx), "no queue state and no limiter: neither table's new columns are read")
	})
}
