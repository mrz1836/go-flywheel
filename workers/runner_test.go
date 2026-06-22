package workers_test

import (
	"context"
	"encoding/json"
	"testing"

	flywheel "github.com/mrz1836/go-flywheel"
	"github.com/mrz1836/go-flywheel/workers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// newRunnerDB opens an in-memory SQLite database with the flywheel schema applied.
func newRunnerDB(t *testing.T) *gorm.DB {
	t.Helper()
	dsn := "file:workers-" + t.Name() + "?mode=memory&cache=shared"
	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	require.NoError(t, err)
	t.Cleanup(func() {
		if sqlDB, derr := db.DB(); derr == nil {
			_ = sqlDB.Close()
		}
	})
	require.NoError(t, flywheel.Migrate(db))
	return db
}

// TestExecWorkerThroughRunnerRecordsAudit proves ExecWorker plugs into the real
// runtime: registered, enqueued, claimed, run, and recorded in the audit trail.
func TestExecWorkerThroughRunnerRecordsAudit(t *testing.T) {
	t.Parallel()
	db := newRunnerDB(t)

	reg := flywheel.NewRegistry()
	flywheel.Register(reg, workers.ExecWorker{})
	runner, err := flywheel.NewRunner(flywheel.RunnerConfig{
		DB: db, Driver: flywheel.NewSQLiteDriver(db), Registry: reg,
		Queues: []string{"default"}, ClaimAnyClass: true, Concurrency: 1,
	})
	require.NoError(t, err)

	args, err := json.Marshal(workers.ExecArgs{Command: "sh", Args: []string{"-c", "printf done"}})
	require.NoError(t, err)
	id, err := flywheel.Enqueue(context.Background(), flywheel.NewClient(db), workers.ExecKind, args, flywheel.InsertOpts{})
	require.NoError(t, err)

	require.NoError(t, runner.RunUntilIdle(context.Background()))

	view, err := flywheel.FindJob(context.Background(), db, id)
	require.NoError(t, err)
	assert.Equal(t, string(flywheel.StateSucceeded), view.State)

	runs, err := flywheel.ListRuns(context.Background(), db, id, flywheel.ListRunsParams{})
	require.NoError(t, err)
	require.Len(t, runs, 1)
	assert.Equal(t, string(flywheel.OutcomeSuccess), runs[0].Outcome, "the exec run is recorded as a success")
}

// TestExecWorkerThroughRunnerRetriesOnNonZeroExit proves a failing command is
// retried by the runtime and ultimately discarded after exhausting attempts.
func TestExecWorkerThroughRunnerRetriesOnNonZeroExit(t *testing.T) {
	t.Parallel()
	db := newRunnerDB(t)

	reg := flywheel.NewRegistry()
	flywheel.Register(reg, workers.ExecWorker{})
	runner, err := flywheel.NewRunner(flywheel.RunnerConfig{
		DB: db, Driver: flywheel.NewSQLiteDriver(db), Registry: reg,
		Queues: []string{"default"}, ClaimAnyClass: true, Concurrency: 1,
		RetryBackoffBase: 1, // 1ns base keeps the retry loop fast
	})
	require.NoError(t, err)

	args, err := json.Marshal(workers.ExecArgs{Command: "sh", Args: []string{"-c", "exit 1"}})
	require.NoError(t, err)
	id, err := flywheel.Enqueue(context.Background(), flywheel.NewClient(db), workers.ExecKind, args,
		flywheel.InsertOpts{MaxAttempts: 3})
	require.NoError(t, err)

	require.NoError(t, runner.RunUntilIdle(context.Background()))

	view, err := flywheel.FindJob(context.Background(), db, id)
	require.NoError(t, err)
	assert.Equal(t, string(flywheel.StateDiscarded), view.State, "a persistently failing command is discarded after its attempts")

	runs, err := flywheel.ListRuns(context.Background(), db, id, flywheel.ListRunsParams{})
	require.NoError(t, err)
	assert.Len(t, runs, 3, "each retry is an audited attempt")
}
