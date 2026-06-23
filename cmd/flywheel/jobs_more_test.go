package main

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// enqueueOne enqueues a single exec job through the CLI and returns its ID.
func enqueueOne(t *testing.T, cfg string) string {
	t.Helper()
	out, err := runRoot(context.Background(), "--config", cfg, "enqueue", "exec", `{"command":"true"}`)
	require.NoError(t, err)
	id := strings.TrimSpace(out)
	require.NotEmpty(t, id)
	return id
}

func TestCLIJobsLsTextTable(t *testing.T) {
	t.Parallel()
	cfg := migratedCLIConfig(t)
	id := enqueueOne(t, cfg)

	out, err := runRoot(context.Background(), "--config", cfg, "jobs", "ls")
	require.NoError(t, err)
	assert.Contains(t, out, "KIND", "the table header is printed")
	assert.Contains(t, out, "exec")
	assert.Contains(t, out, id, "the enqueued job appears in the table")
}

func TestCLIJobsInspectTextAndJSON(t *testing.T) {
	t.Parallel()
	cfg := migratedCLIConfig(t)
	id := enqueueOne(t, cfg)

	out, err := runRoot(context.Background(), "--config", cfg, "jobs", "inspect", id)
	require.NoError(t, err)
	assert.Contains(t, out, "Job "+id, "the text inspect prints the job header")
	assert.Contains(t, out, "kind:")
	assert.Contains(t, out, "runs:", "the run-history section is rendered")

	out, err = runRoot(context.Background(), "--config", cfg, "jobs", "inspect", id, "--json")
	require.NoError(t, err)
	assert.Contains(t, out, `"job"`)
	assert.Contains(t, out, `"runs"`)
}

func TestCLIJobsInspectListsRunHistory(t *testing.T) {
	t.Parallel()
	cfg := migratedCLIConfig(t)

	// Seed a job with a run row directly so inspect's run-history loop renders a row.
	loaded, err := LoadConfig(cfg)
	require.NoError(t, err)
	db, _, err := openDB(loaded)
	require.NoError(t, err)
	now := "2026-06-22 12:00:00"
	later := "2026-06-22 12:00:30"
	require.NoError(t, db.Exec(
		`INSERT INTO jobs(id, kind, queue, args, priority, state, attempt, max_attempts, scheduled_at, executor_class, tags, created_at, updated_at, metadata)
		 VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		"j-runs", "exec", "default", "{}", 100, "succeeded", 2, 25, now, "", "[]", now, now, "{}",
	).Error)
	// A failed first attempt carries an error message and captured output...
	require.NoError(t, db.Exec(
		`INSERT INTO job_runs(id, job_id, attempt, executor_class, executor_id, started_at, outcome, error_class, error_message, output, created_at)
		 VALUES (?,?,?,?,?,?,?,?,?,?,?)`,
		"j-runs-1", "j-runs", 1, "local", "host", now, "error", "transient", `exec "sh" exited with code 1`, `{"exit_code":1,"stdout":"partial","stderr":"boom"}`, now,
	).Error)
	// ...and the successful retry carries the captured stdout.
	require.NoError(t, db.Exec(
		`INSERT INTO job_runs(id, job_id, attempt, executor_class, executor_id, started_at, outcome, error_class, error_message, output, created_at)
		 VALUES (?,?,?,?,?,?,?,?,?,?,?)`,
		"j-runs-2", "j-runs", 2, "local", "host", later, "success", "", "", `{"exit_code":0,"stdout":"done","stderr":""}`, later,
	).Error)
	closeDB(db)

	out, err := runRoot(context.Background(), "--config", cfg, "jobs", "inspect", "j-runs")
	require.NoError(t, err)
	assert.Contains(t, out, "runs:     2", "the run count is reported")
	assert.Contains(t, out, "j-runs-1", "the run row is listed")
	assert.Contains(t, out, "local", "the run's executor class is shown")
	assert.Contains(t, out, `exec "sh" exited with code 1`, "the failed run's error is shown")
	assert.Contains(t, out, `"stdout":"done"`, "the captured output is shown")
}

func TestCLIJobsRetryAndCancel(t *testing.T) {
	t.Parallel()
	cfg := migratedCLIConfig(t)

	// Retry moves an available job back to available and reports it.
	retryID := enqueueOne(t, cfg)
	out, err := runRoot(context.Background(), "--config", cfg, "jobs", "retry", retryID)
	require.NoError(t, err)
	assert.Contains(t, out, "retried "+retryID)

	// Cancel moves a job to the terminal cancelled state and reports it.
	cancelID := enqueueOne(t, cfg)
	out, err = runRoot(context.Background(), "--config", cfg, "jobs", "cancel", cancelID)
	require.NoError(t, err)
	assert.Contains(t, out, "cancelled "+cancelID)

	out, err = runRoot(context.Background(), "--config", cfg, "jobs", "inspect", cancelID, "--json")
	require.NoError(t, err)
	assert.Contains(t, out, `"state": "cancelled"`, "the cancel is persisted")
}

func TestCLIJobsRetryUnknownIDErrors(t *testing.T) {
	t.Parallel()
	cfg := migratedCLIConfig(t)
	_, err := runRoot(context.Background(), "--config", cfg, "jobs", "retry", "ghost")
	require.Error(t, err, "retrying a non-existent job is an error")
}

func TestCLIJobsCancelUnknownIDErrors(t *testing.T) {
	t.Parallel()
	cfg := migratedCLIConfig(t)
	_, err := runRoot(context.Background(), "--config", cfg, "jobs", "cancel", "ghost")
	require.Error(t, err, "cancelling a non-existent job is an error")
}

func TestCLIJobsInspectUnknownIDErrors(t *testing.T) {
	t.Parallel()
	cfg := migratedCLIConfig(t)
	_, err := runRoot(context.Background(), "--config", cfg, "jobs", "inspect", "ghost")
	require.Error(t, err, "inspecting a non-existent job surfaces the lookup error")
}

// TestCLIJobsCommandsFailOnUnopenableDB drives every jobs subcommand's
// loadAndOpen error branch with a config whose database cannot be opened.
func TestCLIJobsCommandsFailOnUnopenableDB(t *testing.T) {
	t.Parallel()
	cfg := unopenableConfig(t)
	cases := [][]string{
		{"jobs", "ls"},
		{"jobs", "inspect", "x"},
		{"jobs", "retry", "x"},
		{"jobs", "cancel", "x"},
	}
	for _, args := range cases {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			t.Parallel()
			_, err := runRoot(context.Background(), append([]string{"--config", cfg}, args...)...)
			require.Error(t, err, "a database that cannot be opened is a command error")
		})
	}
}

func TestOrDash(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "-", orDash(""), "the wildcard executor class renders as a dash")
	assert.Equal(t, "local", orDash("local"))
}
