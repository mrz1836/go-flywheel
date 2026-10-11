package main

import (
	"context"
	"strings"
	"testing"

	flywheel "github.com/mrz1836/go-flywheel"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// seedCLIJob writes one job in state into the CLI config's database.
func seedCLIJob(t *testing.T, cfg, id, state string) {
	t.Helper()
	loaded, err := LoadConfig(cfg)
	require.NoError(t, err)
	db, _, err := openDB(loaded)
	require.NoError(t, err)
	defer closeDB(db)
	now := "2026-06-22 12:00:00"
	require.NoError(t, db.Exec(
		`INSERT INTO jobs(id, kind, queue, args, priority, state, attempt, max_attempts, scheduled_at, executor_class, tags, created_at, updated_at, metadata, lease_token)
		 VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		id, "exec", "default", "{}", 100, state, 1, 25, now, "", "[]", now, now, "{}", "tok",
	).Error)
}

// TestCLIJobsRetryRefusesARunningOrTerminalJob proves the CLI's retry does what
// its help says: a running job and a terminal one are each refused with their
// own error and left as they were.
func TestCLIJobsRetryRefusesARunningOrTerminalJob(t *testing.T) {
	t.Parallel()
	cfg := migratedCLIConfig(t)
	seedCLIJob(t, cfg, "j-running", "running")
	seedCLIJob(t, cfg, "j-done", "succeeded")

	_, err := runRoot(context.Background(), "--config", cfg, "jobs", "retry", "j-running")
	require.ErrorIs(t, err, flywheel.ErrJobRunning, "a running job is never started a second time")
	out, err := runRoot(context.Background(), "--config", cfg, "jobs", "inspect", "j-running", "--json")
	require.NoError(t, err)
	assert.Contains(t, out, `"state": "running"`, "the refused job keeps running")

	_, err = runRoot(context.Background(), "--config", cfg, "jobs", "retry", "j-done")
	require.ErrorIs(t, err, flywheel.ErrJobTerminal, "finished work is not re-run")
	out, err = runRoot(context.Background(), "--config", cfg, "jobs", "inspect", "j-done", "--json")
	require.NoError(t, err)
	assert.Contains(t, out, `"state": "succeeded"`)
}

// TestCLIJobsRetryHelpSaysWhatItDoesWithRunningAndTerminalJobs pins the help to
// the behavior: the command's own help (cobra prints its Long text) names the
// jobs it retries and the two it refuses, and the group's help lists it by its
// Short text.
func TestCLIJobsRetryHelpSaysWhatItDoesWithRunningAndTerminalJobs(t *testing.T) {
	t.Parallel()
	out, err := runRoot(context.Background(), "jobs", "retry", "--help")
	require.NoError(t, err)
	help := strings.Join(strings.Fields(out), " ")
	for _, want := range []string{
		"available, scheduled, retryable, or paused",
		`A running job is refused with "flywheel: job is running"`,
		"wait for the attempt to finish",
		`A terminal job (succeeded, cancelled, or discarded) is refused with "flywheel: job has already reached a terminal state"`,
	} {
		assert.Contains(t, help, want)
	}
	assert.NotContains(t, help, "Force a job back to available", "the command offers no forced retry")

	out, err = runRoot(context.Background(), "jobs", "--help")
	require.NoError(t, err)
	assert.Contains(t, out, "Make a waiting, retrying, or paused job available to run now")
}
