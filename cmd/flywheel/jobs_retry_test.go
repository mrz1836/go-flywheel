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

// TestCLIJobsRetryRefusesARunningJobUnlessAllowed proves the CLI's retry does
// what its help says: a running job is refused, and left running, until
// --allow-running re-queues it; a terminal job is refused, and left as it was,
// until --force retries it.
func TestCLIJobsRetryRefusesARunningJobUnlessAllowed(t *testing.T) {
	t.Parallel()
	cfg := migratedCLIConfig(t)
	seedCLIJob(t, cfg, "j-running", "running")
	seedCLIJob(t, cfg, "j-done", "succeeded")
	state := func(id string) string {
		t.Helper()
		out, err := runRoot(context.Background(), "--config", cfg, "jobs", "inspect", id, "--json")
		require.NoError(t, err)
		return out
	}

	_, err := runRoot(context.Background(), "--config", cfg, "jobs", "retry", "j-running")
	require.ErrorIs(t, err, flywheel.ErrJobRunning, "a running job is never started a second time by default")
	assert.Contains(t, state("j-running"), `"state": "running"`, "the refused job keeps running")

	out, err := runRoot(context.Background(), "--config", cfg, "jobs", "retry", "j-running", "--allow-running")
	require.NoError(t, err)
	assert.Contains(t, out, "retried j-running")
	assert.Contains(t, state("j-running"), `"state": "available"`, "--allow-running re-queues a running job")

	_, err = runRoot(context.Background(), "--config", cfg, "jobs", "retry", "j-done")
	require.ErrorIs(t, err, flywheel.ErrJobTerminal, "finished work is not re-run by default")
	assert.Contains(t, state("j-done"), `"state": "succeeded"`)

	out, err = runRoot(context.Background(), "--config", cfg, "jobs", "retry", "j-done", "--force")
	require.NoError(t, err)
	assert.Contains(t, out, "retried j-done")
	assert.Contains(t, state("j-done"), `"state": "available"`, "--force re-runs a terminal job")
}

// TestCLIJobsRetryHelpExplainsBothFlags pins the help to the behavior: the
// command's own help (cobra prints its Long text) says which jobs it retries,
// which it refuses, and what each flag changes, and the group's help lists it
// by its Short text.
func TestCLIJobsRetryHelpExplainsBothFlags(t *testing.T) {
	t.Parallel()
	out, err := runRoot(context.Background(), "jobs", "retry", "--help")
	require.NoError(t, err)
	help := strings.Join(strings.Fields(out), " ")
	for _, want := range []string{
		"A waiting, failed, or paused job returns to available.",
		"A terminal job (succeeded, cancelled, discarded) needs --force.",
		"A cancelled job whose attempt still runs is refused until that attempt returns.",
		"A running job is refused: cancel it first, or pass --allow-running to re-queue it while its current " +
			"attempt keeps running; that attempt's result is then discarded, and the work runs twice.",
		"--force retry a job that reached a terminal state",
		"--allow-running re-queue a running job; its current attempt keeps running and its result is discarded",
	} {
		assert.Contains(t, help, want)
	}

	out, err = runRoot(context.Background(), "jobs", "--help")
	require.NoError(t, err)
	assert.Contains(t, out, "Return a job to available so it runs again")
}
