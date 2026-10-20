package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"

	flywheel "github.com/mrz1836/go-flywheel"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// cliLogArgs is the args of the jobs logs test's job.
type cliLogArgs struct{}

func (cliLogArgs) Kind() string { return "cli.logs" }

// cliLogWorker logs three lines and succeeds.
type cliLogWorker struct{}

func (cliLogWorker) Kind() string { return "cli.logs" }

func (cliLogWorker) Work(_ context.Context, job *flywheel.Job[cliLogArgs]) (flywheel.Result, error) {
	job.Logger.Info("fetching", "page", 1)
	job.Logger.Warn("slow response", "ms", 900)
	job.Logger.Info("done")
	return flywheel.Result{}, nil
}

// TestJobsLogsCommandPrintsAnAttemptsLines proves `flywheel jobs logs` prints
// one attempt's captured lines as a table and as JSON, after a sequence number,
// at or above a level, and a page at a time, and refuses a bad level.
func TestJobsLogsCommandPrintsAnAttemptsLines(t *testing.T) {
	t.Parallel()
	cfg := migratedCLIConfig(t)
	ctx := context.Background()
	_, db, _, err := loadAndOpen(cfg)
	require.NoError(t, err)
	reg := flywheel.NewRegistry()
	flywheel.Register[cliLogArgs](reg, cliLogWorker{})
	id, err := flywheel.Insert(ctx, flywheel.NewClient(db), cliLogArgs{}, flywheel.InsertOpts{})
	require.NoError(t, err)
	r, err := flywheel.NewRunner(flywheel.RunnerConfig{
		DB: db, Driver: flywheel.NewSQLiteDriver(db), Registry: reg, Queues: []string{"default"},
		ClaimAnyClass: true, QueueStateRefresh: -1, CaptureLogs: &flywheel.RunLogCapture{},
		Logger: slog.New(slog.DiscardHandler),
	})
	require.NoError(t, err)
	require.NoError(t, r.RunUntilIdle(ctx))
	run, ok, err := flywheel.LatestRun(ctx, db, id)
	require.NoError(t, err)
	require.True(t, ok)
	closeDB(db)

	out, err := runRoot(ctx, "--config", cfg, "jobs", "logs", run.ID)
	require.NoError(t, err)
	lines := strings.Split(strings.TrimSpace(out), "\n")
	require.Len(t, lines, 4, out)
	assert.Equal(t, []string{"SEQ", "TIME", "LEVEL", "MESSAGE", "ATTRS"}, strings.Fields(lines[0]))
	first := strings.Fields(lines[1])
	assert.Equal(t, "1", first[0])
	assert.Equal(t, []string{"INFO", "fetching", `{"page":1}`}, first[2:])
	assert.Equal(t, []string{"INFO", "done", "-"}, strings.Fields(lines[3])[2:], "a line with no attributes")

	out, err = runRoot(ctx, "--config", cfg, "jobs", "logs", run.ID, "--after", "1", "--level", "warn")
	require.NoError(t, err)
	lines = strings.Split(strings.TrimSpace(out), "\n")
	require.Len(t, lines, 2, out)
	assert.Equal(t, []string{"2", "WARN", "slow", "response", `{"ms":900}`}, append(strings.Fields(lines[1])[:1],
		strings.Fields(lines[1])[2:]...))

	out, err = runRoot(ctx, "--config", cfg, "jobs", "logs", run.ID, "--limit", "1", "--json")
	require.NoError(t, err)
	var got []flywheel.RunLogLine
	require.NoError(t, json.Unmarshal([]byte(out), &got))
	require.Len(t, got, 1)
	assert.Equal(t, "fetching", got[0].Message)

	out, err = runRoot(ctx, "--config", cfg, "jobs", "logs", "no-such-run")
	require.NoError(t, err)
	assert.Equal(t, []string{"SEQ", "TIME", "LEVEL", "MESSAGE", "ATTRS"}, strings.Fields(out), "an unknown run has no lines")

	_, err = runRoot(ctx, "--config", cfg, "jobs", "logs", run.ID, "--level", "loud")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "--level")
	_, err = runRoot(ctx, "--config", cfg, "jobs", "logs")
	require.Error(t, err, "a run id is required")
}
