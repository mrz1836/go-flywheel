package main

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	flywheel "github.com/mrz1836/go-flywheel"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
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

// insertTreeJob writes one jobs row directly, a child of parent when it is set.
func insertTreeJob(t *testing.T, db *gorm.DB, id, parent, kind, state string, at time.Time) {
	t.Helper()
	var parentID any
	if parent != "" {
		parentID = parent
	}
	require.NoError(t, db.Exec(
		`INSERT INTO jobs(id, kind, queue, args, priority, state, attempt, max_attempts, scheduled_at, parent_job_id, executor_class, tags, created_at, updated_at, metadata)
		 VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		id, kind, "default", "{}", 100, state, 0, 25, at, parentID, "", "[]", at, at, "{}",
	).Error)
}

// TestCLIJobsTreePrintsTheTreeIndented proves `jobs tree` prints one line per
// node, indented by depth, newest child first, each with its kind, state, and
// child count; a final truncated line when --depth or --nodes cut the walk; the
// JobTreeResult with --json; and a not-found error for an unknown root.
func TestCLIJobsTreePrintsTheTreeIndented(t *testing.T) {
	t.Parallel()
	cfg := migratedCLIConfig(t)
	db, _ := openCLIConfigDB(t, cfg)
	at := statusAnchor
	insertTreeJob(t, db, "t-root", "", "batch", "running", at)
	insertTreeJob(t, db, "t-a", "t-root", "item", "succeeded", at)
	insertTreeJob(t, db, "t-b", "t-root", "item", "running", at)
	insertTreeJob(t, db, "t-b1", "t-b", "part", "available", at)
	insertTreeJob(t, db, "t-b2", "t-b", "part", "retryable", at)
	ctx := context.Background()

	out, err := runRoot(ctx, "--config", cfg, "jobs", "tree", "t-root")
	require.NoError(t, err)
	assert.Equal(t, strings.Join([]string{
		"t-root  batch  running  children: 2",
		"  t-b  item  running  children: 2",
		"    t-b2  part  retryable  children: 0",
		"    t-b1  part  available  children: 0",
		"  t-a  item  succeeded  children: 0",
	}, "\n")+"\n", out)

	out, err = runRoot(ctx, "--config", cfg, "jobs", "tree", "t-root", "--depth", "1")
	require.NoError(t, err)
	assert.NotContains(t, out, "t-b1", "--depth stops the walk")
	assert.Contains(t, out, "  t-b  item  running  children: 2", "a node past the depth is still counted")
	assert.Regexp(t, `(?m)^truncated: .*--depth.*--nodes`, out)

	out, err = runRoot(ctx, "--config", cfg, "jobs", "tree", "t-root", "--nodes", "2")
	require.NoError(t, err)
	assert.Contains(t, out, "  t-b  ")
	assert.NotContains(t, out, "t-a", "--nodes stops the walk at the newest child")
	assert.Contains(t, out, "truncated: ")

	out, err = runRoot(ctx, "--config", cfg, "jobs", "tree", "t-root", "--json")
	require.NoError(t, err)
	var res flywheel.JobTreeResult
	require.NoError(t, json.Unmarshal([]byte(out), &res))
	assert.Equal(t, 5, res.Nodes)
	assert.False(t, res.Truncated)
	assert.Equal(t, "t-root", res.Root.ID)
	assert.Equal(t, int64(2), res.Root.ChildCount)
	require.Len(t, res.Root.Children, 2)
	assert.Equal(t, "t-b", res.Root.Children[0].ID)

	_, err = runRoot(ctx, "--config", cfg, "jobs", "tree", "no-such-job")
	require.ErrorIs(t, err, flywheel.ErrJobNotFound)
}
