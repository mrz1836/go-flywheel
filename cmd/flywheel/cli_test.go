package main

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// runRoot executes the root command with args, capturing combined output. A nil
// update nudge keeps the command tree network-free in tests.
func runRoot(ctx context.Context, args ...string) (string, error) {
	root := newRootCmd(nil)
	var buf bytes.Buffer
	root.SetOut(&buf)
	root.SetErr(&buf)
	root.SetArgs(args)
	err := root.ExecuteContext(ctx)
	return buf.String(), err
}

// writeCLIConfig writes a SQLite-backed flywheel.yaml in dir, appending extra
// top-level YAML (e.g. a schedules block).
func writeCLIConfig(t *testing.T, dir, extra string) string {
	t.Helper()
	body := fmt.Sprintf(`db:
  sqlite: %s/flywheel.db
runtime:
  queues: [default, periodic]
  concurrency: 1
  poll_interval: 30ms
log:
  level: error
%s`, dir, extra)
	p := filepath.Join(dir, "flywheel.yaml")
	require.NoError(t, os.WriteFile(p, []byte(body), 0o600))
	return p
}

func TestCLIMigrateEnqueueListDoctor(t *testing.T) {
	t.Parallel()
	cfg := writeCLIConfig(t, t.TempDir(), "")
	ctx := context.Background()

	out, err := runRoot(ctx, "--config", cfg, "migrate")
	require.NoError(t, err)
	assert.Contains(t, out, "schema up to date")

	out, err = runRoot(ctx, "--config", cfg, "enqueue", "exec", `{"command":"true"}`)
	require.NoError(t, err)
	jobID := strings.TrimSpace(out)
	require.NotEmpty(t, jobID)

	out, err = runRoot(ctx, "--config", cfg, "jobs", "ls", "--json")
	require.NoError(t, err)
	assert.Contains(t, out, jobID)

	out, err = runRoot(ctx, "--config", cfg, "doctor")
	require.NoError(t, err)
	assert.Contains(t, out, "status:")
	assert.Contains(t, out, "OK")
}

func TestCLIEnqueueRejectsInvalidJSON(t *testing.T) {
	t.Parallel()
	cfg := writeCLIConfig(t, t.TempDir(), "")
	_, err := runRoot(context.Background(), "--config", cfg, "migrate")
	require.NoError(t, err)

	_, err = runRoot(context.Background(), "--config", cfg, "enqueue", "exec", "not-json")
	require.Error(t, err)
}

func TestCLIScheduleAddAndList(t *testing.T) {
	t.Parallel()
	cfg := writeCLIConfig(t, t.TempDir(), "")
	ctx := context.Background()
	_, err := runRoot(ctx, "--config", cfg, "migrate")
	require.NoError(t, err)

	_, err = runRoot(ctx, "--config", cfg, "schedule", "add", "nightly", "exec", "--cron", "0 2 * * *", "--args", `{"command":"true"}`)
	require.NoError(t, err)

	out, err := runRoot(ctx, "--config", cfg, "schedule", "ls")
	require.NoError(t, err)
	assert.Contains(t, out, "nightly")
	assert.Contains(t, out, "0 2 * * *")
}

func TestCLIScheduleDisableEnableRm(t *testing.T) {
	t.Parallel()
	cfg := writeCLIConfig(t, t.TempDir(), "")
	ctx := context.Background()
	_, err := runRoot(ctx, "--config", cfg, "migrate")
	require.NoError(t, err)

	_, err = runRoot(ctx, "--config", cfg, "schedule", "add", "job1", "exec", "--every", "1m", "--args", `{"command":"true"}`)
	require.NoError(t, err)

	out, err := runRoot(ctx, "--config", cfg, "schedule", "disable", "job1")
	require.NoError(t, err)
	assert.Contains(t, out, "disabled job1")
	out, err = runRoot(ctx, "--config", cfg, "schedule", "ls", "--json")
	require.NoError(t, err)
	assert.Contains(t, out, `"active": false`)

	out, err = runRoot(ctx, "--config", cfg, "schedule", "enable", "job1")
	require.NoError(t, err)
	assert.Contains(t, out, "enabled job1")
	out, err = runRoot(ctx, "--config", cfg, "schedule", "ls", "--json")
	require.NoError(t, err)
	assert.Contains(t, out, `"active": true`)

	out, err = runRoot(ctx, "--config", cfg, "schedule", "rm", "job1")
	require.NoError(t, err)
	assert.Contains(t, out, "removed job1")
	out, err = runRoot(ctx, "--config", cfg, "schedule", "ls", "--json")
	require.NoError(t, err)
	assert.NotContains(t, out, "job1")

	// A missing slug is an error, not a silent success.
	_, err = runRoot(ctx, "--config", cfg, "schedule", "rm", "ghost")
	require.Error(t, err)
}

func TestCLIPruneDeletesOldFinishedJobs(t *testing.T) {
	t.Parallel()
	cfg := writeCLIConfig(t, t.TempDir(), "")
	ctx := context.Background()
	_, err := runRoot(ctx, "--config", cfg, "migrate")
	require.NoError(t, err)

	// Seed one old terminal job directly into the daemon's database.
	loaded, err := LoadConfig(cfg)
	require.NoError(t, err)
	db, _, err := openDB(loaded)
	require.NoError(t, err)
	old := time.Now().Add(-30 * 24 * time.Hour)
	require.NoError(t, db.Exec(
		`INSERT INTO jobs(id, kind, queue, args, priority, state, attempt, max_attempts, scheduled_at, finalized_at, executor_class, tags, created_at, updated_at, metadata)
		 VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		"old-done", "exec", "default", "{}", 100, "succeeded", 1, 25, old, old, "", "[]", old, old, "{}",
	).Error)
	closeDB(db)

	out, err := runRoot(ctx, "--config", cfg, "prune", "--older-than", "14d")
	require.NoError(t, err)
	assert.Contains(t, out, "pruned 1")

	out, err = runRoot(ctx, "--config", cfg, "jobs", "ls", "--json")
	require.NoError(t, err)
	assert.NotContains(t, out, "old-done", "the old finished job is pruned")
}

func TestCLIServeProcessesEnqueuedAndScheduledJobs(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	cfg := writeCLIConfig(t, dir, "schedules:\n"+
		"  - slug: tick\n"+
		"    every: 1s\n"+
		"    worker: exec\n"+
		"    exec:\n"+
		"      command: sh\n"+
		"      args: [\"-c\", \"true\"]\n")
	ctx := context.Background()

	_, err := runRoot(ctx, "--config", cfg, "migrate")
	require.NoError(t, err)
	out, err := runRoot(ctx, "--config", cfg, "enqueue", "exec", `{"command":"sh","args":["-c","true"]}`)
	require.NoError(t, err)
	jobID := strings.TrimSpace(out)

	// Run the daemon in the background; cancel it once the work is done.
	serveCtx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		_, _ = runRoot(serveCtx, "--config", cfg, "serve")
		close(done)
	}()

	// The enqueued exec job reaches succeeded, and the schedule fires at least
	// one job (proving the cron-replacement path end to end).
	require.Eventually(t, func() bool {
		inspect, ierr := runRoot(context.Background(), "--config", cfg, "jobs", "inspect", jobID, "--json")
		return ierr == nil && strings.Contains(inspect, `"state": "succeeded"`)
	}, 8*time.Second, 50*time.Millisecond, "serve processed the enqueued exec job")

	require.Eventually(t, func() bool {
		ls, lerr := runRoot(context.Background(), "--config", cfg, "jobs", "ls", "--kind", "exec", "--json")
		// More than one exec job means the periodic schedule fired beyond the one
		// we enqueued by hand.
		return lerr == nil && strings.Count(ls, `"id"`) >= 2
	}, 8*time.Second, 50*time.Millisecond, "the periodic schedule fired exec jobs")

	cancel()
	<-done
}
