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

	flywheel "github.com/mrz1836/go-flywheel"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// runRoot executes the root command with args, capturing combined output. The
// passive update check wired by cobracmd is a no-op under a dev build (the
// version tests resolve to in the test binary), so the command tree stays
// network-free in tests.
func runRoot(ctx context.Context, args ...string) (string, error) {
	root := newRootCmd()
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
	assert.Contains(t, out, "queue:", "doctor surfaces the queue-health summary")
	assert.Contains(t, out, "ready=", "doctor reports the ready count")
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

	// The stats rollup is on by default and has not run: the prune holds for it
	// rather than deleting runs it has yet to count.
	out, err := runRoot(ctx, "--config", cfg, "prune", "--older-than", "14d")
	require.NoError(t, err)
	assert.Contains(t, out, "pruned 0")
	assert.Contains(t, out, "held for the stats rollup")

	out, err = runRoot(ctx, "--config", cfg, "prune", "--older-than", "14d", "--ignore-stats-rollup")
	require.NoError(t, err)
	assert.Contains(t, out, "pruned 1")
	assert.NotContains(t, out, "held for")

	out, err = runRoot(ctx, "--config", cfg, "jobs", "ls", "--json")
	require.NoError(t, err)
	assert.NotContains(t, out, "old-done", "the old finished job is pruned")
}

// TestCLIPruneHoldsForTheStatsRollup proves `flywheel prune` follows the
// rollup's progress: once the rollup has counted the old job's run it may go,
// and with the rollup off in the config there is nothing to hold for.
func TestCLIPruneHoldsForTheStatsRollup(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	old := time.Now().Add(-30 * 24 * time.Hour)
	seed := func(t *testing.T, cfg string) *Config {
		t.Helper()
		_, err := runRoot(ctx, "--config", cfg, "migrate")
		require.NoError(t, err)
		loaded, err := LoadConfig(cfg)
		require.NoError(t, err)
		db, _, err := openDB(loaded)
		require.NoError(t, err)
		defer closeDB(db)
		require.NoError(t, db.Exec(
			`INSERT INTO jobs(id, kind, queue, args, priority, state, attempt, max_attempts, scheduled_at, finalized_at, executor_class, tags, created_at, updated_at, metadata)
			 VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
			"old-done", "exec", "default", "{}", 100, "succeeded", 1, 25, old, old, "", "[]", old, old, "{}",
		).Error)
		return loaded
	}

	t.Run("after the rollup has run", func(t *testing.T) {
		t.Parallel()
		cfg := writeCLIConfig(t, t.TempDir(), "")
		loaded := seed(t, cfg)
		db, _, err := openDB(loaded)
		require.NoError(t, err)
		_, err = flywheel.RebuildStats(ctx, db, flywheel.RebuildOpts{From: old.Add(-time.Hour), To: time.Now()})
		require.NoError(t, err)
		closeDB(db)

		out, err := runRoot(ctx, "--config", cfg, "prune", "--older-than", "14d")
		require.NoError(t, err)
		assert.Contains(t, out, "pruned 1", "the rollup covers the job's hour, so it may go")
	})

	t.Run("with the rollup off", func(t *testing.T) {
		t.Parallel()
		cfg := writeCLIConfig(t, t.TempDir(), "")
		require.NoError(t, os.WriteFile(cfg, []byte(strings.Replace(mustRead(t, cfg),
			"  poll_interval: 30ms\n", "  poll_interval: 30ms\n  stats_rollup: -1s\n", 1)), 0o600))
		seed(t, cfg)
		out, err := runRoot(ctx, "--config", cfg, "prune", "--older-than", "14d")
		require.NoError(t, err)
		assert.Contains(t, out, "pruned 1")
		assert.NotContains(t, out, "held for")
	})
}

// mustRead returns a file's contents.
func mustRead(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path) //nolint:gosec // a test's own temp file
	require.NoError(t, err)
	return string(b)
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

// TestCLIMigrateReportsAnUpgradeAndTakesLiveFlags proves `flywheel migrate` is
// the upgrade: on a database missing the analytics columns it reports what it
// added, accepts the live-upgrade flags (no-ops on SQLite), and a re-run reports
// nothing new.
func TestCLIMigrateReportsAnUpgradeAndTakesLiveFlags(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	cfg := writeCLIConfig(t, dir, "")
	ctx := context.Background()

	_, err := runRoot(ctx, "--config", cfg, "migrate")
	require.NoError(t, err)
	loaded, err := LoadConfig(cfg)
	require.NoError(t, err)
	db, _, err := openDB(loaded)
	require.NoError(t, err)
	require.NoError(t, db.Exec(`ALTER TABLE job_runs DROP COLUMN queue_wait_ms`).Error)
	closeDB(db)

	out, err := runRoot(ctx, "--config", cfg, "migrate", "--concurrently", "--lock-timeout", "5s")
	require.NoError(t, err)
	assert.Contains(t, out, "added: job_runs.queue_wait_ms")

	out, err = runRoot(ctx, "--config", cfg, "migrate")
	require.NoError(t, err)
	assert.NotContains(t, out, "added:", "an up-to-date schema adds nothing")
}
