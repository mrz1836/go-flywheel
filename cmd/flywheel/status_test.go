package main

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	flywheel "github.com/mrz1836/go-flywheel"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// statusAnchor is the fixed clock instant the status tests seed and sample
// against, so ready/scheduled/lag and the failure window are exact and
// independent of the wall clock.
//
//nolint:gochecknoglobals // shared deterministic test anchor
var statusAnchor = time.Date(2026, 6, 22, 12, 0, 0, 0, time.UTC)

// statusClockCtx returns a context carrying a fixed clock at statusAnchor, so the
// status command reads "now" deterministically.
func statusClockCtx() context.Context {
	return flywheel.WithClock(context.Background(), flywheel.NewFixedClock(statusAnchor))
}

// seedStatusJobs inserts a known spread of jobs — one ready, one running, one
// succeeded, one scheduled-ahead, and one discarded with a failure run — directly
// into the daemon's database, timed relative to statusAnchor so the status
// snapshot is exact under the fixed test clock.
func seedStatusJobs(t *testing.T, cfgPath string) {
	t.Helper()
	loaded, err := LoadConfig(cfgPath)
	require.NoError(t, err)
	db, _, err := openDB(loaded)
	require.NoError(t, err)
	defer closeDB(db)

	now := statusAnchor
	past := now.Add(-time.Minute)
	ahead := now.Add(time.Hour)
	finalized := now.Add(-time.Hour)

	insertJob := func(id, kind, state string, scheduledAt time.Time, finalizedAt any) {
		require.NoError(t, db.Exec(
			`INSERT INTO jobs(id, kind, queue, args, priority, state, attempt, max_attempts, scheduled_at, finalized_at, executor_class, tags, created_at, updated_at, metadata)
			 VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
			id, kind, "default", "{}", 100, state, 1, 25, scheduledAt, finalizedAt, "", "[]", now, now, "{}",
		).Error)
	}
	insertJob("st-ready", "exec", "available", past, nil)
	insertJob("st-run", "exec", "running", past, nil)
	insertJob("st-done", "exec", "succeeded", past, finalized)
	insertJob("st-ahead", "exec", "scheduled", ahead, nil)
	insertJob("st-fail", "exec", "discarded", past, finalized)

	require.NoError(t, db.Exec(
		`INSERT INTO job_runs(id, job_id, attempt, executor_class, executor_id, started_at, outcome, error_class, error_message, created_at)
		 VALUES (?,?,?,?,?,?,?,?,?,?)`,
		"st-fail-run", "st-fail", 1, "local", "h", finalized, "error", "permanent", "kaboom", finalized,
	).Error)
}

func TestCLIStatusTextReport(t *testing.T) {
	t.Parallel()
	cfg := writeCLIConfig(t, t.TempDir(), "")
	_, err := runRoot(context.Background(), "--config", cfg, "migrate")
	require.NoError(t, err)
	_, err = runRoot(context.Background(), "--config", cfg, "schedule", "add", "nightly", "exec", "--cron", "0 2 * * *", "--args", `{"command":"true"}`)
	require.NoError(t, err)

	seedStatusJobs(t, cfg)

	out, err := runRoot(statusClockCtx(), "--config", cfg, "status")
	require.NoError(t, err)
	assert.Contains(t, out, "flywheel status:")
	assert.Regexp(t, `ready:\s+1`, out, "one job is claimable now")
	assert.Regexp(t, `in-flight:\s+1`, out, "one job is running")
	assert.Regexp(t, `scheduled:\s+1`, out, "one job is scheduled ahead")
	assert.Contains(t, out, "lag:          1m0s", "the oldest ready job is one minute behind")
	assert.Regexp(t, `jobs:\s+5 total`, out)
	assert.Contains(t, out, "schedules:    1 active")
	assert.Contains(t, out, "nightly", "the active schedule is listed")
	assert.Contains(t, out, "recent failures (last 24h): 1")
	assert.Contains(t, out, "st-fail", "the failed job is listed")
	assert.Contains(t, out, "kaboom", "the failure's error message is shown")
}

func TestCLIStatusJSON(t *testing.T) {
	t.Parallel()
	cfg := writeCLIConfig(t, t.TempDir(), "")
	_, err := runRoot(context.Background(), "--config", cfg, "migrate")
	require.NoError(t, err)

	seedStatusJobs(t, cfg)

	out, err := runRoot(statusClockCtx(), "--config", cfg, "status", "--json")
	require.NoError(t, err)

	var report statusReport
	require.NoError(t, json.Unmarshal([]byte(out), &report))
	assert.EqualValues(t, 1, report.Health.Ready)
	assert.EqualValues(t, 1, report.Health.InFlight)
	assert.EqualValues(t, 1, report.Health.ScheduledAhead)
	assert.Equal(t, time.Minute, report.Health.OldestReadyAge, "lag serializes as a duration")
	assert.Equal(t, 5, report.Overview.Total)
	require.Len(t, report.Failures, 1)
	assert.Equal(t, "st-fail", report.Failures[0].JobID)
	assert.Equal(t, "permanent", report.Failures[0].ErrorClass)
	assert.Equal(t, "kaboom", report.Failures[0].ErrorMessage)
}

func TestCLIStatusEmptyQueue(t *testing.T) {
	t.Parallel()
	cfg := writeCLIConfig(t, t.TempDir(), "")
	_, err := runRoot(context.Background(), "--config", cfg, "migrate")
	require.NoError(t, err)

	out, err := runRoot(statusClockCtx(), "--config", cfg, "status")
	require.NoError(t, err)
	assert.Regexp(t, `ready:\s+0`, out)
	assert.Contains(t, out, "lag:          none", "an empty queue reports no lag")
	assert.Contains(t, out, "recent failures (last 24h): 0")
}

func TestCLIStatusWatchRendersThenStopsOnCancel(t *testing.T) {
	t.Parallel()
	cfg := writeCLIConfig(t, t.TempDir(), "")
	_, err := runRoot(context.Background(), "--config", cfg, "migrate")
	require.NoError(t, err)
	seedStatusJobs(t, cfg)

	// A short-lived context: --watch renders at least one frame, then returns
	// cleanly when the context is cancelled (the Ctrl-C path).
	ctx, cancel := context.WithTimeout(statusClockCtx(), 150*time.Millisecond)
	defer cancel()
	out, err := runRoot(ctx, "--config", cfg, "status", "--watch", "--interval", "20ms")
	require.NoError(t, err, "watch returns nil when its context is cancelled")
	assert.Contains(t, out, "flywheel status:", "watch rendered at least one frame")
}
