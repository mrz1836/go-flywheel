package main

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCLIDBBackedCommandsFailOnUnopenableDB drives the loadAndOpen error branch of
// every remaining DB-backed command with a config whose database cannot be opened,
// proving each surfaces the open error rather than panicking.
func TestCLIDBBackedCommandsFailOnUnopenableDB(t *testing.T) {
	t.Parallel()
	cfg := unopenableConfig(t)
	cases := [][]string{
		{"migrate"},
		{"doctor"},
		{"status"},
		{"prune"},
		{"enqueue", "exec", `{"command":"true"}`},
		{"schedule", "ls"},
		{"schedule", "add", "s", "exec", "--every", "1m", "--args", `{"command":"true"}`},
		{"schedule", "rm", "s"},
		{"schedule", "enable", "s"},
		{"schedule", "disable", "s"},
	}
	for _, args := range cases {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			t.Parallel()
			_, err := runRoot(context.Background(), append([]string{"--config", cfg}, args...)...)
			require.Error(t, err, "a database that cannot be opened is a command error")
		})
	}
}

func TestCLIScheduleAddRejectsInvalidArgsJSON(t *testing.T) {
	t.Parallel()
	cfg := migratedCLIConfig(t)
	_, err := runRoot(context.Background(), "--config", cfg, "schedule", "add", "s", "exec", "--every", "1m", "--args", "not-json")
	require.Error(t, err, "a non-JSON --args template is rejected before opening the database")
	assert.Contains(t, err.Error(), "valid JSON")
}

func TestCLIScheduleLsTextRendersIntervalCadence(t *testing.T) {
	t.Parallel()
	cfg := migratedCLIConfig(t)
	_, err := runRoot(context.Background(), "--config", cfg, "schedule", "add", "tick", "exec", "--every", "90s", "--args", `{"command":"true"}`)
	require.NoError(t, err)

	out, err := runRoot(context.Background(), "--config", cfg, "schedule", "ls")
	require.NoError(t, err)
	assert.Contains(t, out, "SLUG", "the table header is printed")
	assert.Contains(t, out, "tick")
	assert.Contains(t, out, "every 90s", "an interval schedule renders its cadence in the table")
}

func TestCLIScheduleLsJSONEmpty(t *testing.T) {
	t.Parallel()
	cfg := migratedCLIConfig(t)
	out, err := runRoot(context.Background(), "--config", cfg, "schedule", "ls", "--json")
	require.NoError(t, err)
	assert.Contains(t, out, "[]", "an empty schedule list renders as an empty JSON array")
}

func TestCLIScheduleAddDefaultArgsTemplate(t *testing.T) {
	t.Parallel()
	cfg := migratedCLIConfig(t)
	// No --args provided: the command falls back to the "{}" template.
	out, err := runRoot(context.Background(), "--config", cfg, "schedule", "add", "bare", "exec", "--every", "5m")
	require.NoError(t, err)
	assert.Contains(t, out, "scheduled bare (exec)")
}

func TestCLIScheduleAddRequiresValidSchedule(t *testing.T) {
	t.Parallel()
	cfg := migratedCLIConfig(t)
	// Neither --cron nor --every: UpsertPeriodic rejects a schedule with no cadence.
	_, err := runRoot(context.Background(), "--config", cfg, "schedule", "add", "nocadence", "exec", "--args", `{"command":"true"}`)
	require.Error(t, err, "a schedule with no cron or interval is rejected")
}

func TestCLIEnqueueWithScheduleAtAndFlags(t *testing.T) {
	t.Parallel()
	cfg := migratedCLIConfig(t)
	out, err := runRoot(context.Background(), "--config", cfg,
		"enqueue", "exec", `{"command":"true"}`,
		"--queue", "default", "--unique", "u1", "--priority", "5",
		"--at", "2999-01-01T00:00:00Z")
	require.NoError(t, err)
	assert.NotEmpty(t, strings.TrimSpace(out), "enqueue prints the new job id")
}

func TestCLIEnqueueRejectsBadAtTimestamp(t *testing.T) {
	t.Parallel()
	cfg := migratedCLIConfig(t)
	_, err := runRoot(context.Background(), "--config", cfg, "enqueue", "exec", `{"command":"true"}`, "--at", "not-a-time")
	require.Error(t, err, "a non-RFC3339 --at is rejected")
	assert.Contains(t, err.Error(), "RFC3339")
}

func TestCLIPruneRejectsBadOlderThan(t *testing.T) {
	t.Parallel()
	cfg := migratedCLIConfig(t)
	_, err := runRoot(context.Background(), "--config", cfg, "prune", "--older-than", "not-a-duration")
	require.Error(t, err, "an unparseable --older-than is rejected before opening the database")
	assert.Contains(t, err.Error(), "older-than")
}

func TestCLIDoctorReportsSchedules(t *testing.T) {
	t.Parallel()
	// A config with a schedule exercises doctor's schedule-listing loop.
	dir := t.TempDir()
	cfg := writeCLIConfig(t, dir, "schedules:\n"+
		"  - slug: nightly\n"+
		"    cron: \"0 2 * * *\"\n"+
		"    worker: exec\n"+
		"    exec:\n"+
		"      command: true\n"+
		"  - slug: ping\n"+
		"    every: 1m\n"+
		"    worker: http\n"+
		"    http:\n"+
		"      url: https://x.test/health\n")
	_, err := runRoot(context.Background(), "--config", cfg, "migrate")
	require.NoError(t, err)

	out, err := runRoot(context.Background(), "--config", cfg, "doctor")
	require.NoError(t, err)
	assert.Contains(t, out, "schedules:    2")
	assert.Contains(t, out, "nightly", "a cron schedule is listed")
	assert.Contains(t, out, "every 1m0s", "an interval schedule shows its cadence")
	assert.Contains(t, out, "sqlite:", "the sqlite hardening line is printed")
}
