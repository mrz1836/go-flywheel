package main

import (
	"context"
	"strings"
	"testing"

	flywheel "github.com/mrz1836/go-flywheel"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// pausedColumn returns the PAUSED cell of slug's `flywheel schedule ls` line:
// the third field from the end, before the next run's date and time, since the
// schedule cell itself can hold spaces.
func pausedColumn(t *testing.T, out, slug string) string {
	t.Helper()
	for _, line := range strings.Split(out, "\n") {
		if fields := strings.Fields(line); len(fields) > 3 && fields[0] == slug {
			return fields[len(fields)-3]
		}
	}
	require.Failf(t, "no schedule row", "%s is not listed:\n%s", slug, out)
	return ""
}

// TestCLISchedulePauseResume proves `schedule pause` and `resume` hold and
// release a schedule, that `ls` shows it in its table and its JSON, that
// pausing twice is harmless, and that an unknown slug is refused.
func TestCLISchedulePauseResume(t *testing.T) {
	t.Parallel()
	cfg := migratedCLIConfig(t)
	ctx := context.Background()
	_, err := runRoot(ctx, "--config", cfg, "schedule", "add", "job1", "exec", "--every", "1m", "--args", `{"command":"true"}`)
	require.NoError(t, err)

	out, err := runRoot(ctx, "--config", cfg, "schedule", "pause", "job1")
	require.NoError(t, err)
	assert.Contains(t, out, "paused job1")
	out, err = runRoot(ctx, "--config", cfg, "schedule", "pause", "job1")
	require.NoError(t, err, "pausing a paused schedule is not an error")
	assert.Contains(t, out, "paused job1")

	out, err = runRoot(ctx, "--config", cfg, "schedule", "ls", "--json")
	require.NoError(t, err)
	assert.Contains(t, out, `"paused": true`)
	assert.Contains(t, out, `"paused_at":`)
	assert.Contains(t, out, `"active": true`, "a pause leaves the schedule active")
	out, err = runRoot(ctx, "--config", cfg, "schedule", "ls")
	require.NoError(t, err)
	assert.Equal(t, []string{"SLUG", "KIND", "SCHEDULE", "ACTIVE", "PAUSED", "NEXT", "RUN"},
		strings.Fields(strings.SplitN(out, "\n", 2)[0]))
	assert.Equal(t, "yes", pausedColumn(t, out, "job1"))

	out, err = runRoot(ctx, "--config", cfg, "schedule", "resume", "job1")
	require.NoError(t, err)
	assert.Contains(t, out, "resumed job1")
	out, err = runRoot(ctx, "--config", cfg, "schedule", "ls", "--json")
	require.NoError(t, err)
	assert.Contains(t, out, `"paused": false`)
	assert.NotContains(t, out, "paused_at")
	out, err = runRoot(ctx, "--config", cfg, "schedule", "ls")
	require.NoError(t, err)
	assert.Equal(t, "-", pausedColumn(t, out, "job1"))

	_, err = runRoot(ctx, "--config", cfg, "schedule", "pause", "ghost")
	require.ErrorIs(t, err, flywheel.ErrPeriodicNotFound)
	_, err = runRoot(ctx, "--config", cfg, "schedule", "resume", "ghost")
	require.ErrorIs(t, err, flywheel.ErrPeriodicNotFound)
}

// TestCLIScheduleHelpSaysWhichSwitchHoldsThroughAStart pins the help to the
// behavior: enable and disable are the config's switch, which serve's next
// start re-applies; pause holds through every start until resume, which runs
// none of the fires the pause skipped.
func TestCLIScheduleHelpSaysWhichSwitchHoldsThroughAStart(t *testing.T) {
	t.Parallel()
	help := func(args ...string) string {
		t.Helper()
		out, err := runRoot(context.Background(), args...)
		require.NoError(t, err)
		return strings.Join(strings.Fields(out), " ")
	}

	for _, cmd := range []string{"enable", "disable"} {
		h := help("schedule", cmd, "--help")
		assert.Contains(t, h, "the next `flywheel serve` start re-applies", cmd)
		assert.Contains(t, h, "flywheel schedule pause", cmd)
	}
	h := help("schedule", "pause", "--help")
	assert.Contains(t, h, "stays paused through every `flywheel serve` start")
	assert.Contains(t, h, "until `flywheel schedule resume`")
	h = help("schedule", "resume", "--help")
	assert.Contains(t, h, "first fire time after now")
	assert.Contains(t, h, "missed while paused are not run")

	h = help("schedule", "--help")
	assert.Contains(t, h, "pause Hold a schedule until it is resumed, through every restart")
	assert.Contains(t, h, "resume Resume a paused schedule at its next fire time")
}
