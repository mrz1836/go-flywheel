package main

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	flywheel "github.com/mrz1836/go-flywheel"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// scheduleCell returns the cell under column on slug's `flywheel schedule ls`
// line, read at the column's offset in the header: the table writer aligns
// every cell with its header, and a cell can hold spaces.
func scheduleCell(t *testing.T, out, slug, column string) string {
	t.Helper()
	lines := strings.Split(out, "\n")
	start := strings.Index(lines[0], column)
	require.GreaterOrEqual(t, start, 0, "no %s column:\n%s", column, out)
	end := start + len(column)
	for end < len(lines[0]) && lines[0][end] == ' ' {
		end++
	}
	for _, line := range lines[1:] {
		if fields := strings.Fields(line); len(fields) > 0 && fields[0] == slug {
			if end >= len(lines[0]) || end > len(line) {
				return strings.TrimSpace(line[start:])
			}
			return strings.TrimSpace(line[start:end])
		}
	}
	require.Failf(t, "no schedule row", "%s is not listed:\n%s", slug, out)
	return ""
}

// scheduleAudit reads slug's control history, oldest first.
func scheduleAudit(t *testing.T, cfg, slug string) []flywheel.AuditEntry {
	t.Helper()
	_, db, _, err := loadAndOpen(cfg)
	require.NoError(t, err)
	defer closeDB(db)
	page, err := flywheel.ListAudit(context.Background(), db, flywheel.ListAuditParams{
		TargetType: flywheel.AuditTargetPeriodic, TargetID: slug, Order: flywheel.AuditOldest,
	})
	require.NoError(t, err)
	return page.Entries
}

// TestCLISchedulePauseResume proves `schedule pause` and `resume` hold and
// release a schedule under the operator's name and --reason, that `ls` shows
// the pause and who made it in its table and its JSON, that pausing twice is
// harmless and records once, and that an unknown slug is refused.
func TestCLISchedulePauseResume(t *testing.T) {
	t.Parallel()
	cfg := migratedCLIConfig(t)
	ctx := context.Background()
	_, err := runRoot(ctx, "--config", cfg, "schedule", "add", "job1", "exec", "--every", "1m", "--args", `{"command":"true"}`)
	require.NoError(t, err)

	out, err := runRoot(ctx, "--config", cfg, "schedule", "pause", "job1", "--reason", "provider outage")
	require.NoError(t, err)
	assert.Contains(t, out, "paused job1")
	out, err = runRoot(ctx, "--config", cfg, "schedule", "pause", "job1")
	require.NoError(t, err, "pausing a paused schedule is not an error")
	assert.Contains(t, out, "paused job1")

	out, err = runRoot(ctx, "--config", cfg, "schedule", "ls", "--json")
	require.NoError(t, err)
	assert.Contains(t, out, `"paused": true`)
	assert.Contains(t, out, `"paused_at":`)
	assert.Contains(t, out, `"reason": "provider outage"`, "the JSON says why")
	assert.Contains(t, out, `"active": true`, "a pause leaves the schedule active")
	out, err = runRoot(ctx, "--config", cfg, "schedule", "ls")
	require.NoError(t, err)
	assert.Equal(t, []string{"SLUG", "KIND", "SCHEDULE", "ACTIVE", "PAUSED", "SOURCE", "NEXT", "RUN"},
		strings.Fields(strings.SplitN(out, "\n", 2)[0]))
	assert.Equal(t, "yes (user:"+osUserName()+")", scheduleCell(t, out, "job1", "PAUSED"), "the table says who paused it")

	out, err = runRoot(ctx, "--config", cfg, "schedule", "resume", "job1", "--reason", "provider back")
	require.NoError(t, err)
	assert.Contains(t, out, "resumed job1")
	out, err = runRoot(ctx, "--config", cfg, "schedule", "ls", "--json")
	require.NoError(t, err)
	assert.Contains(t, out, `"paused": false`)
	assert.NotContains(t, out, "paused_at")
	assert.NotContains(t, out, "paused_by")
	out, err = runRoot(ctx, "--config", cfg, "schedule", "ls")
	require.NoError(t, err)
	assert.Equal(t, "-", scheduleCell(t, out, "job1", "PAUSED"))

	entries := scheduleAudit(t, cfg, "job1")
	require.Len(t, entries, 2, "one entry each for the pause and the resume; the second pause recorded nothing")
	assert.Equal(t, flywheel.AuditPaused, entries[0].Action)
	assertOperator(t, entries[0], "provider outage")
	assert.Equal(t, flywheel.AuditResumed, entries[1].Action)
	assertOperator(t, entries[1], "provider back")

	_, err = runRoot(ctx, "--config", cfg, "schedule", "pause", "ghost")
	require.ErrorIs(t, err, flywheel.ErrPeriodicNotFound)
	_, err = runRoot(ctx, "--config", cfg, "schedule", "resume", "ghost")
	require.ErrorIs(t, err, flywheel.ErrPeriodicNotFound)
}

// TestCLIScheduleTriggerAndHistory proves `schedule trigger` runs a paused
// schedule now and prints its job, recorded under the operator's name and
// --reason; that `history` lists the run as a manual fire, newest first, in
// its table and its JSON; and that both refuse an unknown slug.
func TestCLIScheduleTriggerAndHistory(t *testing.T) {
	t.Parallel()
	cfg := migratedCLIConfig(t)
	ctx := context.Background()
	_, err := runRoot(ctx, "--config", cfg, "schedule", "add", "nightly", "exec", "--every", "1h", "--args", `{"command":"true"}`)
	require.NoError(t, err)
	out, err := runRoot(ctx, "--config", cfg, "schedule", "history", "nightly")
	require.NoError(t, err)
	assert.Equal(t, []string{"JOB", "ID", "FIRED", "AT", "MANUAL", "STATE", "OUTCOME", "FINISHED"}, strings.Fields(out),
		"a schedule that has not fired prints the header alone")
	_, err = runRoot(ctx, "--config", cfg, "schedule", "pause", "nightly")
	require.NoError(t, err)

	out, err = runRoot(ctx, "--config", cfg, "schedule", "trigger", "nightly", "--reason", "re-run last night")
	require.NoError(t, err)
	first := strings.TrimPrefix(strings.TrimSpace(out), "triggered nightly: job ")
	require.NotEqual(t, strings.TrimSpace(out), first, "trigger prints the job it enqueued: %q", out)
	out, err = runRoot(ctx, "--config", cfg, "schedule", "trigger", "nightly")
	require.NoError(t, err)
	second := strings.TrimPrefix(strings.TrimSpace(out), "triggered nightly: job ")
	assert.NotEqual(t, first, second, "a second trigger enqueues a second job")

	out, err = runRoot(ctx, "--config", cfg, "schedule", "history", "nightly")
	require.NoError(t, err)
	lines := strings.Split(strings.TrimSpace(out), "\n")
	require.Len(t, lines, 3, out)
	for i, id := range []string{second, first} {
		fields := strings.Fields(lines[i+1])
		require.Len(t, fields, 6, lines[i+1])
		assert.Equal(t, id, fields[0], "newest first")
		assert.Equal(t, "yes", fields[2], "a manual run")
		assert.Equal(t, string(flywheel.StateAvailable), fields[3])
		assert.Equal(t, []string{"-", "-"}, fields[4:], "no run yet")
	}
	out, err = runRoot(ctx, "--config", cfg, "schedule", "history", "nightly", "--json")
	require.NoError(t, err)
	var history []flywheel.PeriodicFire
	require.NoError(t, json.Unmarshal([]byte(out), &history), out)
	require.Len(t, history, 2)
	assert.Equal(t, second, history[0].JobID)
	assert.True(t, history[0].Manual)

	entries := scheduleAudit(t, cfg, "nightly")
	require.Len(t, entries, 3, "the pause and two triggers")
	assert.Equal(t, flywheel.AuditTriggered, entries[1].Action)
	assertOperator(t, entries[1], "re-run last night")
	assert.JSONEq(t, `{"job_id":"`+first+`"}`, string(entries[1].Details))
	out, err = runRoot(ctx, "--config", cfg, "schedule", "ls")
	require.NoError(t, err)
	assert.Equal(t, "upserted", scheduleCell(t, out, "nightly", "SOURCE"), "schedule add upserts")

	_, err = runRoot(ctx, "--config", cfg, "schedule", "trigger", "ghost")
	require.ErrorIs(t, err, flywheel.ErrPeriodicNotFound)
	_, err = runRoot(ctx, "--config", cfg, "schedule", "history", "ghost")
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

	h = help("schedule", "trigger", "--help")
	assert.Contains(t, h, "works on a paused or disabled schedule")
	assert.Contains(t, h, "--reason")
	h = help("schedule", "history", "--help")
	assert.Contains(t, h, "newest first")
	assert.Contains(t, h, "--json")
	for _, cmd := range []string{"pause", "resume"} {
		assert.Contains(t, help("schedule", cmd, "--help"), "--reason", cmd)
	}

	h = help("schedule", "--help")
	assert.Contains(t, h, "pause Hold a schedule until it is resumed, through every restart")
	assert.Contains(t, h, "resume Resume a paused schedule at its next fire time")
	assert.Contains(t, h, "trigger Run a schedule now, paused or not")
	assert.Contains(t, h, "history Show a schedule's latest fires and how each ended")
}
