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

// historyJSON runs `jobs history <id> --json` and decodes the page.
func historyJSON(t *testing.T, cfg, id string) flywheel.AuditPage {
	t.Helper()
	out, err := runRoot(context.Background(), "--config", cfg, "jobs", "history", id, "--json")
	require.NoError(t, err)
	var page flywheel.AuditPage
	require.NoError(t, json.Unmarshal([]byte(out), &page), out)
	return page
}

// assertOperator asserts e was recorded under the CLI's actor with reason.
func assertOperator(t *testing.T, e flywheel.AuditEntry, reason string) {
	t.Helper()
	assert.Equal(t, flywheel.ActorUser, e.Actor.Type)
	assert.Equal(t, osUserName(), e.Actor.ID, "the operating-system user")
	assert.Equal(t, "cli", e.Actor.Source)
	assert.Equal(t, reason, e.Actor.Reason)
}

// TestCLIControlsRecordTheOperatorAndReason proves each control command records
// the operating-system user, the source cli, and its --reason.
func TestCLIControlsRecordTheOperatorAndReason(t *testing.T) {
	t.Parallel()
	cfg := migratedCLIConfig(t)
	ctx := context.Background()

	cancelID := enqueueOne(t, cfg)
	out, err := runRoot(ctx, "--config", cfg, "jobs", "cancel", cancelID, "--reason", "stuck behind an outage")
	require.NoError(t, err)
	assert.Contains(t, out, "cancelled "+cancelID)
	page := historyJSON(t, cfg, cancelID)
	require.Len(t, page.Entries, 1)
	assert.Equal(t, flywheel.AuditCancelled, page.Entries[0].Action)
	assertOperator(t, page.Entries[0], "stuck behind an outage")

	retryID := enqueueOne(t, cfg)
	_, err = runRoot(ctx, "--config", cfg, "jobs", "retry", retryID, "--reason", "the dependency is back")
	require.NoError(t, err)
	page = historyJSON(t, cfg, retryID)
	require.Len(t, page.Entries, 1)
	assert.Equal(t, flywheel.AuditRetried, page.Entries[0].Action)
	assertOperator(t, page.Entries[0], "the dependency is back")

	_, err = runRoot(ctx, "--config", cfg, "schedule", "add", "nightly", "exec", "--every", "1h", "--args", `{"command":"true"}`)
	require.NoError(t, err)
	for _, args := range [][]string{
		{"schedule", "disable", "nightly", "--reason", "maintenance window"},
		{"schedule", "enable", "nightly", "--reason", "window closed"},
		{"schedule", "rm", "nightly", "--reason", "retired"},
	} {
		_, err = runRoot(ctx, append([]string{"--config", cfg}, args...)...)
		require.NoError(t, err, args)
	}
	_, db, _, err := loadAndOpen(cfg)
	require.NoError(t, err)
	defer closeDB(db)
	schedule, err := flywheel.ListAudit(ctx, db, flywheel.ListAuditParams{
		TargetType: flywheel.AuditTargetPeriodic, TargetID: "nightly", Order: flywheel.AuditOldest,
	})
	require.NoError(t, err)
	require.Len(t, schedule.Entries, 3)
	for i, want := range []struct {
		action flywheel.AuditAction
		reason string
	}{
		{flywheel.AuditDeactivated, "maintenance window"},
		{flywheel.AuditActivated, "window closed"},
		{flywheel.AuditDeleted, "retired"},
	} {
		assert.Equal(t, want.action, schedule.Entries[i].Action)
		assertOperator(t, schedule.Entries[i], want.reason)
	}
}

// TestCLIJobsHistoryPrintsTheJobsEntries proves `jobs history` prints a job's
// entries newest first, pages with --limit and --cursor, and writes the page as
// JSON with --json.
func TestCLIJobsHistoryPrintsTheJobsEntries(t *testing.T) {
	t.Parallel()
	cfg := migratedCLIConfig(t)
	ctx := context.Background()
	id := enqueueOne(t, cfg)
	for _, args := range [][]string{
		{"jobs", "retry", id, "--reason", "first"},
		{"jobs", "cancel", id, "--reason", "second"},
		{"jobs", "retry", id, "--force", "--reason", "third"},
	} {
		_, err := runRoot(ctx, append([]string{"--config", cfg}, args...)...)
		require.NoError(t, err, args)
	}
	actor := "user:" + osUserName()
	if osUserName() == "" {
		actor = "user"
	}

	out, err := runRoot(ctx, "--config", cfg, "jobs", "history", id)
	require.NoError(t, err)
	lines := strings.Split(strings.TrimSpace(out), "\n")
	require.Len(t, lines, 4, out)
	assert.Equal(t, []string{"WHEN", "ACTION", "ACTOR", "SOURCE", "REASON"}, strings.Fields(lines[0]))
	for i, want := range []struct{ action, reason string }{
		{"retried", "third"}, {"cancelled", "second"}, {"retried", "first"},
	} {
		fields := strings.Fields(lines[i+1])
		require.Len(t, fields, 5, lines[i+1])
		assert.True(t, strings.HasSuffix(fields[0], "Z"), "when, in UTC: %s", fields[0])
		assert.Equal(t, []string{want.action, actor, "cli", want.reason}, fields[1:], "newest first")
	}

	out, err = runRoot(ctx, "--config", cfg, "jobs", "history", id, "--limit", "2")
	require.NoError(t, err)
	lines = strings.Split(strings.TrimSpace(out), "\n")
	require.Len(t, lines, 4, out)
	cursor, ok := strings.CutPrefix(lines[3], "next: --cursor ")
	require.True(t, ok, "a further page is announced: %s", lines[3])
	out, err = runRoot(ctx, "--config", cfg, "jobs", "history", id, "--limit", "2", "--cursor", cursor)
	require.NoError(t, err)
	lines = strings.Split(strings.TrimSpace(out), "\n")
	require.Len(t, lines, 2, out)
	assert.Contains(t, lines[1], "first", "the cursor reads the rest")

	page := historyJSON(t, cfg, id)
	require.Len(t, page.Entries, 3)
	assert.Empty(t, page.NextCursor)
	assert.Equal(t, id, page.Entries[0].TargetID)

	quiet := enqueueOne(t, cfg)
	out, err = runRoot(ctx, "--config", cfg, "jobs", "history", quiet)
	require.NoError(t, err)
	assert.Len(t, strings.Split(strings.TrimSpace(out), "\n"), 1, "a job with no entries prints the header alone")
}
