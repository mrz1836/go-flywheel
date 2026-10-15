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

// inspectedJob is the part of `jobs inspect --json` the control tests read.
type inspectedJob struct {
	State       string    `json:"state"`
	Priority    int       `json:"priority"`
	ScheduledAt time.Time `json:"scheduled_at"`
}

// inspectJob reads a job through `jobs inspect --json`.
func inspectJob(t *testing.T, cfg, id string) inspectedJob {
	t.Helper()
	out, err := runRoot(context.Background(), "--config", cfg, "jobs", "inspect", id, "--json")
	require.NoError(t, err)
	var doc struct {
		Job inspectedJob `json:"job"`
	}
	require.NoError(t, json.Unmarshal([]byte(out), &doc), out)
	return doc.Job
}

// TestCLISingleJobControls proves the five single-job commands each run their
// control as the operating-system user with --reason, print what they did, and
// refuse a job they can't act on, changing nothing.
func TestCLISingleJobControls(t *testing.T) {
	t.Parallel()
	cfg := migratedCLIConfig(t)
	ctx := context.Background()
	run := func(args ...string) (string, error) {
		return runRoot(ctx, append([]string{"--config", cfg}, args...)...)
	}

	id := enqueueOne(t, cfg)
	out, err := run("jobs", "reschedule", id, "--at", "2026-10-11T09:00:00Z", "--reason", "after the vendor window")
	require.NoError(t, err)
	assert.Contains(t, out, "rescheduled "+id)
	assert.True(t, inspectJob(t, cfg, id).ScheduledAt.Equal(time.Date(2026, 10, 11, 9, 0, 0, 0, time.UTC)))
	page := historyJSON(t, cfg, id)
	require.Len(t, page.Entries, 1)
	assert.Equal(t, flywheel.AuditAction("rescheduled"), page.Entries[0].Action)
	assertOperator(t, page.Entries[0], "after the vendor window")

	_, err = run("jobs", "reschedule", id, "--at", "tomorrow", "--reason", "later")
	require.ErrorContains(t, err, "invalid --at (want RFC3339)")
	_, err = run("jobs", "reschedule", id, "--reason", "later")
	require.ErrorContains(t, err, `"at"`, "--at is required")
	assert.True(t, inspectJob(t, cfg, id).ScheduledAt.Equal(time.Date(2026, 10, 11, 9, 0, 0, 0, time.UTC)),
		"a refused reschedule changes nothing")
	assert.Len(t, historyJSON(t, cfg, id).Entries, 1)

	_, err = run("jobs", "priority", id, "five", "--reason", "sooner")
	require.ErrorContains(t, err, `invalid priority "five" (want an integer)`)
	assert.Equal(t, 100, inspectJob(t, cfg, id).Priority)

	steps := []struct {
		args           []string
		prints, reason string
	}{
		{[]string{"priority", id, "5"}, "reprioritized " + id, "sooner"},
		{[]string{"pause", id}, "paused " + id, "hold for review"},
		{[]string{"resume", id}, "resumed " + id, "reviewed"},
	}
	for _, s := range steps {
		out, err := run(append(append([]string{"jobs"}, s.args...), "--reason", s.reason)...)
		require.NoError(t, err, s.args)
		assert.Contains(t, out, s.prints)
	}
	job := inspectJob(t, cfg, id)
	assert.Equal(t, 5, job.Priority)
	assert.Equal(t, "available", job.State)
	page = historyJSON(t, cfg, id)
	require.Len(t, page.Entries, 4, "jobs history lists every entry")
	for i, s := range []struct{ action, reason string }{
		{"resumed", "reviewed"},
		{"paused", "hold for review"},
		{"reprioritized", "sooner"},
		{"rescheduled", "after the vendor window"},
	} {
		assert.Equal(t, flywheel.AuditAction(s.action), page.Entries[i].Action, "newest first")
		assertOperator(t, page.Entries[i], s.reason)
	}

	done := enqueueOne(t, cfg)
	_, err = run("jobs", "delete", done, "--reason", "a test job")
	require.ErrorContains(t, err, "flywheel: job has not reached a terminal state", "a waiting job isn't deleted")
	assert.Equal(t, "available", inspectJob(t, cfg, done).State)
	_, err = run("jobs", "cancel", done, "--reason", "a test job")
	require.NoError(t, err)
	out, err = run("jobs", "delete", done, "--reason", "a test job")
	require.NoError(t, err)
	assert.Contains(t, out, "deleted "+done)
	_, err = run("jobs", "inspect", done)
	require.Error(t, err, "the deleted job is gone")
	page = historyJSON(t, cfg, done)
	require.Len(t, page.Entries, 2, "its history stays")
	assert.Equal(t, flywheel.AuditAction("deleted"), page.Entries[0].Action)
	assertOperator(t, page.Entries[0], "a test job")

	seedCLIJob(t, cfg, "j-running", "running")
	for _, args := range [][]string{
		{"reschedule", "j-running", "--at", "2026-10-11T09:00:00Z"},
		{"priority", "j-running", "5"},
		{"pause", "j-running"},
		{"resume", "j-running"},
		{"delete", "j-running"},
	} {
		_, err := run(append(append([]string{"jobs"}, args...), "--reason", "while it runs")...)
		require.ErrorIs(t, err, flywheel.ErrJobRunning, args)
		assert.ErrorContains(t, err, "flywheel: job is running")
	}
	running := inspectJob(t, cfg, "j-running")
	assert.Equal(t, "running", running.State, "a running job is never changed")
	assert.Equal(t, 100, running.Priority)
	assert.Empty(t, historyJSON(t, cfg, "j-running").Entries)
}
