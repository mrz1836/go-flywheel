package main

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	flywheel "github.com/mrz1836/go-flywheel"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// failAfterWriter returns an error once it has accepted okWrites successful writes,
// so a tabwriter Flush over rendered content surfaces a write failure deep in
// renderStatusText.
type failAfterWriter struct {
	okWrites int
	n        int
}

func (w *failAfterWriter) Write(p []byte) (int, error) {
	w.n++
	if w.n > w.okWrites {
		return 0, errors.New("write failed")
	}
	return len(p), nil
}

func TestRenderStatusTextSkipsInactiveSchedulesAndListsFailures(t *testing.T) {
	t.Parallel()
	// A report mixing active and inactive schedules with a failure exercises the
	// inactive-skip branch, the active-schedule table, and the failures table.
	report := statusReport{
		Database: "sqlite:/x.db",
		Health:   flywheel.QueueHealth{Ready: 2, InFlight: 1, ScheduledAhead: 3, OldestReadyAge: 90 * time.Second},
		Overview: flywheel.JobsOverview{Total: 6, CountsByState: map[string]int{"available": 2, "running": 1}},
		Schedules: []flywheel.PeriodicView{
			{Slug: "active-cron", Kind: "exec", Cron: "0 2 * * *", Active: true, NextRunAt: time.Unix(0, 0).UTC()},
			{Slug: "inactive", Kind: "exec", IntervalSeconds: 60, Active: false},
			{Slug: "active-every", Kind: "http", IntervalSeconds: 30, Active: true, NextRunAt: time.Unix(0, 0).UTC()},
		},
		Failures: []flywheel.FailureView{
			{Kind: "exec", JobID: "j1", ErrorClass: "permanent", ErrorMessage: "boom"},
			{Kind: "http", JobID: "j2", ErrorClass: "", ErrorMessage: "timeout"},
		},
	}

	var buf bytes.Buffer
	require.NoError(t, renderStatusText(&buf, report))
	out := buf.String()
	assert.Contains(t, out, "schedules:    2 active / 3 total", "the inactive schedule is excluded from the active count")
	assert.Contains(t, out, "active-cron", "an active cron schedule is listed")
	assert.Contains(t, out, "active-every", "an active interval schedule is listed")
	assert.NotContains(t, out, "inactive\t", "an inactive schedule is skipped in the table")
	assert.Contains(t, out, "recent failures (last 24h): 2")
	assert.Contains(t, out, "j1", "a failing job is listed")
	assert.Contains(t, out, "-", "a failure with no error class renders a dash")
}

func TestRenderStatusTextNoSchedulesNoFailures(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	require.NoError(t, renderStatusText(&buf, statusReport{Database: "sqlite:/x.db"}))
	out := buf.String()
	assert.Contains(t, out, "schedules:    0 active / 0 total")
	assert.Contains(t, out, "recent failures (last 24h): 0")
}

func TestRenderStatusTextSurfacesScheduleFlushError(t *testing.T) {
	t.Parallel()
	report := statusReport{
		Schedules: []flywheel.PeriodicView{{Slug: "a", Kind: "exec", Cron: "* * * * *", Active: true, NextRunAt: time.Unix(0, 0)}},
	}
	// Let the leading header lines through, then fail when the active-schedule
	// tabwriter flushes, exercising the schedule-flush error return.
	err := renderStatusText(&failAfterWriter{okWrites: 9}, report)
	require.Error(t, err, "a write failure flushing the schedule table is returned")
}

func TestRenderStatusTextSurfacesFailureFlushError(t *testing.T) {
	t.Parallel()
	report := statusReport{
		Failures: []flywheel.FailureView{{Kind: "exec", JobID: "j1", ErrorClass: "permanent", ErrorMessage: "boom"}},
	}
	// No active schedules, so the next tabwriter is the failures table; fail its flush.
	err := renderStatusText(&failAfterWriter{okWrites: 11}, report)
	require.Error(t, err, "a write failure flushing the failures table is returned")
}

// TestCLIStatusCommandsFailOnUnmigratedDB drives the query-error branch of the
// read-only status and list commands against a database that opens but has no
// schema, so the underlying read fails cleanly and offline.
func TestCLIStatusCommandsFailOnUnmigratedDB(t *testing.T) {
	t.Parallel()
	cfg := writeCLIConfig(t, t.TempDir(), "")
	ctx := context.Background()
	cases := [][]string{
		{"status"},
		{"status", "--json"},
		{"jobs", "ls"},
		{"jobs", "ls", "--json"},
		{"schedule", "ls"},
		{"schedule", "ls", "--json"},
		{"enqueue", "exec", `{"command":"true"}`},
		{"prune", "--older-than", "1d"},
	}
	for _, args := range cases {
		args := args
		t.Run(args[0]+"_"+args[len(args)-1], func(t *testing.T) {
			t.Parallel()
			_, err := runRoot(ctx, append([]string{"--config", cfg}, args...)...)
			require.Error(t, err, "a query against the unmigrated schema fails")
		})
	}
}
