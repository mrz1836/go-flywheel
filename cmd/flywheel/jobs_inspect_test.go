package main

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCLIJobsInspectShowsTheFullDetailWithArgsBounded proves `jobs inspect`
// shows a job in full, with its args left out past --max-args-bytes and shown
// within it, and that --json prints the full detail beside the runs.
func TestCLIJobsInspectShowsTheFullDetailWithArgsBounded(t *testing.T) {
	t.Parallel()
	cfg := migratedCLIConfig(t)
	db, _ := openCLIConfigDB(t, cfg)

	// 100 KiB of args, stored verbatim: {"blob":"xxx…"}.
	args := `{"blob":"` + strings.Repeat("x", 102400-len(`{"blob":""}`)) + `"}`
	require.Len(t, args, 102400)
	now := "2026-06-22 12:00:00"
	require.NoError(t, db.Exec(
		`INSERT INTO jobs(id, kind, queue, args, priority, state, attempt, max_attempts, scheduled_at, executor_class,
		 tags, metadata, unique_key, unique_active_key, timeout_ms, created_at, updated_at)
		 VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		"j-detail", "exec", "reports", args, 7, "available", 0, 3, now, "",
		`["nightly","eu"]`, `{"source":"test"}`, "report:2026-06-22", "report:eu", 30000, now, now,
	).Error)

	out, err := runRoot(context.Background(), "--config", cfg, "jobs", "inspect", "j-detail")
	require.NoError(t, err)
	assert.Contains(t, out, "Job j-detail")
	assert.Contains(t, out, "kind:")
	assert.Contains(t, out, "queue:")
	assert.Contains(t, out, "reports")
	assert.Contains(t, out, "0/3", "attempt of max attempts")
	assert.Contains(t, out, "nightly, eu", "the tags")
	assert.Contains(t, out, "report:2026-06-22", "the unique key")
	assert.Contains(t, out, "report:eu", "the unique active key")
	assert.Contains(t, out, "timeout:")
	assert.Contains(t, out, `{"source":"test"}`, "the metadata as stored")
	assert.Contains(t, out, "args: omitted (102400 bytes; raise --max-args-bytes to show)")
	assert.NotContains(t, out, "xxxxxxxx", "the payload is not printed past the bound")
	assert.Contains(t, out, "runs:")

	out, err = runRoot(context.Background(), "--config", cfg, "jobs", "inspect", "j-detail", "--max-args-bytes", "200000")
	require.NoError(t, err)
	assert.Contains(t, out, args, "within the bound the args print as stored")
	assert.NotContains(t, out, "omitted")

	out, err = runRoot(context.Background(), "--config", cfg, "jobs", "inspect", "j-detail", "--json")
	require.NoError(t, err)
	var got struct {
		Job  map[string]any   `json:"job"`
		Runs []map[string]any `json:"runs"`
	}
	require.NoError(t, json.Unmarshal([]byte(out), &got))
	assert.Equal(t, "j-detail", got.Job["id"])
	assert.Equal(t, true, got.Job["args_omitted"])
	assert.InDelta(t, 102400, got.Job["args_bytes"], 0)
	assert.Equal(t, map[string]any{"source": "test"}, got.Job["metadata"])
	assert.Equal(t, "report:2026-06-22", got.Job["unique_key"])
	assert.NotContains(t, got.Job, "args", "args past the bound are left out")
	assert.NotNil(t, got.Runs)
}
