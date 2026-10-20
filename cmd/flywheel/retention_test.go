package main

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	flywheel "github.com/mrz1836/go-flywheel"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// seedRetentionCLIJobs writes four finished jobs into cfg's database: a
// succeeded and a discarded job finished 40 days ago, a job of a noisy kind
// finished 10 days ago, and a succeeded job finished 10 days ago.
func seedRetentionCLIJobs(t *testing.T, cfg string) {
	t.Helper()
	_, db, _, err := loadAndOpen(cfg)
	require.NoError(t, err)
	defer closeDB(db)
	now := time.Now()
	for _, j := range []struct {
		id, kind, state string
		age             time.Duration
	}{
		{"succeeded-40d", "exec", "succeeded", 40 * 24 * time.Hour},
		{"discarded-40d", "exec", "discarded", 40 * 24 * time.Hour},
		{"noisy-10d", "noisy", "succeeded", 10 * 24 * time.Hour},
		{"succeeded-10d", "exec", "succeeded", 10 * 24 * time.Hour},
	} {
		at := now.Add(-j.age)
		require.NoError(t, db.Exec(
			`INSERT INTO jobs(id, kind, queue, args, priority, state, attempt, max_attempts, scheduled_at, finalized_at, executor_class, tags, created_at, updated_at, metadata)
			 VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
			j.id, j.kind, "default", "{}", 100, j.state, 1, 25, at, at, "", "[]", at, at, "{}",
		).Error)
	}
}

// TestParseRetentionRuleSpec proves the --rule form: kind, state, and keep, as
// key=value pairs, keep required and at least one of kind and state.
func TestParseRetentionRuleSpec(t *testing.T) {
	t.Parallel()
	for spec, want := range map[string]flywheel.RetentionRule{
		"kind=noisy,keep=7d":                  {Kind: "noisy", MaxAge: 7 * 24 * time.Hour},
		"state=discarded,keep=90d":            {State: flywheel.StateDiscarded, MaxAge: 90 * 24 * time.Hour},
		"kind=report,state=discarded,keep=2w": {Kind: "report", State: flywheel.StateDiscarded, MaxAge: 14 * 24 * time.Hour},
		" keep=36h , kind=noisy ":             {Kind: "noisy", MaxAge: 36 * time.Hour},
	} {
		got, err := parseRetentionRule(spec)
		require.NoError(t, err, spec)
		assert.Equal(t, want, got, spec)
	}
	for spec, wantErr := range map[string]string{
		"keep=7d":                    "a kind, a state, or both",
		"kind=noisy":                 "keep is required",
		"kind=noisy,keep=soon":       "keep",
		"kind=noisy,keep=7d,color=x": `unknown key "color"`,
		"kind=a,kind=b,keep=7d":      "kind is given twice",
		"kind":                       "key=value",
		"kind=,keep=7d":              "key=value",
		"":                           "key=value",
	} {
		_, err := parseRetentionRule(spec)
		require.Error(t, err, spec)
		assert.Contains(t, err.Error(), wantErr, spec)
	}
	assert.Equal(t, "kind=report,state=discarded,keep=14d", formatRetentionRule(flywheel.RetentionRule{
		Kind: "report", State: flywheel.StateDiscarded, MaxAge: 14 * 24 * time.Hour,
	}))
	assert.Equal(t, "kind=noisy,keep=36h0m0s", formatRetentionRule(flywheel.RetentionRule{Kind: "noisy", MaxAge: 36 * time.Hour}))
}

// TestPruneCommandAppliesRetentionRules proves `flywheel prune --rule` keeps
// finished jobs by kind and state, reports the rules it applied, and refuses a
// bad rule before deleting anything.
func TestPruneCommandAppliesRetentionRules(t *testing.T) {
	t.Parallel()
	cfg := migratedCLIConfig(t)
	seedRetentionCLIJobs(t, cfg)
	ctx := context.Background()

	_, err := runRoot(ctx, "--config", cfg, "prune", "--older-than", "30d", "--ignore-stats-rollup", "--rule", "keep=7d")
	require.Error(t, err)
	_, err = runRoot(ctx, "--config", cfg, "prune", "--older-than", "30d", "--ignore-stats-rollup",
		"--rule", "state=running,keep=7d")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "rules[0].state", "the library's refusal names the rule")

	out, err := runRoot(ctx, "--config", cfg, "prune", "--older-than", "30d", "--ignore-stats-rollup",
		"--rule", "state=discarded,keep=90d", "--rule", "kind=noisy,keep=7d")
	require.NoError(t, err)
	assert.Contains(t, out, "pruned 2 finished job(s)")
	assert.Contains(t, out, "rule state=discarded,keep=90d")
	assert.Contains(t, out, "rule kind=noisy,keep=7d")

	out, err = runRoot(ctx, "--config", cfg, "jobs", "ls", "--json")
	require.NoError(t, err)
	assert.Contains(t, out, "discarded-40d", "the discarded rule keeps it")
	assert.Contains(t, out, "succeeded-10d", "inside the default")
	assert.NotContains(t, out, "succeeded-40d", "past the default")
	assert.NotContains(t, out, "noisy-10d", "past its kind's rule")
}

// TestRetentionCommandReportsEachRule proves `flywheel retention` prints a row
// per rule and the default's, as a table and as JSON, with the default age
// from --older-than, else the config's runtime.retention, else 14 days.
func TestRetentionCommandReportsEachRule(t *testing.T) {
	t.Parallel()
	cfg := migratedCLIConfig(t)
	seedRetentionCLIJobs(t, cfg)
	ctx := context.Background()

	out, err := runRoot(ctx, "--config", cfg, "retention", "--older-than", "30d",
		"--rule", "state=discarded,keep=90d", "--rule", "kind=noisy,keep=7d")
	require.NoError(t, err)
	lines := strings.Split(strings.TrimSpace(out), "\n")
	require.Len(t, lines, 4, out)
	assert.Equal(t, []string{"KIND", "STATE", "KEEP", "CUTOFF", "OLDEST", "DUE"}, strings.Fields(lines[0]))
	row := func(i int) []string {
		f := strings.Fields(lines[i])
		return []string{f[0], f[1], f[2], f[5]} // the cutoff and the oldest are times
	}
	assert.Equal(t, []string{"-", "discarded", "90d", "0"}, row(1))
	assert.Equal(t, []string{"noisy", "-", "7d", "1"}, row(2))
	assert.Equal(t, []string{"-", "-", "30d", "1"}, row(3), "the default row, last")

	out, err = runRoot(ctx, "--config", cfg, "retention", "--older-than", "30d", "--rule", "kind=noisy,keep=7d", "--json")
	require.NoError(t, err)
	var rows []flywheel.RetentionStatusRow
	require.NoError(t, json.Unmarshal([]byte(out), &rows))
	require.Len(t, rows, 2)
	assert.Equal(t, "noisy", rows[0].Kind)
	assert.EqualValues(t, 1, rows[0].Due.N)
	require.NotNil(t, rows[1].Oldest)

	out, err = runRoot(ctx, "--config", cfg, "retention")
	require.NoError(t, err)
	assert.Equal(t, "14d", strings.Fields(strings.Split(strings.TrimSpace(out), "\n")[1])[2], "14 days by default")
	require.NoError(t, os.WriteFile(cfg, []byte(strings.Replace(mustRead(t, cfg),
		"  poll_interval: 30ms\n", "  poll_interval: 30ms\n  retention: 45d\n", 1)), 0o600))
	out, err = runRoot(ctx, "--config", cfg, "retention")
	require.NoError(t, err)
	assert.Equal(t, "45d", strings.Fields(strings.Split(strings.TrimSpace(out), "\n")[1])[2], "the config's retention")

	_, err = runRoot(ctx, "--config", cfg, "retention", "--rule", "kind=noisy")
	require.Error(t, err)
	assert.Equal(t, "10000+", dueCell(flywheel.JobCount{N: flywheel.DefaultCountCap, Capped: true}))
	assert.Equal(t, "12", dueCell(flywheel.JobCount{N: 12}))
}
