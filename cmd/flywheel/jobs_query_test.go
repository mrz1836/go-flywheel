package main

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"testing"
	"time"

	flywheel "github.com/mrz1836/go-flywheel"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// nextCursorLine matches the line `jobs ls` prints under a page that has another.
//
//nolint:gochecknoglobals // compiled once
var nextCursorLine = regexp.MustCompile(`(?m)^next: --cursor (\S+)$`)

// enqueueInState enqueues one job of kind with tags through db and moves it to
// state, returning its id.
func enqueueInState(t *testing.T, db *gorm.DB, kind, state string, tags ...string) string {
	t.Helper()
	id, err := flywheel.Enqueue(context.Background(), flywheel.NewClient(db), kind, []byte(`{}`),
		flywheel.InsertOpts{Tags: tags})
	require.NoError(t, err)
	require.NoError(t, db.Exec(`UPDATE jobs SET state = ? WHERE id = ?`, state, id).Error)
	return id
}

// TestCLIJobsLsFiltersThroughTheQueryAndPagesByCursor proves `jobs ls` lists
// through the query: repeated filter flags, newest first, the next page's
// cursor under the table, --cursor to read it, and --json as a page object.
func TestCLIJobsLsFiltersThroughTheQueryAndPagesByCursor(t *testing.T) {
	t.Parallel()
	cfg := migratedCLIConfig(t)
	db, _ := openCLIConfigDB(t, cfg)
	ctx := context.Background()

	// Four jobs match --state discarded --state retryable --kind a --tag x; each
	// of the others misses one of those filters.
	m1 := enqueueInState(t, db, "a", "discarded", "x")
	enqueueInState(t, db, "a", "available", "x")
	m2 := enqueueInState(t, db, "a", "retryable", "x", "y")
	enqueueInState(t, db, "b", "discarded", "x")
	m3 := enqueueInState(t, db, "a", "discarded", "y", "x")
	enqueueInState(t, db, "a", "retryable", "y")
	m4 := enqueueInState(t, db, "a", "retryable", "x")
	filters := []string{
		"--config", cfg, "jobs", "ls", "--state", "discarded", "--state", "retryable", "--kind", "a", "--tag", "x",
		"--limit", "2",
	}

	out, err := runRoot(ctx, filters...)
	require.NoError(t, err)
	assert.Contains(t, out, "QUEUE", "the table keeps its columns")
	require.Contains(t, out, m4)
	require.Contains(t, out, m3)
	assert.Less(t, strings.Index(out, m4), strings.Index(out, m3), "newest first")
	assert.NotContains(t, out, m2)
	assert.NotContains(t, out, m1)
	next := nextCursorLine.FindStringSubmatch(out)
	require.NotNil(t, next, "another page follows, so its cursor is printed:\n%s", out)
	assert.True(t, strings.HasPrefix(next[1], "jq1."))

	out, err = runRoot(ctx, append(filters, "--cursor", next[1])...)
	require.NoError(t, err)
	assert.Contains(t, out, m2)
	assert.Contains(t, out, m1)
	assert.NotContains(t, out, m4)
	assert.Less(t, strings.Index(out, m2), strings.Index(out, m1))
	assert.NotRegexp(t, nextCursorLine, out, "the last page prints no cursor")

	out, err = runRoot(ctx, append(filters, "--json")...)
	require.NoError(t, err)
	var raw map[string]json.RawMessage
	require.NoError(t, json.Unmarshal([]byte(out), &raw))
	assert.Contains(t, raw, "items")
	assert.Contains(t, raw, "next_cursor")
	var page flywheel.JobPage
	require.NoError(t, json.Unmarshal([]byte(out), &page))
	require.Len(t, page.Items, 2)
	assert.Equal(t, m4, page.Items[0].ID)
	assert.Equal(t, m3, page.Items[1].ID)
	assert.Equal(t, next[1], page.NextCursor)

	out, err = runRoot(ctx, append(filters, "--sort", "oldest")...)
	require.NoError(t, err)
	assert.Less(t, strings.Index(out, m1), strings.Index(out, m2), "--sort oldest lists the oldest first")

	later := time.Now().Add(time.Hour).UTC().Format(time.RFC3339)
	out, err = runRoot(ctx, "--config", cfg, "jobs", "ls", "--created-after", later)
	require.NoError(t, err)
	assert.NotContains(t, out, m1, "--created-after bounds the created window")
	out, err = runRoot(ctx, "--config", cfg, "jobs", "ls", "--created-before", later, "--kind", "a", "--tag", "x")
	require.NoError(t, err)
	assert.Contains(t, out, m1)

	_, err = runRoot(ctx, "--config", cfg, "jobs", "ls", "--sort", "finished")
	require.Error(t, err, "the finished sort needs terminal states")
	assert.Contains(t, err.Error(), "states")
}

// TestCLIJobsCountPrintsTheCap proves `jobs count` prints an exact count below
// its cap, the cap with a plus past it, the estimate when the database has one,
// and the JobCount with --json.
func TestCLIJobsCountPrintsTheCap(t *testing.T) {
	t.Parallel()
	cfg := migratedCLIConfig(t)
	db, _ := openCLIConfigDB(t, cfg)
	ctx := context.Background()
	for range 6 {
		enqueueInState(t, db, "a", "available")
	}
	enqueueInState(t, db, "b", "available")

	out, err := runRoot(ctx, "--config", cfg, "jobs", "count", "--kind", "a", "--cap", "5")
	require.NoError(t, err)
	assert.Equal(t, "5+\n", out)

	out, err = runRoot(ctx, "--config", cfg, "jobs", "count", "--kind", "a")
	require.NoError(t, err)
	assert.Equal(t, "6\n", out)

	out, err = runRoot(ctx, "--config", cfg, "jobs", "count", "--kind", "a", "--cap", "5", "--json")
	require.NoError(t, err)
	var got flywheel.JobCount
	require.NoError(t, json.Unmarshal([]byte(out), &got))
	assert.Equal(t, flywheel.JobCount{N: 5, Capped: true}, got)

	assert.Equal(t, "5+ (estimate 1234)", formatJobCount(flywheel.JobCount{N: 5, Capped: true, Estimate: 1234}),
		"a PostgreSQL count past its cap carries the planner's estimate")

	_, err = runRoot(ctx, "--config", cfg, "jobs", "count", "--state", "sideways")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "states")
}

// failAttempt records attempt n of a job as a failure with class, fingerprint,
// and message, and makes it the attempt the job names.
func failAttempt(t *testing.T, db *gorm.DB, jobID string, n int, class, fingerprint, message string) {
	t.Helper()
	at := time.Now().UTC()
	require.NoError(t, db.Exec(
		`INSERT INTO job_runs(id, job_id, attempt, kind, queue, executor_class, executor_id, started_at, finished_at, outcome, error_class, error_message, error_fingerprint, superseded, enqueued_children, created_at)
		 VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		fmt.Sprintf("%s-%d", jobID, n), jobID, n, "a", "default", "", "host", at, at, "error", class, message, fingerprint,
		false, 0, at,
	).Error)
	require.NoError(t, db.Exec(`UPDATE jobs SET attempt = ? WHERE id = ?`, n, jobID).Error)
}

// TestCLIJobsLsFiltersByTheLatestRun proves `jobs ls` and `jobs count` take
// the latest-run filters: a repeated --error-class matching any of its classes,
// --fingerprint, and --error-contains, each passed to the query, which matches
// a job's latest attempt only and prints its own refusals.
func TestCLIJobsLsFiltersByTheLatestRun(t *testing.T) {
	t.Parallel()
	cfg := migratedCLIConfig(t)
	db, _ := openCLIConfigDB(t, cfg)
	ctx := context.Background()
	const fp, other = "abcdef0123456789", "0123456789abcdef"

	permanent := enqueueInState(t, db, "a", "discarded")
	failAttempt(t, db, permanent, 1, "permanent", fp, "bad input")
	timeout := enqueueInState(t, db, "a", "retryable")
	failAttempt(t, db, timeout, 1, "timeout", other, "deadline exceeded")
	retried := enqueueInState(t, db, "a", "retryable")
	failAttempt(t, db, retried, 1, "permanent", fp, "bad input")
	failAttempt(t, db, retried, 2, "transient", other, "connection refused")
	never := enqueueInState(t, db, "a", "available")

	out, err := runRoot(ctx, "--config", cfg, "jobs", "ls", "--error-class", "permanent", "--error-class", "timeout")
	require.NoError(t, err)
	assert.Contains(t, out, permanent)
	assert.Contains(t, out, timeout)
	assert.NotContains(t, out, retried, "only the latest attempt's class counts")
	assert.NotContains(t, out, never)

	out, err = runRoot(ctx, "--config", cfg, "jobs", "ls", "--fingerprint", fp)
	require.NoError(t, err)
	assert.Contains(t, out, permanent)
	assert.NotContains(t, out, retried)
	assert.NotContains(t, out, timeout)

	out, err = runRoot(ctx, "--config", cfg, "jobs", "count", "--fingerprint", other)
	require.NoError(t, err)
	assert.Equal(t, "2\n", out, "jobs count takes the same filters")
	out, err = runRoot(ctx, "--config", cfg, "jobs", "count", "--error-class", "permanent")
	require.NoError(t, err)
	assert.Equal(t, "1\n", out)

	recent := time.Now().Add(-time.Hour).UTC().Format(time.RFC3339)
	_, err = runRoot(ctx, "--config", cfg, "jobs", "ls", "--error-contains", "refused", "--created-after", recent)
	require.ErrorIs(t, err, flywheel.ErrUnsupportedDialect, "the text search reaches the query, which SQLite refuses")
	_, err = runRoot(ctx, "--config", cfg, "jobs", "ls", "--error-contains", "refused")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "error_contains", "a search without a created window is refused")
	_, err = runRoot(ctx, "--config", cfg, "jobs", "ls", "--error-class", "fatal")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "error_classes")
	_, err = runRoot(ctx, "--config", cfg, "jobs", "count", "--fingerprint", "ABC")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "fingerprint")
}
