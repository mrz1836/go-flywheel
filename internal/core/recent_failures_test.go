package core

import (
	"context"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/datatypes"
	"gorm.io/gorm"
)

// selectedColumns returns the table a SELECT reads from and the bare names of
// the columns it selects (qualifier, quotes, and alias removed), or ok false for
// a statement that is not a SELECT.
func selectedColumns(sql string) (table string, cols []string, ok bool) {
	trimmed := strings.TrimSpace(sql)
	upper := strings.ToUpper(trimmed)
	if !strings.HasPrefix(upper, "SELECT ") {
		return "", nil, false
	}
	from := strings.Index(upper, " FROM ")
	if from < 0 {
		return "", nil, false
	}
	for _, raw := range strings.Split(trimmed[len("SELECT "):from], ",") {
		col := strings.ToLower(strings.TrimSpace(raw))
		if i := strings.Index(col, " as "); i >= 0 {
			col = col[:i]
		}
		if i := strings.LastIndex(col, "."); i >= 0 {
			col = col[i+1:]
		}
		cols = append(cols, strings.Trim(col, "\"`"))
	}
	rest := strings.Fields(trimmed[from+len(" FROM "):])
	if len(rest) > 0 {
		table = strings.Trim(rest[0], "\"`")
	}
	return table, cols, true
}

// assertRecentFailuresLoadsNoArgsOrOutput runs RecentFailures over discarded
// jobs whose args, outputs, and error payloads are large, records every
// statement it issues, and asserts that each one selects only the columns a
// FailureView reports, that the runs read takes each job's own final attempt
// rather than every attempt, and that the rows are what the read has always
// returned. Shared by the SQLite and PostgreSQL suites.
func assertRecentFailuresLoadsNoArgsOrOutput(t *testing.T, db *gorm.DB) {
	t.Helper()
	ctx := context.Background()
	big := datatypes.JSON(`{"blob":"` + strings.Repeat("x", 4096) + `"}`)

	// Two discarded jobs with heavy args; the first failed twice, and only its
	// second attempt (the job's own) ended it.
	for i, id := range []string{"rf-a", "rf-b"} {
		at := healthAnchor.Add(-time.Duration(i+1) * time.Minute)
		attempts := 2 - i
		seedJob(t, db, jobRow{
			ID: id, Kind: "rf.kind", Queue: "rf-queue", State: string(StateDiscarded), Args: big,
			Attempt: attempts, MaxAttempts: attempts, ScheduledAt: at, FinalizedAt: ptrTime(at), CreatedAt: at, UpdatedAt: at,
		})
		for attempt := 1; attempt <= attempts; attempt++ {
			class := string(ErrorTransient)
			msg := "retry me"
			if attempt == attempts {
				class, msg = string(ErrorPermanent), "the end of "+id
			}
			seedRun(t, db, jobRunRow{
				ID: id + "-r" + string(rune('0'+attempt)), JobID: id, Attempt: attempt, ExecutorID: "h",
				StartedAt: at, Outcome: string(OutcomeError), ErrorClass: &class, ErrorMessage: &msg,
				ErrorPayload: big, Output: big, CreatedAt: at,
			})
		}
	}

	sqls, recorded := countStatements(db)
	got, err := RecentFailures(ctx, recorded, RecentFailuresParams{Limit: 10})
	require.NoError(t, err)

	require.Len(t, got, 2)
	assert.Equal(t, FailureView{
		JobID: "rf-a", Kind: "rf.kind", Queue: "rf-queue", Attempt: 2,
		FinalizedAt: got[0].FinalizedAt, ErrorClass: string(ErrorPermanent), ErrorMessage: "the end of rf-a",
	}, got[0], "the error is the final attempt's")
	assert.True(t, got[0].FinalizedAt.Equal(healthAnchor.Add(-time.Minute)))
	assert.Equal(t, FailureView{
		JobID: "rf-b", Kind: "rf.kind", Queue: "rf-queue", Attempt: 1,
		FinalizedAt: got[1].FinalizedAt, ErrorClass: string(ErrorPermanent), ErrorMessage: "the end of rf-b",
	}, got[1])

	reported := map[string]map[string]bool{
		"jobs":     {"id": true, "kind": true, "queue": true, "attempt": true, "finalized_at": true},
		"job_runs": {"job_id": true, "attempt": true, "error_class": true, "error_message": true},
	}
	ownAttempt := regexp.MustCompile(`(?i)\battempt\s*=\s*"?\w+"?\."?attempt\b`)
	seen := map[string]bool{}
	for _, sql := range *sqls {
		table, cols, ok := selectedColumns(sql)
		if !ok {
			continue
		}
		allowed, known := reported[table]
		if !known {
			continue
		}
		seen[table] = true
		for _, col := range cols {
			assert.NotEqual(t, "*", col, "a %s read selects every column, args and payloads included: %s", table, sql)
			assert.True(t, allowed[col], "a %s read selects %q, which RecentFailures never reports: %s", table, col, sql)
		}
		if table == "job_runs" {
			assert.Regexp(t, ownAttempt, sql, "the runs read takes each job's own attempt, not every attempt: %s", sql)
		}
	}
	assert.True(t, seen["jobs"], "the page is read from jobs")
	assert.True(t, seen["job_runs"], "the errors are read from job_runs")
}

func TestRecentFailuresLoadsNoArgsOrOutput(t *testing.T) {
	t.Parallel()
	assertRecentFailuresLoadsNoArgsOrOutput(t, newDB(t))
}
