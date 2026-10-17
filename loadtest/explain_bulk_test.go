//go:build loadtest

package loadtest

import (
	"slices"
	"strings"
	"testing"
	"time"
)

// TestBulkBatchSpansSplitsARunAtEachBatchRead proves a batch's time runs from
// its read to the next batch's read, or for the last to the call's return, and
// that every other statement, a pre-count, a locked re-read, an update, a
// probe, belongs to the batch it falls in.
func TestBulkBatchSpansSplitsARunAtEachBatchRead(t *testing.T) {
	at := func(ms int) time.Time {
		return time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC).Add(time.Duration(ms) * time.Millisecond)
	}
	statements := []bulkStatement{
		{sql: `SELECT count(*) FROM (SELECT 1 FROM "jobs" WHERE jobs.kind IN ($1) LIMIT 1001) AS s`, begin: at(0)},
		{sql: `SELECT jobs.id,jobs.state,jobs.kind FROM "jobs" ORDER BY jobs.id LIMIT 500`, begin: at(5)},
		{sql: `SELECT jobs.id, jobs.state, jobs.kind FROM "jobs" WHERE jobs.id IN ($1) FOR UPDATE`, begin: at(7)},
		{sql: `UPDATE "jobs" SET "state"=$1 WHERE id IN (SELECT jobs.id FROM "jobs") RETURNING "id"`, begin: at(9)},
		{sql: `SELECT batch.*, EXISTS (SELECT 1) AS held FROM (SELECT jobs.id FROM "jobs") AS batch`, begin: at(15)},
		{sql: `SELECT jobs.id,jobs.state,jobs.kind FROM "jobs" WHERE jobs.id > $1 ORDER BY jobs.id LIMIT 500`, begin: at(40)},
		{sql: `SELECT jobs.id,jobs.unique_key FROM "jobs" WHERE jobs.id > $1 ORDER BY jobs.id LIMIT 500`, begin: at(52)},
	}
	spans, reads := bulkBatchSpans(statements, at(60))
	want := []time.Duration{10 * time.Millisecond, 25 * time.Millisecond, 20 * time.Millisecond}
	if !slices.Equal(spans, want) {
		t.Fatalf("spans = %v, want %v", spans, want)
	}
	if len(reads) != 3 || !reads[2].begin.Equal(at(40)) {
		t.Fatalf("reads = %v, want the three batch reads", reads)
	}
}

// TestPercentileIsNearestRank pins the batch percentiles: nearest rank, over
// spans in any order, and zero for a run with no batch.
func TestPercentileIsNearestRank(t *testing.T) {
	spans := make([]time.Duration, 20)
	for i := range spans {
		spans[i] = time.Duration(20-i) * time.Millisecond
	}
	cases := []struct {
		p    float64
		want time.Duration
	}{{0.50, 10 * time.Millisecond}, {0.95, 19 * time.Millisecond}, {1, 20 * time.Millisecond}}
	for _, c := range cases {
		if got := percentile(spans, c.p); got != c.want {
			t.Errorf("percentile(%v) = %v, want %v", c.p, got, c.want)
		}
	}
	if got := percentile(nil, 0.95); got != 0 {
		t.Errorf("percentile of no spans = %v, want 0", got)
	}
}

// TestBulkExplainReportRenders proves the artifact carries the fixture, a row
// per run with its batch times, each run's explained read, and no credentials.
func TestBulkExplainReportRenders(t *testing.T) {
	run := func(dry, index bool) BulkExplainRun {
		return BulkExplainRun{
			Cohort: "cohort.small", Size: 10_000, DryRun: dry, Index: index, Wall: 2 * time.Second,
			Matched: 10_000, Changed: 10_000, Batches: 20,
			P50: 90 * time.Millisecond, P95: 120 * time.Millisecond, Max: 150 * time.Millisecond,
			Plan: ProgressExplainStatement{
				SQL:  `SELECT jobs.id,jobs.state FROM "jobs" WHERE jobs.kind IN ('cohort.small') LIMIT 500`,
				Plan: []string{"Index Scan using jobs_explore_kind on jobs"},
			},
		}
	}
	r := BulkExplainReport{
		Target: redactDSN("postgres://user:secret@db.example:5432/flywheel?sslmode=disable"),
		Schema: "lt_x", Jobs: 1_000_000, JobsBytes: 300 << 20, JobsIndex: 120 << 20, KindIndex: 30 << 20,
		Runs: []BulkExplainRun{run(true, true), run(false, true), run(true, false)},
	}
	text := r.Text()
	for _, want := range []string{
		"bulk controls by filter", "1000000 jobs over 30 days", "jobs_explore_kind 30.0 MB",
		"cohort.small (10000), dry run", "cohort.small (10000), apply", "90.000ms", "120.000ms",
		"=== cohort.small (10000), apply, with jobs_explore_kind: one batch's read ===",
		"=== cohort.small (10000), dry run, without jobs_explore_kind: one batch's read ===",
		"Index Scan using jobs_explore_kind on jobs",
	} {
		if !strings.Contains(text, want) {
			t.Errorf("the artifact lacks %q:\n%s", want, text)
		}
	}
	if strings.Contains(text, "secret") {
		t.Errorf("the artifact carries a credential:\n%s", text)
	}
}
