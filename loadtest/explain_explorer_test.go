//go:build loadtest

package loadtest

import (
	"slices"
	"strings"
	"testing"
	"time"
)

// TestPlanJobsAccessNamesTheIndexesOnJobs proves the artifact's access column:
// each index a plan reads jobs through, in order and once each, a sequential
// scan of jobs, and never the run join's own index.
func TestPlanJobsAccessNamesTheIndexesOnJobs(t *testing.T) {
	read := StatsExplainRead{Statements: []ProgressExplainStatement{
		{Plan: []string{
			"Limit  (cost=0.43..80.12 rows=51 width=299)",
			"  ->  Nested Loop Left Join",
			"        ->  Index Scan Backward using jobs_explore_kind on jobs",
			"        ->  Index Scan using job_runs_job_attempt on job_runs r",
		}},
		{Plan: []string{
			"Bitmap Heap Scan on jobs",
			"  ->  Bitmap Index Scan on jobs_explore_tags",
			"Index Only Scan using jobs_explore_kind on jobs",
			"Seq Scan on jobs",
		}},
	}}
	got := planJobsAccess(read)
	want := []string{"jobs_explore_kind", "jobs_explore_tags", "Seq Scan"}
	if !slices.Equal(got, want) {
		t.Fatalf("planJobsAccess = %v, want %v", got, want)
	}
}

// TestExplorerExplainReportRenders proves the artifact carries the fixture, the
// latency table with each measurement's access path, and every plan, with and
// without the index a shape is meant for, and no credentials.
func TestExplorerExplainReportRenders(t *testing.T) {
	plan := func(index string) StatsExplainRead {
		return StatsExplainRead{
			Median: 1500 * time.Microsecond, Min: time.Millisecond, Max: 2 * time.Millisecond,
			Statements: []ProgressExplainStatement{{
				SQL:  "SELECT jobs.id FROM jobs WHERE jobs.kind IN ('kind-cap') LIMIT 51",
				Plan: []string{"Index Scan Backward using " + index + " on jobs"},
			}},
		}
	}
	without := plan("jobs_pkey")
	without.Median = 40 * time.Millisecond
	r := ExplorerExplainReport{
		Target: redactDSN("postgres://user:secret@db.example:5432/flywheel?sslmode=disable"),
		Schema: "lt_x", Jobs: 1_000_000, JobsBytes: 300 << 20, JobsIndex: 120 << 20,
		IndexSizes: []ExplorerIndexSize{{Name: "jobs_explore_kind", Bytes: 30 << 20}},
		Reads: []ExplorerExplainRead{
			{Name: "kind, newest first: 1% of jobs", Index: "jobs_explore_kind", With: plan("jobs_explore_kind"), Without: &without},
			{Name: "CountJobs past the cap: every job", With: plan("jobs_state")},
		},
	}
	text := r.Text()
	for _, want := range []string{
		"explorer read plans",
		"1000000 jobs over 30 days",
		"jobs_explore_kind 30.0 MB",
		"kind, newest first: 1% of jobs",
		"1.500ms",
		"40.000ms",
		"=== kind, newest first: 1% of jobs (with every explorer index)",
		"=== kind, newest first: 1% of jobs (without jobs_explore_kind)",
		"Index Scan Backward using jobs_pkey on jobs",
		"CountJobs past the cap: every job",
	} {
		if !strings.Contains(text, want) {
			t.Errorf("artifact lacks %q:\n%s", want, text)
		}
	}
	if strings.Contains(text, "secret") || strings.Contains(text, "user:") {
		t.Errorf("artifact carries a credential:\n%s", text)
	}
	row := ""
	for _, line := range strings.Split(text, "\n") {
		if strings.HasPrefix(line, "kind, newest first: 1% of jobs ") {
			row = line
		}
	}
	if !strings.Contains(row, "jobs_explore_kind") || !strings.Contains(row, "jobs_pkey") {
		t.Errorf("the table row names each arm's access path: %q", row)
	}
}
