//go:build loadtest

package loadtest

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"
)

// databaseURLEnv points the harness's own tests at a running PostgreSQL
// instance.
//
// It is deliberately not FLYWHEEL_TEST_DATABASE_URL, which the integration
// workflow sets. Sharing that variable would start harness runs inside a job
// budgeted in seconds, and a benchmark that seeds 100k rows would blow it apart.
// Opting in to the harness has to be an explicit act.
const databaseURLEnv = "FLYWHEEL_LOADTEST_DATABASE_URL"

// requireDSN resolves the target for a database-backed harness test, or skips.
//
// Short mode is checked first, so `go test -short` never reaches a database even
// when one is configured.
func requireDSN(t *testing.T) string {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping the load harness in short mode")
	}
	dsn := os.Getenv(databaseURLEnv)
	if dsn == "" {
		t.Skipf("%s is not set; skipping the database-backed harness tests", databaseURLEnv)
	}
	return dsn
}

// TestRunDrainsEverySeededJob is the harness's end-to-end proof against a real
// server: seed, drain, account for every job, and drop the schema.
//
// It is deliberately small. This is an assertion about the harness, not a
// measurement of the runtime — the measurements are the benchmarks and the
// scenario main, and a test that took a minute would not survive in a suite.
func TestRunDrainsEverySeededJob(t *testing.T) {
	dsn := requireDSN(t)

	report, err := Run(context.Background(), Config{
		DSN:     dsn,
		Jobs:    200,
		Seed:    1,
		Runners: 2,
		Workers: 4,
		Mix:     WorkloadDrainOnly,
		Indexes: IndexesFull,
		Timeout: 2 * time.Minute,
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	if report.Enqueued != 200 {
		t.Errorf("Enqueued = %d, want 200", report.Enqueued)
	}
	if report.Drained != 200 {
		t.Errorf("Drained = %d, want 200 — every seeded job must reach succeeded", report.Drained)
	}
	if report.Discarded != 0 {
		t.Errorf("Discarded = %d, want 0", report.Discarded)
	}
	if len(report.Errors) != 0 {
		t.Errorf("the run collected errors: %v", report.Errors)
	}
	if report.EnqueueThroughput <= 0 || report.DrainThroughput <= 0 {
		t.Errorf("throughput must be positive, got enqueue=%v drain=%v",
			report.EnqueueThroughput, report.DrainThroughput)
	}
	if report.Schema == "" || !validSchemaName(report.Schema) {
		t.Errorf("the report must name the schema it provisioned, got %q", report.Schema)
	}
}

// TestRunEnqueueOnlyDoesNotDrain proves the enqueue mix measures what it says:
// its number is an insert rate, so nothing may drain the rows underneath it.
func TestRunEnqueueOnlyDoesNotDrain(t *testing.T) {
	dsn := requireDSN(t)

	report, err := Run(context.Background(), Config{
		DSN: dsn, Jobs: 100, Mix: WorkloadEnqueueOnly, Timeout: time.Minute,
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if report.Enqueued != 100 {
		t.Errorf("Enqueued = %d, want 100", report.Enqueued)
	}
	if report.Drained != 0 {
		t.Errorf("Drained = %d, want 0: an enqueue-rate measurement must not be drained", report.Drained)
	}
	if report.EnqueueThroughput <= 0 {
		t.Error("the enqueue mix must report a positive insert rate")
	}
}

// TestRunInstallsTheRequestedIndexCondition proves the two conditions really
// differ, and that the report records which one the target got. The
// index-condition delta is the point of running two of these, and it means
// nothing if both arms silently installed the same schema.
func TestRunInstallsTheRequestedIndexCondition(t *testing.T) {
	dsn := requireDSN(t)

	full, err := Run(context.Background(), Config{
		DSN: dsn, Jobs: 10, Mix: WorkloadEnqueueOnly, Indexes: IndexesFull, Timeout: time.Minute,
	})
	if err != nil {
		t.Fatalf("Run(full): %v", err)
	}
	reduced, err := Run(context.Background(), Config{
		DSN: dsn, Jobs: 10, Mix: WorkloadEnqueueOnly, Indexes: IndexesCorrectness, Timeout: time.Minute,
	})
	if err != nil {
		t.Fatalf("Run(correctness-only): %v", err)
	}

	fullNote := findNote(full.Notes, "Index condition")
	reducedNote := findNote(reduced.Notes, "Index condition")
	if fullNote == "" || reducedNote == "" {
		t.Fatalf("both runs must record the indexes actually installed:\n full: %v\n reduced: %v",
			full.Notes, reduced.Notes)
	}
	if fullNote == reducedNote {
		t.Fatalf("the two conditions installed the same indexes, so any delta between them means nothing:\n%s",
			fullNote)
	}
	// The claim-path index is the one the comparison is about.
	if !strings.Contains(fullNote, "jobs_ready") {
		t.Errorf("the full condition must install jobs_ready: %s", fullNote)
	}
	if strings.Contains(reducedNote, "jobs_ready") {
		t.Errorf("the correctness-only condition must not install jobs_ready: %s", reducedNote)
	}
	// ...and the correctness indexes are in both, which is what keeps the two
	// arms comparable rather than merely different.
	for _, name := range []string{"jobs_unique_key", "job_runs_job_attempt"} {
		if !strings.Contains(reducedNote, name) {
			t.Errorf("the correctness-only condition must still install %s: %s", name, reducedNote)
		}
	}
}

// TestRunRejectsAnUnreachableTarget proves a bad DSN fails rather than hanging
// or reporting an empty success.
func TestRunRejectsAnUnreachableTarget(t *testing.T) {
	t.Parallel()

	_, err := Run(context.Background(), Config{
		DSN:     "postgres://nobody@127.0.0.1:1/nothing?sslmode=disable&connect_timeout=1",
		Jobs:    1,
		Timeout: 30 * time.Second,
	})
	if err == nil {
		t.Fatal("expected an unreachable target to fail")
	}
}

// TestRunValidatesBeforeTouchingTheDatabase proves a bad config is rejected
// without a connection attempt, which is what lets the config tests run with no
// database at all.
func TestRunValidatesBeforeTouchingTheDatabase(t *testing.T) {
	t.Parallel()

	_, err := Run(context.Background(), Config{Jobs: 1})
	if !errors.Is(err, ErrNoDSN) {
		t.Fatalf("Run error = %v, want ErrNoDSN", err)
	}
}

// findNote returns the first note containing prefix, or "".
func findNote(notes []string, prefix string) string {
	for _, n := range notes {
		if strings.Contains(n, prefix) {
			return n
		}
	}
	return ""
}
