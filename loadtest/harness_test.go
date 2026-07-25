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

// TestRunTimesClaimFinalizeAndSweep is the acceptance criterion for the
// harness's own instrumentation: a drain run must report non-zero percentiles
// for all three operations, produced without importing the observer or metrics
// packages.
//
// Sweep is the one that could silently report nothing. The Runner does not
// sweep — nothing in its dispatch loop calls Sweep — so those observations exist
// only because the harness runs its own sweeper goroutine. If that goroutine
// ever stops being wired up, this is what catches it.
func TestRunTimesClaimFinalizeAndSweep(t *testing.T) {
	dsn := requireDSN(t)

	report, err := Run(context.Background(), Config{
		DSN: dsn, Jobs: 400, Seed: 5, Runners: 2, Workers: 4,
		Mix: WorkloadDrainOnly, Indexes: IndexesFull,
		// Long enough for the one-second sweeper to tick at least twice.
		WorkDuration: 5 * time.Millisecond,
		Timeout:      2 * time.Minute,
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	for name, l := range map[string]Latency{
		"claim": report.Claim, "finalize": report.Finalize, "sweep": report.Sweep,
	} {
		if l.Count == 0 {
			t.Errorf("%s recorded no observations", name)
			continue
		}
		if l.P50 <= 0 || l.P99 <= 0 {
			t.Errorf("%s reported non-positive percentiles: %+v", name, l)
		}
		if l.Min > l.P50 || l.P50 > l.P99 || l.P99 > l.Max {
			t.Errorf("%s percentiles are not ordered: %+v", name, l)
		}
	}

	// Every finalize is one attempt, so the count is a lower bound on the jobs
	// that ran — a sharded histogram that lost updates would show up here.
	if report.Finalize.Count < report.Drained {
		t.Errorf("finalize recorded %d observations for %d drained jobs",
			report.Finalize.Count, report.Drained)
	}
	if report.Histogram.Buckets == 0 || report.Histogram.MaxRelativeError <= 0 {
		t.Errorf("the report must publish the bucketing behind its percentiles, got %+v", report.Histogram)
	}
}

// TestRunSamplesStorageAndContention proves the sampler produces a real series
// against a real server, and that every number it carries is either measured or
// explicitly disclosed as absent.
func TestRunSamplesStorageAndContention(t *testing.T) {
	dsn := requireDSN(t)

	// Sized to run for a couple of seconds, which is not incidental. PostgreSQL's
	// cumulative statistics are reported by each backend rather than written
	// live, and no more often than about once a second, so a sub-second drain
	// finishes before its own scan and tuple counters ever appear. A shorter
	// version of this test would assert that the sampler collects nothing.
	report, err := Run(context.Background(), Config{
		DSN: dsn, Jobs: 1200, Seed: 8, Runners: 1, Workers: 2,
		Mix: WorkloadDrainOnly, Indexes: IndexesFull,
		WorkDuration:   4 * time.Millisecond,
		SampleInterval: 100 * time.Millisecond,
		Timeout:        2 * time.Minute,
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	if len(report.Storage) < 2 {
		t.Fatalf("got %d storage samples, want at least 2 — a single sample has no trajectory",
			len(report.Storage))
	}

	last := report.Storage[len(report.Storage)-1]
	for _, table := range sampledTables {
		if _, ok := last.LiveTuples[table]; !ok {
			t.Errorf("no live-tuple count for %s", table)
		}
		if last.TableBytes[table] <= 0 {
			t.Errorf("%s reports %d bytes, want a positive size", table, last.TableBytes[table])
		}
	}

	// The scan counters are what make the index-condition delta direct evidence
	// rather than an inference about timing, so a run that collected none of them
	// has lost the strongest number this harness produces. Under the full index
	// set the claim has jobs_ready available, so the index scans must dominate.
	if last.IdxScans["jobs"] == 0 {
		t.Error("jobs reports no index scans under the full index set: the plan evidence is missing")
	}
	if last.IdxScans["jobs"] <= last.SeqScans["jobs"] {
		t.Errorf("jobs: %d index scans against %d sequential — the claim is not using jobs_ready",
			last.IdxScans["jobs"], last.SeqScans["jobs"])
	}

	// WAL is a delta, so the first sample has none; a later one must.
	var sawWAL bool
	for _, s := range report.Storage[1:] {
		if s.WALBytes > 0 {
			sawWAL = true
			break
		}
	}
	if !sawWAL {
		t.Error("no sample recorded WAL generation, though the run wrote 1200 jobs and their audit rows")
	}

	// Every caveat travels with the numbers it qualifies.
	for _, want := range []string{"LockWaits is an instantaneous sample", "cluster-wide", "not the PostgreSQL server"} {
		if findNote(report.Notes, want) == "" {
			t.Errorf("the report must disclose %q; notes were %v", want, report.Notes)
		}
	}

	if report.PeakRSS == 0 && findNote(report.Notes, "none available") == "" {
		t.Error("a platform that can read RSS must report a non-zero peak")
	}
}

// TestRunEveryMixCompletes drives each declared shape against a real server.
//
// Every mix is a string in a committed report, so a shape that does not actually
// run is a name published for something that does not exist. Each case is small:
// this asserts that the shape works, and the benchmarks measure it.
func TestRunEveryMixCompletes(t *testing.T) {
	dsn := requireDSN(t)

	for _, mix := range []Workload{
		WorkloadEnqueueOnly, WorkloadDrainOnly, WorkloadSteady, WorkloadFanOut, WorkloadMixedSpeed,
	} {
		t.Run(string(mix), func(t *testing.T) {
			cfg := Config{
				DSN: dsn, Jobs: 200, Seed: 3, Runners: 2, Workers: 4,
				Mix: mix, Indexes: IndexesFull, Timeout: 2 * time.Minute,
			}
			report, err := Run(context.Background(), cfg)
			if err != nil {
				t.Fatalf("Run(%s): %v", mix, err)
			}
			if report.WorkloadDigest == "" {
				t.Error("every run must record the digest of the workload it generated")
			}
			if len(report.Errors) != 0 {
				t.Errorf("the run collected errors: %v", report.Errors)
			}
			if mix == WorkloadEnqueueOnly {
				return
			}
			// Every job the mix planned must reach a terminal state — including
			// the children a fan-out run creates, which is why the target is the
			// plan's total rather than the seeded row count.
			want := int64(plan(cfg).TotalJobs())
			if report.Drained != want {
				t.Errorf("Drained = %d, want %d", report.Drained, want)
			}
		})
	}
}

// TestRunIsReproducibleAcrossRuns is acceptance A3 reduced to a comparison of
// two strings: two runs with identical configs must generate the same workload.
func TestRunIsReproducibleAcrossRuns(t *testing.T) {
	dsn := requireDSN(t)

	cfg := Config{
		DSN: dsn, Jobs: 300, Seed: 99, Runners: 1, Workers: 1,
		Mix: WorkloadDrainOnly, Timeout: time.Minute,
	}
	first, err := Run(context.Background(), cfg)
	if err != nil {
		t.Fatalf("first run: %v", err)
	}

	// The second run differs in concurrency, which must not reach the workload.
	cfg.Runners, cfg.Workers = 4, 8
	second, err := Run(context.Background(), cfg)
	if err != nil {
		t.Fatalf("second run: %v", err)
	}

	if first.WorkloadDigest != second.WorkloadDigest {
		t.Fatalf("the two runs generated different workloads:\n %s\n %s",
			first.WorkloadDigest, second.WorkloadDigest)
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
