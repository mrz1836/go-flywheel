//go:build loadtest

package loadtest

import (
	"context"
	"testing"
	"time"
)

// TestWorkerKilledOnItsLastAttemptLeavesTheJobDiscarded is the worker-kill
// scenario on a one-attempt budget: every job the killed runner held was on its
// last attempt, so the lease sweep must end each one discarded rather than hand
// it to another runner.
//
// It is the crash-loop bound under real concurrency. A sweep that returned such
// a job to the queue would let another runner claim it past its budget, and a
// job whose worker kills its process would go on doing so for as long as it is
// claimed.
func TestWorkerKilledOnItsLastAttemptLeavesTheJobDiscarded(t *testing.T) {
	dsn := requireDSN(t)
	ctx := context.Background()

	const jobs = 2000
	cfg := Config{
		DSN: dsn, Jobs: jobs, Seed: 22,
		// Four runners so killing one leaves three, and the sweep's reclaim, to
		// finish the drain.
		Runners: 4,
		Workers: 4,
		Mix:     WorkloadDrainOnly,
		Indexes: IndexesFull,
		// Enough per-job work that the kill lands while jobs are genuinely in
		// flight rather than in the gap between two claims.
		WorkDuration:   5 * time.Millisecond,
		SampleInterval: 500 * time.Millisecond,
		Timeout:        3 * time.Minute,
		// One attempt each: whatever the killed runner was working on was its
		// job's last attempt.
		MaxAttempts: 1,
		Faults:      KillWorker{Fraction: 0.40, Runner: 0},
	}

	// Driven through the harness's own seams, as the worker-kill scenario is,
	// because every assertion below is a query against what the run left behind.
	h, err := newHarness(ctx, cfg)
	if h == nil {
		t.Fatalf("newHarness: %v", err)
	}
	defer func() {
		if closeErr := h.Close(ctx); closeErr != nil {
			t.Errorf("close: %v", closeErr)
		}
	}()
	if err != nil {
		t.Fatalf("newHarness: %v", err)
	}

	p, specs := h.prepareWorkload()
	if _, err := h.insert(ctx, p, specs); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if err := h.drive(ctx); err != nil {
		t.Fatalf("drive: %v", err)
	}

	// (1) The fault fired, so the assertions below test recovery rather than a
	// healthy run.
	if h.killed.Load() == 0 {
		t.Fatal("the fault never fired: no runner was killed, so nothing below is a test of recovery")
	}
	if orphaned := h.orphanedByFaults(); orphaned == 0 {
		t.Fatal("the kill blocked no finalize, so it left no orphan and nothing below tests recovery")
	}

	// (2) The sweep ended what the kill orphaned: a crashed run records the
	// discard, and every crashed run's job is discarded.
	if n := countRuns(t, h, `outcome = 'crashed' AND job_state = 'discarded'`); n == 0 {
		t.Fatal("no crashed run records a discard: the sweep did not end a job whose last attempt crashed")
	}
	if n := countRuns(t, h, `outcome = 'crashed' AND (job_state IS NULL OR job_state <> 'discarded')`); n != 0 {
		t.Errorf("%d crashed runs record a state other than discarded: a job with no attempts left went back to the queue", n)
	}
	if n := countRuns(t, h, `outcome = 'crashed' AND NOT EXISTS (`+
		`SELECT 1 FROM jobs j WHERE j.id = job_runs.job_id AND j.state = 'discarded')`); n != 0 {
		t.Errorf("%d crashed runs belong to a job that is not discarded", n)
	}

	// (3) No job was claimed past its budget.
	if n := countJobs(t, h, `attempt > max_attempts`); n != 0 {
		t.Errorf("%d jobs ran more attempts than their budget allows: a reclaimed last attempt was claimed again", n)
	}

	// (4) Nothing non-terminal, and no stub still reading 'started', is left.
	if active := countJobs(t, h, `state IN ('available','running','retryable','scheduled')`); active != 0 {
		t.Errorf("%d jobs are still non-terminal after the drain", active)
	}
	if started := countRuns(t, h, `outcome = 'started'`); started != 0 {
		t.Errorf("%d run stubs still read 'started': a killed executor's stub must be marked crashed", started)
	}

	// (5) Every job the kill did not end succeeded.
	succeeded := countJobs(t, h, `state = 'succeeded'`)
	discarded := countJobs(t, h, `state = 'discarded'`)
	if succeeded+discarded != jobs {
		t.Errorf("%d succeeded and %d discarded, want %d between them", succeeded, discarded, jobs)
	}
	t.Logf("the kill orphaned %d attempts; %d jobs ended discarded and %d succeeded",
		h.orphanedByFaults(), discarded, succeeded)

	// (6) Nothing ran twice.
	if dup := countRuns(t, h, `outcome = 'success'`) - succeeded; dup != 0 {
		t.Errorf("%d more success rows than succeeded jobs: a job succeeded more than once", dup)
	}
	if n := overlappingAttempts(t, h); n != 0 {
		t.Errorf("%d pairs of attempts on the same job overlap in time: two executions ran at once", n)
	}
	if got := h.exec.count(); got != 0 {
		t.Errorf("%d concurrent executions observed in process", got)
	}
}
