//go:build loadtest

package loadtest

import (
	"context"
	"errors"
	"testing"
	"time"

	flywheel "github.com/mrz1836/go-flywheel"
)

// TestWorkerKillMidDrainReclaimsEveryJob is the plan's one asserting chaos
// scenario: a runner dies mid-drain, and the lease sweep must recover
// everything it was holding without any job running twice.
//
// Two thousand jobs, not a hundred thousand. This is an assertion, not a
// measurement — the measurements are the benchmarks — and it has to fit inside
// the Postgres workflow's budget.
func TestWorkerKillMidDrainReclaimsEveryJob(t *testing.T) {
	dsn := requireDSN(t)
	ctx := context.Background()

	const jobs = 2000
	cfg := Config{
		DSN: dsn, Jobs: jobs, Seed: 21,
		// Four runners so killing one leaves three to reclaim its work. With one
		// runner the drain would simply stall, which is why KillWorker.Validate
		// rejects that configuration outright.
		Runners: 4,
		Workers: 4,
		Mix:     WorkloadDrainOnly,
		Indexes: IndexesFull,
		// Enough per-job work that the kill lands while jobs are genuinely in
		// flight rather than in the gap between two claims.
		WorkDuration:   5 * time.Millisecond,
		SampleInterval: 500 * time.Millisecond,
		Timeout:        3 * time.Minute,
		Faults:         KillWorker{Fraction: 0.40, Runner: 0},
	}

	// The harness is driven through its own seams rather than through Run,
	// because Run drops the schema on the way out and every assertion below is a
	// query against the audit trail the run left behind.
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

	// (1) The fault actually fired.
	//
	// This assertion is first because it is the one most easily left out, and
	// without it the other three pass vacuously: a scenario whose fault never
	// fired proves only that a healthy run is healthy.
	if h.killed.Load() == 0 {
		t.Fatal("the fault never fired: no runner was killed, so nothing below is a test of recovery")
	}
	if orphaned := h.orphanedByFaults(); orphaned == 0 {
		t.Fatal("the kill blocked no finalize, so it left no orphan and nothing below tests recovery")
	}
	if crashed := countRuns(t, h, `outcome = 'crashed'`); crashed == 0 {
		t.Fatal("no run stub was marked crashed: the sweep did not recover the orphan the kill created")
	}

	// (2) Nothing non-terminal, and no stub still reading 'started', is left.
	if active := countJobs(t, h, `state IN ('available','running','retryable','scheduled')`); active != 0 {
		t.Errorf("%d jobs are still non-terminal after the drain", active)
	}
	if started := countRuns(t, h, `outcome = 'started'`); started != 0 {
		t.Errorf("%d run stubs still read 'started': a killed executor's stub must be marked crashed", started)
	}
	if succeeded := countJobs(t, h, `state = 'succeeded'`); succeeded != jobs {
		t.Errorf("%d jobs succeeded, want %d — every job must survive the kill", succeeded, jobs)
	}

	// (3) Every stale stub was crashed rather than abandoned, which is what makes
	// the audit trail readable after a crash: an attempt that never finished has
	// an outcome saying so.
	if unfinished := countRuns(t, h, `finished_at IS NULL`); unfinished != 0 {
		t.Errorf("%d run rows have no finished_at: an attempt with no end is an attempt nobody can audit",
			unfinished)
	}

	// (4) No job ran twice, by three independent checks. One check that happened
	// to be wrong would be indistinguishable from a guarantee that held.
	if dup := countRuns(t, h, `outcome = 'success'`) - int64(jobs); dup != 0 {
		t.Errorf("%d success rows for %d jobs: a difference of %d means a job succeeded more than once",
			countRuns(t, h, `outcome = 'success'`), jobs, dup)
	}
	if n := overlappingAttempts(t, h); n != 0 {
		t.Errorf("%d pairs of attempts on the same job overlap in time: two executions ran at once", n)
	}
	if got := h.exec.count(); got != 0 {
		t.Errorf("%d concurrent executions observed in process", got)
	}
}

// countJobs counts jobs matching a predicate.
func countJobs(t *testing.T, h *Harness, where string) int64 {
	t.Helper()
	var n int64
	if err := h.probe.Raw(`SELECT count(*) FROM jobs WHERE ` + where).Scan(&n).Error; err != nil {
		t.Fatalf("count jobs where %s: %v", where, err)
	}
	return n
}

// countRuns counts job_runs rows matching a predicate.
func countRuns(t *testing.T, h *Harness, where string) int64 {
	t.Helper()
	var n int64
	if err := h.probe.Raw(`SELECT count(*) FROM job_runs WHERE ` + where).Scan(&n).Error; err != nil {
		t.Fatalf("count runs where %s: %v", where, err)
	}
	return n
}

// overlappingAttempts counts pairs of attempts on the same job whose execution
// windows overlap, which is the audit trail's own statement of exactly-once.
//
// Crashed stubs are excluded from the comparison, and the reason is not
// convenience. A crashed stub's finished_at is stamped by the sweep at reclaim
// time, not by the executor that died — the process stopped executing at some
// unknowable earlier moment. Comparing against a bookkeeping timestamp would
// report the legitimate sequence "attempt crashed, sweep reclaimed, retry ran"
// as an overlap.
func overlappingAttempts(t *testing.T, h *Harness) int64 {
	t.Helper()
	var n int64
	if err := h.probe.Raw(`
		SELECT count(*)
		FROM job_runs a
		JOIN job_runs b ON a.job_id = b.job_id AND a.id < b.id
		WHERE a.outcome <> 'crashed' AND b.outcome <> 'crashed'
		  AND a.finished_at IS NOT NULL AND b.finished_at IS NOT NULL
		  AND a.started_at < b.finished_at AND b.started_at < a.finished_at`).Scan(&n).Error; err != nil {
		t.Fatalf("check for overlapping attempts: %v", err)
	}
	return n
}

// TestFaultSchedulingAndValidation covers the fault machinery without a
// database, so the rules a scenario depends on are checked even where no server
// is configured.
func TestFaultSchedulingAndValidation(t *testing.T) {
	t.Parallel()

	t.Run("KillWorker needs a second runner", func(t *testing.T) {
		t.Parallel()
		_, err := Config{
			DSN: testDSN, Jobs: 10, Runners: 1,
			Faults: KillWorker{Fraction: 0.5},
		}.validate()
		if !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("error = %v, want ErrInvalidConfig: killing the only runner stalls the drain", err)
		}
	})

	t.Run("KillWorker rejects an out-of-range runner", func(t *testing.T) {
		t.Parallel()
		_, err := Config{
			DSN: testDSN, Jobs: 10, Runners: 2,
			Faults: KillWorker{Fraction: 0.5, Runner: 7},
		}.validate()
		if !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("error = %v, want ErrInvalidConfig", err)
		}
	})

	t.Run("a fraction outside (0,1) is rejected", func(t *testing.T) {
		t.Parallel()
		for _, at := range []float64{0, 1, -0.5, 1.5} {
			_, err := Config{
				DSN: testDSN, Jobs: 10, Runners: 2,
				Faults: KillWorker{Fraction: at},
			}.validate()
			if !errors.Is(err, ErrInvalidConfig) {
				t.Errorf("fraction %v: error = %v, want ErrInvalidConfig", at, err)
			}
		}
	})

	t.Run("PauseDatabase needs a duration", func(t *testing.T) {
		t.Parallel()
		_, err := Config{
			DSN: testDSN, Jobs: 10, Runners: 2,
			Faults: PauseDatabase{Fraction: 0.5},
		}.validate()
		if !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("error = %v, want ErrInvalidConfig: a zero-length pause never happened", err)
		}
	})

	t.Run("a valid fault passes", func(t *testing.T) {
		t.Parallel()
		if _, err := (Config{
			DSN: testDSN, Jobs: 10, Runners: 2,
			Faults: KillWorker{Fraction: 0.4},
		}).validate(); err != nil {
			t.Fatalf("a valid fault was rejected: %v", err)
		}
	})

	t.Run("every fault describes itself", func(t *testing.T) {
		t.Parallel()
		// A Fault is an interface, so it cannot be marshaled; the description is
		// the only thing a committed report can carry about the experiment it
		// records.
		for _, f := range []Fault{
			KillWorker{Fraction: 0.4, Runner: 1},
			PauseDatabase{Fraction: 0.6, For: 2 * time.Second},
			MassLeaseExpiry{Fraction: 0.5},
		} {
			if f.Describe() == "" {
				t.Errorf("%T does not describe itself, so a report of it identifies nothing", f)
			}
		}
	})

	t.Run("MassLeaseExpiry needs a mix that drains", func(t *testing.T) {
		t.Parallel()
		_, err := Config{
			DSN: testDSN, Jobs: 10, Mix: WorkloadEnqueueOnly,
			Faults: MassLeaseExpiry{Fraction: 0.5},
		}.validate()
		if !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("error = %v, want ErrInvalidConfig: the enqueue mix holds no lease to expire", err)
		}

		if _, err := (Config{
			DSN: testDSN, Jobs: 10, Mix: WorkloadDrainOnly,
			Faults: MassLeaseExpiry{Fraction: 0.5},
		}).validate(); err != nil {
			t.Fatalf("a draining mix was rejected: %v", err)
		}
	})
}

// TestWindowedFaultIsDeclaredNotInferred pins the scheduler's revert rule to the
// interface rather than to a type switch.
//
// The rule it encodes: a fault that declares a window is reverted when the
// window closes, and one that does not is reverted at the end of the run. Under
// the type switch this file used to rely on, a new windowed fault would silently
// take the second branch — compiling, running, and quietly ignoring its own
// duration.
func TestWindowedFaultIsDeclaredNotInferred(t *testing.T) {
	t.Parallel()

	pause := PauseDatabase{Fraction: 0.5, For: 2 * time.Second}
	windowed, ok := any(pause).(windowedFault)
	if !ok {
		t.Fatal("PauseDatabase does not declare a window, so the scheduler would never close it")
	}
	if got := windowed.Window(); got != pause.For {
		t.Errorf("Window() = %s, want %s", got, pause.For)
	}

	// The permanent faults must not declare one: a window on a fault with no
	// revert would be a duration nothing acts on.
	for _, f := range []Fault{KillWorker{Fraction: 0.4, Runner: 1}, MassLeaseExpiry{Fraction: 0.5}} {
		if _, isWindowed := f.(windowedFault); isWindowed {
			t.Errorf("%T declares a window but is permanent, so nothing would ever close it", f)
		}
	}
}

// TestGateBlocksTheDriver proves the gate stops every method that reaches the
// database and leaves alone the one that cannot be reached through a decorator.
func TestGateBlocksTheDriver(t *testing.T) {
	t.Parallel()

	g := &gate{}
	d := &gateDriver{inner: panicDriver{}, gate: g}
	ctx := context.Background()

	// Open: every call reaches the inner driver, which panics to prove it.
	assertReaches := func(name string, call func()) {
		defer func() {
			if recover() == nil {
				t.Errorf("%s did not reach the inner driver", name)
			}
		}()
		call()
	}
	assertReaches("Dequeue", func() { _, _ = d.Dequeue(ctx, nil, "", false, 1, time.Second) })

	// Shut: the calls that touch the database fail without reaching it.
	g.shutGate()
	if _, err := d.Dequeue(ctx, nil, "", false, 1, time.Second); !errors.Is(err, ErrGated) {
		t.Errorf("Dequeue error = %v, want ErrGated", err)
	}
	if err := d.InsertRunStub(ctx, "r", flywheel.RawJob{}, time.Now(), "", "e"); !errors.Is(err, ErrGated) {
		t.Errorf("InsertRunStub error = %v, want ErrGated", err)
	}
	if _, err := d.Finalize(ctx, flywheel.RawJob{}, "r", flywheel.Result{}, nil, time.Now()); !errors.Is(err, ErrGated) {
		t.Errorf("Finalize error = %v, want ErrGated", err)
	}
	if _, err := d.Sweep(ctx, time.Now()); !errors.Is(err, ErrGated) {
		t.Errorf("Sweep error = %v, want ErrGated", err)
	}
	// Gating renewal is what lets a killed runner's leases actually expire: a
	// heartbeat that kept running would hold the orphan's lease for as long as
	// the process lived, and the sweep would never see it.
	if _, err := d.RenewLease(ctx, "j", "tok", time.Now()); !errors.Is(err, ErrGated) {
		t.Errorf("RenewLease error = %v, want ErrGated", err)
	}

	// Reopened: the gate is reversible, which is what lets PauseDatabase promise
	// a revert rather than lie about one.
	g.openGate()
	assertReaches("Dequeue after reopen", func() { _, _ = d.Dequeue(ctx, nil, "", false, 1, time.Second) })
}

// TestExecTrackerCountsConcurrency proves the in-process half of the
// exactly-once check actually detects the thing it is watching for. A tracker
// that could not fail would make every clean run's zero meaningless.
func TestExecTrackerCountsConcurrency(t *testing.T) {
	t.Parallel()

	var tr execTracker
	tr.enter("job-1")
	if got := tr.count(); got != 0 {
		t.Fatalf("count = %d after one entry, want 0", got)
	}

	tr.enter("job-1") // the same job, still running: a violation
	if got := tr.count(); got != 1 {
		t.Fatalf("count = %d after a concurrent entry, want 1", got)
	}

	tr.enter("job-2") // a different job: not a violation
	if got := tr.count(); got != 1 {
		t.Fatalf("count = %d, want 1", got)
	}

	// A job that finished and runs again is a retry, not a violation: after a
	// crash the runtime is supposed to run it a second time.
	tr.exit("job-1")
	tr.enter("job-1")
	if got := tr.count(); got != 1 {
		t.Fatalf("count = %d after a sequential re-run, want 1: a retry is not a concurrent execution", got)
	}
}

// panicDriver panics on every call, so a test can prove a decorator reached it.
type panicDriver struct{ flywheel.Driver }

func (panicDriver) Dequeue(
	context.Context, []string, flywheel.ExecutorClass, bool, int, time.Duration,
) ([]flywheel.RawJob, error) {
	panic("reached")
}
