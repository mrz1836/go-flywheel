//go:build loadtest

package loadtest

import (
	"context"
	"errors"
	"testing"
	"time"
)

// TestReplayScenarioReconverges is the harness's end-to-end proof of the replay
// path against a real server, at a size that survives a test suite: seed a fan-out
// cohort where most children fail transiently, drain it to a mix of succeeded and
// discarded, replay the discarded under their parent with a restored budget, and
// assert the cohort re-converges — every child terminal, no succeeded child re-run,
// and no job run past its restored budget.
//
// It is the small counterpart to the committed 30k artifact: the artifact is the
// measurement, this is the assertion.
func TestReplayScenarioReconverges(t *testing.T) {
	dsn := requireDSN(t)

	report, err := Run(context.Background(), Config{
		DSN:           dsn,
		Jobs:          200, // ignored for the cohort size in the replay shape; Children is the cohort
		Children:      200,
		Seed:          1,
		Runners:       2,
		Workers:       4,
		Mix:           WorkloadFanOut,
		Indexes:       IndexesFull,
		FailFraction:  0.75,
		Replay:        true,
		ReplayStagger: time.Second,
		Timeout:       2 * time.Minute,
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(report.Errors) > 0 {
		t.Fatalf("replay run reported errors: %v", report.Errors)
	}

	rep := report.Replay
	if rep == nil {
		t.Fatal("report.Replay is nil on a -replay run")
	}
	if rep.Parents != 1 {
		t.Fatalf("Parents = %d, want 1", rep.Parents)
	}
	if rep.Replayed <= 0 {
		t.Fatalf("Replayed = %d, want a positive count of recovered failures", rep.Replayed)
	}
	if rep.SkippedTerminal <= 0 {
		t.Fatalf("SkippedTerminal = %d, want the succeeded children left alone", rep.SkippedTerminal)
	}
	if rep.SucceededBeforeReplay != rep.SucceededAfterReplay {
		t.Fatalf("succeeded count changed across replay: before=%d after=%d (a replay must not re-run succeeded work)",
			rep.SucceededBeforeReplay, rep.SucceededAfterReplay)
	}
	if rep.MaxRunsOverBudget > 0 {
		t.Fatalf("a replayed job ran %d times past its restored budget", rep.MaxRunsOverBudget)
	}
	// The fail-children always fail, so the replayed cohort re-converges to discarded.
	if rep.DiscardedAfterReplay != rep.Replayed {
		t.Fatalf("DiscardedAfterReplay = %d, want it to equal Replayed = %d (the cohort re-converged)",
			rep.DiscardedAfterReplay, rep.Replayed)
	}
	// Every child ended terminal: the replayed (re-discarded) plus the succeeded.
	if got := rep.DiscardedAfterReplay + rep.SucceededAfterReplay; got != 200 {
		t.Fatalf("terminal children = %d, want 200 (every child re-converged)", got)
	}
}

// TestReplayScenarioRoundTripsThroughReport proves the ReplayReport survives the
// report's JSON wire form — it is added to reportJSON and both marshalers, so a
// committed artifact carries the re-convergence account rather than dropping it.
func TestReplayScenarioRoundTripsThroughReport(t *testing.T) {
	t.Parallel()
	want := Report{
		Config: Config{Jobs: 1, Mix: WorkloadFanOut, Timeout: time.Minute},
		Replay: &ReplayReport{
			Parents: 2, Replayed: 30000, SkippedTerminal: 5000,
			SucceededBeforeReplay: 5000, SucceededAfterReplay: 5000,
			DiscardedAfterReplay: 30000, MaxRunsOverBudget: 0,
		},
	}
	data, err := want.MarshalJSON()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var got Report
	if err := got.UnmarshalJSON(data); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.Replay == nil || *got.Replay != *want.Replay {
		t.Fatalf("Replay did not round-trip: got %+v, want %+v", got.Replay, want.Replay)
	}
}

// TestConfigReplayValidation covers the replay-specific config rules: the fail
// fraction range, the fan-out and fail-fraction requirements, and the small
// defaults a replay run applies.
func TestConfigReplayValidation(t *testing.T) {
	t.Parallel()

	base := Config{DSN: "postgres://localhost/x", Jobs: 100, Children: 100, Mix: WorkloadFanOut}

	t.Run("fail fraction at one is refused", func(t *testing.T) {
		t.Parallel()
		c := base
		c.FailFraction = 1
		if _, err := c.validate(); !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("FailFraction 1: got %v, want ErrInvalidConfig", err)
		}
	})

	t.Run("replay requires the fan-out mix", func(t *testing.T) {
		t.Parallel()
		c := base
		c.Mix = WorkloadDrainOnly
		c.FailFraction = 0.5
		c.Replay = true
		if _, err := c.validate(); !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("replay on drain mix: got %v, want ErrInvalidConfig", err)
		}
	})

	t.Run("replay requires failures to recover", func(t *testing.T) {
		t.Parallel()
		c := base
		c.Replay = true // FailFraction stays zero
		if _, err := c.validate(); !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("replay with no fail fraction: got %v, want ErrInvalidConfig", err)
		}
	})

	t.Run("a replay run defaults a small budget", func(t *testing.T) {
		t.Parallel()
		c := base
		c.FailFraction = 0.8
		c.Replay = true
		got, err := c.validate()
		if err != nil {
			t.Fatalf("validate: %v", err)
		}
		if got.MaxAttempts != defaultReplayMaxAttempts {
			t.Fatalf("MaxAttempts = %d, want the small replay default %d", got.MaxAttempts, defaultReplayMaxAttempts)
		}
		if got.ReplayBudget != defaultReplayBudget {
			t.Fatalf("ReplayBudget = %d, want the default %d", got.ReplayBudget, defaultReplayBudget)
		}
	})
}

// TestShouldFailIsDeterministicAndProportional proves the fail decision is a stable
// function of the ordinal and honours the fraction: zero never fails, and a
// fraction over a large population lands near it.
func TestShouldFailIsDeterministicAndProportional(t *testing.T) {
	t.Parallel()

	for n := range 100 {
		first := shouldFail(7, n, 0.5)
		if first != shouldFail(7, n, 0.5) {
			t.Fatalf("shouldFail is not deterministic at n=%d", n)
		}
		if shouldFail(7, n, 0) {
			t.Fatalf("shouldFail must never fail at fraction 0, failed at n=%d", n)
		}
	}

	const population = 20000
	failed := 0
	for n := range population {
		if shouldFail(1, n, 0.857) {
			failed++
		}
	}
	frac := float64(failed) / population
	if frac < 0.84 || frac > 0.87 {
		t.Fatalf("fail fraction landed at %.3f over %d, want near 0.857", frac, population)
	}
}
