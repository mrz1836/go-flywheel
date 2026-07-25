//go:build loadtest

package loadtest

import (
	"errors"
	"strings"
	"testing"
	"time"
)

// testDSN is a syntactically valid target for tests that never connect. It
// carries a password on purpose: every test that renders it must prove the
// password does not come out the other side.
const testDSN = "postgres://user:hunter2@localhost:5432/flywheel_test?sslmode=disable"

// TestValidateAppliesDefaults proves a minimally specified run is runnable, and
// that validation is a pure function of its receiver: the source Config must not
// be mutated, because the benchmarks derive one config from another and mutate a
// single field, and that idiom is only safe if validation has no side effects.
func TestValidateAppliesDefaults(t *testing.T) {
	t.Parallel()

	src := Config{DSN: testDSN, Jobs: 10}
	got, err := src.validate()
	if err != nil {
		t.Fatalf("validate: %v", err)
	}

	if src.Runners != 0 || src.Mix != "" || src.Timeout != 0 {
		t.Errorf("validate mutated its receiver: %+v", src)
	}

	checks := []struct {
		name      string
		got, want any
	}{
		{"Runners", got.Runners, defaultRunners},
		{"Workers", got.Workers, defaultWorkers},
		{"Mix", got.Mix, WorkloadDrainOnly},
		{"Indexes", got.Indexes, IndexesFull},
		{"SampleInterval", got.SampleInterval, defaultSampleInterval},
		{"Timeout", got.Timeout, time.Duration(defaultTimeout)},
		{"Queue", got.Queue, defaultQueue},
		{"ExecutorClass", got.ExecutorClass, defaultExecutorClass},
	}
	for _, c := range checks {
		if c.got != c.want {
			t.Errorf("%s = %v, want %v", c.name, c.got, c.want)
		}
	}
}

// TestValidateRejects covers every way a Config fails to be runnable. Each case
// asserts the sentinel, not the message, so the errors stay matchable for a
// caller that wants to tell a bad config from a failed run.
func TestValidateRejects(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		cfg  Config
		want error
	}{
		{"no dsn", Config{Jobs: 1}, ErrNoDSN},
		{"zero jobs", Config{DSN: testDSN}, ErrInvalidConfig},
		{"negative jobs", Config{DSN: testDSN, Jobs: -1}, ErrInvalidConfig},
		{"negative runners", Config{DSN: testDSN, Jobs: 1, Runners: -1}, ErrInvalidConfig},
		{"negative workers", Config{DSN: testDSN, Jobs: 1, Workers: -1}, ErrInvalidConfig},
		{"unknown mix", Config{DSN: testDSN, Jobs: 1, Mix: "sideways"}, ErrInvalidConfig},
		{"unknown index condition", Config{DSN: testDSN, Jobs: 1, Indexes: "some"}, ErrInvalidConfig},
		{"negative work duration", Config{DSN: testDSN, Jobs: 1, WorkDuration: -time.Second}, ErrInvalidConfig},
		{"negative work jitter", Config{DSN: testDSN, Jobs: 1, WorkJitter: -time.Second}, ErrInvalidConfig},
		{"connection budget", Config{DSN: testDSN, Jobs: 1, Runners: 16, Workers: 16}, ErrTooManyConnections},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := tc.cfg.validate()
			if !errors.Is(err, tc.want) {
				t.Fatalf("validate() error = %v, want %v", err, tc.want)
			}
		})
	}
}

// TestConnectionBudgetErrorShowsItsArithmetic proves the rejection is
// actionable. "Too many connections" without the numbers leaves an operator
// guessing which of the two knobs to turn and by how much; the arithmetic in the
// message answers both.
func TestConnectionBudgetErrorShowsItsArithmetic(t *testing.T) {
	t.Parallel()

	_, err := Config{DSN: testDSN, Jobs: 1, Runners: 16, Workers: 16}.validate()
	if err == nil {
		t.Fatal("expected a rejection")
	}
	msg := err.Error()
	for _, want := range []string{"16×17", "276", "90", "max_connections=100"} {
		if !strings.Contains(msg, want) {
			t.Errorf("error message must contain %q, got: %s", want, msg)
		}
	}
}

// TestConnectionBudgetBoundary pins the edge exactly, so a later change to the
// overhead or the budget has to be deliberate rather than accidental.
func TestConnectionBudgetBoundary(t *testing.T) {
	t.Parallel()

	// 8 runners × (10 workers + 1) + 4 = 92 > 90: rejected.
	// 8 runners × ( 9 workers + 1) + 4 = 84 ≤ 90: accepted.
	if _, err := (Config{DSN: testDSN, Jobs: 1, Runners: 8, Workers: 10}).validate(); err == nil {
		t.Error("8×11+4 = 92 exceeds the budget and must be rejected")
	}
	if _, err := (Config{DSN: testDSN, Jobs: 1, Runners: 8, Workers: 9}).validate(); err != nil {
		t.Errorf("8×10+4 = 84 is within the budget: %v", err)
	}
}

// TestWorkloadAndIndexConditionValidity proves the two enums recognize exactly
// their declared values. The strings are persisted in committed JSON reports, so
// a typo that validated would produce an artifact naming a shape that does not
// exist.
func TestWorkloadAndIndexConditionValidity(t *testing.T) {
	t.Parallel()

	valid := []Workload{
		WorkloadEnqueueOnly, WorkloadDrainOnly, WorkloadSteady, WorkloadFanOut, WorkloadMixedSpeed,
	}
	for _, w := range valid {
		if !w.Valid() {
			t.Errorf("Workload %q must be valid", w)
		}
	}
	for _, w := range []Workload{"", "fan-in", "FAN-OUT", "drain "} {
		if w.Valid() {
			t.Errorf("Workload %q must not be valid", w)
		}
	}

	for _, c := range []IndexCondition{IndexesFull, IndexesCorrectness} {
		if !c.Valid() {
			t.Errorf("IndexCondition %q must be valid", c)
		}
	}
	for _, c := range []IndexCondition{"", "none", "Full"} {
		if c.Valid() {
			t.Errorf("IndexCondition %q must not be valid", c)
		}
	}
}

// TestWorkloadFanOutIsNamedForItsShape guards the wire string specifically.
// It is written into every committed report, so renaming it later is a breaking
// change to an artifact — and "fan-in" would name a shape (many converging on
// one) that the runtime has no primitive for.
func TestWorkloadFanOutIsNamedForItsShape(t *testing.T) {
	t.Parallel()

	if got := string(WorkloadFanOut); got != "fan-out" {
		t.Fatalf("WorkloadFanOut = %q, want %q", got, "fan-out")
	}
	if Workload("fan-in").Valid() {
		t.Fatal("fan-in must not be a recognized workload: the runtime has no join primitive")
	}
}
