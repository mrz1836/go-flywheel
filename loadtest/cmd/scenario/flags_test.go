//go:build loadtest

package main

import (
	"io"
	"strings"
	"testing"
	"time"

	"github.com/mrz1836/go-flywheel/loadtest"
)

// testDSN is a syntactically valid target. parseFlags never connects, which is
// the whole reason this file needs no database.
const testDSN = "postgres://localhost:5432/flywheel_test?sslmode=disable"

// TestParseFlagsDefaults pins every default the command line offers, because a
// default is what most runs actually use and a silent change to one would move
// every published number without appearing in any command line.
func TestParseFlagsDefaults(t *testing.T) {
	t.Setenv(dsnEnv, testDSN)

	opts, err := parseFlags(nil, io.Discard)
	if err != nil {
		t.Fatalf("parseFlags: %v", err)
	}

	checks := []struct {
		name      string
		got, want any
	}{
		{"DSN", opts.cfg.DSN, testDSN},
		{"Jobs", opts.cfg.Jobs, 100_000},
		{"Seed", opts.cfg.Seed, int64(1)},
		{"Runners", opts.cfg.Runners, 4},
		{"Workers", opts.cfg.Workers, 8},
		{"Mix", opts.cfg.Mix, loadtest.WorkloadDrainOnly},
		{"Indexes", opts.cfg.Indexes, loadtest.IndexesFull},
		{"WorkDuration", opts.cfg.WorkDuration, time.Duration(0)},
		{"Lease", opts.cfg.Lease, time.Duration(0)},
		{"SampleInterval", opts.cfg.SampleInterval, time.Second},
		{"Timeout", opts.cfg.Timeout, 30 * time.Minute},
		{"Limiter", opts.cfg.Limiter, loadtest.LimiterNone},
		{"Rate", opts.cfg.Rate, 0},
		{"WorkerSnooze", opts.cfg.WorkerSnooze, 0},
		{"out", opts.out, ""},
		{"quiet", opts.quiet, false},
	}
	for _, c := range checks {
		if c.got != c.want {
			t.Errorf("%s = %v, want %v", c.name, c.got, c.want)
		}
	}
	// The default must be *no* fault, and it must be an untyped nil: Config.Faults
	// is an interface, so a typed nil stored here would make the harness's
	// `Faults != nil` check true for a run that asked for none.
	if opts.cfg.Faults != nil {
		t.Errorf("Faults = %v, want nil by default", opts.cfg.Faults)
	}
}

// TestParseFlagsMapsEveryFlag proves each flag reaches the field it names. A
// flag wired to the wrong field would produce a report whose Config disagreed
// with the command line that made it — and the command line is what the report
// claims is reproducible.
func TestParseFlagsMapsEveryFlag(t *testing.T) {
	opts, err := parseFlags([]string{
		"-dsn", testDSN,
		"-jobs", "250",
		"-seed", "77",
		"-runners", "3",
		"-workers", "6",
		"-mix", "mixed-speed",
		"-indexes", "correctness-only",
		"-work", "5ms",
		"-jitter", "2ms",
		"-lease", "2s",
		"-sample-interval", "250ms",
		"-timeout", "90s",
		"-queue", "bench",
		"-executor-class", "gpu",
		"-limiter", "token-bucket",
		"-rate", "10",
		"-burst", "20",
		"-max-concurrent", "5",
		"-out", "/tmp/report.json",
		"-quiet",
	}, io.Discard)
	if err != nil {
		t.Fatalf("parseFlags: %v", err)
	}

	checks := []struct {
		name      string
		got, want any
	}{
		{"Jobs", opts.cfg.Jobs, 250},
		{"Seed", opts.cfg.Seed, int64(77)},
		{"Runners", opts.cfg.Runners, 3},
		{"Workers", opts.cfg.Workers, 6},
		{"Mix", opts.cfg.Mix, loadtest.WorkloadMixedSpeed},
		{"Indexes", opts.cfg.Indexes, loadtest.IndexesCorrectness},
		{"WorkDuration", opts.cfg.WorkDuration, 5 * time.Millisecond},
		{"WorkJitter", opts.cfg.WorkJitter, 2 * time.Millisecond},
		{"Lease", opts.cfg.Lease, 2 * time.Second},
		{"SampleInterval", opts.cfg.SampleInterval, 250 * time.Millisecond},
		{"Timeout", opts.cfg.Timeout, 90 * time.Second},
		{"Queue", opts.cfg.Queue, "bench"},
		{"ExecutorClass", opts.cfg.ExecutorClass, "gpu"},
		{"Limiter", opts.cfg.Limiter, loadtest.LimiterTokenBucket},
		{"Rate", opts.cfg.Rate, 10},
		{"Burst", opts.cfg.Burst, 20},
		{"MaxConcurrent", opts.cfg.MaxConcurrent, 5},
		{"out", opts.out, "/tmp/report.json"},
		{"quiet", opts.quiet, true},
	}
	for _, c := range checks {
		if c.got != c.want {
			t.Errorf("%s = %v, want %v", c.name, c.got, c.want)
		}
	}
}

// TestParseFlagsAcceptsEveryDeclaredMix proves the command line can actually ask
// for each shape the harness declares. A mix the harness supports but the
// command line rejects is a shape nobody can run.
func TestParseFlagsAcceptsEveryDeclaredMix(t *testing.T) {
	for _, mix := range []loadtest.Workload{
		loadtest.WorkloadEnqueueOnly, loadtest.WorkloadDrainOnly, loadtest.WorkloadSteady,
		loadtest.WorkloadFanOut, loadtest.WorkloadMixedSpeed,
	} {
		opts, err := parseFlags([]string{"-dsn", testDSN, "-mix", string(mix)}, io.Discard)
		if err != nil {
			t.Errorf("-mix %s: %v", mix, err)
			continue
		}
		if opts.cfg.Mix != mix {
			t.Errorf("-mix %s produced %s", mix, opts.cfg.Mix)
		}
	}
	for _, cond := range []loadtest.IndexCondition{loadtest.IndexesFull, loadtest.IndexesCorrectness} {
		if _, err := parseFlags([]string{"-dsn", testDSN, "-indexes", string(cond)}, io.Discard); err != nil {
			t.Errorf("-indexes %s: %v", cond, err)
		}
	}
}

// TestParseFlagsRejects covers the ways a command line fails, each with a
// message naming the flag the operator typed rather than a field they did not.
//
// Nothing in this file runs in parallel: several cases set the environment, and
// t.Setenv is incompatible with t.Parallel. Parsing a command line takes
// microseconds, so there is nothing to gain by it.
func TestParseFlagsRejects(t *testing.T) {
	// An empty environment, so the "no target" case is not rescued by a
	// developer's own shell.
	t.Setenv(dsnEnv, "")

	cases := []struct {
		name string
		args []string
		want string
	}{
		{"unknown mix", []string{"-dsn", testDSN, "-mix", "fan-in"}, "-mix"},
		{"unknown index condition", []string{"-dsn", testDSN, "-indexes", "some"}, "-indexes"},
		{"no target", []string{"-jobs", "10"}, "-dsn"},
		{"unknown flag", []string{"-dsn", testDSN, "-nope"}, "nope"},
		{"stray argument", []string{"-dsn", testDSN, "extra"}, "unexpected argument"},
		{"bad duration", []string{"-dsn", testDSN, "-work", "soon"}, "work"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := parseFlags(tc.args, io.Discard)
			if err == nil {
				t.Fatalf("parseFlags(%v) succeeded, want an error", tc.args)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error %q does not mention %q", err, tc.want)
			}
		})
	}
}

// TestParseFlagsPrefersTheExplicitDSN proves -dsn beats the environment, so a
// scenario aimed at one target cannot be silently redirected by a variable left
// in a shell.
func TestParseFlagsPrefersTheExplicitDSN(t *testing.T) {
	t.Setenv(dsnEnv, "postgres://env-host:5432/wrong")

	opts, err := parseFlags([]string{"-dsn", testDSN}, io.Discard)
	if err != nil {
		t.Fatalf("parseFlags: %v", err)
	}
	if opts.cfg.DSN != testDSN {
		t.Fatalf("DSN = %q, want the flag's value", opts.cfg.DSN)
	}
}

// TestParseFlagsUsesNoGlobalState is the reason this file can exist. Parsing
// twice in one process must be independent; a parser built on flag.CommandLine
// would have panicked on the second registration.
func TestParseFlagsUsesNoGlobalState(t *testing.T) {
	first, err := parseFlags([]string{"-dsn", testDSN, "-jobs", "10"}, io.Discard)
	if err != nil {
		t.Fatalf("first parse: %v", err)
	}
	second, err := parseFlags([]string{"-dsn", testDSN, "-jobs", "20"}, io.Discard)
	if err != nil {
		t.Fatalf("second parse: %v", err)
	}
	if first.cfg.Jobs != 10 || second.cfg.Jobs != 20 {
		t.Fatalf("parses interfered: %d and %d", first.cfg.Jobs, second.cfg.Jobs)
	}
}
