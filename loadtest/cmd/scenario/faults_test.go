//go:build loadtest

package main

import (
	"io"
	"strings"
	"testing"
	"time"

	"github.com/mrz1836/go-flywheel/loadtest"
)

// TestParseFaultGrammar covers `name[@fraction][:duration]` in every shape the
// plans already write, plus the defaults that make a bare name usable.
//
// The bare-name cases matter more than they look: Config.validate requires the
// fraction to be strictly inside (0,1), so a bare name with no default fraction
// would be a command line that parses and then fails to run.
func TestParseFaultGrammar(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want loadtest.Fault
	}{
		{"empty is no fault", "", nil},
		{"none is no fault", "none", nil},
		{
			"bare name takes the default fraction", "kill-worker",
			loadtest.KillWorker{Fraction: defaultFaultFraction},
		},
		{
			"name@fraction", "kill-worker@0.4",
			loadtest.KillWorker{Fraction: 0.4},
		},
		{
			"name:duration takes the default fraction", "pause-database:60s",
			loadtest.PauseDatabase{Fraction: defaultFaultFraction, For: time.Minute},
		},
		{
			"name@fraction:duration", "pause-database@0.25:90s",
			loadtest.PauseDatabase{Fraction: 0.25, For: 90 * time.Second},
		},
		{
			"bare pause takes the default window", "pause-database",
			loadtest.PauseDatabase{Fraction: defaultFaultFraction, For: defaultPauseWindow},
		},
		{
			"surrounding space is tolerated", "  kill-worker@0.9  ",
			loadtest.KillWorker{Fraction: 0.9},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseFault(tc.in)
			if err != nil {
				t.Fatalf("parseFault(%q): %v", tc.in, err)
			}
			if got != tc.want {
				t.Errorf("parseFault(%q) = %#v, want %#v", tc.in, got, tc.want)
			}
		})
	}
}

// TestParseFaultRejects covers the ways a fault spec fails. Each message names
// the value the operator typed, since that is the only thing they can correct.
func TestParseFaultRejects(t *testing.T) {
	cases := []struct {
		name, in, want string
	}{
		{"unknown name", "melt-database", "melt-database"},
		{"unknown name lists the alternatives", "melt-database", "kill-worker"},
		{"non-numeric fraction", "kill-worker@half", "not a fraction"},
		{"non-duration window", "pause-database:soon", "not a duration"},
		{"zero window", "pause-database:0s", "never happens"},
		{"empty name with a fraction", "@0.4", "names no fault"},
		{"duration on a permanent fault", "kill-worker:30s", "takes no duration"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := parseFault(tc.in)
			if err == nil {
				t.Fatalf("parseFault(%q) succeeded, want an error", tc.in)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error %q does not mention %q", err, tc.want)
			}
		})
	}
}

// TestParseFaultBuildsEveryDeclaredName proves the flag can ask for each fault
// the registry declares. A fault reachable from Go but not from the command
// line is the gap this flag exists to close, and it would reopen silently the
// next time one is added.
func TestParseFaultBuildsEveryDeclaredName(t *testing.T) {
	for _, name := range faultNames() {
		got, err := parseFault(name)
		if err != nil {
			t.Errorf("-fault %s: %v", name, err)
			continue
		}
		if name == faultNone {
			if got != nil {
				t.Errorf("-fault %s produced %#v, want no fault", name, got)
			}
			continue
		}
		if got == nil {
			t.Errorf("-fault %s produced no fault", name)
			continue
		}
		// Every default must satisfy the harness's own bounds, or a bare name is a
		// command line that parses and then refuses to run.
		if at := got.At(); at <= 0 || at >= 1 {
			t.Errorf("-fault %s defaults to fraction %v, which is outside (0,1)", name, at)
		}
		if got.Describe() == "" {
			t.Errorf("-fault %s describes itself as empty, so a report of it identifies nothing", name)
		}
	}
}

// TestParseFlagsWiresTheFault is the end-to-end half: the flag reaches
// Config.Faults, and a bad value is rejected with a message naming -fault.
func TestParseFlagsWiresTheFault(t *testing.T) {
	opts, err := parseFlags([]string{"-dsn", testDSN, "-fault", "kill-worker@0.4"}, io.Discard)
	if err != nil {
		t.Fatalf("parseFlags: %v", err)
	}
	kill, ok := opts.cfg.Faults.(loadtest.KillWorker)
	if !ok {
		t.Fatalf("Faults = %#v, want a KillWorker", opts.cfg.Faults)
	}
	if kill.Fraction != 0.4 {
		t.Errorf("Fraction = %v, want 0.4", kill.Fraction)
	}

	if _, err := parseFlags([]string{"-dsn", testDSN, "-fault", "nonsense"}, io.Discard); err == nil {
		t.Error("parseFlags accepted an unknown fault")
	} else if !strings.Contains(err.Error(), "-fault") {
		t.Errorf("error %q does not mention -fault", err)
	}
}
