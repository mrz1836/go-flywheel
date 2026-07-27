//go:build loadtest

package main

import (
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mrz1836/go-flywheel/loadtest"
)

// testDSN is a syntactically valid target. parseFlags never connects, which is
// the whole reason this file needs no database.
const testDSN = "postgres://localhost:5432/flywheel_test?sslmode=disable"

// TestParseFlagsDefaults pins every default the command line offers. The
// defaults are what the committed artifacts were produced with, so a silent
// change to one would move every published plan without appearing in any command
// line.
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
		{"Jobs", opts.cfg.Jobs, 1_000_000},
		{"Queues", opts.cfg.Queues, 3},
		{"Seed", opts.cfg.Seed, int64(1)},
		{"Limit", opts.cfg.Limit, 8},
		{"ExecutorClass", opts.cfg.ExecutorClass, ""},
		{"Lease", opts.cfg.Lease, time.Duration(0)},
		{"timeout", opts.timeout, 60 * time.Minute},
		{"out", opts.out, ""},
		{"quiet", opts.quiet, false},
	}
	for _, c := range checks {
		if c.got != c.want {
			t.Errorf("%s = %v, want %v", c.name, c.got, c.want)
		}
	}
}

// TestParseFlagsMapsEveryFlag proves each flag reaches the field it names. A
// flag wired to the wrong field would produce an artifact whose header
// disagreed with the command line that made it — and the header is what the
// artifact claims is reproducible.
func TestParseFlagsMapsEveryFlag(t *testing.T) {
	opts, err := parseFlags([]string{
		"-dsn", testDSN,
		"-jobs", "500",
		"-queues", "2",
		"-seed", "77",
		"-limit", "16",
		"-executor-class", "gpu",
		"-lease", "45s",
		"-timeout", "90s",
		"-out", "/tmp/plans.txt",
		"-quiet",
	}, io.Discard)
	if err != nil {
		t.Fatalf("parseFlags: %v", err)
	}

	checks := []struct {
		name      string
		got, want any
	}{
		{"DSN", opts.cfg.DSN, testDSN},
		{"Jobs", opts.cfg.Jobs, 500},
		{"Queues", opts.cfg.Queues, 2},
		{"Seed", opts.cfg.Seed, int64(77)},
		{"Limit", opts.cfg.Limit, 16},
		{"ExecutorClass", opts.cfg.ExecutorClass, "gpu"},
		{"Lease", opts.cfg.Lease, 45 * time.Second},
		{"timeout", opts.timeout, 90 * time.Second},
		{"out", opts.out, "/tmp/plans.txt"},
		{"quiet", opts.quiet, true},
	}
	for _, c := range checks {
		if c.got != c.want {
			t.Errorf("%s = %v, want %v", c.name, c.got, c.want)
		}
	}
}

func TestParseFlagsRejections(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want string
	}{
		{name: "no target", args: []string{"-jobs", "10"}, want: dsnEnv},
		{name: "unexpected argument", args: []string{"-dsn", testDSN, "stray"}, want: "unexpected argument"},
		{name: "unknown flag", args: []string{"-dsn", testDSN, "-nope"}, want: "nope"},
		{name: "non-positive timeout", args: []string{"-dsn", testDSN, "-timeout", "0"}, want: "-timeout"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// The env var is cleared rather than set, so the "no target" case is
			// genuinely targetless on a developer machine that exports one.
			t.Setenv(dsnEnv, "")

			_, err := parseFlags(tc.args, io.Discard)
			if err == nil {
				t.Fatalf("parseFlags(%v) must fail", tc.args)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error must name %q, got: %v", tc.want, err)
			}
		})
	}
}

// TestWriteArtifactIsNotWorldReadable pins the file mode. These artifacts are
// written into a repository, and a tool that created them world-writable would
// be a finding in every consumer's security scan.
func TestWriteArtifactIsNotWorldReadable(t *testing.T) {
	path := filepath.Join(t.TempDir(), "plans.txt")

	report := loadtest.ExplainReport{Target: "postgres://localhost:5432/flywheel_test", Jobs: 1}
	if err := writeArtifact(report, path, os.Stdout); err != nil {
		t.Fatalf("writeArtifact: %v", err)
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Errorf("mode = %v, want 0600", info.Mode().Perm())
	}
	data, err := os.ReadFile(path) //nolint:gosec // a path this test created
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if !strings.Contains(string(data), "claim predicate characterization") {
		t.Errorf("the artifact must carry its own header:\n%s", data)
	}
}

func TestCollapseSpace(t *testing.T) {
	if got := collapseSpace("a\n  b\tc "); got != "a b c" {
		t.Errorf("collapseSpace = %q", got)
	}
}
