//go:build loadtest

package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/mrz1836/go-flywheel/loadtest"
)

// dsnEnv is the environment variable -dsn falls back to.
//
// It is the harness's own variable, not the integration suite's, for the same
// reason the scenario main uses it: a characterization run seeds six figures of
// rows, and silently adopting the integration DSN would point that at whatever
// database CI had configured.
const dsnEnv = "FLYWHEEL_LOADTEST_DATABASE_URL"

// options is everything the explain main takes from its command line.
type options struct {
	cfg loadtest.ExplainConfig
	// out is where the artifact is written; empty means stdout.
	out string
	// quiet suppresses the human-readable summary, for a run whose output is
	// being piped.
	quiet bool
	// timeout bounds the whole run. Seeding a million rows and building six
	// indexes over them is minutes of work, and a run that wedged would
	// otherwise hold a schema open indefinitely.
	timeout time.Duration
}

// parseFlags maps a command line onto an ExplainConfig.
//
// It builds its own FlagSet rather than using flag.CommandLine, for the same
// reason the scenario main does: no global mutable state, and therefore a
// mapping that can be table-tested without a database, a process, or an
// os.Exit.
func parseFlags(args []string, stderr io.Writer) (options, error) {
	fs := flag.NewFlagSet("explain", flag.ContinueOnError)
	fs.SetOutput(stderr)

	var opts options

	fs.StringVar(&opts.cfg.DSN, "dsn", os.Getenv(dsnEnv),
		"PostgreSQL target (default $"+dsnEnv+")")
	fs.IntVar(&opts.cfg.Jobs, "jobs", 1_000_000, "number of claimable rows to seed")
	fs.IntVar(&opts.cfg.Queues, "queues", 3, "number of distinct queues the rows are spread across")
	fs.Int64Var(&opts.cfg.Seed, "seed", 1, "run seed; equal seeds produce byte-identical workloads")
	fs.IntVar(&opts.cfg.Limit, "limit", 8, "the claim's batch size, which is a runner's Concurrency")
	fs.StringVar(&opts.cfg.ExecutorClass, "executor-class", "",
		"executor class a routed claim asks for (default: the harness default)")
	fs.DurationVar(&opts.cfg.Lease, "lease", 0,
		"lease duration stamped by the captured claim; it changes no plan (default 1m)")
	fs.DurationVar(&opts.timeout, "timeout", 60*time.Minute, "hard bound on the whole run")
	fs.StringVar(&opts.out, "out", "", "write the artifact here (default: stdout)")
	fs.BoolVar(&opts.quiet, "quiet", false, "suppress the human-readable summary")

	if err := fs.Parse(args); err != nil {
		return options{}, fmt.Errorf("explain: %w", err)
	}
	if fs.NArg() > 0 {
		return options{}, fmt.Errorf("explain: unexpected argument %q", fs.Arg(0))
	}
	if opts.timeout <= 0 {
		return options{}, fmt.Errorf("explain: -timeout must be positive, got %s", opts.timeout)
	}
	if opts.cfg.DSN == "" {
		return options{}, fmt.Errorf("explain: no target: pass -dsn or set %s", dsnEnv)
	}
	return opts, nil
}
