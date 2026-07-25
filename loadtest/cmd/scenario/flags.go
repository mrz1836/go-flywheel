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
// It is the harness's own variable, not the integration suite's. A scenario that
// silently adopted the integration DSN would seed six figures of rows into
// whatever database CI had configured.
const dsnEnv = "FLYWHEEL_LOADTEST_DATABASE_URL"

// options is everything the scenario main takes from its command line.
type options struct {
	cfg loadtest.Config
	// out is where the JSON report is written; empty means stdout.
	out string
	// quiet suppresses the human-readable summary, for a run whose output is
	// being piped.
	quiet bool
}

// parseFlags maps a command line onto a Config.
//
// It builds its own FlagSet rather than using flag.CommandLine, for two reasons
// that are really one: no global mutable state, and therefore a mapping that can
// be table-tested without a database, a process, or an os.Exit. A flag parser
// that can only be exercised by running the program is a flag parser whose
// defaults are verified by whoever runs it next.
func parseFlags(args []string, stderr io.Writer) (options, error) {
	fs := flag.NewFlagSet("scenario", flag.ContinueOnError)
	fs.SetOutput(stderr)

	var (
		opts    options
		mix     string
		indexes string
	)

	fs.StringVar(&opts.cfg.DSN, "dsn", os.Getenv(dsnEnv),
		"PostgreSQL target (default $"+dsnEnv+")")
	fs.IntVar(&opts.cfg.Jobs, "jobs", 100_000, "number of jobs to seed")
	fs.Int64Var(&opts.cfg.Seed, "seed", 1, "run seed; equal seeds produce byte-identical workloads")
	fs.IntVar(&opts.cfg.Runners, "runners", 4, "number of independent runner loops")
	fs.IntVar(&opts.cfg.Workers, "workers", 8, "per-runner concurrency")
	fs.StringVar(&mix, "mix", string(loadtest.WorkloadDrainOnly),
		"workload shape: enqueue, drain, steady, fan-out, mixed-speed")
	fs.StringVar(&indexes, "indexes", string(loadtest.IndexesFull),
		"schema condition: full, correctness-only")
	fs.DurationVar(&opts.cfg.WorkDuration, "work", 0, "simulated per-job work time; zero isolates the database path")
	fs.DurationVar(&opts.cfg.WorkJitter, "jitter", 0, "spread applied to the per-job work time")
	fs.DurationVar(&opts.cfg.SampleInterval, "sample-interval", time.Second, "storage sampling cadence")
	fs.DurationVar(&opts.cfg.Timeout, "timeout", 30*time.Minute, "hard bound on the whole run")
	fs.StringVar(&opts.cfg.Queue, "queue", "", "queue to enqueue onto (default: the harness default)")
	fs.StringVar(&opts.cfg.ExecutorClass, "executor-class", "",
		"executor class the runners serve (default: the harness default)")
	fs.StringVar(&opts.out, "out", "", "write the JSON report here (default: stdout)")
	fs.BoolVar(&opts.quiet, "quiet", false, "suppress the human-readable summary")

	if err := fs.Parse(args); err != nil {
		return options{}, fmt.Errorf("scenario: %w", err)
	}
	if fs.NArg() > 0 {
		return options{}, fmt.Errorf("scenario: unexpected argument %q", fs.Arg(0))
	}

	opts.cfg.Mix = loadtest.Workload(mix)
	opts.cfg.Indexes = loadtest.IndexCondition(indexes)

	// The enum checks live here rather than being left to the harness so a typo
	// costs a message instead of a provisioned schema, and so the message names
	// the flag the operator typed.
	if !opts.cfg.Mix.Valid() {
		return options{}, fmt.Errorf("scenario: -mix %q is not one of enqueue, drain, steady, fan-out, mixed-speed", mix)
	}
	if !opts.cfg.Indexes.Valid() {
		return options{}, fmt.Errorf("scenario: -indexes %q is not one of full, correctness-only", indexes)
	}
	if opts.cfg.DSN == "" {
		return options{}, fmt.Errorf("scenario: no target: pass -dsn or set %s", dsnEnv)
	}
	return opts, nil
}
