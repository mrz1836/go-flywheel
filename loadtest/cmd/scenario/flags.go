//go:build loadtest

package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
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
		fault   string
		tuned   bool
		limiter string
	)

	fs.StringVar(&opts.cfg.DSN, "dsn", os.Getenv(dsnEnv),
		"PostgreSQL target (default $"+dsnEnv+")")
	fs.IntVar(&opts.cfg.Jobs, "jobs", 100_000, "number of jobs to seed")
	fs.Int64Var(&opts.cfg.Seed, "seed", 1, "run seed; equal seeds produce byte-identical workloads")
	fs.IntVar(&opts.cfg.Runners, "runners", 4, "number of independent runner loops")
	fs.IntVar(&opts.cfg.Workers, "workers", 8, "per-runner concurrency")
	fs.StringVar(&mix, "mix", string(loadtest.WorkloadDrainOnly),
		"workload shape: enqueue, drain, steady, fan-out, barrier, mixed-speed")
	fs.IntVar(&opts.cfg.Children, "children", 0,
		"children per parent in the fan-out and barrier mixes (0 selects the mix default)")
	fs.StringVar(&indexes, "indexes", string(loadtest.IndexesFull),
		"schema condition: full, correctness-only")
	fs.DurationVar(&opts.cfg.WorkDuration, "work", 0, "simulated per-job work time; zero isolates the database path")
	fs.DurationVar(&opts.cfg.WorkJitter, "jitter", 0, "spread applied to the per-job work time")
	fs.DurationVar(&opts.cfg.Lease, "lease", 0,
		"runner lease duration (default: derived from -work; set it below the work time to exercise renewal)")
	fs.StringVar(&fault, "fault", faultNone,
		"fault to inject, as name[@fraction][:duration]; one of "+strings.Join(faultNames(), ", "))
	fs.DurationVar(&opts.cfg.SampleInterval, "sample-interval", time.Second, "storage sampling cadence")
	fs.DurationVar(&opts.cfg.Timeout, "timeout", 30*time.Minute, "hard bound on the whole run")
	fs.StringVar(&opts.cfg.Queue, "queue", "", "queue to enqueue onto (default: the harness default)")
	fs.StringVar(&opts.cfg.ExecutorClass, "executor-class", "",
		"executor class the runners serve (default: the harness default)")
	fs.StringVar(&opts.out, "out", "", "write the JSON report here (default: stdout)")
	fs.BoolVar(&opts.quiet, "quiet", false, "suppress the human-readable summary")

	fs.DurationVar(&opts.cfg.Duration, "duration", 0,
		"steady mix only: hold a constant population for this long instead of draining it "+
			"(must be below -timeout)")
	fs.DurationVar(&opts.cfg.RetentionMaxAge, "retention", 0,
		"enable the scheduler's retention sweep with this window (zero disables it)")
	fs.DurationVar(&opts.cfg.RetentionInterval, "retention-interval", 0,
		"cadence of the retention sweep (default: the runtime's own)")
	fs.IntVar(&opts.cfg.RetentionBatchSize, "retention-batch", 0,
		"jobs deleted per retention transaction (default: the runtime's own; never unbounded)")
	fs.IntVar(&opts.cfg.SweepBatchSize, "sweep-batch", 0,
		"expired leases reclaimed per sweep transaction (default: the runtime's own; never unbounded)")
	fs.BoolVar(&tuned, "storage-tuning", false,
		"apply the tuned fillfactor and autovacuum settings to jobs")
	fs.IntVar(&opts.cfg.TerminalSeed, "terminal-seed", 0,
		"seed this many already-finalized jobs so retention has a backlog to prune")
	fs.DurationVar(&opts.cfg.TerminalSeedAge, "terminal-seed-age", 0,
		"how far back to date the seeded terminal jobs (default: 90 days)")

	fs.Float64Var(&opts.cfg.FailFraction, "fail-fraction", 0,
		"fan-out mix: share of children that fail transiently, in [0,1); seeds the cohort under one parent")
	fs.IntVar(&opts.cfg.MaxAttempts, "max-attempts", 0,
		"seeded jobs' retry budget (0 selects the runtime default; a replay run defaults it small)")
	fs.BoolVar(&opts.cfg.Replay, "replay", false,
		"replay the discarded children after the initial drain and assert the cohort re-converges")
	fs.DurationVar(&opts.cfg.ReplayStagger, "replay-stagger", 0,
		"spread the replayed cohort's scheduled_at across this window (zero replays them at once)")
	fs.IntVar(&opts.cfg.ReplayBudget, "replay-budget", 0,
		"attempts the replay restores per job (0 selects a small default)")

	fs.StringVar(&limiter, "limiter", string(loadtest.LimiterNone),
		"pre-claim admission gate: none, token-bucket, db")
	fs.IntVar(&opts.cfg.Rate, "rate", 0, "limiter rate in operations/second (0 disables the rate cap)")
	fs.IntVar(&opts.cfg.Burst, "burst", 0, "limiter burst capacity (0 defaults to the rate)")
	fs.IntVar(&opts.cfg.MaxConcurrent, "max-concurrent", 0,
		"limiter concurrency ceiling (0 disables the concurrency cap)")
	fs.IntVar(&opts.cfg.WorkerSnooze, "worker-snooze", 0,
		"claim-then-snooze baseline: hold worker completions to this many/second (mutually exclusive with -limiter)")

	if err := fs.Parse(args); err != nil {
		return options{}, fmt.Errorf("scenario: %w", err)
	}
	if fs.NArg() > 0 {
		return options{}, fmt.Errorf("scenario: unexpected argument %q", fs.Arg(0))
	}

	opts.cfg.Mix = loadtest.Workload(mix)
	opts.cfg.Indexes = loadtest.IndexCondition(indexes)
	opts.cfg.Limiter = loadtest.LimiterKind(limiter)
	// A bool flag rather than a string enum: there are exactly two conditions and
	// the tuned one is the opt-in, so `-storage-tuning` reads as what it does.
	// The Config keeps the enum, because that is what the report records.
	opts.cfg.Storage = loadtest.StorageDefault
	if tuned {
		opts.cfg.Storage = loadtest.StorageTuned
	}

	// The enum checks live here rather than being left to the harness so a typo
	// costs a message instead of a provisioned schema, and so the message names
	// the flag the operator typed.
	if !opts.cfg.Mix.Valid() {
		return options{}, fmt.Errorf(
			"scenario: -mix %q is not one of enqueue, drain, steady, fan-out, barrier, mixed-speed", mix,
		)
	}
	if !opts.cfg.Indexes.Valid() {
		return options{}, fmt.Errorf("scenario: -indexes %q is not one of full, correctness-only", indexes)
	}
	if !opts.cfg.Limiter.Valid() {
		return options{}, fmt.Errorf("scenario: -limiter %q is not one of none, token-bucket, db", limiter)
	}
	// The fault is built here, not in the harness, for the same reason: an
	// unknown name costs a message rather than a provisioned schema, and the
	// message names the flag the operator typed.
	injected, err := parseFault(fault)
	if err != nil {
		return options{}, err
	}
	// Only assign a non-nil fault: Config.Faults is an interface, so storing a
	// typed nil would make the harness's `Faults != nil` check true for a run
	// that asked for no fault.
	if injected != nil {
		opts.cfg.Faults = injected
	}
	if opts.cfg.DSN == "" {
		return options{}, fmt.Errorf("scenario: no target: pass -dsn or set %s", dsnEnv)
	}
	return opts, nil
}
