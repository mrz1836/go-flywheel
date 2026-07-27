//go:build loadtest

// Command scenario runs one configured load, soak, or chaos scenario against a
// real PostgreSQL server and writes a JSON report.
//
// It is the long-form counterpart to the Benchmark functions: those exist for
// statistical comparison across a change, this exists for the runs that take
// minutes to hours and produce a committed artifact.
//
//	go run -tags=loadtest ./loadtest/cmd/scenario \
//	  -dsn "$FLYWHEEL_LOADTEST_DATABASE_URL" -jobs 100000 -runners 4 -workers 8 \
//	  -mix drain -indexes full -out docs/benchmarks/baseline-100k.json
//
// The report it writes carries no credentials: the DSN is redacted to host,
// port, and database, which is what makes these files safe to commit.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/mrz1836/go-flywheel/loadtest"
)

func main() {
	if err := run(os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// run is main's testable body.
func run(args []string, stdout, stderr *os.File) error {
	opts, err := parseFlags(args, stderr)
	if err != nil {
		return err
	}

	// Ctrl-C ends the run rather than killing the process, so an interrupted
	// scenario still drops its schema and still writes what it measured. A soak
	// run that leaked a 100k-row schema on every interrupt would fill the target
	// database within a day.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	report, runErr := loadtest.Run(ctx, opts.cfg)

	// The report is written even when the run failed. A run that timed out
	// mid-drain produced real numbers up to that point, and discarding them would
	// make a partial result indistinguishable from no result.
	if writeErr := writeReport(report, opts.out, stdout); writeErr != nil {
		return errors.Join(runErr, writeErr)
	}
	if !opts.quiet {
		printSummary(stderr, report)
	}
	return runErr
}

// writeReport renders the report as JSON to the chosen destination.
func writeReport(report loadtest.Report, path string, stdout *os.File) error {
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return fmt.Errorf("scenario: render report: %w", err)
	}
	data = append(data, '\n')

	if path == "" {
		if _, err := stdout.Write(data); err != nil {
			return fmt.Errorf("scenario: write report: %w", err)
		}
		return nil
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		return fmt.Errorf("scenario: write %s: %w", path, err)
	}
	return nil
}

// printSummary writes a human-readable digest to stderr, leaving stdout for the
// JSON so the command composes in a pipeline.
//
// Write errors are deliberately ignored: this is a progress summary on a stream
// the operator is reading, the report itself has already been written, and
// failing a completed run because its console output was truncated would throw
// away the measurement.
func printSummary(w *os.File, r loadtest.Report) {
	p := func(format string, args ...any) {
		_, _ = fmt.Fprintf(w, format, args...)
	}

	p("\nmix=%s indexes=%s jobs=%d runners=%d workers=%d seed=%d\n",
		r.Config.Mix, r.Config.Indexes, r.Config.Jobs, r.Config.Runners, r.Config.Workers, r.Config.Seed)
	p("workload digest: %s\n", r.WorkloadDigest)
	p("duration: %s   enqueue: %.0f jobs/s   drain: %.0f jobs/s\n",
		r.Duration.Round(time.Millisecond), r.EnqueueThroughput, r.DrainThroughput)
	p("slot utilization: %.1f%% of %d runners × %d workers\n",
		r.SlotUtilization*100, r.Config.Runners, r.Config.Workers)
	p("enqueued=%d drained=%d retried=%d discarded=%d superseded=%d\n",
		r.Enqueued, r.Drained, r.Retried, r.Discarded, r.Superseded)
	if r.BlockedClaims > 0 {
		p("blocked claims (refused by a fault's gate): %d\n", r.BlockedClaims)
	}

	for name, l := range map[string]loadtest.Latency{
		"claim": r.Claim, "finalize": r.Finalize, "sweep": r.Sweep,
	} {
		if l.Count == 0 {
			p("%-9s not observed\n", name)
			continue
		}
		p("%-9s n=%-8d p50=%-10s p95=%-10s p99=%-10s max=%s\n",
			name, l.Count, l.P50, l.P95, l.P99, l.Max)
	}
	p("peak RSS (harness client): %d bytes\n", r.PeakRSS)

	// The caveats print with the numbers, not in a file somewhere else. A
	// percentile whose error bar is elsewhere is a percentile without one.
	if len(r.Notes) > 0 {
		p("\nnotes:\n")
		for _, n := range r.Notes {
			p("  - %s\n", n)
		}
	}
	if len(r.Errors) > 0 {
		p("\nerrors:\n")
		for _, e := range r.Errors {
			p("  - %s\n", e)
		}
	}
}
