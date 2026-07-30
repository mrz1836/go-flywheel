//go:build loadtest

// Command explain characterizes the claim predicate against a real PostgreSQL
// server and writes the resulting query plans as a committed artifact.
//
// It exists because the claim's SQL is unreachable from anywhere else: it is
// built by fmt.Sprintf inside an unexported driver method, so there is no
// constant to import and no builder to call. Re-typing it into a tool would be
// the same class of mistake as a hand-copied index list — the tool would drift
// from the runtime silently, and the published plans would describe a query
// nobody runs. So this captures the statement the driver actually emits, through
// a recording GORM logger, and explains that.
//
//	go run -tags=loadtest ./loadtest/cmd/explain \
//	  -dsn "$FLYWHEEL_LOADTEST_DATABASE_URL" -jobs 1000000 -queues 3 \
//	  -out docs/benchmarks/claim-plans-1m-before.txt
//
// The artifact it writes carries no credentials: the DSN is redacted to host,
// port, and database, which is what makes these files safe to commit.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

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
	// characterization still drops its schema. A run that leaked a million-row
	// schema on every interrupt would fill the target database in an afternoon.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, opts.timeout)
	defer cancel()

	// The progress query is a single read plan, not a matrix: it has its own
	// characterization and its own artifact text, but shares the claim path's
	// write-even-on-failure and console-summary contract.
	if opts.query == "progress" {
		report, runErr := loadtest.ExplainProgress(ctx, opts.cfg)
		if writeErr := writeArtifact(report.Text(), opts.out, stdout); writeErr != nil {
			return errors.Join(runErr, writeErr)
		}
		if !opts.quiet {
			printProgressSummary(stderr, report)
		}
		return runErr
	}

	report, runErr := loadtest.ExplainClaim(ctx, opts.cfg)

	// The artifact is written even when the run failed. A matrix that died on its
	// last variant still measured every one before it, and discarding those would
	// make a partial result indistinguishable from no result.
	if writeErr := writeArtifact(report.Text(), opts.out, stdout); writeErr != nil {
		return errors.Join(runErr, writeErr)
	}
	if !opts.quiet {
		printSummary(stderr, report)
	}
	return runErr
}

// writeArtifact renders the artifact text to the chosen destination.
func writeArtifact(text, path string, stdout *os.File) error {
	data := []byte(text)

	if path == "" {
		if _, err := stdout.Write(data); err != nil {
			return fmt.Errorf("explain: write artifact: %w", err)
		}
		return nil
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		return fmt.Errorf("explain: write %s: %w", path, err)
	}
	return nil
}

// printSummary writes the finding to stderr, leaving stdout for the artifact so
// the command composes in a pipeline.
//
// It prints one line per cell and names the failure signature outright: a Sort
// above the scan, over more rows than the claim's LIMIT, is the whole reason
// this tool exists, and a reader should not have to derive it from a table.
//
// Write errors are deliberately ignored: this is a summary on a stream the
// operator is reading, the artifact itself has already been written, and failing
// a completed run because its console output was truncated would throw away the
// measurement.
func printSummary(w *os.File, r loadtest.ExplainReport) {
	p := func(format string, args ...any) {
		_, _ = fmt.Fprintf(w, format, args...)
	}

	if len(r.Cells) == 0 {
		p("\nno cells measured\n")
		return
	}
	p("\njobs=%d queues=%d limit=%d seed=%d schema=%s\n", r.Jobs, len(r.Queues), r.Limit, r.Seed, r.Schema)
	p("%-10s %-42s %12s %6s %10s\n", "cell", "scan node", "scan rows", "sort", "exec ms")

	var sorted int
	for _, cell := range r.Cells {
		s := cell.Summary
		mark := "no"
		if s.Sorted {
			mark = "YES"
			sorted++
		}
		scan := s.Scan
		if len(scan) > 42 {
			scan = scan[:41] + "…"
		}
		p("%-10s %-42s %12d %6s %10.3f\n", cell.Key(), scan, s.ScanRows, mark, s.ExecutionMS)
	}

	p("\n%d of %d cells sort above the scan", sorted, len(r.Cells))
	if sorted > 0 {
		p("; a sort over more than LIMIT=%d rows is work the plan did not need to do", r.Limit)
	}
	p("\n")

	if len(r.Notes) > 0 {
		p("\nnotes:\n")
		for _, n := range r.Notes {
			p("  - %s\n", collapseSpace(n))
		}
	}
}

// collapseSpace flattens a note onto one console line. The artifact wraps them;
// a terminal summary should not.
func collapseSpace(s string) string { return strings.Join(strings.Fields(s), " ") }

// printProgressSummary writes the progress-plan finding to stderr: one line per
// captured statement naming its top scan node, so a reader sees whether the rollup
// reached an Index Only Scan without reading the full plan.
//
// Write errors are ignored for the same reason printSummary ignores them: the
// artifact is already written, and a truncated console line must not fail a
// completed run.
func printProgressSummary(w *os.File, r loadtest.ProgressExplainReport) {
	p := func(format string, args ...any) { _, _ = fmt.Fprintf(w, format, args...) }
	p("\njobs=%d parent-children=%d schema=%s\n", r.Jobs, r.ParentChildren, r.Schema)
	for i, stmt := range r.Statements {
		scan := "(no scan node found)"
		for _, line := range stmt.Plan {
			t := strings.TrimSpace(line)
			if strings.HasPrefix(t, "->") {
				t = strings.TrimSpace(t[2:])
			}
			if strings.Contains(t, "Scan") {
				scan = t
				break
			}
		}
		if len(scan) > 90 {
			scan = scan[:89] + "…"
		}
		p("  statement %d: %s\n", i+1, scan)
	}
}
