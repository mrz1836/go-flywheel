//go:build loadtest

// Command matrix runs the load matrix — the cross product of queue depth, worker
// count, workload mix, and index condition — writing one JSON report per cell into
// a directory. It is the batch counterpart to the scenario command: where that
// runs one configured scenario, this runs a grid of them for the benchmark report.
//
//	go run -tags=loadtest ./loadtest/cmd/matrix \
//	  -dsn "$FLYWHEEL_LOADTEST_DATABASE_URL" -out docs/benchmarks/matrix/
//
// The default grid is a representative subset (fast enough to run in one sitting);
// -full expands it to the documented 96-cell matrix. A cell that fails to build or
// run is recorded and the grid continues, so one bad combination does not discard
// the rest.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/mrz1836/go-flywheel/loadtest"
)

const dsnEnv = "FLYWHEEL_LOADTEST_DATABASE_URL"

func main() {
	if err := run(os.Args[1:], os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// gridOptions is the parsed matrix configuration.
type gridOptions struct {
	dsn      string
	out      string
	depths   []int
	workers  []int
	mixes    []string
	indexes  []string
	runners  int
	children int
	work     time.Duration
	timeout  time.Duration
	sample   time.Duration
	quiet    bool
}

// run is main's testable body.
func run(args []string, stderr *os.File) error {
	opts, err := parseFlags(args, stderr)
	if err != nil {
		return err
	}
	if opts.out == "" {
		return fmt.Errorf("matrix: -out directory is required")
	}
	if err := os.MkdirAll(opts.out, 0o750); err != nil {
		return fmt.Errorf("matrix: create out dir: %w", err)
	}

	// Ctrl-C ends the grid between cells and after the current cell's own teardown,
	// so an interrupted matrix still drops every schema it provisioned.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	logf := func(format string, a ...any) { _, _ = fmt.Fprintf(stderr, format, a...) }

	cells := opts.cells()
	logf("matrix: %d cells into %s\n", len(cells), opts.out)

	var failures int
	for i, c := range cells {
		if ctx.Err() != nil {
			logf("matrix: interrupted after %d/%d cells\n", i, len(cells))
			break
		}
		name := c.name()
		if !opts.quiet {
			logf("matrix: [%d/%d] %s ... ", i+1, len(cells), name)
		}

		report, runErr := loadtest.Run(ctx, c.config(opts))
		path := filepath.Join(opts.out, name+".json")
		if writeErr := writeReport(report, path); writeErr != nil {
			return writeErr
		}
		if runErr != nil {
			failures++
			logf("matrix: [%d/%d] %s FAILED: %v (report written)\n", i+1, len(cells), name, runErr)
			continue
		}
		if !opts.quiet {
			logf("drain=%.0f j/s claim.p99=%s\n", report.DrainThroughput, report.Claim.P99)
		}
	}

	if failures > 0 {
		return fmt.Errorf("matrix: %d of %d cells failed (reports written for inspection)", failures, len(cells))
	}
	return nil
}

// cell is one point in the grid.
type cell struct {
	depth   int
	workers int
	mix     string
	indexes string
}

// name is the cell's stable filename stem.
func (c cell) name() string {
	return fmt.Sprintf("%s_d%d_w%d_%s", c.mix, c.depth, c.workers, c.indexes)
}

// config builds the run Config for this cell from the shared grid options.
func (c cell) config(o gridOptions) loadtest.Config {
	cfg := loadtest.Config{
		DSN:            o.dsn,
		Jobs:           c.depth,
		Seed:           1,
		Runners:        o.runners,
		Workers:        c.workers,
		Mix:            loadtest.Workload(c.mix),
		Indexes:        loadtest.IndexCondition(c.indexes),
		WorkDuration:   o.work,
		Timeout:        o.timeout,
		SampleInterval: o.sample,
	}
	// The fan-out and barrier mixes seed parents that spawn children, so they need a
	// child count; the depth becomes the parent count for them.
	switch loadtest.Workload(c.mix) {
	case loadtest.WorkloadFanOut, loadtest.WorkloadBarrier:
		cfg.Children = o.children
	default:
	}
	return cfg
}

// cells expands the grid into its cross product, depth-major so a run's early
// cells are the cheap shallow ones.
func (o gridOptions) cells() []cell {
	var out []cell
	for _, depth := range o.depths {
		for _, mix := range o.mixes {
			for _, indexes := range o.indexes {
				for _, workers := range o.workers {
					out = append(out, cell{depth: depth, workers: workers, mix: mix, indexes: indexes})
				}
			}
		}
	}
	return out
}

// parseFlags parses the grid flags, applying the -full expansion.
func parseFlags(args []string, stderr *os.File) (gridOptions, error) {
	fs := flag.NewFlagSet("matrix", flag.ContinueOnError)
	fs.SetOutput(stderr)

	var (
		o                               gridOptions
		depths, workers, mixes, indexes string
		full                            bool
	)
	fs.StringVar(&o.dsn, "dsn", os.Getenv(dsnEnv), "PostgreSQL DSN (default: $"+dsnEnv+")")
	fs.StringVar(&o.out, "out", "", "directory to write one JSON report per cell (required)")
	fs.StringVar(&depths, "depths", "100000", "comma-separated queue depths")
	fs.StringVar(&workers, "workers", "8,32", "comma-separated per-runner worker counts")
	fs.StringVar(&mixes, "mixes", "enqueue,drain,steady", "comma-separated workload mixes")
	fs.StringVar(&indexes, "indexes", "full", "comma-separated index conditions (full, correctness-only)")
	fs.IntVar(&o.runners, "runners", 4, "runner loops per cell")
	fs.IntVar(&o.children, "children", 100, "children per parent for the fan-out and barrier mixes")
	fs.DurationVar(&o.work, "work", 0, "simulated per-job work time; zero isolates the database path")
	fs.DurationVar(&o.timeout, "timeout", 30*time.Minute, "hard bound on each cell")
	fs.DurationVar(&o.sample, "sample-interval", time.Second, "storage sampling cadence")
	fs.BoolVar(&o.quiet, "quiet", false, "suppress per-cell progress")
	fs.BoolVar(&full, "full", false, "expand to the full 96-cell documented matrix")

	if err := fs.Parse(args); err != nil {
		return gridOptions{}, err
	}
	if o.dsn == "" {
		return gridOptions{}, fmt.Errorf("matrix: -dsn or $%s is required", dsnEnv)
	}

	if full {
		depths, workers = "100000,500000,1000000", "8,16,32,64"
		mixes, indexes = "enqueue,drain,steady,barrier", "full,correctness-only"
	}

	var err error
	if o.depths, err = parseInts(depths); err != nil {
		return gridOptions{}, fmt.Errorf("matrix: -depths: %w", err)
	}
	if o.workers, err = parseInts(workers); err != nil {
		return gridOptions{}, fmt.Errorf("matrix: -workers: %w", err)
	}
	o.mixes = splitCSV(mixes)
	o.indexes = splitCSV(indexes)
	return o, nil
}

// parseInts parses a comma-separated list of positive integers.
func parseInts(s string) ([]int, error) {
	parts := splitCSV(s)
	out := make([]int, 0, len(parts))
	for _, p := range parts {
		n, err := strconv.Atoi(p)
		if err != nil {
			return nil, fmt.Errorf("%q is not an integer", p)
		}
		out = append(out, n)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("empty list")
	}
	return out, nil
}

// splitCSV splits a comma-separated list, trimming spaces and dropping empties.
func splitCSV(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// writeReport renders one cell's report to path.
func writeReport(report loadtest.Report, path string) error {
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return fmt.Errorf("matrix: render %s: %w", path, err)
	}
	if err := os.WriteFile(path, append(data, '\n'), 0o600); err != nil {
		return fmt.Errorf("matrix: write %s: %w", path, err)
	}
	return nil
}
