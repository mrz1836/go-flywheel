//go:build loadtest

// Package loadtest is the runtime's load, soak, and chaos harness. It targets a
// real PostgreSQL instance with the full production schema, seeds deterministic
// workloads at depth, and reports throughput, latency percentiles, and storage
// behavior.
//
// # Why it is behind its own build tag
//
// Every file here carries //go:build loadtest, including this one. A single
// untagged file would pull the whole directory into `go build ./...`, the
// linter, and the coverage run — which is the opposite of the point: these runs
// take minutes to hours and must never join the ordinary test matrix. With every
// file constrained out, `./...` skips the directory silently (the *build.NoGoError
// branch of the package matcher), while naming the path explicitly still errors,
// so a command that means to run the harness has to say so:
//
//	go test -tags=loadtest ./loadtest/...
//	go run  -tags=loadtest ./loadtest/cmd/scenario -dsn "$FLYWHEEL_LOADTEST_DATABASE_URL" ...
//
// The tag also buys freedom: a scenario main and a JSON report writer have no
// business in the shipped module surface, and behind the tag they are not in it.
// What the tag does *not* buy is dependency isolation — an import here is still a
// published module requirement for every consumer of the library, so the harness
// stays on the standard library, gorm, and the runtime itself.
//
// # What it measures, and why it measures it itself
//
// The harness times the [github.com/mrz1836/go-flywheel.Driver] directly: it
// wraps the driver the runners use and records the wall time of every claim,
// finalize, and sweep into its own histograms. It needs no observer, no metrics
// recorder, and nothing from the operator-facing telemetry work — those are for
// operators in production; this is for this project's numbers, and the two are
// independent by construction.
//
// # Determinism
//
// Everything that affects the workload derives from Config.Seed through
// math/rand/v2 PCG streams, and generation completes before anything concurrent
// starts. Two runs with equal Config produce byte-identical workloads and equal
// Report.WorkloadDigest values. The wall clock is used for measurement only.
package loadtest
