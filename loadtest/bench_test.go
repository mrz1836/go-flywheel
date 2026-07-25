//go:build loadtest

package loadtest

import (
	"context"
	"testing"
	"time"
)

// benchJobs is the depth every benchmark here runs at.
const benchJobs = 100_000

// benchDSN resolves the target for a benchmark, or skips.
//
// The order is deliberate: short mode first, then the harness's own variable.
// There is no fallback to FLYWHEEL_TEST_DATABASE_URL, and that omission is the
// point — the integration workflow sets that one, and a 100k benchmark that
// started itself there would run for minutes inside a job budgeted in seconds.
func benchDSN(b *testing.B) string {
	b.Helper()
	if testing.Short() {
		b.Skip("skipping the load harness in short mode")
	}
	dsn := envOrEmpty(databaseURLEnv)
	if dsn == "" {
		b.Skipf("%s is not set; skipping the load benchmarks", databaseURLEnv)
	}
	return dsn
}

// baselineConfig is the shape every benchmark derives from.
//
// One function owns it so the benchmarks differ only where they mean to. In
// particular BenchmarkClaimNoPerfIndexes100k mutates a single field of this
// config, which makes it structurally impossible for the index-condition
// comparison to differ in more than the index condition.
func baselineConfig(dsn string) Config {
	return Config{
		DSN:     dsn,
		Jobs:    benchJobs,
		Seed:    1,
		Runners: 4,
		Workers: 8,
		Mix:     WorkloadDrainOnly,
		Indexes: IndexesFull,
		// A no-op worker isolates the database path: with no simulated work, the
		// reported throughput is the runtime's own ceiling rather than a
		// measurement of how long the harness chose to sleep.
		WorkDuration:   0,
		SampleInterval: time.Second,
		Timeout:        30 * time.Minute,
	}
}

// runBenchmark drives cfg for b.N iterations, timing only the drive phase.
//
// StopTimer and StartTimer bracket provisioning and seeding on every iteration.
// b.ResetTimer would not do: it only discards setup that ran once before the
// loop, and here the setup — a fresh schema and 100k rows — runs inside it and
// is most of the wall clock.
//
// The reported metrics are throughput and the two percentiles, because ns/op for
// a whole 100k run is a number benchstat can compare but nobody can interpret.
func runBenchmark(b *testing.B, cfg Config) {
	b.Helper()

	var (
		totalJobs   float64
		totalTime   float64
		claimP99    float64
		finalizeP99 float64
	)

	b.ResetTimer()
	for range b.N {
		b.StopTimer()
		h, err := newHarness(context.Background(), cfg)
		if h == nil {
			b.Fatalf("newHarness: %v", err)
		}
		if err != nil {
			_ = h.Close(context.Background())
			b.Fatalf("newHarness: %v", err)
		}
		p, specs := h.prepareWorkload()
		if _, err := h.insert(context.Background(), p, specs); err != nil {
			_ = h.Close(context.Background())
			b.Fatalf("seed: %v", err)
		}
		b.StartTimer()

		start := time.Now()
		driveErr := h.drive(context.Background())
		elapsed := time.Since(start)

		b.StopTimer()
		if driveErr != nil {
			_ = h.Close(context.Background())
			b.Fatalf("drive: %v", driveErr)
		}
		totalJobs += float64(p.TotalJobs())
		totalTime += elapsed.Seconds()
		claimP99 += float64(h.timings.claim.latency().P99.Nanoseconds())
		finalizeP99 += float64(h.timings.finalize.latency().P99.Nanoseconds())
		if err := h.Close(context.Background()); err != nil {
			b.Fatalf("close: %v", err)
		}
		b.StartTimer()
	}
	b.StopTimer()

	if totalTime > 0 {
		b.ReportMetric(totalJobs/totalTime, "jobs/s")
	}
	if b.N > 0 {
		b.ReportMetric(claimP99/float64(b.N), "claim-p99-ns")
		b.ReportMetric(finalizeP99/float64(b.N), "finalize-p99-ns")
	}
}

// BenchmarkClaim100k drains 100,000 pre-seeded jobs with the full production
// index set. It is the baseline every other number here is compared against.
func BenchmarkClaim100k(b *testing.B) {
	runBenchmark(b, baselineConfig(benchDSN(b)))
}

// BenchmarkClaimNoPerfIndexes100k is the same run with only the
// correctness-bearing indexes installed, which is what quantifies the
// performance indexes.
//
// It derives its config from BenchmarkClaim100k's and mutates exactly one field.
// That is not tidiness: two hand-written configs would eventually drift in a
// second field, and the delta between them would then measure something nobody
// named.
func BenchmarkClaimNoPerfIndexes100k(b *testing.B) {
	cfg := baselineConfig(benchDSN(b))
	cfg.Indexes = IndexesCorrectness
	runBenchmark(b, cfg)
}

// BenchmarkFinalize100k drains the same depth with a small per-job work
// duration, so finalize is measured against a dispatch loop that is doing
// something rather than one spinning as fast as the claim allows.
func BenchmarkFinalize100k(b *testing.B) {
	cfg := baselineConfig(benchDSN(b))
	cfg.WorkDuration = time.Millisecond
	runBenchmark(b, cfg)
}

// BenchmarkEnqueue100k measures the producer API: 100,000 single-row inserts
// through Enqueue, with nothing draining them.
func BenchmarkEnqueue100k(b *testing.B) {
	cfg := baselineConfig(benchDSN(b))
	cfg.Mix = WorkloadEnqueueOnly
	runEnqueueBenchmark(b, cfg)
}

// runEnqueueBenchmark times the seed rather than the drive, since for this mix
// the insert is the measurement.
func runEnqueueBenchmark(b *testing.B, cfg Config) {
	b.Helper()

	var totalJobs, totalTime float64

	b.ResetTimer()
	for range b.N {
		b.StopTimer()
		h, err := newHarness(context.Background(), cfg)
		if h == nil {
			b.Fatalf("newHarness: %v", err)
		}
		if err != nil {
			_ = h.Close(context.Background())
			b.Fatalf("newHarness: %v", err)
		}
		p, specs := h.prepareWorkload()
		b.StartTimer()

		elapsed, seedErr := h.insert(context.Background(), p, specs)

		b.StopTimer()
		if seedErr != nil {
			_ = h.Close(context.Background())
			b.Fatalf("seed: %v", seedErr)
		}
		totalJobs += float64(len(specs))
		totalTime += elapsed.Seconds()
		if err := h.Close(context.Background()); err != nil {
			b.Fatalf("close: %v", err)
		}
		b.StartTimer()
	}
	b.StopTimer()

	if totalTime > 0 {
		b.ReportMetric(totalJobs/totalTime, "jobs/s")
	}
}

// BenchmarkSweep100k measures a sweep that reclaims nothing.
//
// That is deliberate and the doc comment has to say so, because "sweep with
// 100,000 rows present and no expired leases" sounds like a test that does
// nothing. It is the opposite: a no-op sweep is the fixed cost a scheduler pays
// forever, on every interval, for the entire life of a deployment — and it is
// exactly where jobs_running_leased earns its keep, because without that index
// the sweep's "find expired leases" predicate scans the whole table to find
// nothing.
//
// Read it as steady-state cost, never as reclaim throughput.
func BenchmarkSweep100k(b *testing.B) {
	cfg := baselineConfig(benchDSN(b))

	h, err := newHarness(context.Background(), cfg)
	if h == nil {
		b.Fatalf("newHarness: %v", err)
	}
	defer func() {
		if closeErr := h.Close(context.Background()); closeErr != nil {
			b.Errorf("close: %v", closeErr)
		}
	}()
	if err != nil {
		b.Fatalf("newHarness: %v", err)
	}

	p, specs := h.prepareWorkload()
	if _, err := h.insert(context.Background(), p, specs); err != nil {
		b.Fatalf("seed: %v", err)
	}

	// Every seeded job is available, not running, so no lease has expired and
	// every sweep reclaims zero.
	sweeper := newTimingDriver(h.inner, h.timings, 0)

	b.ResetTimer()
	for range b.N {
		reclaimed, err := sweeper.Sweep(context.Background(), time.Now())
		if err != nil {
			b.Fatalf("sweep: %v", err)
		}
		if reclaimed != 0 {
			b.Fatalf("this benchmark measures the no-op sweep; it reclaimed %d", reclaimed)
		}
	}
	b.StopTimer()

	b.ReportMetric(float64(h.timings.sweep.latency().P99.Nanoseconds()), "sweep-p99-ns")
}
