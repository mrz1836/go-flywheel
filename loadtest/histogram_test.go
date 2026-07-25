//go:build loadtest

package loadtest

import (
	"math"
	"math/rand/v2"
	"slices"
	"sync"
	"testing"
	"time"
)

// TestHistogramGeometry pins the shape the report publishes, so a change to the
// bucketing cannot silently invalidate every committed HistogramSpec.
func TestHistogramGeometry(t *testing.T) {
	t.Parallel()

	spec := histogramSpec()
	// 83 is written out rather than derived, so the published shape has an
	// independent oracle: a change to the octave count that also changed the
	// derivation would otherwise agree with itself.
	if spec.Buckets != 83 {
		t.Errorf("Buckets = %d, want 83 (27 octaves × 3, plus underflow and overflow)", spec.Buckets)
	}
	if spec.Buckets != histBuckets {
		t.Errorf("the published bucket count %d disagrees with the histogram's own %d",
			spec.Buckets, histBuckets)
	}
	if spec.SubBucketsPerOctave != 3 {
		t.Errorf("SubBucketsPerOctave = %d, want 3", spec.SubBucketsPerOctave)
	}
	if spec.MinExponent != 10 || spec.MaxExponent != 37 {
		t.Errorf("range = 2^%d..2^%d, want 2^10..2^37", spec.MinExponent, spec.MaxExponent)
	}
	if spec.MaxRelativeError <= 0 || spec.MaxRelativeError >= 1 {
		t.Errorf("MaxRelativeError = %v, which is not a usable error bar", spec.MaxRelativeError)
	}
}

// TestBucketsPartitionTheRange proves the bucketing is a partition: every bucket
// is non-empty and contiguous with its neighbours, and every value maps to the
// bucket whose bounds contain it.
//
// A gap would silently drop observations; an overlap would double-count them.
// Either makes every quantile wrong in a way no amount of sampling reveals.
func TestBucketsPartitionTheRange(t *testing.T) {
	t.Parallel()

	for i := range histBuckets {
		lo, hi := bucketBounds(i)
		if lo >= hi {
			t.Fatalf("bucket %d is empty: [%d, %d)", i, lo, hi)
		}
		if i > 0 {
			_, prevHi := bucketBounds(i - 1)
			if prevHi != lo {
				t.Fatalf("gap or overlap between buckets %d and %d: %d vs %d", i-1, i, prevHi, lo)
			}
		}
		// The bounds and the assignment must agree, or a value lands in a bucket
		// that does not claim it.
		if got := bucketOf(lo); got != i {
			t.Errorf("bucketOf(%d) = %d, want %d (its own lower bound)", lo, got, i)
		}
		if i < histOverflow {
			if got := bucketOf(hi - 1); got != i {
				t.Errorf("bucketOf(%d) = %d, want %d (its own upper bound)", hi-1, got, i)
			}
		}
	}

	if lo, _ := bucketBounds(0); lo != 0 {
		t.Errorf("the underflow bucket must start at zero, got %d", lo)
	}
	if _, hi := bucketBounds(histOverflow); hi != math.MaxInt64 {
		t.Error("the overflow bucket must be unbounded above")
	}
}

// TestBucketWidthMatchesThePublishedError derives the error bar from the
// geometry rather than trusting the constant. A percentile with no error bar is
// a claim; an error bar nobody checked is the same claim with decoration.
func TestBucketWidthMatchesThePublishedError(t *testing.T) {
	t.Parallel()

	for i := 1; i < histOverflow; i++ {
		lo, hi := bucketBounds(i)
		est := float64(bucketEstimate(i))
		// The estimate must sit inside its own bucket, or the clamp is doing all
		// the work and the bound means nothing.
		if est < float64(lo) || est >= float64(hi) {
			t.Fatalf("bucket %d estimate %v is outside [%d, %d)", i, est, lo, hi)
		}
	}

	// The independent oracle: the analytic bound for an octave split three ways
	// is sqrt(4/3) - 1. The published number is measured from the integer
	// buckets, so it may sit a hair above that, but not meaningfully above — a
	// bound far larger than the geometry is a way of never being wrong.
	analytic := math.Sqrt(4.0/3.0) - 1
	if histMaxRelativeError < analytic {
		t.Fatalf("the published bound %.6f is below the analytic %.6f: it cannot hold",
			histMaxRelativeError, analytic)
	}
	if histMaxRelativeError > analytic+0.001 {
		t.Fatalf("the published bound %.6f is padded well past the analytic %.6f",
			histMaxRelativeError, analytic)
	}
}

// TestQuantilesAreWithinThePublishedError is the property test the error bar
// exists for: over 100,000 durations spanning five orders of magnitude, every
// reported quantile is within the published relative error of the exact one.
func TestQuantilesAreWithinThePublishedError(t *testing.T) {
	t.Parallel()

	const samples = 100_000
	rng := rand.New(rand.NewPCG(11, 22)) //nolint:gosec // reproducibility, not security

	rec := newRecorder(4)
	exact := make([]time.Duration, samples)
	for i := range samples {
		// Log-uniform across [2 µs, 200 ms], which is the range a claim and a
		// finalize actually span, and comfortably inside the histogram's.
		exponent := 11.0 + rng.Float64()*(27.5-11.0)
		d := time.Duration(math.Pow(2, exponent))
		exact[i] = d
		rec.record(i%4, d)
	}
	slices.Sort(exact)

	got := rec.latency()
	if got.Count != samples {
		t.Fatalf("Count = %d, want %d — the count must be exact", got.Count, samples)
	}
	if got.Min != exact[0] {
		t.Errorf("Min = %v, want %v — the minimum must be exact", got.Min, exact[0])
	}
	if got.Max != exact[samples-1] {
		t.Errorf("Max = %v, want %v — the maximum must be exact", got.Max, exact[samples-1])
	}

	for _, tc := range []struct {
		name string
		q    float64
		got  time.Duration
	}{
		{"p50", 0.50, got.P50},
		{"p95", 0.95, got.P95},
		{"p99", 0.99, got.P99},
	} {
		want := exact[int(math.Ceil(tc.q*samples))-1]
		relErr := math.Abs(float64(tc.got-want)) / float64(want)
		if relErr > histMaxRelativeError {
			t.Errorf("%s = %v, exact %v: relative error %.4f exceeds the published %.4f",
				tc.name, tc.got, want, relErr, histMaxRelativeError)
		}
	}

	// The mean is exact to integer division.
	var total time.Duration
	for _, d := range exact {
		total += d
	}
	if want := total / samples; got.Mean != want {
		t.Errorf("Mean = %v, want %v — the mean must be exact", got.Mean, want)
	}
}

// TestSmallSamplesAreExact covers the case the clamp exists for. A distribution
// that fits inside one bucket would otherwise be reported as that bucket's
// midpoint, so a run with three observations would publish a percentile no
// observation had.
func TestSmallSamplesAreExact(t *testing.T) {
	t.Parallel()

	rec := newRecorder(1)
	rec.record(0, 1234*time.Microsecond)

	got := rec.latency()
	for name, v := range map[string]time.Duration{
		"Min": got.Min, "P50": got.P50, "P95": got.P95, "P99": got.P99, "Max": got.Max, "Mean": got.Mean,
	} {
		if v != 1234*time.Microsecond {
			t.Errorf("%s = %v, want the single observed value 1.234ms", name, v)
		}
	}
	if got.Count != 1 {
		t.Errorf("Count = %d, want 1", got.Count)
	}
}

// TestEmptyRecorderReportsNothing proves an unobserved distribution is zero
// rather than a plausible-looking number. Combined with the report's omission of
// zero-Count latencies, that is what keeps "never measured" from reading as
// "instant".
func TestEmptyRecorderReportsNothing(t *testing.T) {
	t.Parallel()

	if got := newRecorder(4).latency(); got != (Latency{}) {
		t.Fatalf("an unobserved recorder reported %+v", got)
	}
}

// TestQuantilesAreMonotone guards an ordering that a bucketing bug could break
// without changing any single value enough to fail the error bound.
func TestQuantilesAreMonotone(t *testing.T) {
	t.Parallel()

	rng := rand.New(rand.NewPCG(5, 6)) //nolint:gosec // reproducibility, not security
	rec := newRecorder(2)
	for i := range 10_000 {
		rec.record(i%2, time.Duration(rng.Int64N(int64(500*time.Millisecond))))
	}

	got := rec.latency()
	if got.Min > got.P50 || got.P50 > got.P95 || got.P95 > got.P99 || got.P99 > got.Max {
		t.Fatalf("quantiles are not monotone: %+v", got)
	}
}

// TestRecorderIsConcurrencySafe runs the recorder the way a runner does: several
// worker goroutines writing one shard at once. Shards are per-runner, not
// per-goroutine, so this is the real access pattern rather than a stress test.
func TestRecorderIsConcurrencySafe(t *testing.T) {
	t.Parallel()

	rec := newRecorder(2)
	var wg sync.WaitGroup
	for g := range 8 {
		wg.Go(func() {
			for i := range 1000 {
				rec.record(g%2, time.Duration(i+1)*time.Microsecond)
			}
		})
	}
	wg.Wait()

	got := rec.latency()
	if got.Count != 8000 {
		t.Errorf("Count = %d, want 8000 — a lost update means every quantile is wrong", got.Count)
	}
	if got.Min != time.Microsecond {
		t.Errorf("Min = %v, want 1µs", got.Min)
	}
	if got.Max != 1000*time.Microsecond {
		t.Errorf("Max = %v, want 1ms", got.Max)
	}
}

// TestNegativeDurationsAreDropped covers the clock-went-backwards case. Folding
// a negative into the distribution would silently pull the reported minimum
// below anything that happened.
func TestNegativeDurationsAreDropped(t *testing.T) {
	t.Parallel()

	rec := newRecorder(1)
	rec.record(0, -time.Second)
	rec.record(0, time.Millisecond)

	got := rec.latency()
	if got.Count != 1 || got.Min != time.Millisecond {
		t.Fatalf("got %+v, want a single 1ms observation", got)
	}
}

// TestOutOfRangeValuesAreCountedNotDropped proves the unbounded buckets still
// account for their observations. A dropped outlier would make the count
// disagree with the sample and quietly improve every percentile.
func TestOutOfRangeValuesAreCountedNotDropped(t *testing.T) {
	t.Parallel()

	rec := newRecorder(1)
	rec.record(0, 100*time.Nanosecond) // under 2^10 ns
	rec.record(0, 300*time.Second)     // over 2^37 ns
	rec.record(0, 10*time.Millisecond) // in range

	m := rec.merge()
	if m.n != 3 {
		t.Fatalf("n = %d, want 3", m.n)
	}
	if m.under != 1 || m.over != 1 {
		t.Fatalf("under = %d, over = %d, want 1 and 1 — a report cannot disclose what it does not count",
			m.under, m.over)
	}
	got := rec.latency()
	if got.Min != 100*time.Nanosecond || got.Max != 300*time.Second {
		t.Errorf("Min/Max must stay exact even out of range, got %v/%v", got.Min, got.Max)
	}
}
