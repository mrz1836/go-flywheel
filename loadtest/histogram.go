//go:build loadtest

package loadtest

import (
	"math"
	"math/bits"
	"sync/atomic"
	"time"
)

// Histogram geometry.
//
// The bucketing is integer base-2 with a fixed number of linear sub-buckets per
// octave. Two properties drive the choice, and both are about not perturbing the
// thing being measured:
//
//   - No math.Log10 and no floating-point arithmetic on the hot path. Bucket
//     assignment is a bit-length and a shift, so recording an observation costs a
//     few nanoseconds against operations measured in microseconds.
//   - No dependence on floating-point rounding. A bucket boundary decided by
//     float comparison can land differently across architectures, which would
//     make two reports from two machines incomparable for reasons that have
//     nothing to do with the runtime.
//
// The covered range is 2^10 ns (about 1 µs) to 2^37 ns (about 137 s). Below it
// is faster than any database round trip; above it, a single operation has taken
// longer than most runs.
const (
	histSubBuckets = 3
	histMinExp     = 10
	histMaxExp     = 37
	histOctaves    = histMaxExp - histMinExp
	// histBuckets counts the in-range buckets plus one underflow and one
	// overflow: 27 octaves × 3 + 2 = 83.
	histBuckets   = histOctaves*histSubBuckets + 2
	histUnderflow = 0
	histOverflow  = histBuckets - 1
)

// histMaxRelativeError is the worst-case relative error on a reported quantile.
//
// Analytically it is sqrt(4/3) − 1, about 15.5%: sub-dividing an octave into
// three equal parts gives widths [1, 4/3), [4/3, 5/3), [5/3, 2) of the octave's
// base, so the widest bucket has an upper-to-lower ratio of 4/3, and reporting
// the geometric mean puts the estimate within sqrt(4/3) of anything in it.
//
// But the published number is measured over the actual buckets rather than taken
// from that formula, because the buckets are integers and the analytic value is
// not: the boundaries round, the geometric means round, and the true worst case
// sits a hair above the ideal. Publishing the ideal would be publishing a bound
// the implementation does not quite meet — which is the exact failure this whole
// file exists to avoid. The analytic value survives as the test's independent
// oracle instead.
//
// Count, Min, Max, and Mean are exact and are not subject to this, and quantiles
// clamp into [Min, Max], so a small sample is reported exactly. It applies to
// quantiles drawn from the in-range buckets: a value outside [2^10, 2^37) lands
// in the underflow or overflow bucket, and a run in which that happened says so
// in Notes rather than pretending to the bound.
var histMaxRelativeError = measureMaxRelativeError() //nolint:gochecknoglobals // measured once from the geometry

// measureMaxRelativeError walks every in-range bucket and returns the largest
// relative error its estimate can have against a value the bucket holds.
//
// For a bucket [lo, hi) reporting est, a true value v costs |est−v|/v, which is
// maximized either at v = lo — giving (est−lo)/lo — or as v approaches hi,
// giving (hi−est)/hi.
func measureMaxRelativeError() float64 {
	worst := 0.0
	for i := 1; i < histOverflow; i++ {
		lo, hi := bucketBounds(i)
		est := float64(bucketEstimate(i))
		worst = max(worst, (est-float64(lo))/float64(lo), (float64(hi)-est)/float64(hi))
	}
	return worst
}

// cacheLine is the padding between shards.
const cacheLine = 64

// histShard is one lock-free histogram shard.
//
// Shards are handed out at construction — one per runner, plus one for the
// sweeper — rather than selected per observation, so the hot path costs an
// index it already has instead of a lookup.
//
// They are emphatically not per-goroutine, and the distinction is worth being
// precise about: a runner dispatches its batch across Workers goroutines, all of
// which finalize into the same shard. Pinning a shard to a goroutine or a P
// would need runtime.procPin, which is not available outside the runtime, so
// claiming per-goroutine isolation here would be false. The counters are atomic
// for exactly that reason.
type histShard struct {
	counts [histBuckets]atomic.Uint64
	n      atomic.Uint64
	sum    atomic.Uint64
	minNS  atomic.Int64
	maxNS  atomic.Int64
	// Padding keeps one shard's counters off the next shard's cache lines.
	_ [cacheLine]byte
}

// recorder is a sharded duration histogram with exact count, sum, min, and max.
type recorder struct {
	shards []histShard
}

// newRecorder builds a recorder with the given shard count.
func newRecorder(shards int) *recorder {
	if shards < 1 {
		shards = 1
	}
	r := &recorder{shards: make([]histShard, shards)}
	for i := range r.shards {
		r.shards[i].minNS.Store(math.MaxInt64)
		r.shards[i].maxNS.Store(math.MinInt64)
	}
	return r
}

// record adds one observation to the given shard.
//
// A negative duration is dropped rather than recorded as zero: it can only come
// from a clock that went backwards, and folding it into the distribution would
// silently pull the reported minimum down.
func (r *recorder) record(shard int, d time.Duration) {
	if d < 0 {
		return
	}
	s := &r.shards[shard%len(r.shards)]
	ns := d.Nanoseconds()

	s.counts[bucketOf(ns)].Add(1)
	s.n.Add(1)
	s.sum.Add(uint64(ns)) //nolint:gosec // non-negative by the guard above

	for {
		cur := s.minNS.Load()
		if ns >= cur || s.minNS.CompareAndSwap(cur, ns) {
			break
		}
	}
	for {
		cur := s.maxNS.Load()
		if ns <= cur || s.maxNS.CompareAndSwap(cur, ns) {
			break
		}
	}
}

// bucketOf maps a nanosecond duration to its bucket index, using only integer
// arithmetic.
func bucketOf(ns int64) int {
	if ns < int64(1)<<histMinExp {
		return histUnderflow
	}
	if ns >= int64(1)<<histMaxExp {
		return histOverflow
	}
	// bits.Len gives 1 + floor(log2(ns)), so e is the octave.
	e := bits.Len64(uint64(ns)) - 1 //nolint:gosec // positive by the guards above
	// Position within the octave, scaled to [0, histSubBuckets).
	sub := int((ns - (int64(1) << e)) * histSubBuckets >> e)
	return 1 + (e-histMinExp)*histSubBuckets + sub
}

// bucketBounds returns the half-open nanosecond range a bucket covers. The
// overflow bucket's upper bound is reported as math.MaxInt64, since it has none.
func bucketBounds(i int) (lo, hi int64) {
	switch {
	case i <= histUnderflow:
		return 0, int64(1) << histMinExp
	case i >= histOverflow:
		return int64(1) << histMaxExp, math.MaxInt64
	}
	idx := i - 1
	e := histMinExp + idx/histSubBuckets
	sub := int64(idx % histSubBuckets)
	base := int64(1) << e
	return base + subOffset(base, sub), base + subOffset(base, sub+1)
}

// subOffset is where sub-bucket sub begins within an octave of the given base.
//
// It rounds up, and it has to: bucketOf assigns ns to sub-bucket s exactly when
// (ns − base)·3 ≥ s·base, so the first integer in the sub-bucket is
// base + ceil(s·base/3). Truncating instead — the obvious spelling — puts the
// declared boundary one nanosecond below the real one, and every bucket's
// declared lower bound then maps to the previous bucket. That was the first
// version of this function, and the partition test caught it.
func subOffset(base, sub int64) int64 {
	return (base*sub + histSubBuckets - 1) / histSubBuckets
}

// bucketEstimate is the value a bucket reports: the geometric mean of its
// bounds, which is what makes the relative error symmetric across the bucket and
// bounded by histMaxRelativeError.
//
// The unbounded buckets report their finite edge. That is the honest answer —
// the underflow bucket knows only "below 1 µs" — and the caller clamps into the
// exactly tracked [Min, Max], which recovers the true value whenever the bucket
// holds the extremum.
func bucketEstimate(i int) int64 {
	lo, hi := bucketBounds(i)
	switch {
	case i <= histUnderflow:
		return hi - 1
	case i >= histOverflow:
		return lo
	}
	return int64(math.Round(math.Sqrt(float64(lo) * float64(hi))))
}

// merged is a recorder's shards collapsed into one distribution.
type merged struct {
	counts   [histBuckets]uint64
	n, sum   uint64
	min, max int64
	// under and over count observations outside the histogram's range, so a
	// report can disclose that its quantiles fall outside the published error
	// bound rather than quietly claiming it.
	under, over uint64
}

// merge collapses every shard. It is called after the runners have stopped, so
// it sees a settled distribution rather than a moving one.
func (r *recorder) merge() merged {
	out := merged{min: math.MaxInt64, max: math.MinInt64}
	for i := range r.shards {
		s := &r.shards[i]
		for b := range s.counts {
			out.counts[b] += s.counts[b].Load()
		}
		out.n += s.n.Load()
		out.sum += s.sum.Load()
		if v := s.minNS.Load(); v < out.min {
			out.min = v
		}
		if v := s.maxNS.Load(); v > out.max {
			out.max = v
		}
	}
	out.under = out.counts[histUnderflow]
	out.over = out.counts[histOverflow]
	return out
}

// latency renders the recorder as a reportable distribution.
func (r *recorder) latency() Latency {
	m := r.merge()
	if m.n == 0 {
		return Latency{}
	}
	return Latency{
		Count: int64(m.n), //nolint:gosec // bounded by the observation count
		Min:   time.Duration(m.min),
		P50:   m.quantile(0.50),
		P95:   m.quantile(0.95),
		P99:   m.quantile(0.99),
		Max:   time.Duration(m.max),
		Mean:  time.Duration(m.sum / m.n), //nolint:gosec // n is nonzero here
	}
}

// quantile reports the q-th quantile.
//
// The walk stops at the bucket holding the rank-k observation, which is the
// bucket that contains the true quantile — bucket counts partition the sorted
// order, so this is exact about *which* bucket, and approximate only about where
// in it. That is what lets histMaxRelativeError be a bound rather than an
// estimate.
//
// The result is clamped into the exactly tracked [Min, Max], so a distribution
// small enough to sit inside one bucket is reported exactly rather than as that
// bucket's midpoint.
func (m merged) quantile(q float64) time.Duration {
	if m.n == 0 {
		return 0
	}
	rank := uint64(math.Ceil(q * float64(m.n)))
	if rank < 1 {
		rank = 1
	}

	var cumulative uint64
	for i := range m.counts {
		cumulative += m.counts[i]
		if cumulative >= rank {
			v := bucketEstimate(i)
			if v < m.min {
				v = m.min
			}
			if v > m.max {
				v = m.max
			}
			return time.Duration(v)
		}
	}
	return time.Duration(m.max)
}

// histogramSpec describes the bucketing, for publication alongside every
// percentile it produced.
func histogramSpec() HistogramSpec {
	return HistogramSpec{
		SubBucketsPerOctave: histSubBuckets,
		MinExponent:         histMinExp,
		MaxExponent:         histMaxExp,
		Buckets:             histBuckets,
		MaxRelativeError:    histMaxRelativeError,
	}
}
