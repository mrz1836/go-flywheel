package core

import (
	"math"
	"math/rand/v2"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestHistBoundsAreFrozen pins version 1's bucket bounds against a golden file.
// Every stored dur_hist and wait_hist is a list of counts against these bounds,
// so moving one re-means history that is already written. A deliberate change is
// a new histVersion, never an edit to version 1; regenerate the golden only for a
// new version.
func TestHistBoundsAreFrozen(t *testing.T) {
	t.Parallel()

	var b strings.Builder
	for _, v := range histBoundsV1 {
		b.WriteString(strconv.FormatInt(v, 10))
		b.WriteByte('\n')
	}
	got := b.String()
	path := filepath.Join("testdata", "hist_bounds_v1.golden")

	if os.Getenv(updateGoldenEnv) != "" {
		require.NoError(t, os.WriteFile(path, []byte(got), 0o600))
		t.Logf("updated golden fixture %s", path)
		return
	}
	want, err := os.ReadFile(path) //nolint:gosec // fixed, in-repo test fixture path
	require.NoError(t, err, "read golden fixture; regenerate with %s=1 only for a new histVersion", updateGoldenEnv)
	assert.Equal(t, string(want), got,
		"histogram bounds moved: every stored histogram is counted against them. Bump histVersion instead.")
}

// TestHistBoundsShape pins the properties the estimator relies on: strictly
// increasing integer bounds from 1 ms to exactly 24 h, no step wider than a
// quarter octave once past the rounding-collapsed bottom.
func TestHistBoundsShape(t *testing.T) {
	t.Parallel()

	require.NotEmpty(t, histBoundsV1)
	assert.EqualValues(t, 1, histBoundsV1[0])
	assert.EqualValues(t, histMaxMs, histBoundsV1[len(histBoundsV1)-1])
	assert.InDelta(t, 105, len(histBoundsV1), 10, "about 105 bounds")
	step := math.Pow(2, 0.25)
	for i := 1; i < len(histBoundsV1); i++ {
		require.Greater(t, histBoundsV1[i], histBoundsV1[i-1], "bounds strictly increase")
		if histBoundsV1[i-1] >= 16 && i < len(histBoundsV1)-1 {
			ratio := float64(histBoundsV1[i]) / float64(histBoundsV1[i-1])
			assert.InDelta(t, step, ratio, 0.03, "bound %d is a quarter-octave step", i)
		}
	}
}

// TestHistBucketPlacement proves a duration lands in the bucket whose
// inclusive upper bound is the first at or above it.
func TestHistBucketPlacement(t *testing.T) {
	t.Parallel()

	assert.Equal(t, 0, histBucket(0), "zero lands in the first bucket")
	assert.Equal(t, 0, histBucket(1))
	assert.Equal(t, 1, histBucket(2))
	for i, bound := range histBoundsV1 {
		assert.Equal(t, i, histBucket(bound), "a bound is inclusive")
		if i+1 < len(histBoundsV1) {
			assert.Equal(t, i+1, histBucket(bound+1), "one past a bound is the next bucket")
		}
	}
	assert.Equal(t, len(histBoundsV1), histBucket(histMaxMs+1), "past 24h is the overflow bucket")
}

// exactQuantile is the reference the estimator is judged against: the
// ceil(q·n)-th smallest value.
func exactQuantile(sorted []int64, q float64) int64 {
	idx := max(int(math.Ceil(q*float64(len(sorted))))-1, 0)
	return sorted[idx]
}

// lognormalSample draws n lognormal durations in ms with median e^mu.
func lognormalSample(rng *rand.Rand, n int, mu, sigma float64) []int64 {
	out := make([]int64, n)
	for i := range out {
		out[i] = int64(math.Round(math.Exp(mu + sigma*rng.NormFloat64())))
	}
	return out
}

// TestHistQuantileErrorIsSmall is the estimator's property test: across
// lognormal distributions spanning milliseconds to minutes, the estimated p50,
// p95, and p99 stay within a few percent of the exact order statistics.
func TestHistQuantileErrorIsSmall(t *testing.T) {
	t.Parallel()

	rng := rand.New(rand.NewPCG(7, 11)) //nolint:gosec // deterministic test data
	worst := 0.0
	for _, medianMs := range []float64{40, 350, 2_000, 45_000, 600_000} {
		for _, sigma := range []float64{0.2, 0.6, 1.2} {
			samples := lognormalSample(rng, 5_000, math.Log(medianMs), sigma)
			h := newHistogram()
			var maxMs int64
			for _, v := range samples {
				h.observe(v)
				maxMs = max(maxMs, v)
			}
			sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
			for _, q := range []float64{0.50, 0.95, 0.99} {
				exact := float64(exactQuantile(samples, q))
				est := float64(h.quantile(q, maxMs))
				rel := math.Abs(est-exact) / exact
				worst = math.Max(worst, rel)
				assert.LessOrEqual(t, rel, 0.06,
					"median=%vms sigma=%v q=%v: estimate %v vs exact %v", medianMs, sigma, q, est, exact)
			}
		}
	}
	t.Logf("worst relative quantile error: %.4f", worst)
}

// TestHistQuantileReadsATrueDoublingAsDoubling is the property the regression
// detector depends on: a distribution shifted by exactly 2× must read as about
// 2× at every percentile the detector compares, or the 2× threshold would be
// crossed (or missed) by the estimator's own error.
func TestHistQuantileReadsATrueDoublingAsDoubling(t *testing.T) {
	t.Parallel()

	rng := rand.New(rand.NewPCG(3, 5)) //nolint:gosec // deterministic test data
	for _, medianMs := range []float64{120, 900, 30_000} {
		base := lognormalSample(rng, 2_000, math.Log(medianMs), 0.4)
		slow := make([]int64, len(base))
		hb, hs := newHistogram(), newHistogram()
		var maxB, maxS int64
		for i, v := range base {
			slow[i] = 2 * v
			hb.observe(v)
			hs.observe(slow[i])
			maxB, maxS = max(maxB, v), max(maxS, slow[i])
		}
		for _, q := range []float64{0.50, 0.95} {
			ratio := float64(hs.quantile(q, maxS)) / float64(hb.quantile(q, maxB))
			assert.InDelta(t, 2.0, ratio, 0.12, "median=%vms q=%v: a true 2x shift reads %.3fx", medianMs, q, ratio)
		}
	}
}

// TestHistQuantileEdges covers the boundaries: an empty histogram, the clamp to
// the exact max, and the overflow bucket.
func TestHistQuantileEdges(t *testing.T) {
	t.Parallel()

	assert.Zero(t, newHistogram().quantile(0.5, 0), "an empty histogram estimates zero")

	h := newHistogram()
	h.observe(1000)
	assert.LessOrEqual(t, h.quantile(0.99, 1000), int64(1000), "never above the exact max")
	assert.Greater(t, h.quantile(0.99, 1000), int64(800))

	over := newHistogram()
	over.observe(histMaxMs + 5_000)
	assert.EqualValues(t, histMaxMs+5_000, over.quantile(0.5, histMaxMs+5_000),
		"the overflow bucket reads as the observed max")

	zero := newHistogram()
	zero.observe(0)
	assert.Zero(t, zero.quantile(0.5, 0), "a zero duration is clamped to its max of zero")
}

// TestHistMarshalRoundTrip proves the sparse stored form round-trips through
// the hand-written parser, omits empty buckets, reads PostgreSQL's jsonb
// rendering (which inserts spaces), and rejects anything that is not the shape
// rather than half-reading it.
func TestHistMarshalRoundTrip(t *testing.T) {
	t.Parallel()

	h := newHistogram()
	for _, v := range []int64{0, 3, 3, 250, 250, 250, 90_000, histMaxMs + 1} {
		h.observe(v)
	}
	raw := h.marshal()
	assert.NotContains(t, string(raw), ",0]", "empty buckets are omitted")
	back, err := parseSparseHist(raw)
	require.NoError(t, err)
	assert.Equal(t, h, back)

	jsonb, err := parseSparseHist([]byte("[[3, 2], [41, 1]]\n"))
	require.NoError(t, err)
	assert.EqualValues(t, 2, jsonb[3])
	assert.EqualValues(t, 1, jsonb[41])

	empty, err := parseSparseHist(nil)
	require.NoError(t, err)
	assert.Zero(t, empty.total())
	empty, err = parseSparseHist([]byte("[]"))
	require.NoError(t, err)
	assert.Zero(t, empty.total())
	assert.Equal(t, "[]", string(newHistogram().marshal()))

	for _, bad := range []string{`[[9999,1]]`, `{`, `[[1,2,3]]`, `[[1]]`, `[[1,2]`, `[1,2]`, `[[[1,2]]]`, `[["a",1]]`, `]`} {
		_, err := parseSparseHist([]byte(bad))
		assert.Error(t, err, "%q must be rejected", bad)
	}
}

// TestHistogramBoundsIsTheVersionKey proves the exported bounds match the
// stored version, return a copy a caller cannot corrupt, and are nil for a
// version that does not exist.
func TestHistogramBoundsIsTheVersionKey(t *testing.T) {
	t.Parallel()

	got := HistogramBounds(histVersion)
	assert.Equal(t, histBoundsV1, got)
	got[0] = 999
	assert.EqualValues(t, 1, histBoundsV1[0], "the caller's slice is a copy")
	assert.Nil(t, HistogramBounds(histVersion+1))
}
