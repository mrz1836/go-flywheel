package core

import (
	"encoding/json"
	"fmt"
	"math"
	"sort"

	"gorm.io/datatypes"
)

// histVersion names the bucket bounds a stored histogram was counted against.
// It is written on every job_stats_hourly row, and the bounds behind a version
// never change: a new set of bounds is a new version, so counts taken under the
// old bounds are never merged with counts taken under the new ones.
const histVersion = 1

// histMaxMs is the last finite bound: 24 hours in milliseconds. A duration above
// it lands in the overflow bucket, whose percentile reads as the observed max.
const histMaxMs = 24 * 60 * 60 * 1000

// histBoundsV1 are the inclusive upper bounds, in milliseconds, of version 1's
// buckets: 2^(i/4) rounded, from 1 ms to 24 h, with the duplicates the rounding
// produces below 8 ms removed. Bucket i holds durations in (bounds[i-1],
// bounds[i]]; bucket 0 holds everything at or below 1 ms, zero included; one
// more bucket past the last bound holds the overflow.
//
// Quarter-octave steps are the coarsest that keep the regression signal honest.
// A bucket spans a factor of about 1.19, so a log-interpolated percentile is off
// by a few percent at worst and a true 2× shift reads as 2×; the 1.5× steps first
// tried distorted it enough to move a doubling across the detector's threshold.
//
// It is computed rather than written out, and TestHistBoundsAreFrozen pins the
// result against a golden file, which is what makes it frozen: a change to the
// computation that moved any bound fails that test instead of silently
// re-meaning every stored histogram.
//
//nolint:gochecknoglobals // frozen, computed-once lookup table
var histBoundsV1 = computeHistBounds()

// computeHistBounds builds the version-1 bounds. See histBoundsV1.
func computeHistBounds() []int64 {
	var bounds []int64
	for i := 0; ; i++ {
		v := int64(math.Round(math.Pow(2, float64(i)/4)))
		if v >= histMaxMs {
			return append(bounds, histMaxMs)
		}
		if len(bounds) == 0 || v > bounds[len(bounds)-1] {
			bounds = append(bounds, v)
		}
	}
}

// histogram is a fixed-bucket count of durations, one slot per histBoundsV1
// bucket plus the overflow slot. Counts are exact, so histograms from any number
// of hours merge by addition — which is what lets a week's percentile be
// computed from 168 hourly rows without the raw runs.
type histogram []int64

// newHistogram returns an empty version-1 histogram.
func newHistogram() histogram {
	return make(histogram, len(histBoundsV1)+1)
}

// histBucket returns the bucket index for a duration of ms milliseconds.
func histBucket(ms int64) int {
	return sort.Search(len(histBoundsV1), func(i int) bool { return histBoundsV1[i] >= ms })
}

// observe counts one duration.
func (h histogram) observe(ms int64) {
	h[histBucket(ms)]++
}

// add merges other's counts into h. Both must be version-1 histograms.
func (h histogram) add(other histogram) {
	for i := range other {
		if i < len(h) {
			h[i] += other[i]
		}
	}
}

// total is the number of durations counted.
func (h histogram) total() int64 {
	var n int64
	for _, c := range h {
		n += c
	}
	return n
}

// quantile estimates the q-th quantile (0 < q ≤ 1) in milliseconds, clamped to
// maxMs — the exact largest duration counted, which every row stores beside the
// histogram and which no estimate may exceed.
//
// It finds the bucket holding the q·n-th smallest value and interpolates within
// it geometrically: the bucket spans [lower, upper], and a value a fraction f of
// the way through the bucket's count is estimated at lower·(upper/lower)^f.
// Geometric rather than linear because the bounds are geometric — durations are
// roughly log-uniform within a bucket this narrow — and linear interpolation
// would bias every estimate toward the bucket's top. The first bucket, whose
// lower bound is zero, interpolates linearly; the overflow bucket has no upper
// bound and reads as the max.
func (h histogram) quantile(q float64, maxMs int64) int64 {
	n := h.total()
	if n == 0 {
		return 0
	}
	rank := q * float64(n)
	var before float64
	for i, c := range h {
		if c == 0 {
			continue
		}
		if before+float64(c) < rank && i < len(h)-1 {
			before += float64(c)
			continue
		}
		if i >= len(histBoundsV1) {
			return maxMs
		}
		f := (rank - before) / float64(c)
		f = math.Min(math.Max(f, 0), 1)
		upper := float64(histBoundsV1[i])
		var est float64
		if i == 0 {
			est = f * upper
		} else {
			lower := float64(histBoundsV1[i-1])
			est = lower * math.Pow(upper/lower, f)
		}
		return min(int64(math.Round(est)), maxMs)
	}
	return maxMs
}

// marshal renders h in its stored form: a sparse JSON array of [bucket, count]
// pairs in bucket order, omitting empty buckets. An hour of one kind touches a
// handful of the ~100 buckets, so the sparse form is a few dozen bytes where the
// dense one would be several hundred.
func (h histogram) marshal() datatypes.JSON {
	pairs := make([][2]int64, 0, 8)
	for i, c := range h {
		if c != 0 {
			pairs = append(pairs, [2]int64{int64(i), c})
		}
	}
	out, err := json.Marshal(pairs)
	if err != nil {
		// Marshaling a slice of integer pairs cannot fail.
		return datatypes.JSON("[]")
	}
	return datatypes.JSON(out)
}

// parseSparseHist parses the stored sparse form — [[bucket, count], …] — without
// encoding/json. It is the hot half of every rollup read, so it is a single pass
// over the bytes: digits accumulate, a comma or bracket ends a number, and
// whitespace (PostgreSQL's jsonb renders "[[38, 1]]") is skipped. Anything that
// is not that shape is rejected rather than half-read.
func parseSparseHist(raw []byte) (histogram, error) {
	h := newHistogram()
	var (
		depth, field int
		num          int64
		inNum        bool
		pair         [2]int64
	)
	for _, c := range raw {
		switch {
		case c >= '0' && c <= '9':
			num = num*10 + int64(c-'0')
			inNum = true
			continue
		case c == ' ' || c == '\t' || c == '\n' || c == '\r':
			continue
		}
		if inNum {
			if depth != 2 || field > 1 {
				return nil, fmt.Errorf("decode histogram: unexpected number")
			}
			pair[field] = num
			num, inNum = 0, false
		}
		switch c {
		case '[':
			depth++
			field = 0
			if depth > 2 {
				return nil, fmt.Errorf("decode histogram: nested too deep")
			}
		case ',':
			if depth == 2 {
				field++
			}
		case ']':
			if depth == 2 {
				if field != 1 {
					return nil, fmt.Errorf("decode histogram: a pair needs two numbers")
				}
				if pair[0] >= int64(len(h)) {
					return nil, fmt.Errorf("decode histogram: bucket %d out of range", pair[0])
				}
				h[pair[0]] += pair[1]
			}
			depth--
			if depth < 0 {
				return nil, fmt.Errorf("decode histogram: unbalanced brackets")
			}
		default:
			return nil, fmt.Errorf("decode histogram: unexpected byte %q", c)
		}
	}
	if depth != 0 || inNum {
		return nil, fmt.Errorf("decode histogram: truncated")
	}
	return h, nil
}

// HistogramBounds returns the inclusive upper bounds, in milliseconds, of the
// buckets job_stats_hourly.dur_hist and wait_hist count against, for the
// hist_version a row carries. Bucket i of a stored histogram holds durations in
// (bounds[i-1], bounds[i]]; bucket 0 holds everything at or below bounds[0];
// bucket len(bounds) holds everything above the last bound. It returns nil for
// an unknown version.
//
// It is the key a direct-SQL consumer needs to read the stored histograms; a Go
// caller reads percentiles through Stats and never needs it.
func HistogramBounds(version int) []int64 {
	if version != histVersion {
		return nil
	}
	return append([]int64(nil), histBoundsV1...)
}
