//go:build loadtest

package loadtest

import (
	"context"
	"io"
	"math"
	"net"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/mrz1836/go-flywheel/observers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestMetricsOracleClaimP99AgreesWithHarness is acceptance A1: the runtime's
// Prometheus histogram and the harness's independent timing are two measurements
// of the same claims, computed by different code, and must agree. It runs a real
// drain with -metrics-addr, scrapes /metrics, validates the exposition format,
// computes histogram_quantile(0.99) over the claim histogram in Go, and asserts it
// agrees with the harness's own Claim.P99 within one bucket width.
//
// The harness is the oracle: the two never share a code path, so agreement is
// evidence the buckets are right and disagreement is a real bug in one of them.
func TestMetricsOracleClaimP99AgreesWithHarness(t *testing.T) {
	dsn := os.Getenv(oracleDSNEnv)
	if dsn == "" {
		t.Skipf("set %s to run the metrics oracle against PostgreSQL", oracleDSNEnv)
	}

	addr := freeLocalAddr(t)
	cfg := Config{
		DSN:            dsn,
		Jobs:           20_000,
		Seed:           1,
		Runners:        4,
		Workers:        8,
		Mix:            WorkloadDrainOnly,
		Indexes:        IndexesFull,
		Timeout:        3 * time.Minute,
		SampleInterval: time.Second,
		MetricsAddr:    addr,
	}

	ctx := context.Background()
	var (
		report Report
		runErr error
	)
	done := make(chan struct{})
	go func() {
		report, runErr = Run(ctx, cfg)
		close(done)
	}()

	// Scrape the endpoint repeatedly while the run drains, keeping the last body a
	// scrape returned. The last one before teardown carries the complete histogram:
	// the runners have stopped recording by then, so no claim is missed. When
	// pollMetrics returns, done is closed, so report/runErr are safe to read.
	lastBody := pollMetrics(t, "http://"+addr+"/metrics", done)

	require.NoError(t, runErr, "the drain must complete cleanly")
	require.NotEmpty(t, lastBody, "at least one scrape must have succeeded during the run")

	// Self-contained exposition-format validation: the scrape must be well-formed
	// Prometheus text, and it must carry the claim histogram as a histogram family.
	requireValidExposition(t, lastBody)
	require.Contains(t, lastBody, "# TYPE flywheel_claim_duration_seconds histogram")

	buckets := parseHistogramBuckets(t, lastBody, "flywheel_claim_duration_seconds")
	require.NotEmpty(t, buckets, "the claim histogram must have bucket series")

	metricsP99 := histogramQuantile(0.99, buckets)
	harnessP99 := report.Claim.P99.Seconds()
	require.Positive(t, harnessP99, "the harness must have measured claims")

	tol := bucketWidthTolerance(harnessP99)
	t.Logf("claim p99: harness=%.4gs metrics=%.4gs (diff=%.4gs, tol=%.4gs)",
		harnessP99, metricsP99, math.Abs(harnessP99-metricsP99), tol)
	assert.InDeltaf(t, harnessP99, metricsP99, tol,
		"claim p99: harness=%.4gs metrics=%.4gs tol=%.4gs (must agree within one bucket width)",
		harnessP99, metricsP99, tol)
}

// oracleDSNEnv is the DSN the oracle reads; it matches the scenario command's.
const oracleDSNEnv = "FLYWHEEL_LOADTEST_DATABASE_URL"

// freeLocalAddr returns a loopback host:port currently free, so a parallel test
// run does not collide on a fixed port.
func freeLocalAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := ln.Addr().String()
	require.NoError(t, ln.Close())
	return addr
}

// pollMetrics scrapes url every 25ms until done fires, returning the last body a
// scrape successfully returned.
func pollMetrics(t *testing.T, url string, done <-chan struct{}) string {
	t.Helper()
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	client := &http.Client{Timeout: 2 * time.Second}

	var last string
	for {
		select {
		case <-done:
			return last
		case <-ticker.C:
			if body, ok := scrapeOnce(client, url); ok {
				last = body
			}
		}
	}
}

// scrapeOnce fetches url once, returning the body and whether it was a 200.
func scrapeOnce(client *http.Client, url string) (string, bool) {
	resp, err := client.Get(url)
	if err != nil {
		return "", false
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil || resp.StatusCode != http.StatusOK {
		return "", false
	}
	return string(body), true
}

// requireValidExposition checks that body is well-formed Prometheus text: every
// non-comment line is `name{labels}? value`, and every family it names has a
// preceding # TYPE. It is the self-contained validator that stands in for
// promtool when promtool is not installed.
func requireValidExposition(t *testing.T, body string) {
	t.Helper()
	typed := map[string]bool{}
	for _, line := range strings.Split(body, "\n") {
		if line == "" {
			continue
		}
		if strings.HasPrefix(line, "# TYPE ") {
			fields := strings.Fields(line)
			require.Len(t, fields, 4, "# TYPE line malformed: %q", line)
			typed[fields[2]] = true
			continue
		}
		if strings.HasPrefix(line, "# HELP ") {
			require.GreaterOrEqual(t, len(strings.SplitN(line, " ", 4)), 3, "# HELP line malformed: %q", line)
			continue
		}
		// A sample line: metric name, optional {labels}, a space, then a value.
		name, value, ok := strings.Cut(strings.TrimSpace(line), " ")
		require.True(t, ok, "sample line has no value: %q", line)
		_, parseErr := strconv.ParseFloat(strings.TrimSpace(value), 64)
		require.NoError(t, parseErr, "sample value not a float: %q", line)
		require.NotEmpty(t, name, "sample line has no metric name: %q", line)
	}
	require.NotEmpty(t, typed, "the exposition declared no metric families")
}

// bucketPoint is one cumulative histogram bucket: an upper bound and the count of
// observations at or below it.
type bucketPoint struct {
	le    float64
	count float64
}

// parseHistogramBuckets extracts the cumulative buckets of one histogram family,
// summing counts across every label set so the result is the aggregate the
// oracle's quantile is computed over.
func parseHistogramBuckets(t *testing.T, body, metric string) []bucketPoint {
	t.Helper()
	prefix := metric + "_bucket"
	sums := map[float64]float64{}
	for _, line := range strings.Split(body, "\n") {
		if !strings.HasPrefix(line, prefix) {
			continue
		}
		le, ok := labelValue(line, "le")
		require.True(t, ok, "bucket line without an le label: %q", line)
		bound := parseLe(t, le)

		_, value, ok := strings.Cut(strings.TrimSpace(line), " ")
		require.True(t, ok, "bucket line without a value: %q", line)
		count, err := strconv.ParseFloat(strings.TrimSpace(value), 64)
		require.NoError(t, err)
		sums[bound] += count
	}

	out := make([]bucketPoint, 0, len(sums))
	for le, count := range sums {
		out = append(out, bucketPoint{le: le, count: count})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].le < out[j].le })
	return out
}

// labelValue returns the value of the named label in a Prometheus sample line.
func labelValue(line, key string) (string, bool) {
	open := strings.IndexByte(line, '{')
	end := strings.IndexByte(line, '}')
	if open < 0 || end < open {
		return "", false
	}
	for _, pair := range strings.Split(line[open+1:end], ",") {
		k, v, ok := strings.Cut(pair, "=")
		if ok && k == key {
			return strings.Trim(v, `"`), true
		}
	}
	return "", false
}

// parseLe parses a bucket bound, mapping "+Inf" to positive infinity.
func parseLe(t *testing.T, le string) float64 {
	t.Helper()
	if le == "+Inf" {
		return math.Inf(1)
	}
	v, err := strconv.ParseFloat(le, 64)
	require.NoError(t, err, "unparseable le %q", le)
	return v
}

// histogramQuantile computes phi over cumulative buckets, the way Prometheus does:
// find the bucket the rank falls in and interpolate linearly within it. A rank in
// the +Inf bucket returns the largest finite bound.
func histogramQuantile(phi float64, buckets []bucketPoint) float64 {
	n := len(buckets)
	if n == 0 {
		return math.NaN()
	}
	total := buckets[n-1].count // the +Inf bucket carries the total
	if total == 0 {
		return math.NaN()
	}
	rank := phi * total

	b := 0
	for b < n && buckets[b].count < rank {
		b++
	}
	if b == n {
		return buckets[n-1].le
	}
	if math.IsInf(buckets[b].le, 1) {
		// The rank lands in the open-topped bucket: report the largest finite bound.
		if b == 0 {
			return 0
		}
		return buckets[b-1].le
	}

	bucketStart, countStart := 0.0, 0.0
	if b > 0 {
		bucketStart, countStart = buckets[b-1].le, buckets[b-1].count
	}
	bucketEnd, countEnd := buckets[b].le, buckets[b].count
	if countEnd == countStart {
		return bucketEnd
	}
	return bucketStart + (bucketEnd-bucketStart)*(rank-countStart)/(countEnd-countStart)
}

// bucketWidthTolerance returns the width of the DefaultLatencyBuckets bucket the
// value falls in — the "one bucket width" the oracle allows the two independent
// measurements to differ by. A value below the first or above the last bound
// falls back to a relative tolerance so the assertion still means something.
func bucketWidthTolerance(v float64) float64 {
	b := observers.DefaultLatencyBuckets
	lo := 0.0
	for _, hi := range b {
		if v <= hi {
			return math.Max(hi-lo, 0.25*v)
		}
		lo = hi
	}
	return math.Max(0.5*v, b[len(b)-1]-b[len(b)-2])
}
