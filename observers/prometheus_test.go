package observers

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	flywheel "github.com/mrz1836/go-flywheel"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWritePrometheusRendersKnownExposition(t *testing.T) {
	t.Parallel()
	m := NewMemRecorder()
	m.Count(MetricJobsClaimed, 5, map[string]string{TagExecutorClass: "local"})
	m.Observe(MetricJobDuration, 1.5, map[string]string{TagKind: "k", TagOutcome: "success"})

	qh := flywheel.QueueHealth{
		CountsByState:  map[string]int64{"available": 2, "running": 1},
		Ready:          2,
		InFlight:       1,
		ScheduledAhead: 0,
		OldestReadyAge: 600 * time.Second,
	}

	var buf bytes.Buffer
	require.NoError(t, WritePrometheus(&buf, m, qh))

	want := "# HELP flywheel_jobs_claimed_total Total jobs claimed across all batches.\n" +
		"# TYPE flywheel_jobs_claimed_total counter\n" +
		"flywheel_jobs_claimed_total{executor_class=\"local\"} 5\n" +
		"# HELP flywheel_job_duration_seconds Worker attempt duration in seconds (sum and count).\n" +
		"# TYPE flywheel_job_duration_seconds summary\n" +
		"flywheel_job_duration_seconds_sum{kind=\"k\",outcome=\"success\"} 1.5\n" +
		"flywheel_job_duration_seconds_count{kind=\"k\",outcome=\"success\"} 1\n" +
		"# HELP flywheel_queue_jobs Jobs in the queue by state.\n" +
		"# TYPE flywheel_queue_jobs gauge\n" +
		"flywheel_queue_jobs{state=\"available\"} 2\n" +
		"flywheel_queue_jobs{state=\"running\"} 1\n" +
		"# HELP flywheel_queue_ready Jobs claimable right now.\n" +
		"# TYPE flywheel_queue_ready gauge\n" +
		"flywheel_queue_ready 2\n" +
		"# HELP flywheel_queue_inflight Jobs currently running.\n" +
		"# TYPE flywheel_queue_inflight gauge\n" +
		"flywheel_queue_inflight 1\n" +
		"# HELP flywheel_queue_oldest_ready_seconds Age in seconds of the oldest claimable job (queue lag).\n" +
		"# TYPE flywheel_queue_oldest_ready_seconds gauge\n" +
		"flywheel_queue_oldest_ready_seconds 600\n"
	assert.Equal(t, want, buf.String(), "the full exposition is deterministic and well-formed")
}

func TestWritePrometheusAccumulatesEqualTagCounters(t *testing.T) {
	t.Parallel()
	m := NewMemRecorder()
	m.Count(MetricJobsStarted, 1, map[string]string{TagKind: "k", TagQueue: "q"})
	m.Count(MetricJobsStarted, 1, map[string]string{TagKind: "k", TagQueue: "q"})

	var buf bytes.Buffer
	require.NoError(t, WritePrometheus(&buf, m, flywheel.QueueHealth{}))
	assert.Contains(t, buf.String(), "flywheel_jobs_started_total{kind=\"k\",queue=\"q\"} 2",
		"two starts of the same series sum to 2")
}

func TestWritePrometheusEscapesLabelValues(t *testing.T) {
	t.Parallel()
	m := NewMemRecorder()
	m.Count(MetricJobsStarted, 1, map[string]string{TagKind: `we"ird\kind`, TagQueue: "q"})

	var buf bytes.Buffer
	require.NoError(t, WritePrometheus(&buf, m, flywheel.QueueHealth{}))
	assert.Contains(t, buf.String(), `kind="we\"ird\\kind"`, "quotes and backslashes in label values are escaped")
}

func TestWritePrometheusRendersRecorderGauge(t *testing.T) {
	t.Parallel()
	m := NewMemRecorder()
	m.Gauge("flywheel_custom_gauge", 12.5, map[string]string{"region": "us"})

	var buf bytes.Buffer
	require.NoError(t, WritePrometheus(&buf, m, flywheel.QueueHealth{}))
	out := buf.String()
	assert.Contains(t, out, "# TYPE flywheel_custom_gauge gauge")
	assert.Contains(t, out, `flywheel_custom_gauge{region="us"} 12.5`)
}

func TestWritePrometheusEmptyRecorderStillRendersQueueGauges(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	require.NoError(t, WritePrometheus(&buf, NewMemRecorder(), flywheel.QueueHealth{Ready: 0}))
	out := buf.String()
	assert.Contains(t, out, "# TYPE flywheel_queue_ready gauge")
	assert.Contains(t, out, "flywheel_queue_ready 0", "a zero gauge still renders")
}

// errAfterWriter fails the nth write to exercise WritePrometheus error handling.
type errAfterWriter struct {
	remaining int
}

func (e *errAfterWriter) Write(p []byte) (int, error) {
	if e.remaining <= 0 {
		return 0, errors.New("write failed")
	}
	e.remaining--
	return len(p), nil
}

func TestWritePrometheusReturnsWriteError(t *testing.T) {
	t.Parallel()
	m := NewMemRecorder()
	m.Count(MetricJobsClaimed, 1, nil)
	err := WritePrometheus(&errAfterWriter{remaining: 1}, m, flywheel.QueueHealth{})
	require.Error(t, err, "a failed underlying write surfaces")
}

func TestMetricsHandlerServesCountersAndSampledGauges(t *testing.T) {
	t.Parallel()
	m := NewMemRecorder()
	m.Count(MetricJobsClaimed, 1, map[string]string{TagExecutorClass: "local"})

	sample := func(context.Context) (flywheel.QueueHealth, error) {
		return flywheel.QueueHealth{Ready: 7, CountsByState: map[string]int64{"available": 7}}, nil
	}
	h := MetricsHandler(m, sample)

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))

	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Header().Get("Content-Type"), "text/plain; version=0.0.4")
	body := rec.Body.String()
	assert.Contains(t, body, `flywheel_jobs_claimed_total{executor_class="local"} 1`, "process counters render")
	assert.Contains(t, body, "flywheel_queue_ready 7", "the freshly sampled gauge renders")
	assert.Contains(t, body, `flywheel_queue_jobs{state="available"} 7`)
}

func TestMetricsHandlerNilSampleRendersCountersOnly(t *testing.T) {
	t.Parallel()
	m := NewMemRecorder()
	m.Count(MetricJobsClaimed, 4, nil)
	h := MetricsHandler(m, nil)

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))

	require.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "flywheel_jobs_claimed_total 4")
	assert.Contains(t, body, "flywheel_queue_ready 0", "queue gauges render from the zero-value health")
}

func TestMetricsHandlerSampleErrorIs500(t *testing.T) {
	t.Parallel()
	sample := func(context.Context) (flywheel.QueueHealth, error) {
		return flywheel.QueueHealth{}, errors.New("db down")
	}
	h := MetricsHandler(NewMemRecorder(), sample)

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))

	require.Equal(t, http.StatusInternalServerError, rec.Code, "a sampling failure fails the scrape")
	assert.Contains(t, rec.Body.String(), "db down")
}

func TestMetricsHandlerSampleSeesRequestContext(t *testing.T) {
	t.Parallel()
	type ctxKey struct{}
	var gotValue any
	sample := func(ctx context.Context) (flywheel.QueueHealth, error) {
		gotValue = ctx.Value(ctxKey{})
		return flywheel.QueueHealth{}, nil
	}
	h := MetricsHandler(NewMemRecorder(), sample)

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil).
		WithContext(context.WithValue(context.Background(), ctxKey{}, "present"))
	h.ServeHTTP(httptest.NewRecorder(), req)
	assert.Equal(t, "present", gotValue, "the sample function receives the request context")
}
