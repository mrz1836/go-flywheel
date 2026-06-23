package observers

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	flywheel "github.com/mrz1836/go-flywheel"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// recordedCall captures one MetricsRecorder method invocation for assertion.
type recordedCall struct {
	method string // "count" | "gauge" | "observe"
	name   string
	delta  int64
	value  float64
	tags   map[string]string
}

// fakeRecorder is a MetricsRecorder test double that captures every call in
// order, so a test can assert the exact metric/tag mapping a MetricsObserver
// produces. It is the mock the adapter mapping is verified against — no real
// metrics backend is involved.
type fakeRecorder struct {
	mu    sync.Mutex
	calls []recordedCall
}

func (f *fakeRecorder) Count(name string, delta int64, tags map[string]string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, recordedCall{method: "count", name: name, delta: delta, tags: copyTags(tags)})
}

func (f *fakeRecorder) Gauge(name string, value float64, tags map[string]string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, recordedCall{method: "gauge", name: name, value: value, tags: copyTags(tags)})
}

func (f *fakeRecorder) Observe(name string, value float64, tags map[string]string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, recordedCall{method: "observe", name: name, value: value, tags: copyTags(tags)})
}

// only returns the single captured call matching method and name, failing the
// test when there is not exactly one.
func (f *fakeRecorder) only(t *testing.T, method, name string) recordedCall {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	var found []recordedCall
	for _, c := range f.calls {
		if c.method == method && c.name == name {
			found = append(found, c)
		}
	}
	require.Len(t, found, 1, "expected exactly one %s %s call", method, name)
	return found[0]
}

func TestMetricsObserverOnClaimCountsByExecutorClass(t *testing.T) {
	t.Parallel()
	rec := &fakeRecorder{}
	NewMetrics(rec).OnClaim(context.Background(), flywheel.ClaimEvent{
		ExecutorClass: "local", Queues: []string{"default"}, Claimed: 3,
	})

	require.Len(t, rec.calls, 1)
	c := rec.only(t, "count", MetricJobsClaimed)
	assert.EqualValues(t, 3, c.delta, "the batch size is the counter delta")
	assert.Equal(t, map[string]string{TagExecutorClass: "local"}, c.tags)
}

func TestMetricsObserverOnStartCountsByKindAndQueue(t *testing.T) {
	t.Parallel()
	rec := &fakeRecorder{}
	NewMetrics(rec).OnStart(context.Background(), flywheel.JobEvent{
		JobID: "j1", Kind: "k", Queue: "q", Attempt: 1,
	})

	require.Len(t, rec.calls, 1)
	c := rec.only(t, "count", MetricJobsStarted)
	assert.EqualValues(t, 1, c.delta)
	assert.Equal(t, map[string]string{TagKind: "k", TagQueue: "q"}, c.tags)
}

func TestMetricsObserverOnFinishSuccessCountsAndObservesNoError(t *testing.T) {
	t.Parallel()
	rec := &fakeRecorder{}
	NewMetrics(rec).OnFinish(context.Background(), flywheel.FinishEvent{
		JobEvent: flywheel.JobEvent{Kind: "k", Queue: "q"},
		Outcome:  flywheel.OutcomeSuccess,
		Duration: 1500 * time.Millisecond,
	})

	// Success: a finished count and a duration observation, and no error count.
	require.Len(t, rec.calls, 2)
	finished := rec.only(t, "count", MetricJobsFinished)
	assert.Equal(t, map[string]string{TagKind: "k", TagQueue: "q", TagOutcome: "success"}, finished.tags)

	dur := rec.only(t, "observe", MetricJobDuration)
	assert.InDelta(t, 1.5, dur.value, 1e-9, "duration is observed in seconds")
	assert.Equal(t, map[string]string{TagKind: "k", TagOutcome: "success"}, dur.tags)

	for _, c := range rec.calls {
		assert.NotEqual(t, MetricJobsErrored, c.name, "a success records no error counter")
	}
}

func TestMetricsObserverOnFinishErrorAlsoCountsErrored(t *testing.T) {
	t.Parallel()
	rec := &fakeRecorder{}
	NewMetrics(rec).OnFinish(context.Background(), flywheel.FinishEvent{
		JobEvent:   flywheel.JobEvent{Kind: "k", Queue: "q"},
		Outcome:    flywheel.OutcomeError,
		ErrorClass: flywheel.ErrorTransient,
		Err:        errors.New("boom"),
		Duration:   2 * time.Second,
	})

	// Error: finished count, duration observation, and an errored count by class.
	require.Len(t, rec.calls, 3)
	finished := rec.only(t, "count", MetricJobsFinished)
	assert.Equal(t, "error", finished.tags[TagOutcome])

	errored := rec.only(t, "count", MetricJobsErrored)
	assert.EqualValues(t, 1, errored.delta)
	assert.Equal(t, map[string]string{TagKind: "k", TagErrorClass: "transient"}, errored.tags)

	dur := rec.only(t, "observe", MetricJobDuration)
	assert.InDelta(t, 2.0, dur.value, 1e-9)
	assert.Equal(t, "error", dur.tags[TagOutcome])
}

func TestMetricsObserverOnRetryCountsByKindAndErrorClass(t *testing.T) {
	t.Parallel()
	rec := &fakeRecorder{}
	NewMetrics(rec).OnRetry(context.Background(), flywheel.RetryEvent{
		JobEvent:    flywheel.JobEvent{Kind: "k"},
		NextAttempt: 2,
		Delay:       time.Second,
		ErrorClass:  flywheel.ErrorTransient,
	})

	require.Len(t, rec.calls, 1)
	c := rec.only(t, "count", MetricJobsRetried)
	assert.EqualValues(t, 1, c.delta)
	assert.Equal(t, map[string]string{TagKind: "k", TagErrorClass: "transient"}, c.tags)
}

func TestMetricsObserverImplementsObserver(t *testing.T) {
	t.Parallel()
	var obs flywheel.Observer = NewMetrics(NewMemRecorder())
	assert.NotNil(t, obs)
}

func TestMemRecorderAccumulatesCountGaugeObserve(t *testing.T) {
	t.Parallel()
	m := NewMemRecorder()
	m.Count("c", 2, map[string]string{"a": "1"})
	m.Count("c", 3, map[string]string{"a": "1"}) // same series accumulates
	m.Count("c", 1, map[string]string{"a": "2"}) // distinct series
	m.Gauge("g", 4.5, nil)
	m.Gauge("g", 5.5, nil) // last write wins
	m.Observe("o", 1.0, map[string]string{"k": "v"})
	m.Observe("o", 3.0, map[string]string{"k": "v"})

	snap := m.Snapshot()

	require.Len(t, snap.Counters, 2)
	assert.Equal(t, "c", snap.Counters[0].Name)
	assert.Equal(t, map[string]string{"a": "1"}, snap.Counters[0].Tags)
	assert.EqualValues(t, 5, snap.Counters[0].Value, "equal-tag counts accumulate")
	assert.EqualValues(t, 1, snap.Counters[1].Value, "a distinct tag set is its own series")

	require.Len(t, snap.Gauges, 1)
	assert.InDelta(t, 5.5, snap.Gauges[0].Value, 1e-9, "the latest gauge write wins")

	require.Len(t, snap.Observations, 1)
	assert.InDelta(t, 4.0, snap.Observations[0].Sum, 1e-9)
	assert.EqualValues(t, 2, snap.Observations[0].Count)
}

func TestMemRecorderSnapshotIsAnIsolatedCopy(t *testing.T) {
	t.Parallel()
	m := NewMemRecorder()
	m.Count("c", 1, map[string]string{"a": "1"})

	snap := m.Snapshot()
	snap.Counters[0].Tags["a"] = "mutated" // mutating the snapshot must not bleed back

	again := m.Snapshot()
	assert.Equal(t, "1", again.Counters[0].Tags["a"], "the snapshot is a private copy")
}

func TestMemRecorderSnapshotSortsEverySeriesType(t *testing.T) {
	t.Parallel()
	m := NewMemRecorder()
	// Insert each series type out of sorted order so the snapshot must sort it.
	m.Gauge("g_z", 1, nil)
	m.Gauge("g_a", 2, nil)
	m.Observe("o_z", 1, nil)
	m.Observe("o_a", 1, nil)
	m.Count("c_z", 1, nil)
	m.Count("c_a", 1, nil)

	snap := m.Snapshot()
	require.Len(t, snap.Counters, 2)
	require.Len(t, snap.Gauges, 2)
	require.Len(t, snap.Observations, 2)
	assert.Equal(t, "c_a", snap.Counters[0].Name, "counters are name-sorted")
	assert.Equal(t, "g_a", snap.Gauges[0].Name, "gauges are name-sorted")
	assert.Equal(t, "o_a", snap.Observations[0].Name, "observations are name-sorted")
}

func TestMemRecorderConcurrentRecordingIsRaceFree(t *testing.T) {
	t.Parallel()
	m := NewMemRecorder()
	const goroutines, perG = 8, 1000

	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perG; i++ {
				m.Count("c", 1, map[string]string{"shard": "x"})
				m.Gauge("g", float64(i), nil)
				m.Observe("o", 2.0, nil)
			}
		}()
	}
	wg.Wait()

	snap := m.Snapshot()
	require.Len(t, snap.Counters, 1)
	assert.EqualValues(t, goroutines*perG, snap.Counters[0].Value, "every increment is counted")
	require.Len(t, snap.Observations, 1)
	assert.EqualValues(t, goroutines*perG, snap.Observations[0].Count)
	assert.InDelta(t, float64(goroutines*perG)*2.0, snap.Observations[0].Sum, 1e-6)
}
