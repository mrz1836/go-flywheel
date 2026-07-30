package observers

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"testing"
	"time"

	flywheel "github.com/mrz1836/go-flywheel"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// recordedCall captures one MetricsRecorder method invocation for assertion.
type recordedCall struct {
	method string // "count" | "gauge" | "observe" | "histogram"
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

func (f *fakeRecorder) Histogram(name string, value float64, tags map[string]string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, recordedCall{method: "histogram", name: name, value: value, tags: copyTags(tags)})
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

// TestMetricsObserverOnSupersedeCountsByKindAndQueue asserts the supersede
// mapping, including what it deliberately omits: the discarded outcome is not a
// label. A supersede is a supersede whatever the attempt would have recorded,
// and slicing by outcome would split the one series an operator alerts on into
// five that each look small.
func TestMetricsObserverOnSupersedeCountsByKindAndQueue(t *testing.T) {
	t.Parallel()
	rec := &fakeRecorder{}
	NewMetrics(rec).OnSupersede(context.Background(), flywheel.SupersedeEvent{
		JobEvent:   flywheel.JobEvent{Kind: "k", Queue: "q", Attempt: 2},
		Outcome:    flywheel.OutcomeSuccess,
		State:      flywheel.StateRunning,
		Duration:   time.Second,
		LeaseToken: "stale-token",
	})

	require.Len(t, rec.calls, 1, "a supersede counts once and nothing else")
	c := rec.only(t, "count", MetricJobsSuperseded)
	assert.EqualValues(t, 1, c.delta)
	assert.Equal(t, map[string]string{TagKind: "k", TagQueue: "q"}, c.tags)
}

// TestMetricsObserverSupersedeIsNotCountedAsFinished is the taxonomy guarantee
// stated as a test: a discarded attempt must not land in
// flywheel_jobs_finished_total, or an operator watching success counts during a
// double-execution incident sees two successes for one job — the exact blindness
// the event exists to remove.
func TestMetricsObserverSupersedeIsNotCountedAsFinished(t *testing.T) {
	t.Parallel()
	rec := &fakeRecorder{}
	m := NewMetrics(rec)
	ev := flywheel.JobEvent{Kind: "k", Queue: "q"}

	m.OnFinish(context.Background(), flywheel.FinishEvent{JobEvent: ev, Outcome: flywheel.OutcomeSuccess})
	m.OnSupersede(context.Background(), flywheel.SupersedeEvent{JobEvent: ev, Outcome: flywheel.OutcomeSuccess})

	finished := 0
	for _, c := range rec.calls {
		if c.method == "count" && c.name == MetricJobsFinished {
			finished++
		}
	}
	assert.Equal(t, 1, finished,
		"only the state-advancing finalization counts as finished; the discarded one has its own series")
	assert.EqualValues(t, 1, rec.only(t, "count", MetricJobsSuperseded).delta)
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

// TestMemRecorderHistogramUsesDefaultBuckets is A2's "given none" half: a
// recorder built with the zero config records into DefaultLatencyBuckets, and a
// value lands in the first bound at or above it (le semantics).
func TestMemRecorderHistogramUsesDefaultBuckets(t *testing.T) {
	t.Parallel()
	m := NewMemRecorder()
	// 3 ms falls in the .005 bucket (index 3 of DefaultLatencyBuckets); 40 ms in
	// the .05 bucket (index 6); 20 s past the last bound (.,10) lands in +Inf only.
	m.Histogram("h", 0.003, map[string]string{"q": "default"})
	m.Histogram("h", 0.040, map[string]string{"q": "default"})
	m.Histogram("h", 20.0, map[string]string{"q": "default"})

	snap := m.Snapshot()
	require.Len(t, snap.Histograms, 1)
	got := snap.Histograms[0]
	assert.Equal(t, DefaultLatencyBuckets, got.Buckets, "default config uses DefaultLatencyBuckets")
	require.Len(t, got.Counts, len(DefaultLatencyBuckets))

	assert.EqualValues(t, 1, got.Counts[3], "3ms lands in the .005 bucket")
	assert.EqualValues(t, 1, got.Counts[6], "40ms lands in the .05 bucket")
	assert.EqualValues(t, 3, got.Count, "count includes the +Inf overflow value")
	assert.InDelta(t, 20.043, got.Sum, 1e-9)

	// The overflow value is in no explicit bucket: the cumulative sum of the
	// per-bucket counts is one short of Count, and Count is the +Inf bucket.
	var bucketed uint64
	for _, c := range got.Counts {
		bucketed += c
	}
	assert.EqualValues(t, 2, bucketed, "the 20s value is in +Inf only, not any explicit bucket")
}

// TestMemRecorderHistogramUsesCustomBuckets is A2's "given custom buckets" half:
// a recorder built with an unsorted custom set records against a sorted copy of
// exactly those bounds.
func TestMemRecorderHistogramUsesCustomBuckets(t *testing.T) {
	t.Parallel()
	m := NewMemRecorderWithConfig(HistogramConfig{Buckets: []float64{10, 1, 5}})
	m.Histogram("h", 3, nil) // 3 <= 5 -> the middle bucket of the sorted {1,5,10}

	snap := m.Snapshot()
	require.Len(t, snap.Histograms, 1)
	got := snap.Histograms[0]
	assert.Equal(t, []float64{1, 5, 10}, got.Buckets, "custom buckets are used, sorted ascending")
	assert.EqualValues(t, 0, got.Counts[0], "3 is above the 1 bound")
	assert.EqualValues(t, 1, got.Counts[1], "3 falls in the 5 bound")
	assert.EqualValues(t, 0, got.Counts[2])
	assert.EqualValues(t, 1, got.Count)
}

// TestMemRecorderHistogramAccumulatesPerSeries proves equal-tag observations fold
// into one series and a distinct tag set is its own.
func TestMemRecorderHistogramAccumulatesPerSeries(t *testing.T) {
	t.Parallel()
	m := NewMemRecorder()
	m.Histogram("h", 0.001, map[string]string{"kind": "a"})
	m.Histogram("h", 0.001, map[string]string{"kind": "a"}) // same series
	m.Histogram("h", 0.001, map[string]string{"kind": "b"}) // distinct series

	snap := m.Snapshot()
	require.Len(t, snap.Histograms, 2)
	assert.Equal(t, map[string]string{"kind": "a"}, snap.Histograms[0].Tags)
	assert.EqualValues(t, 2, snap.Histograms[0].Count, "equal-tag observations accumulate")
	assert.EqualValues(t, 1, snap.Histograms[1].Count, "a distinct tag set is its own series")
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

// counterFor returns the counter series whose tag k equals v, or a zero value.
func counterFor(snap Snapshot, k, v string) CounterSeries {
	for _, c := range snap.Counters {
		if c.Tags[k] == v {
			return c
		}
	}
	return CounterSeries{}
}

// TestMemRecorderBoundDropsNewKeepsEstablished is A4: with MaxSeries 100 and
// 10,000 distinct tag combinations, the map is bounded at 100, DroppedSeries
// reports 9,900, and the first-100 established series keep accumulating.
func TestMemRecorderBoundDropsNewKeepsEstablished(t *testing.T) {
	t.Parallel()
	m := NewMemRecorderWithConfig(HistogramConfig{MaxSeries: 100})
	for i := 0; i < 10_000; i++ {
		m.Count("c", 1, map[string]string{"id": strconv.Itoa(i)})
	}

	snap := m.Snapshot()
	assert.Len(t, snap.Counters, 100, "the map is bounded at MaxSeries")
	assert.EqualValues(t, 9_900, snap.DroppedSeries, "every insert past the ceiling is dropped and counted")

	// id=0 was among the first 100 inserted, so it has a cell. Recording into it
	// accumulates (the drop only ever refuses a brand-new series) and adds no drop.
	m.Count("c", 5, map[string]string{"id": "0"})
	snap = m.Snapshot()
	assert.EqualValues(t, 6, counterFor(snap, "id", "0").Value, "an established series keeps accumulating")
	assert.EqualValues(t, 9_900, snap.DroppedSeries, "recording into an existing series drops nothing")
}

// TestMemRecorderBoundIsCombinedAcrossSeriesTypes proves the ceiling counts every
// series type together: two counters, a gauge, and a histogram exhaust a
// MaxSeries of 3, so the fourth distinct series — whatever its type — is dropped.
func TestMemRecorderBoundIsCombinedAcrossSeriesTypes(t *testing.T) {
	t.Parallel()
	m := NewMemRecorderWithConfig(HistogramConfig{MaxSeries: 3})
	m.Count("c1", 1, nil)
	m.Gauge("g1", 1, nil)
	m.Observe("o1", 1, nil)
	// The budget is spent; a fourth series of any type is refused.
	m.Histogram("h1", 1, nil)
	m.Count("c2", 1, nil)

	snap := m.Snapshot()
	assert.Len(t, snap.Counters, 1)
	assert.Len(t, snap.Gauges, 1)
	assert.Len(t, snap.Observations, 1)
	assert.Empty(t, snap.Histograms, "the histogram was past the combined ceiling")
	assert.EqualValues(t, 2, snap.DroppedSeries, "both the histogram and the second counter were dropped")
}

// TestMemRecorderBoundIsRaceFree exercises the drop path under concurrency: many
// goroutines racing distinct new series against a tight ceiling must not corrupt
// the map or the dropped counter.
func TestMemRecorderBoundIsRaceFree(t *testing.T) {
	t.Parallel()
	m := NewMemRecorderWithConfig(HistogramConfig{MaxSeries: 50})
	const goroutines, perG = 8, 500

	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(base int) {
			defer wg.Done()
			for i := 0; i < perG; i++ {
				m.Count("c", 1, map[string]string{"id": strconv.Itoa(base*perG + i)})
			}
		}(g)
	}
	wg.Wait()

	snap := m.Snapshot()
	assert.Len(t, snap.Counters, 50, "the map never exceeds the ceiling under load")
	// Every attempt either created one of the 50 series or was dropped; nothing is lost.
	assert.EqualValues(t, goroutines*perG-50, snap.DroppedSeries, "kept plus dropped equals every attempt")
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
				m.Histogram("h", 0.002, nil)
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
	require.Len(t, snap.Histograms, 1)
	assert.EqualValues(t, goroutines*perG, snap.Histograms[0].Count, "every histogram observation is counted")
}
