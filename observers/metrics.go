package observers

import (
	"cmp"
	"context"
	"maps"
	"slices"
	"strings"
	"sync"

	flywheel "github.com/mrz1836/go-flywheel"
)

// MetricsRecorder is the consumer-pluggable metrics sink. It is the one seam in
// this package that a third-party stack plugs into: a consumer implements it
// against Prometheus, OpenTelemetry, statsd, or CloudWatch, and flywheel imports
// none of them. MemRecorder is the in-memory reference implementation.
//
// Every method is called on the synchronous dispatch path (via MetricsObserver)
// and must not block. tags is a small label set; an implementation must treat it
// as read-only and not retain it past the call.
type MetricsRecorder interface {
	// Count adds delta to the counter named name with the given tags.
	Count(name string, delta int64, tags map[string]string)
	// Gauge sets the gauge named name with the given tags to value.
	Gauge(name string, value float64, tags map[string]string)
	// Observe records one value into the distribution named name with the given
	// tags (kept as a running sum and count — duration/histogram telemetry).
	Observe(name string, value float64, tags map[string]string)
}

// Metric names. They follow Prometheus convention: a flywheel_ prefix, a unit
// suffix where one applies, and a _total suffix on monotonic counters.
const (
	MetricJobsClaimed  = "flywheel_jobs_claimed_total"
	MetricJobsStarted  = "flywheel_jobs_started_total"
	MetricJobsFinished = "flywheel_jobs_finished_total"
	MetricJobsErrored  = "flywheel_jobs_errored_total"
	MetricJobsRetried  = "flywheel_jobs_retried_total"
	MetricJobDuration  = "flywheel_job_duration_seconds"
)

// Tag keys. They are the label dimensions the taxonomy slices each metric by.
const (
	TagExecutorClass = "executor_class"
	TagKind          = "kind"
	TagQueue         = "queue"
	TagOutcome       = "outcome"
	TagErrorClass    = "error_class"
)

// MetricsObserver implements flywheel.Observer by translating each lifecycle
// event into MetricsRecorder calls, per this taxonomy:
//
//	OnClaim  -> Count(flywheel_jobs_claimed_total, batch, {executor_class})
//	OnStart  -> Count(flywheel_jobs_started_total, 1, {kind, queue})
//	OnFinish -> Count(flywheel_jobs_finished_total, 1, {kind, queue, outcome})
//	            Observe(flywheel_job_duration_seconds, secs, {kind, outcome})
//	            and, when the attempt carried a classified error,
//	            Count(flywheel_jobs_errored_total, 1, {kind, error_class})
//	OnRetry  -> Count(flywheel_jobs_retried_total, 1, {kind, error_class})
//
// It holds no state of its own; all accumulation lives in the recorder.
type MetricsObserver struct {
	rec MetricsRecorder
}

// NewMetrics returns a MetricsObserver that records into rec.
func NewMetrics(rec MetricsRecorder) *MetricsObserver {
	return &MetricsObserver{rec: rec}
}

// Compile-time proof MetricsObserver satisfies the flywheel.Observer contract.
var _ flywheel.Observer = (*MetricsObserver)(nil)

// OnClaim counts the jobs claimed in a batch, sliced by executor class.
func (m *MetricsObserver) OnClaim(_ context.Context, ev flywheel.ClaimEvent) {
	m.rec.Count(MetricJobsClaimed, int64(ev.Claimed), map[string]string{
		TagExecutorClass: string(ev.ExecutorClass),
	})
}

// OnStart counts each started attempt, sliced by kind and queue.
func (m *MetricsObserver) OnStart(_ context.Context, ev flywheel.JobEvent) {
	m.rec.Count(MetricJobsStarted, 1, map[string]string{
		TagKind:  ev.Kind,
		TagQueue: ev.Queue,
	})
}

// OnFinish counts each finished attempt by outcome, records its duration, and —
// when the attempt carried a classified error — counts it by error class.
func (m *MetricsObserver) OnFinish(_ context.Context, ev flywheel.FinishEvent) {
	m.rec.Count(MetricJobsFinished, 1, map[string]string{
		TagKind:    ev.Kind,
		TagQueue:   ev.Queue,
		TagOutcome: string(ev.Outcome),
	})
	m.rec.Observe(MetricJobDuration, ev.Duration.Seconds(), map[string]string{
		TagKind:    ev.Kind,
		TagOutcome: string(ev.Outcome),
	})
	if ev.ErrorClass != "" {
		m.rec.Count(MetricJobsErrored, 1, map[string]string{
			TagKind:       ev.Kind,
			TagErrorClass: string(ev.ErrorClass),
		})
	}
}

// OnRetry counts each scheduled retry, sliced by kind and the error class that
// triggered it.
func (m *MetricsObserver) OnRetry(_ context.Context, ev flywheel.RetryEvent) {
	m.rec.Count(MetricJobsRetried, 1, map[string]string{
		TagKind:       ev.Kind,
		TagErrorClass: string(ev.ErrorClass),
	})
}

// MemRecorder is a concurrent-safe, in-memory MetricsRecorder. It is three things
// at once: the reference implementation of the interface, the test double the
// adapter tests assert against, and the source of the process-lifetime counters
// the local `/metrics` endpoint renders. A series is identified by its name plus
// its sorted tag set, so repeated calls with equal tags accumulate into one cell.
type MemRecorder struct {
	mu       sync.Mutex
	counters map[string]*counterCell
	gauges   map[string]*gaugeCell
	observed map[string]*observedCell
}

type counterCell struct {
	name  string
	tags  map[string]string
	value int64
}

type gaugeCell struct {
	name  string
	tags  map[string]string
	value float64
}

type observedCell struct {
	name  string
	tags  map[string]string
	sum   float64
	count int64
}

// NewMemRecorder returns an empty MemRecorder ready for concurrent use.
func NewMemRecorder() *MemRecorder {
	return &MemRecorder{
		counters: map[string]*counterCell{},
		gauges:   map[string]*gaugeCell{},
		observed: map[string]*observedCell{},
	}
}

// Compile-time proof MemRecorder satisfies MetricsRecorder.
var _ MetricsRecorder = (*MemRecorder)(nil)

// Count adds delta to the named counter series.
func (m *MemRecorder) Count(name string, delta int64, tags map[string]string) {
	key := seriesKey(name, tags)
	m.mu.Lock()
	defer m.mu.Unlock()
	cell, ok := m.counters[key]
	if !ok {
		cell = &counterCell{name: name, tags: copyTags(tags)}
		m.counters[key] = cell
	}
	cell.value += delta
}

// Gauge sets the named gauge series to value (last write wins).
func (m *MemRecorder) Gauge(name string, value float64, tags map[string]string) {
	key := seriesKey(name, tags)
	m.mu.Lock()
	defer m.mu.Unlock()
	cell, ok := m.gauges[key]
	if !ok {
		cell = &gaugeCell{name: name, tags: copyTags(tags)}
		m.gauges[key] = cell
	}
	cell.value = value
}

// Observe folds value into the named distribution series as a running sum and
// count, so an average is sum/count.
func (m *MemRecorder) Observe(name string, value float64, tags map[string]string) {
	key := seriesKey(name, tags)
	m.mu.Lock()
	defer m.mu.Unlock()
	cell, ok := m.observed[key]
	if !ok {
		cell = &observedCell{name: name, tags: copyTags(tags)}
		m.observed[key] = cell
	}
	cell.sum += value
	cell.count++
}

// CounterSeries is one counter series in a Snapshot.
type CounterSeries struct {
	Name  string
	Tags  map[string]string
	Value int64
}

// GaugeSeries is one gauge series in a Snapshot.
type GaugeSeries struct {
	Name  string
	Tags  map[string]string
	Value float64
}

// ObservationSeries is one distribution series in a Snapshot, as a sum and count.
type ObservationSeries struct {
	Name  string
	Tags  map[string]string
	Sum   float64
	Count int64
}

// Snapshot is an immutable copy of a MemRecorder's series, taken under the lock,
// for rendering or assertion. Each slice is sorted by name then tags so the
// output is deterministic.
type Snapshot struct {
	Counters     []CounterSeries
	Gauges       []GaugeSeries
	Observations []ObservationSeries
}

// Snapshot copies every series out under the lock. The returned maps are private
// copies the caller may read freely without racing concurrent recording.
func (m *MemRecorder) Snapshot() Snapshot {
	m.mu.Lock()
	defer m.mu.Unlock()

	snap := Snapshot{
		Counters:     make([]CounterSeries, 0, len(m.counters)),
		Gauges:       make([]GaugeSeries, 0, len(m.gauges)),
		Observations: make([]ObservationSeries, 0, len(m.observed)),
	}
	for _, c := range m.counters {
		snap.Counters = append(snap.Counters, CounterSeries{Name: c.name, Tags: copyTags(c.tags), Value: c.value})
	}
	for _, g := range m.gauges {
		snap.Gauges = append(snap.Gauges, GaugeSeries{Name: g.name, Tags: copyTags(g.tags), Value: g.value})
	}
	for _, o := range m.observed {
		snap.Observations = append(snap.Observations, ObservationSeries{Name: o.name, Tags: copyTags(o.tags), Sum: o.sum, Count: o.count})
	}

	slices.SortFunc(snap.Counters, func(a, b CounterSeries) int {
		return cmp.Compare(seriesKey(a.Name, a.Tags), seriesKey(b.Name, b.Tags))
	})
	slices.SortFunc(snap.Gauges, func(a, b GaugeSeries) int {
		return cmp.Compare(seriesKey(a.Name, a.Tags), seriesKey(b.Name, b.Tags))
	})
	slices.SortFunc(snap.Observations, func(a, b ObservationSeries) int {
		return cmp.Compare(seriesKey(a.Name, a.Tags), seriesKey(b.Name, b.Tags))
	})
	return snap
}

// seriesKey builds a canonical identity for a metric series: its name plus its
// tags in sorted key order. NUL separators keep distinct (name, tags) sets from
// ever colliding on the joined string.
func seriesKey(name string, tags map[string]string) string {
	if len(tags) == 0 {
		return name
	}
	keys := slices.Sorted(maps.Keys(tags))
	var b strings.Builder
	b.WriteString(name)
	for _, k := range keys {
		b.WriteByte(0)
		b.WriteString(k)
		b.WriteByte(0)
		b.WriteString(tags[k])
	}
	return b.String()
}

// copyTags returns a private copy of tags, or nil for an empty set, so a stored
// cell never aliases the caller's map.
func copyTags(tags map[string]string) map[string]string {
	if len(tags) == 0 {
		return nil
	}
	out := make(map[string]string, len(tags))
	for k, v := range tags {
		out[k] = v
	}
	return out
}
