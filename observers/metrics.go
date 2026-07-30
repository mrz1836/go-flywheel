package observers

import (
	"cmp"
	"context"
	"maps"
	"slices"
	"sort"
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
	// Histogram records value into a bucketed distribution named name with the
	// given tags, so percentiles are derivable. Where Observe keeps only a running
	// sum and count — from which a mean is all that can be computed — Histogram
	// keeps counts per bucket, which is what answers "how slow is the slow tail?".
	//
	// An implementation that cannot bucket may implement this as Observe: the
	// caller still gets a sum and count, and the renderer degrades to a summary.
	// That is acceptable and lossy — the percentiles are gone, not the totals.
	Histogram(name string, value float64, tags map[string]string)
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
	// MetricJobsSuperseded counts attempts whose outcome was discarded because
	// their claim was lost. It is the double-execution signal: every increment is
	// work that ran and did not count.
	MetricJobsSuperseded = "flywheel_jobs_superseded_total"
)

// Tag keys. They are the label dimensions the taxonomy slices each metric by.
const (
	TagExecutorClass = "executor_class"
	TagKind          = "kind"
	TagQueue         = "queue"
	TagOutcome       = "outcome"
	TagErrorClass    = "error_class"
)

// defaultMaxSeries bounds how many distinct series a MemRecorder retains when
// HistogramConfig.MaxSeries is left zero. It is a combined ceiling across
// counters, gauges, observations, and histograms — a cardinality safety net, not
// a tuning knob, sized well above the fixed taxonomy this package records.
const defaultMaxSeries = 10_000

// DefaultLatencyBuckets spans the range a database round trip occupies, from a
// sub-millisecond local claim to a multi-second contended one. The boundaries are
// in seconds — the Prometheus convention every latency dashboard expects — so a
// histogram_quantile over them yields a duration a reader can act on directly.
//
//nolint:gochecknoglobals // an exported, read-only default bucket set
var DefaultLatencyBuckets = []float64{
	.0005, .001, .0025, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10,
}

// HistogramConfig configures a MemRecorder's bucket boundaries and its series
// ceiling.
type HistogramConfig struct {
	// Buckets are the histogram upper bounds, in the metric's own units,
	// ascending. Zero (a nil or empty slice) selects DefaultLatencyBuckets. The
	// recorder keeps its own sorted copy, so the caller's slice is never retained.
	Buckets []float64
	// MaxSeries bounds how many distinct series the recorder retains across
	// counters, gauges, observations, and histograms combined. Zero selects
	// defaultMaxSeries. Once the ceiling is reached a new series is dropped and
	// counted in Snapshot.DroppedSeries rather than growing the maps without limit.
	MaxSeries int
}

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
//	OnSupersede -> Count(flywheel_jobs_superseded_total, 1, {kind, queue})
//
// Note what OnSupersede does *not* do: it does not count into
// flywheel_jobs_finished_total. A superseded attempt advanced nothing, so
// counting it as finished would report two successes for one job — the exact
// blindness the supersede event exists to remove. flywheel_jobs_finished_total
// is therefore a count of state-advancing finalizations, and the two families
// sum to the attempts that ran.
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

// OnSupersede counts each discarded attempt, sliced by kind and queue.
//
// The discarded outcome is deliberately not a label. A supersede is a supersede
// whatever the attempt would have recorded, and slicing by it would split the
// one series an operator alerts on into five that each look small.
func (m *MetricsObserver) OnSupersede(_ context.Context, ev flywheel.SupersedeEvent) {
	m.rec.Count(MetricJobsSuperseded, 1, map[string]string{
		TagKind:  ev.Kind,
		TagQueue: ev.Queue,
	})
}

// MemRecorder is a concurrent-safe, in-memory MetricsRecorder. It is three things
// at once: the reference implementation of the interface, the test double the
// adapter tests assert against, and the source of the process-lifetime counters
// the local `/metrics` endpoint renders. A series is identified by its name plus
// its sorted tag set, so repeated calls with equal tags accumulate into one cell.
//
// # Cardinality
//
// The recorder holds one cell per distinct series and never evicts, so an
// unbounded tag — a job id, a per-request identifier, a raw error string — will
// mint a new series on every call and exhaust the process's memory. Tag by kind,
// queue, class, and outcome: dimensions with a small, fixed range. The MaxSeries
// ceiling (HistogramConfig.MaxSeries, default 10,000, combined across counters,
// gauges, observations, and histograms) is the backstop, not a license: once it
// is reached, new series are dropped and counted in Snapshot.DroppedSeries rather
// than growing without limit. Dropping the *new* series keeps the established
// ones stable — an operator can reason about a fixed set that stopped growing,
// where an LRU under cardinality pressure produces series that appear and vanish.
type MemRecorder struct {
	mu         sync.Mutex
	counters   map[string]*counterCell
	gauges     map[string]*gaugeCell
	observed   map[string]*observedCell
	histograms map[string]*histogramCell
	// buckets is the resolved, sorted bucket boundary set shared by every
	// histogram cell (read-only after construction).
	buckets []float64
	// maxSeries is the resolved combined series ceiling across all four maps.
	maxSeries int
	// droppedSeries counts on-demand inserts refused because maxSeries was reached.
	droppedSeries int64
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

// histogramCell is one bucketed distribution series. counts holds the
// non-cumulative observation count per bucket (counts[i] is the number of values
// that fell in bucket buckets[i]); a value larger than the last bound lands in no
// explicit bucket but still lifts sum and count, so the implicit +Inf bucket the
// renderer emits equals count. buckets aliases the recorder's shared, read-only
// slice.
type histogramCell struct {
	name    string
	tags    map[string]string
	buckets []float64
	counts  []uint64
	sum     float64
	count   uint64
}

// NewMemRecorder returns an empty MemRecorder ready for concurrent use, with the
// default latency buckets and the default series ceiling.
func NewMemRecorder() *MemRecorder {
	return NewMemRecorderWithConfig(HistogramConfig{})
}

// NewMemRecorderWithConfig returns an empty MemRecorder using cfg's bucket
// boundaries and series ceiling, applying the documented default for any field
// left zero. It is how a host records latencies in a range other than database
// round trips (a provider call, say) or tightens the cardinality budget.
func NewMemRecorderWithConfig(cfg HistogramConfig) *MemRecorder {
	buckets := cfg.Buckets
	if len(buckets) == 0 {
		buckets = DefaultLatencyBuckets
	}
	// A private, sorted copy: the caller's slice is never retained, and every
	// cell can rely on ascending order for its bucket search.
	sorted := make([]float64, len(buckets))
	copy(sorted, buckets)
	slices.Sort(sorted)

	maxSeries := cfg.MaxSeries
	if maxSeries <= 0 {
		maxSeries = defaultMaxSeries
	}

	return &MemRecorder{
		counters:   map[string]*counterCell{},
		gauges:     map[string]*gaugeCell{},
		observed:   map[string]*observedCell{},
		histograms: map[string]*histogramCell{},
		buckets:    sorted,
		maxSeries:  maxSeries,
	}
}

// Compile-time proof MemRecorder satisfies MetricsRecorder.
var _ MetricsRecorder = (*MemRecorder)(nil)

// atSeriesCap reports whether the recorder already holds its maximum number of
// distinct series, counted across all four maps combined. The caller holds m.mu.
// A new-series insert past this point is dropped and counted rather than growing
// a map, which is what keeps a runaway tag from exhausting memory.
func (m *MemRecorder) atSeriesCap() bool {
	return len(m.counters)+len(m.gauges)+len(m.observed)+len(m.histograms) >= m.maxSeries
}

// Count adds delta to the named counter series.
func (m *MemRecorder) Count(name string, delta int64, tags map[string]string) {
	key := seriesKey(name, tags)
	m.mu.Lock()
	defer m.mu.Unlock()
	cell, ok := m.counters[key]
	if !ok {
		if m.atSeriesCap() {
			m.droppedSeries++
			return
		}
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
		if m.atSeriesCap() {
			m.droppedSeries++
			return
		}
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
		if m.atSeriesCap() {
			m.droppedSeries++
			return
		}
		cell = &observedCell{name: name, tags: copyTags(tags)}
		m.observed[key] = cell
	}
	cell.sum += value
	cell.count++
}

// Histogram folds value into the named bucketed distribution: it lifts the
// bucket the value falls in, plus the running sum and count. The bucket is the
// first bound at or above value; a value past the last bound lands in no explicit
// bucket but still lifts sum and count, so the +Inf bucket the renderer derives
// equals count.
func (m *MemRecorder) Histogram(name string, value float64, tags map[string]string) {
	key := seriesKey(name, tags)
	m.mu.Lock()
	defer m.mu.Unlock()
	cell, ok := m.histograms[key]
	if !ok {
		if m.atSeriesCap() {
			m.droppedSeries++
			return
		}
		cell = &histogramCell{
			name:    name,
			tags:    copyTags(tags),
			buckets: m.buckets,
			counts:  make([]uint64, len(m.buckets)),
		}
		m.histograms[key] = cell
	}
	// buckets is ascending, so the first bound >= value is the bucket value falls
	// in (le semantics: a value exactly on a boundary counts into that bucket).
	if idx := sort.SearchFloat64s(cell.buckets, value); idx < len(cell.counts) {
		cell.counts[idx]++
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

// HistogramSeries is one bucketed distribution series in a Snapshot. Buckets and
// Counts are parallel: Counts[i] is the non-cumulative number of observations in
// bucket Buckets[i]. Sum and Count are the totals; Count is the +Inf bucket the
// renderer emits.
type HistogramSeries struct {
	Name    string
	Tags    map[string]string
	Buckets []float64
	Counts  []uint64
	Sum     float64
	Count   uint64
}

// Snapshot is an immutable copy of a MemRecorder's series, taken under the lock,
// for rendering or assertion. Each slice is sorted by name then tags so the
// output is deterministic.
type Snapshot struct {
	Counters     []CounterSeries
	Gauges       []GaugeSeries
	Observations []ObservationSeries
	Histograms   []HistogramSeries
	// DroppedSeries is how many on-demand series inserts were refused because the
	// recorder's MaxSeries ceiling was reached. It is the overflow signal the
	// renderer surfaces as flywheel_metrics_dropped_series_total.
	DroppedSeries int64
}

// Snapshot copies every series out under the lock. The returned maps are private
// copies the caller may read freely without racing concurrent recording.
func (m *MemRecorder) Snapshot() Snapshot {
	m.mu.Lock()
	defer m.mu.Unlock()

	snap := Snapshot{
		Counters:      make([]CounterSeries, 0, len(m.counters)),
		Gauges:        make([]GaugeSeries, 0, len(m.gauges)),
		Observations:  make([]ObservationSeries, 0, len(m.observed)),
		Histograms:    make([]HistogramSeries, 0, len(m.histograms)),
		DroppedSeries: m.droppedSeries,
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
	for _, h := range m.histograms {
		snap.Histograms = append(snap.Histograms, HistogramSeries{
			Name:    h.name,
			Tags:    copyTags(h.tags),
			Buckets: slices.Clone(h.buckets),
			Counts:  slices.Clone(h.counts),
			Sum:     h.sum,
			Count:   h.count,
		})
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
	slices.SortFunc(snap.Histograms, func(a, b HistogramSeries) int {
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
