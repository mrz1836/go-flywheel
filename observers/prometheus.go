package observers

import (
	"context"
	"fmt"
	"io"
	"maps"
	"net/http"
	"slices"
	"strconv"
	"strings"

	flywheel "github.com/mrz1836/go-flywheel"
)

// Queue-health gauge metric names. They are sampled fresh on each scrape from a
// flywheel.QueueHealth, not accumulated in the recorder.
const (
	MetricQueueJobs         = "flywheel_queue_jobs"
	MetricQueueReady        = "flywheel_queue_ready"
	MetricQueueInFlight     = "flywheel_queue_inflight"
	MetricQueueOldestReady  = "flywheel_queue_oldest_ready_seconds"
	metricsContentType      = "text/plain; version=0.0.4; charset=utf-8"
	prometheusTypeCounter   = "counter"
	prometheusTypeGauge     = "gauge"
	prometheusTypeSummary   = "summary"
	queueStateLabelStateKey = "state"
)

// metricHelp maps a metric name to its HELP text. A name absent from the map
// falls back to the name itself, so a consumer's custom metric still renders
// validly.
//
//nolint:gochecknoglobals // static, read-only HELP-text registry
var metricHelp = map[string]string{
	MetricJobsClaimed:      "Total jobs claimed across all batches.",
	MetricJobsStarted:      "Total worker attempts started.",
	MetricJobsFinished:     "Total worker attempts finished, by outcome.",
	MetricJobsErrored:      "Total worker attempts that finished with a classified error.",
	MetricJobsRetried:      "Total worker attempts scheduled for a retry.",
	MetricJobDuration:      "Worker attempt duration in seconds (sum and count).",
	MetricQueueJobs:        "Jobs in the queue by state.",
	MetricQueueReady:       "Jobs claimable right now.",
	MetricQueueInFlight:    "Jobs currently running.",
	MetricQueueOldestReady: "Age in seconds of the oldest claimable job (queue lag).",
}

// WritePrometheus renders rec's accumulated counters and distributions plus the
// point-in-time queue-health gauges in qh into the Prometheus text-exposition
// format (version 0.0.4): a # HELP and # TYPE line per metric family followed by
// its series lines. Output is deterministic — series are emitted in sorted order
// — so it is stable to diff and assert against.
func WritePrometheus(w io.Writer, rec *MemRecorder, qh flywheel.QueueHealth) error {
	ew := &errWriter{w: w}
	snap := rec.Snapshot()

	writeCounterFamilies(ew, snap.Counters)
	writeSummaryFamilies(ew, snap.Observations)
	writeGaugeFamilies(ew, snap.Gauges)
	writeQueueHealth(ew, qh)

	return ew.err
}

// MetricsHandler returns an http.Handler that serves rec's counters together with
// queue-health gauges sampled fresh on every scrape via sample. A nil sample
// renders counters only (no gauges). When sample returns an error the handler
// responds 500 so the scrape is recorded as failed rather than silently dropping
// the lag signal.
func MetricsHandler(rec *MemRecorder, sample func(ctx context.Context) (flywheel.QueueHealth, error)) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var qh flywheel.QueueHealth
		if sample != nil {
			sampled, err := sample(r.Context())
			if err != nil {
				http.Error(w, "flywheel: queue health sample failed: "+err.Error(), http.StatusInternalServerError)
				return
			}
			qh = sampled
		}
		w.Header().Set("Content-Type", metricsContentType)
		_ = WritePrometheus(w, rec, qh)
	})
}

// writeCounterFamilies renders the counter series grouped into families.
func writeCounterFamilies(ew *errWriter, series []CounterSeries) {
	var current string
	for i := range series {
		s := series[i]
		if s.Name != current {
			writeFamilyHeader(ew, s.Name, prometheusTypeCounter)
			current = s.Name
		}
		ew.printf("%s%s %s\n", s.Name, formatLabels(s.Tags), strconv.FormatInt(s.Value, 10))
	}
}

// writeGaugeFamilies renders gauge series (recorder-held, e.g. a consumer's own
// gauges) grouped into families.
func writeGaugeFamilies(ew *errWriter, series []GaugeSeries) {
	var current string
	for i := range series {
		s := series[i]
		if s.Name != current {
			writeFamilyHeader(ew, s.Name, prometheusTypeGauge)
			current = s.Name
		}
		ew.printf("%s%s %s\n", s.Name, formatLabels(s.Tags), formatFloat(s.Value))
	}
}

// writeSummaryFamilies renders each distribution as a summary family: a _sum and
// a _count series per label set.
func writeSummaryFamilies(ew *errWriter, series []ObservationSeries) {
	var current string
	for i := range series {
		s := series[i]
		if s.Name != current {
			writeFamilyHeader(ew, s.Name, prometheusTypeSummary)
			current = s.Name
		}
		labels := formatLabels(s.Tags)
		ew.printf("%s_sum%s %s\n", s.Name, labels, formatFloat(s.Sum))
		ew.printf("%s_count%s %s\n", s.Name, labels, strconv.FormatInt(s.Count, 10))
	}
}

// writeQueueHealth renders the four queue-health gauge families from qh.
func writeQueueHealth(ew *errWriter, qh flywheel.QueueHealth) {
	writeFamilyHeader(ew, MetricQueueJobs, prometheusTypeGauge)
	states := slices.Sorted(maps.Keys(qh.CountsByState))
	for _, state := range states {
		labels := formatLabels(map[string]string{queueStateLabelStateKey: state})
		ew.printf("%s%s %s\n", MetricQueueJobs, labels, strconv.FormatInt(qh.CountsByState[state], 10))
	}

	writeFamilyHeader(ew, MetricQueueReady, prometheusTypeGauge)
	ew.printf("%s %s\n", MetricQueueReady, strconv.FormatInt(qh.Ready, 10))

	writeFamilyHeader(ew, MetricQueueInFlight, prometheusTypeGauge)
	ew.printf("%s %s\n", MetricQueueInFlight, strconv.FormatInt(qh.InFlight, 10))

	writeFamilyHeader(ew, MetricQueueOldestReady, prometheusTypeGauge)
	ew.printf("%s %s\n", MetricQueueOldestReady, formatFloat(qh.OldestReadyAge.Seconds()))
}

// writeFamilyHeader writes the # HELP and # TYPE lines that introduce a metric
// family.
func writeFamilyHeader(ew *errWriter, name, typ string) {
	help, ok := metricHelp[name]
	if !ok {
		help = name
	}
	ew.printf("# HELP %s %s\n", name, help)
	ew.printf("# TYPE %s %s\n", name, typ)
}

// formatLabels renders a sorted, escaped Prometheus label set, or "" for no tags.
func formatLabels(tags map[string]string) string {
	if len(tags) == 0 {
		return ""
	}
	keys := slices.Sorted(maps.Keys(tags))
	var b strings.Builder
	b.WriteByte('{')
	for i, k := range keys {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(k)
		b.WriteString(`="`)
		b.WriteString(escapeLabelValue(tags[k]))
		b.WriteByte('"')
	}
	b.WriteByte('}')
	return b.String()
}

// escapeLabelValue escapes the three characters a Prometheus label value must
// escape: backslash, double quote, and newline.
func escapeLabelValue(v string) string {
	if !strings.ContainsAny(v, "\\\"\n") {
		return v
	}
	return strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`).Replace(v)
}

// formatFloat renders a float the Prometheus way: shortest round-trippable form,
// no trailing zeros.
func formatFloat(v float64) string {
	return strconv.FormatFloat(v, 'g', -1, 64)
}

// errWriter accumulates the first write error so a long render can skip explicit
// error checks on every line and report once at the end.
type errWriter struct {
	w   io.Writer
	err error
}

// printf writes one formatted line unless a prior write already failed.
func (ew *errWriter) printf(format string, a ...any) {
	if ew.err != nil {
		return
	}
	_, ew.err = fmt.Fprintf(ew.w, format, a...)
}
