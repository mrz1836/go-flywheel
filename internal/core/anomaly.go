package core

import (
	"context"
	"fmt"
	"sort"
	"time"

	"gorm.io/gorm"
)

// AnomalySignal names what an Anomaly detected.
type AnomalySignal string

// The signals Anomalies evaluates.
const (
	// SignalDurationRegression is a kind whose successful runs got markedly
	// slower than its baseline.
	SignalDurationRegression AnomalySignal = "duration_regression"
	// SignalFailureSpike is a kind discarding jobs at markedly more than its
	// baseline rate.
	SignalFailureSpike AnomalySignal = "failure_spike"
)

// AnomalyThresholds tunes Anomalies. Every field's zero value selects the
// default in its comment, and the defaults are the ones the noise suite proves
// quiet: seeded months of lognormal durations with hourly jitter, a diurnal
// swing, volume spikes, low-volume kinds, and a brand-new kind raise nothing,
// while a real 3× regression and a real failure spike are each flagged once.
type AnomalyThresholds struct {
	// MinSamples is the successful runs an hour needs before its duration is
	// compared at all (default 20). A low-volume kind pools up to PoolHours
	// consecutive hours to reach it.
	MinSamples int64
	// MinSamplesP95 is the sample count at which the comparison moves from the
	// median to the 95th percentile (default 100): a tail is only worth comparing
	// once there is enough of one.
	MinSamplesP95 int64
	// PoolHours bounds how many hours a low-volume kind pools (default 6).
	PoolHours int
	// MinBaselineSamples is the successful runs the baseline needs (default 200).
	MinBaselineSamples int64
	// DurationRatio is how many times its baseline a recent percentile must be
	// (default 2.0), and DurationFloor the absolute amount it must exceed it by
	// (default 250ms) — the floor is what keeps a 4 ms job doubling to 8 ms quiet.
	DurationRatio float64
	DurationFloor time.Duration
	// MinDiscards is the discards an hour needs before its rate is compared
	// (default 5).
	MinDiscards int64
	// FailureRatio is how many times the baseline discard rate a recent rate must
	// be (default 3.0), and FailureDelta the absolute amount it must exceed it by
	// (default 0.05, five percentage points).
	FailureRatio float64
	FailureDelta float64
	// MinBaselineJobs is the ended jobs the baseline discard rate needs (default
	// 200), so a kind with no history has no rate to spike against.
	MinBaselineJobs int64
	// Breaches is how many of the last Hours hours must breach (defaults 2 of 3).
	// A single bad hour is weather; two of three is a trend.
	Breaches int
	Hours    int
}

// withDefaults resolves every zero field.
//
//nolint:gocyclo // a flat list of independent zero-value defaults
func (t AnomalyThresholds) withDefaults() AnomalyThresholds {
	if t.MinSamples <= 0 {
		t.MinSamples = 20
	}
	if t.MinSamplesP95 <= 0 {
		t.MinSamplesP95 = 100
	}
	if t.PoolHours <= 0 {
		t.PoolHours = 6
	}
	if t.MinBaselineSamples <= 0 {
		t.MinBaselineSamples = 200
	}
	if t.DurationRatio <= 0 {
		t.DurationRatio = 2.0
	}
	if t.DurationFloor <= 0 {
		t.DurationFloor = 250 * time.Millisecond
	}
	if t.MinDiscards <= 0 {
		t.MinDiscards = 5
	}
	if t.FailureRatio <= 0 {
		t.FailureRatio = 3.0
	}
	if t.FailureDelta <= 0 {
		t.FailureDelta = 0.05
	}
	if t.MinBaselineJobs <= 0 {
		t.MinBaselineJobs = 200
	}
	if t.Hours <= 0 {
		t.Hours = 3
	}
	if t.Breaches <= 0 {
		t.Breaches = min(2, t.Hours)
	}
	return t
}

// AnomalyParams scopes an Anomalies evaluation. The zero value evaluates the
// latest rolled-up hour against a 7-day baseline with the default thresholds.
type AnomalyParams struct {
	// Hour is the hour to evaluate, floored to its UTC hour. Zero selects the
	// latest hour the rollup has closed.
	Hour time.Time
	// BaselineWindow is the trailing window the baseline is drawn from, ending
	// where the evaluated hours begin — the hours under evaluation are never part
	// of their own baseline. Zero selects 7 days.
	BaselineWindow time.Duration
	// Thresholds tunes the detector; the zero value selects the defaults.
	Thresholds AnomalyThresholds
}

// Anomaly is one kind behaving unlike its baseline at Hour.
type Anomaly struct {
	Kind   string        `json:"kind"`
	Signal AnomalySignal `json:"signal"`
	// Hour is the evaluated hour.
	Hour time.Time `json:"hour"`
	// Metric names what Recent and Baseline measure: "p50" or "p95" in
	// milliseconds for a duration regression, "discard_rate" as a 0–1 fraction for
	// a failure spike.
	Metric   string  `json:"metric"`
	Recent   float64 `json:"recent"`
	Baseline float64 `json:"baseline"`
	// Ratio is Recent / Baseline; zero when the baseline is zero.
	Ratio float64 `json:"ratio"`
	// Samples and BaselineSamples are what each side was drawn from: successful
	// runs for a duration regression, ended jobs for a failure spike.
	Samples         int64 `json:"samples"`
	BaselineSamples int64 `json:"baseline_samples"`
	// Onset is true when the condition did not hold one hour earlier: this is the
	// hour the anomaly began. A sustained regression is an anomaly every hour it
	// lasts and an onset exactly once — which is what an alert should fire on.
	Onset bool `json:"onset"`
}

// Anomalies evaluates every kind's recent hours against its own baseline and
// returns the kinds whose duration regressed or whose discards spiked at the
// evaluated hour, sorted by kind and signal. It reads rollups only, so it needs
// the rollup running and returns an empty slice until it has run.
//
// The rules are deliberately conservative — they are meant to page a person:
//
//   - duration_regression: an hour's p50 (or p95 once it has 100 samples) is at
//     least 2× the baseline's and at least 250ms above it, with 20 samples in the
//     hour and 200 in the baseline. A low-volume kind pools up to 6 hours into
//     each sample to reach 20, and its samples never overlap.
//   - failure_spike: an hour has at least 5 discards, at a rate at least 3× the
//     baseline's and five percentage points above it, against a baseline of at
//     least 200 ended jobs.
//   - Either signal holds only when at least 2 of the last 3 hours (samples)
//     breach.
//
// Durations are successful runs only, so failures cannot muddy the regression
// signal. Every threshold is an AnomalyThresholds field.
func Anomalies(ctx context.Context, db *gorm.DB, p AnomalyParams) ([]Anomaly, error) {
	if db == nil {
		return nil, fmt.Errorf("flywheel: Anomalies: db is nil")
	}
	t := p.Thresholds.withDefaults()
	window := p.BaselineWindow
	if window <= 0 {
		window = 7 * 24 * time.Hour
	}
	hour := floorHour(p.Hour)
	if p.Hour.IsZero() {
		watermark, rolled, err := readStatsWatermark(ctx, db)
		if err != nil {
			return nil, fmt.Errorf("flywheel: Anomalies: %w", err)
		}
		if !rolled {
			return []Anomaly{}, nil
		}
		hour = watermark.Add(-time.Hour)
	}

	// One read covers both evaluations (this hour and the one before, for Onset):
	// the evaluated samples at their widest pooling reach, and the baseline window
	// behind them.
	span := time.Duration(t.Hours*t.PoolHours+1) * time.Hour
	from := hour.Add(-time.Hour - span - window)
	hours, err := loadRolledHours(ctx, db, from, hour.Add(time.Hour), statsFilter{}, kindRows)
	if err != nil {
		return nil, fmt.Errorf("flywheel: Anomalies: %w", err)
	}
	idx := indexHoursByKind(hours)

	var out []Anomaly
	for kind, byHour := range idx {
		for _, sig := range []AnomalySignal{SignalDurationRegression, SignalFailureSpike} {
			now, ok := evaluateSignal(sig, byHour, hour, window, t)
			if !ok {
				continue
			}
			_, before := evaluateSignal(sig, byHour, hour.Add(-time.Hour), window, t)
			now.Kind = kind
			now.Hour = hour
			now.Onset = !before
			out = append(out, now)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Kind != out[j].Kind {
			return out[i].Kind < out[j].Kind
		}
		return out[i].Signal < out[j].Signal
	})
	if out == nil {
		out = []Anomaly{}
	}
	return out, nil
}

// indexHoursByKind reshapes rolled hours into kind → hour start (unix) → the
// kind's aggregate for that hour, merged across queues.
func indexHoursByKind(hours []rolledHour) map[string]map[int64]*runAgg {
	idx := map[string]map[int64]*runAgg{}
	for _, h := range hours {
		for _, a := range h.aggs {
			byHour, ok := idx[a.Kind]
			if !ok {
				byHour = map[int64]*runAgg{}
				idx[a.Kind] = byHour
			}
			cur, ok := byHour[h.start.Unix()]
			if !ok {
				cur = newRunAgg(a.Kind, "")
				byHour[h.start.Unix()] = cur
			}
			cur.merge(a)
		}
	}
	return idx
}

// evaluateSignal applies one signal's 2-of-3 rule at hour, returning the
// anomaly's measurements from the most recent breaching sample.
//
// A failure spike is judged hour by hour. A duration regression is judged on
// t.Hours disjoint samples walking back from hour: each takes one hour, or —
// for a kind too quiet to reach MinSamples in an hour — pools consecutive
// earlier hours, up to PoolHours, until it does. Disjoint is the point. Pools
// that overlapped would share their slow runs, so one slow cluster would breach
// every overlapping pool and satisfy two-of-three on its own; disjoint samples
// make the rule demand two independent pieces of evidence, which is what keeps a
// low-volume kind with a heavy tail quiet. For a kind with MinSamples or more
// runs an hour, a sample is an hour and the rule reads exactly as stated.
//
// The baseline is the window before the oldest hour any sample used, so the
// hours under evaluation are never part of their own baseline.
func evaluateSignal(
	sig AnomalySignal, byHour map[int64]*runAgg, hour time.Time, window time.Duration, t AnomalyThresholds,
) (Anomaly, bool) {
	var samples []*runAgg
	oldest := hour
	if sig == SignalDurationRegression {
		samples, oldest = durationSamples(byHour, hour, t)
	} else {
		oldest = hour.Add(-time.Duration(t.Hours-1) * time.Hour)
		for h := hour; !h.Before(oldest); h = h.Add(-time.Hour) {
			samples = append(samples, byHour[h.Unix()])
		}
	}
	base := mergeHours(byHour, oldest.Add(-window), oldest)

	breaches := 0
	var latest Anomaly
	found := false
	for _, sample := range samples {
		var a Anomaly
		var ok bool
		if sig == SignalDurationRegression {
			a, ok = durationBreach(sample, base, t)
		} else {
			a, ok = failureBreach(sample, base, t)
		}
		if ok {
			breaches++
			if !found {
				latest, found = a, true // samples run newest first
			}
		}
	}
	if breaches < t.Breaches {
		return Anomaly{}, false
	}
	latest.Signal = sig
	return latest, true
}

// durationSamples builds t.Hours disjoint duration samples walking back from
// hour, newest first, and returns them with the oldest hour any of them used.
// See evaluateSignal.
func durationSamples(byHour map[int64]*runAgg, hour time.Time, t AnomalyThresholds) ([]*runAgg, time.Time) {
	samples := make([]*runAgg, 0, t.Hours)
	cursor := hour
	oldest := hour
	for range t.Hours {
		pool := newRunAgg("", "")
		for used := 0; used < t.PoolHours && pool.DurCount < t.MinSamples; used++ {
			if a, ok := byHour[cursor.Unix()]; ok {
				pool.merge(a)
			}
			oldest = cursor
			cursor = cursor.Add(-time.Hour)
		}
		samples = append(samples, pool)
	}
	return samples, oldest
}

// mergeHours merges a kind's hours in [from, to).
func mergeHours(byHour map[int64]*runAgg, from, to time.Time) *runAgg {
	out := newRunAgg("", "")
	for unix, a := range byHour {
		if unix >= from.Unix() && unix < to.Unix() {
			out.merge(a)
		}
	}
	return out
}

// durationBreach reports whether a sample's success duration breaches the
// baseline: p50 against p50, or p95 against p95 once the sample is large enough
// to have a tail worth comparing.
func durationBreach(recent, base *runAgg, t AnomalyThresholds) (Anomaly, bool) {
	if base.DurCount < t.MinBaselineSamples || recent.DurCount < t.MinSamples {
		return Anomaly{}, false
	}
	q, metric := 0.50, "p50"
	if recent.DurCount >= t.MinSamplesP95 {
		q, metric = 0.95, "p95"
	}
	r := float64(recent.DurHist.quantile(q, recent.DurMaxMs))
	b := float64(base.DurHist.quantile(q, base.DurMaxMs))
	floor := float64(t.DurationFloor.Milliseconds())
	if r < t.DurationRatio*b || r-b < floor {
		return Anomaly{}, false
	}
	a := Anomaly{Metric: metric, Recent: r, Baseline: b, Samples: recent.DurCount, BaselineSamples: base.DurCount}
	if b > 0 {
		a.Ratio = r / b
	}
	return a, true
}

// failureBreach reports whether an hour's discard rate breaches the baseline's.
func failureBreach(hour, base *runAgg, t AnomalyThresholds) (Anomaly, bool) {
	if hour == nil || hour.Discarded < t.MinDiscards {
		return Anomaly{}, false
	}
	baseJobs := base.Success + base.Discarded
	if baseJobs < t.MinBaselineJobs {
		return Anomaly{}, false
	}
	jobs := hour.Success + hour.Discarded
	r := float64(hour.Discarded) / float64(jobs)
	b := float64(base.Discarded) / float64(baseJobs)
	if r < t.FailureRatio*b || r-b < t.FailureDelta {
		return Anomaly{}, false
	}
	a := Anomaly{Metric: "discard_rate", Recent: r, Baseline: b, Samples: jobs, BaselineSamples: baseJobs}
	if b > 0 {
		a.Ratio = r / b
	}
	return a, true
}
