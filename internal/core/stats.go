package core

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"gorm.io/gorm"
)

// defaultMaxRawSpan bounds how much raw job_runs history one stats read
// aggregates when StatsParams or SeriesParams leaves MaxRawSpan zero.
const defaultMaxRawSpan = 7 * 24 * time.Hour

// DurationSummary describes a distribution of durations: how many were counted,
// their mean, three percentiles, and the maximum. Percentiles are estimated from
// the stored histograms — within a few percent, and never above Max, which is
// exact. A zero Count means nothing was counted and every other field is zero.
type DurationSummary struct {
	Count int64         `json:"count"`
	Avg   time.Duration `json:"avg"`
	P50   time.Duration `json:"p50"`
	P95   time.Duration `json:"p95"`
	P99   time.Duration `json:"p99"`
	Max   time.Duration `json:"max"`
}

// OutcomeCounts counts attempts by recorded outcome. Superseded attempts —
// whose outcome was recorded but never applied to the job — are counted in
// KindStats.Superseded and not here.
type OutcomeCounts struct {
	Success   int64 `json:"success"`
	Error     int64 `json:"error"`
	Timeout   int64 `json:"timeout"`
	Snooze    int64 `json:"snooze"`
	Cancelled int64 `json:"cancelled"`
	Crashed   int64 `json:"crashed"`
}

// KindStats is one job kind's activity over a window, counted by the instant
// each attempt finished.
type KindStats struct {
	// Kind is the job kind; empty on StatsResult.Total and on a series point that
	// spans every kind.
	Kind string `json:"kind"`
	// Attempts is every finished, non-superseded attempt, by outcome below.
	Attempts int64         `json:"attempts"`
	Outcomes OutcomeCounts `json:"outcomes"`
	// Succeeded and Discarded are job-level outcomes: jobs that finished
	// successfully, and jobs that failed for good — a permanent error, or a
	// retryable one on the last attempt.
	Succeeded int64 `json:"succeeded"`
	Discarded int64 `json:"discarded"`
	// Retries counts attempts after which the job ran again: failed attempts that
	// did not discard it, plus crashed attempts the lease sweep reclaimed.
	Retries int64 `json:"retries"`
	// Superseded counts attempts that finished after their claim was gone — a
	// cancel or a lease reclaim underneath them — so the work may have run twice.
	Superseded int64 `json:"superseded"`
	// SuccessRate is Succeeded / (Succeeded + Discarded): the share of jobs that
	// ended well among those that ended. It is zero when none ended.
	SuccessRate float64 `json:"success_rate"`
	// Duration is successful attempts' run time only, so a burst of fast failures
	// cannot read as a speed-up.
	Duration DurationSummary `json:"duration"`
	// QueueWait is how long attempts waited for a worker after becoming
	// claimable.
	QueueWait DurationSummary `json:"queue_wait"`
	// CostMicros sums the attempts' Result.CostMicros.
	CostMicros int64 `json:"cost_micros"`
	// SlowestRunID is the job_runs id of the slowest successful attempt, for a
	// drill-down through ListRuns; empty when there was none.
	SlowestRunID string `json:"slowest_run_id,omitempty"`
}

// StatsCoverage reports how a stats read was served.
type StatsCoverage struct {
	// RolledThrough is the rollup watermark when the read ran: whole hours before
	// it were served from job_stats_hourly. Zero when nothing has been rolled up.
	RolledThrough time.Time `json:"rolled_through"`
	// RawFrom and RawTo bound the tail of the window aggregated from raw job_runs
	// — the part past the watermark, or the whole window when no rollup covers
	// it. They are equal when no tail was read.
	RawFrom time.Time `json:"raw_from"`
	RawTo   time.Time `json:"raw_to"`
	// RawSpan is the total raw span aggregated: the tail, plus the partial hour
	// at the window's start when From is not on an hour boundary.
	RawSpan time.Duration `json:"raw_span"`
}

// StatsParams scopes a Stats read. From and To are required.
type StatsParams struct {
	// From and To bound the window: attempts whose finished_at is in [From, To).
	From, To time.Time
	// Kind and Queue, when set, restrict the read to one kind and one queue.
	Kind  string
	Queue string
	// MaxRawSpan caps the raw job_runs history one read aggregates. Zero selects
	// 7 days; a negative value removes the cap. A window that would exceed it
	// fails with ErrStatsNotRolledUp rather than scanning a month of raw runs.
	MaxRawSpan time.Duration
}

// StatsResult is Stats' answer: per-kind statistics, their total, and how the
// window was served.
type StatsResult struct {
	From time.Time `json:"from"`
	To   time.Time `json:"to"`
	// Kinds holds one entry per kind with activity in the window, sorted by kind.
	Kinds []KindStats `json:"kinds"`
	// Total sums every kind in Kinds.
	Total    KindStats     `json:"total"`
	Coverage StatsCoverage `json:"coverage"`
}

// Stats returns per-kind job statistics for a window: attempts by outcome, jobs
// succeeded and discarded, retries and superseded attempts, the success rate,
// success-duration and queue-wait distributions, cost, and the slowest run.
//
// # A hybrid read
//
// Whole hours the rollup has closed are read from job_stats_hourly — one row per
// (hour, kind, queue), merged in Go. The rest — the hours past the rollup
// watermark, and a partial hour at either edge — is aggregated from job_runs by
// the same function the rollup itself runs, through the job_run_finishes log.
// Both halves count with the same histogram, so a window served half from rollups
// equals the same window served entirely raw, percentiles included.
//
// With the rollup off nothing is rolled up, and the whole window is read raw:
// correct, only slower, and capped by MaxRawSpan so a dashboard asking for a
// month does not scan a month of runs. The fix ErrStatsNotRolledUp names is to
// enable the rollup or backfill with RebuildStats.
//
// Counts reflect raw history only as far back as retention keeps it; rolled-up
// hours outlive retention, which is what makes month-over-month trends possible.
func Stats(ctx context.Context, db *gorm.DB, p StatsParams) (StatsResult, error) {
	if db == nil {
		return StatsResult{}, fmt.Errorf("flywheel: Stats: db is nil")
	}
	if p.From.IsZero() || p.To.IsZero() || !p.From.Before(p.To) {
		return StatsResult{}, newValidationError("stats window", "requires From before To")
	}
	f := statsFilter{Kind: p.Kind, Queue: p.Queue}
	merged, cov, err := windowAggs(ctx, db, p.From, p.To, f, rowsFor(f, true), p.MaxRawSpan)
	if err != nil {
		return StatsResult{}, fmt.Errorf("flywheel: Stats: %w", err)
	}

	byKind := map[string]*runAgg{}
	for _, a := range merged {
		k, ok := byKind[a.Kind]
		if !ok {
			k = newRunAgg(a.Kind, "")
			byKind[a.Kind] = k
		}
		k.merge(a)
	}
	total := newRunAgg("", "")
	res := StatsResult{From: p.From, To: p.To, Kinds: make([]KindStats, 0, len(byKind)), Coverage: cov}
	for _, a := range byKind {
		res.Kinds = append(res.Kinds, kindStatsFromAgg(a))
		total.merge(a)
	}
	sort.Slice(res.Kinds, func(i, j int) bool { return res.Kinds[i].Kind < res.Kinds[j].Kind })
	res.Total = kindStatsFromAgg(total)
	return res, nil
}

// windowAggs aggregates [from, to) per (kind, queue), serving whole rolled hours
// from rollups and the rest raw. It is the shared core of Stats and of every
// series bucket that straddles the watermark.
func windowAggs(
	ctx context.Context, db *gorm.DB, from, to time.Time, f statsFilter, class rollupRows, maxRaw time.Duration,
) (map[statsGroup]*runAgg, StatsCoverage, error) {
	watermark, rolled, err := readStatsWatermark(ctx, db)
	if err != nil {
		return nil, StatsCoverage{}, err
	}
	plan := planWindow(from, to, watermark, rolled)
	if err := checkRawSpan(plan.rawSpan(), maxRaw); err != nil {
		return nil, StatsCoverage{}, err
	}

	merged := map[statsGroup]*runAgg{}
	if plan.rolledFrom.Before(plan.rolledTo) {
		hours, err := loadRolledHours(ctx, db, plan.rolledFrom, plan.rolledTo, f, class)
		if err != nil {
			return nil, StatsCoverage{}, err
		}
		for _, h := range hours {
			mergeInto(merged, h.aggs)
		}
	}
	for _, seg := range plan.raw {
		aggs, err := aggregateRuns(ctx, db, seg[0], seg[1], f)
		if err != nil {
			return nil, StatsCoverage{}, err
		}
		mergeInto(merged, aggs)
	}

	cov := StatsCoverage{RawSpan: plan.rawSpan()}
	if rolled {
		cov.RolledThrough = watermark
	}
	cov.RawFrom, cov.RawTo = plan.tail[0], plan.tail[1]
	return merged, cov, nil
}

// windowPlan splits a window into the whole hours rollups serve and the raw
// segments around them.
type windowPlan struct {
	rolledFrom, rolledTo time.Time
	// raw holds every raw segment: the partial head hour, and the tail.
	raw [][2]time.Time
	// tail is the raw segment past the rolled hours (equal bounds when none).
	tail [2]time.Time
}

// rawSpan totals the plan's raw segments.
func (p windowPlan) rawSpan() time.Duration {
	var d time.Duration
	for _, seg := range p.raw {
		d += seg[1].Sub(seg[0])
	}
	return d
}

// planWindow decides which part of [from, to) the rollups serve: the whole hours
// at or after from's next hour boundary that end at or before both to and the
// watermark. Everything else is raw. With no rollup, or no whole rolled hour
// inside the window, the whole window is raw.
func planWindow(from, to, watermark time.Time, rolled bool) windowPlan {
	from, to = from.UTC(), to.UTC()
	alignedFrom := floorHour(from)
	if alignedFrom.Before(from) {
		alignedFrom = alignedFrom.Add(time.Hour)
	}
	rolledTo := floorHour(to)
	if rolled && watermark.Before(rolledTo) {
		rolledTo = watermark
	}
	if !rolled || !alignedFrom.Before(rolledTo) {
		return windowPlan{raw: [][2]time.Time{{from, to}}, tail: [2]time.Time{from, to}}
	}
	plan := windowPlan{rolledFrom: alignedFrom, rolledTo: rolledTo, tail: [2]time.Time{rolledTo, to}}
	if from.Before(alignedFrom) {
		plan.raw = append(plan.raw, [2]time.Time{from, alignedFrom})
	}
	if rolledTo.Before(to) {
		plan.raw = append(plan.raw, [2]time.Time{rolledTo, to})
	}
	return plan
}

// checkRawSpan enforces the raw-history cap.
func checkRawSpan(span, maxRaw time.Duration) error {
	if maxRaw == 0 {
		maxRaw = defaultMaxRawSpan
	}
	if maxRaw > 0 && span > maxRaw {
		return fmt.Errorf(
			"%w: serving it would aggregate %s of raw job_runs, over the %s cap; enable the hourly rollup "+
				"(SchedulerConfig.StatsRollupInterval, or runtime.stats_rollup for `flywheel serve`) and let it catch up, "+
				"backfill with RebuildStats, or raise MaxRawSpan",
			ErrStatsNotRolledUp, span.Round(time.Minute), maxRaw,
		)
	}
	return nil
}

// mergeInto merges every aggregate in src into dst by group.
func mergeInto(dst, src map[statsGroup]*runAgg) {
	for g, a := range src {
		cur, ok := dst[g]
		if !ok {
			cur = newRunAgg(g.Kind, g.Queue)
			dst[g] = cur
		}
		cur.merge(a)
	}
}

// rolledHour is one stored hour's aggregates, keyed by group.
type rolledHour struct {
	start time.Time
	aggs  map[statsGroup]*runAgg
}

// rolledColumns is the column list loadRolledHours reads: the key, the
// counters, and the two histograms — not the percentile columns, which exist for
// direct SQL and are recomputed from the merged histograms here.
const rolledColumns = `bucket_start_unix, kind, queue, attempts, success, error, timeout, snooze, cancelled, crashed, ` +
	`superseded, discarded, dur_count, dur_sum_ms, dur_max_ms, dur_hist, slowest_run_id, ` +
	`wait_count, wait_sum_ms, wait_max_ms, wait_hist, cost_micros_sum, hist_version`

// loadRolledHours reads one class of rollup rows for hours in [from, to),
// filtered, in hour order. It is one primary-key range read.
//
// It scans by hand rather than through GORM's struct mapping, and parses the
// histograms with parseSparseHist rather than encoding/json: a month of hourly
// rows across twenty kinds is tens of thousands of rows, and reflection per
// field per row was most of what a 30-day Stats call cost.
func loadRolledHours(
	ctx context.Context, db *gorm.DB, from, to time.Time, f statsFilter, class rollupRows,
) ([]rolledHour, error) {
	query := `SELECT ` + rolledColumns + ` FROM job_stats_hourly WHERE bucket_start_unix >= ? AND bucket_start_unix < ?`
	args := []any{from.Unix(), to.Unix()}
	switch class {
	case groupRows:
		query += ` AND kind <> ? AND queue <> ?`
		args = append(args, statsTotalKey, statsTotalKey)
	case kindRows:
		query += ` AND kind <> ? AND queue = ?`
		args = append(args, statsTotalKey, statsTotalKey)
	case totalRows:
		query += ` AND kind = ? AND queue = ?`
		args = append(args, statsTotalKey, statsTotalKey)
	}
	if f.Kind != "" && class != totalRows {
		query += ` AND kind = ?`
		args = append(args, f.Kind)
	}
	if f.Queue != "" && class == groupRows {
		query += ` AND queue = ?`
		args = append(args, f.Queue)
	}
	rows, err := db.WithContext(ctx).Raw(query+` ORDER BY bucket_start_unix`, args...).Rows()
	if err != nil {
		return nil, fmt.Errorf("read stats rollups: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []rolledHour
	for rows.Next() {
		var (
			bucket            int64
			a                 runAgg
			durHist, waitHist []byte
			slowest           *string
			version           int64
		)
		if err := rows.Scan(&bucket, &a.Kind, &a.Queue, &a.Attempts, &a.Success, &a.Error, &a.Timeout,
			&a.Snooze, &a.Cancelled, &a.Crashed, &a.Superseded, &a.Discarded,
			&a.DurCount, &a.DurSumMs, &a.DurMaxMs, &durHist, &slowest,
			&a.WaitCount, &a.WaitSumMs, &a.WaitMaxMs, &waitHist, &a.CostMicrosSum, &version); err != nil {
			return nil, fmt.Errorf("read stats rollups: scan: %w", err)
		}
		if version != histVersion {
			return nil, fmt.Errorf("stats row %d/%s/%s: unsupported hist_version %d", bucket, a.Kind, a.Queue, version)
		}
		if a.DurHist, err = parseSparseHist(durHist); err != nil {
			return nil, err
		}
		if a.WaitHist, err = parseSparseHist(waitHist); err != nil {
			return nil, err
		}
		if slowest != nil {
			a.SlowestRunID = *slowest
		}
		if len(out) == 0 || out[len(out)-1].start.Unix() != bucket {
			out = append(out, rolledHour{start: time.Unix(bucket, 0).UTC(), aggs: map[statsGroup]*runAgg{}})
		}
		agg := a
		out[len(out)-1].aggs[statsGroup{Kind: a.Kind, Queue: a.Queue}] = &agg
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read stats rollups: %w", err)
	}
	return out, nil
}

// kindStatsFromAgg projects an aggregate into the public KindStats.
func kindStatsFromAgg(a *runAgg) KindStats {
	ks := KindStats{
		Kind:     a.Kind,
		Attempts: a.Attempts,
		Outcomes: OutcomeCounts{
			Success: a.Success, Error: a.Error, Timeout: a.Timeout,
			Snooze: a.Snooze, Cancelled: a.Cancelled, Crashed: a.Crashed,
		},
		Succeeded:    a.Success,
		Discarded:    a.Discarded,
		Retries:      max(a.Error+a.Timeout-a.Discarded, 0) + a.Crashed,
		Superseded:   a.Superseded,
		Duration:     summarize(a.DurHist, a.DurCount, a.DurSumMs, a.DurMaxMs),
		QueueWait:    summarize(a.WaitHist, a.WaitCount, a.WaitSumMs, a.WaitMaxMs),
		CostMicros:   a.CostMicrosSum,
		SlowestRunID: a.SlowestRunID,
	}
	if ended := ks.Succeeded + ks.Discarded; ended > 0 {
		ks.SuccessRate = float64(ks.Succeeded) / float64(ended)
	}
	return ks
}

// summarize renders a histogram and its exact count, sum, and max as a
// DurationSummary.
func summarize(h histogram, count, sumMs, maxMs int64) DurationSummary {
	if count == 0 {
		return DurationSummary{}
	}
	ms := func(v int64) time.Duration { return time.Duration(v) * time.Millisecond }
	return DurationSummary{
		Count: count,
		Avg:   time.Duration(float64(sumMs) / float64(count) * float64(time.Millisecond)),
		P50:   ms(h.quantile(0.50, maxMs)),
		P95:   ms(h.quantile(0.95, maxMs)),
		P99:   ms(h.quantile(0.99, maxMs)),
		Max:   ms(maxMs),
	}
}

// SeriesInterval is a StatsSeries bucket width.
type SeriesInterval string

// The supported series intervals.
const (
	// IntervalHour buckets by UTC hour — which is also the local hour in every
	// zone whose offset is a whole number of hours.
	IntervalHour SeriesInterval = "hour"
	// IntervalDay buckets by calendar day in SeriesParams.Location, summed from
	// hourly rollups. A day across a DST change is 23 or 25 hours long.
	IntervalDay SeriesInterval = "day"
)

// SeriesParams scopes a StatsSeries read. From and To are required.
type SeriesParams struct {
	// From and To bound the series. Buckets are whole: the first is the one
	// containing From and the last the one containing the instant before To, and
	// each counts its whole hour or day.
	From, To time.Time
	// Kind and Queue, when set, restrict every point to one kind and one queue.
	// With Kind empty each point totals every kind.
	Kind  string
	Queue string
	// Interval is the bucket width; empty selects IntervalHour.
	Interval SeriesInterval
	// Location is the zone day buckets start at midnight in, and the zone each
	// point's Start is reported in. Nil selects UTC. A zone whose offset is not a
	// whole number of hours (Asia/Kolkata, +05:30) cannot be summed from hourly
	// rollups and is rejected for IntervalDay with ErrValidation.
	Location *time.Location
	// MaxRawSpan caps the raw job_runs history the series aggregates; see
	// StatsParams.MaxRawSpan.
	MaxRawSpan time.Duration
}

// StatsPoint is one bucket of a StatsSeries.
type StatsPoint struct {
	// Start and End bound the bucket, in SeriesParams.Location.
	Start time.Time `json:"start"`
	End   time.Time `json:"end"`
	// Stats is the bucket's activity. Its Kind is SeriesParams.Kind.
	Stats KindStats `json:"stats"`
}

// maxSeriesPoints bounds a series, so a mistyped range cannot allocate millions
// of empty buckets: 400 days of hours.
const maxSeriesPoints = 400 * 24

// StatsSeries returns a time series of statistics — one point per hour or per
// day, empty buckets included, so a chart's x-axis needs no gap filling. Points
// are served exactly as Stats serves a window: rolled hours from
// job_stats_hourly, the rest raw, with the same MaxRawSpan cap.
//
// A raw bucket costs one indexed aggregate, so a series reaching past the
// watermark costs one query per raw hour (or per raw day part) on top of the one
// rollup read for everything before it.
func StatsSeries(ctx context.Context, db *gorm.DB, p SeriesParams) ([]StatsPoint, error) {
	if db == nil {
		return nil, fmt.Errorf("flywheel: StatsSeries: db is nil")
	}
	if p.From.IsZero() || p.To.IsZero() || !p.From.Before(p.To) {
		return nil, newValidationError("series window", "requires From before To")
	}
	loc := p.Location
	if loc == nil {
		loc = time.UTC
	}
	buckets, err := seriesBuckets(p.From, p.To, p.Interval, loc)
	if err != nil {
		return nil, err
	}

	watermark, rolled, err := readStatsWatermark(ctx, db)
	if err != nil {
		return nil, fmt.Errorf("flywheel: StatsSeries: %w", err)
	}
	var rawSpan time.Duration
	for _, b := range buckets {
		rawSpan += planWindow(b[0], b[1], watermark, rolled).rawSpan()
	}
	if err := checkRawSpan(rawSpan, p.MaxRawSpan); err != nil {
		return nil, fmt.Errorf("flywheel: StatsSeries: %w", err)
	}

	f := statsFilter{Kind: p.Kind, Queue: p.Queue}
	byHour := map[int64]map[statsGroup]*runAgg{}
	if rolled {
		end := buckets[len(buckets)-1][1]
		if watermark.Before(end) {
			end = watermark
		}
		hours, err := loadRolledHours(ctx, db, buckets[0][0], end, f, rowsFor(f, false))
		if err != nil {
			return nil, fmt.Errorf("flywheel: StatsSeries: %w", err)
		}
		for _, h := range hours {
			byHour[h.start.Unix()] = h.aggs
		}
	}

	points := make([]StatsPoint, len(buckets))
	for i, b := range buckets {
		total := newRunAgg(p.Kind, "")
		plan := planWindow(b[0], b[1], watermark, rolled)
		for h := plan.rolledFrom; h.Before(plan.rolledTo); h = h.Add(time.Hour) {
			for _, a := range byHour[h.Unix()] {
				total.merge(a)
			}
		}
		for _, seg := range plan.raw {
			aggs, err := aggregateRuns(ctx, db, seg[0], seg[1], f)
			if err != nil {
				return nil, fmt.Errorf("flywheel: StatsSeries: %w", err)
			}
			for _, a := range aggs {
				total.merge(a)
			}
		}
		total.Kind = p.Kind
		points[i] = StatsPoint{Start: b[0].In(loc), End: b[1].In(loc), Stats: kindStatsFromAgg(total)}
	}
	return points, nil
}

// seriesBuckets lays out the whole buckets covering [from, to).
func seriesBuckets(from, to time.Time, interval SeriesInterval, loc *time.Location) ([][2]time.Time, error) {
	var buckets [][2]time.Time
	switch interval {
	case IntervalHour, "":
		for h := floorHour(from); h.Before(to); h = h.Add(time.Hour) {
			buckets = append(buckets, [2]time.Time{h, h.Add(time.Hour)})
			if len(buckets) > maxSeriesPoints {
				return nil, newValidationError("series window", fmt.Sprintf("exceeds %d points", maxSeriesPoints))
			}
		}
	case IntervalDay:
		local := from.In(loc)
		day := time.Date(local.Year(), local.Month(), local.Day(), 0, 0, 0, 0, loc)
		for day.Before(to) {
			next := day.AddDate(0, 0, 1)
			for _, edge := range []time.Time{day, next} {
				if _, offset := edge.Zone(); offset%3600 != 0 {
					return nil, newValidationError("series location",
						fmt.Sprintf("%s is offset %+.2fh from UTC at %s; day buckets need a whole-hour offset "+
							"to be summed from hourly rollups", loc, float64(offset)/3600, edge.Format(time.DateOnly)))
				}
			}
			buckets = append(buckets, [2]time.Time{day.UTC(), next.UTC()})
			if len(buckets) > maxSeriesPoints {
				return nil, newValidationError("series window", fmt.Sprintf("exceeds %d points", maxSeriesPoints))
			}
			day = next
		}
	default:
		return nil, newValidationError("series interval", fmt.Sprintf("%q is not hour or day", interval))
	}
	return buckets, nil
}

// Baseline is one kind's recent normal: its success-duration percentiles and
// discard rate over a trailing window of rolled-up hours. It is what ListRunning
// flags a slow run against and what Anomalies compares a recent hour to.
type Baseline struct {
	Kind string `json:"kind"`
	// From and To bound the hours the baseline was computed over.
	From time.Time `json:"from"`
	To   time.Time `json:"to"`
	// Samples is the number of successful attempts the percentiles are drawn
	// from. A consumer should not trust a percentile drawn from a handful.
	Samples int64         `json:"samples"`
	P50     time.Duration `json:"p50"`
	P95     time.Duration `json:"p95"`
	P99     time.Duration `json:"p99"`
	// Jobs is jobs ended (succeeded plus discarded); DiscardRate is
	// Discarded / Jobs, zero when none ended.
	Jobs        int64   `json:"jobs"`
	Discarded   int64   `json:"discarded"`
	DiscardRate float64 `json:"discard_rate"`
}

// Baselines returns each kind's baseline over the window of rolled-up hours
// ending at the rollup watermark, sorted by kind. It reads rollups only — a
// baseline is history, and the hour in progress is what gets compared to it — so
// it returns an empty slice until the rollup has run. A non-positive window
// selects 7 days.
//
// The result is memoized per database and window, keyed by the watermark: the
// rows a baseline reads change only when the rollup closes another hour, so a
// dashboard polling ListRunning(WithBaseline) every few seconds pays one
// primary-key probe per call rather than a week of rollup rows. A RebuildStats
// in the same process clears the memo; one run from another process is picked
// up when the watermark next moves, within the hour.
func Baselines(ctx context.Context, db *gorm.DB, window time.Duration) ([]Baseline, error) {
	if db == nil {
		return nil, fmt.Errorf("flywheel: Baselines: db is nil")
	}
	if window <= 0 {
		window = 7 * 24 * time.Hour
	}
	watermark, rolled, err := readStatsWatermark(ctx, db)
	if err != nil {
		return nil, fmt.Errorf("flywheel: Baselines: %w", err)
	}
	if !rolled {
		return []Baseline{}, nil
	}
	key := baselineKey{config: db.Config, window: window}
	if cached, ok := baselineMemo.get(key, watermark); ok {
		return cached, nil
	}
	from := floorHour(watermark.Add(-window))
	hours, err := loadRolledHours(ctx, db, from, watermark, statsFilter{}, kindRows)
	if err != nil {
		return nil, fmt.Errorf("flywheel: Baselines: %w", err)
	}
	out := baselinesFrom(hours, from, watermark)
	baselineMemo.put(key, watermark, out)
	return append([]Baseline(nil), out...), nil
}

// baselineKey identifies a memoized baseline: the database — by its GORM
// configuration, which every session opened from one connection pool shares —
// and the window.
type baselineKey struct {
	config *gorm.Config
	window time.Duration
}

// baselineMemoLimit bounds the memo. A process reads baselines from one or two
// databases; the bound only keeps a test binary that opens hundreds from
// holding every one.
const baselineMemoLimit = 16

// baselineMemoEntry is one memoized result and the watermark it is valid at.
type baselineMemoEntry struct {
	watermark time.Time
	baselines []Baseline
}

// baselineCache is Baselines' memo; see Baselines for when it is valid.
type baselineCache struct {
	mu      sync.Mutex
	entries map[baselineKey]baselineMemoEntry
}

// baselineMemo is the process's memo.
//
//nolint:gochecknoglobals // process-wide memo of immutable-per-watermark data
var baselineMemo = &baselineCache{}

// get returns a copy of the memoized baselines for key at watermark.
func (c *baselineCache) get(key baselineKey, watermark time.Time) ([]Baseline, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.entries[key]
	if !ok || !e.watermark.Equal(watermark) {
		return nil, false
	}
	return append([]Baseline(nil), e.baselines...), true
}

// put memoizes baselines for key at watermark.
func (c *baselineCache) put(key baselineKey, watermark time.Time, baselines []Baseline) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.entries == nil || len(c.entries) >= baselineMemoLimit {
		c.entries = map[baselineKey]baselineMemoEntry{}
	}
	c.entries[key] = baselineMemoEntry{watermark: watermark, baselines: append([]Baseline(nil), baselines...)}
}

// clear drops every memoized baseline.
func (c *baselineCache) clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.entries = nil
}

// baselinesFrom merges hours per kind into baselines.
func baselinesFrom(hours []rolledHour, from, to time.Time) []Baseline {
	byKind := map[string]*runAgg{}
	for _, h := range hours {
		for _, a := range h.aggs {
			k, ok := byKind[a.Kind]
			if !ok {
				k = newRunAgg(a.Kind, "")
				byKind[a.Kind] = k
			}
			k.merge(a)
		}
	}
	out := make([]Baseline, 0, len(byKind))
	for kind, a := range byKind {
		out = append(out, baselineFromAgg(kind, a, from, to))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Kind < out[j].Kind })
	return out
}

// baselineFromAgg projects one kind's merged aggregate into a Baseline.
func baselineFromAgg(kind string, a *runAgg, from, to time.Time) Baseline {
	ms := func(v int64) time.Duration { return time.Duration(v) * time.Millisecond }
	b := Baseline{
		Kind: kind, From: from, To: to,
		Samples:   a.DurCount,
		P50:       ms(a.DurHist.quantile(0.50, a.DurMaxMs)),
		P95:       ms(a.DurHist.quantile(0.95, a.DurMaxMs)),
		P99:       ms(a.DurHist.quantile(0.99, a.DurMaxMs)),
		Jobs:      a.Success + a.Discarded,
		Discarded: a.Discarded,
	}
	if b.Jobs > 0 {
		b.DiscardRate = float64(b.Discarded) / float64(b.Jobs)
	}
	return b
}
