package core

import (
	"context"
	"fmt"
	"sort"
	"time"

	"gorm.io/gorm"
)

// maxSampleMessage is the most bytes of a failure's message an error group
// keeps as its sample.
const maxSampleMessage = 512

// defaultErrorGroupsLimit is how many groups ErrorGroups returns when
// ErrorGroupsParams.Limit is zero.
const defaultErrorGroupsLimit = 50

// maxHourlyWindow is the longest window ErrorGroups counts hour by hour.
const maxHourlyWindow = 7 * 24 * time.Hour

// ErrorGroup is one error group over a window: the failed attempts of one kind
// that share a fingerprint (see ErrorFingerprint). A failed attempt is one whose
// outcome is error or timeout and that was not superseded, as the stats count
// it, so a group never counts more than its kind's error and timeout counters.
type ErrorGroup struct {
	Kind        string `json:"kind"`
	Fingerprint string `json:"fingerprint"`
	// ErrorClass is the latest failure's error class; empty for a failure that
	// recorded none (a seeded one).
	ErrorClass string `json:"error_class"`
	// Failures is the group's failed attempts in the window; Discarded, those of
	// them that ended their job; Retried, the rest.
	Failures  int64 `json:"failures"`
	Discarded int64 `json:"discarded"`
	Retried   int64 `json:"retried"`
	// FirstSeen and FirstRunID are the group's earliest failure in the window or
	// in any retained rollup hour before To, so a group that started before the
	// window says so. LastSeen and LastRunID are its latest failure in the
	// window. Each time is the run's finish while the run is kept, and the start
	// of its rollup hour once retention has deleted it.
	FirstSeen  time.Time `json:"first_seen"`
	LastSeen   time.Time `json:"last_seen"`
	FirstRunID string    `json:"first_run_id,omitempty"`
	LastRunID  string    `json:"last_run_id,omitempty"`
	// SampleMessage is the latest failure's message as stored, cut at 512 bytes;
	// Message is SampleMessage normalized by NormalizeErrorMessage, the text a
	// group shows with its ids and numbers masked.
	SampleMessage string `json:"sample_message"`
	Message       string `json:"message"`
	// Hourly is the failures in each UTC hour the window touches, from the hour
	// holding From to the one holding the instant before To; set only with
	// ErrorGroupsParams.WithHourly.
	Hourly []int64 `json:"hourly,omitempty"`
}

// ErrorGroupsParams scopes an ErrorGroups read. From and To are required.
type ErrorGroupsParams struct {
	// From and To bound the window: failures whose run finished in [From, To).
	From, To time.Time
	// Kind, when set, reads one kind's groups.
	Kind string
	// Fingerprint, when set, reads one group: 16 lowercase hex characters, as
	// ErrorFingerprint returns.
	Fingerprint string
	// Limit caps the groups returned, the most failures first. Zero selects 50;
	// above MaxReadLimit reads MaxReadLimit.
	Limit int
	// WithHourly fills each group's Hourly. It requires a window of at most 7
	// days.
	WithHourly bool
	// MaxRawSpan caps the raw job_runs history the read aggregates, as
	// StatsParams.MaxRawSpan does.
	MaxRawSpan time.Duration
}

// ErrorGroupsResult is ErrorGroups' answer: the groups, the most failures first
// (then by kind and fingerprint), and how the window was served.
type ErrorGroupsResult struct {
	Groups   []ErrorGroup  `json:"groups"`
	Coverage StatsCoverage `json:"coverage"`
}

// ErrorGroups returns a window's failures grouped by kind and fingerprint: how
// many failed, how many of those ended their job, when the group was first and
// last seen, and a sample of its latest message.
//
// It is a hybrid read, planned and capped exactly as Stats is: whole hours the
// rollups cover are read from job_error_groups_hourly, which the stats rollup
// writes with each hour, and the rest — a partial hour at either edge, the hours
// past the watermark, and any older than the rollups reach — is aggregated from
// job_runs through the finish log. A window that would aggregate more than
// MaxRawSpan of raw runs fails with ErrStatsNotRolledUp. A run written before
// fingerprints were stored is grouped by the fingerprint computed from its
// message, as a read of it reports (JobRunView.ErrorFingerprint).
//
// Beyond the window, it probes each returned group's earliest retained rollup
// hour before To, one indexed read per group (job_error_groups_fingerprint), and
// reads the finish times of the first and last runs in one primary-key read.
// Hours the rollup closed before it wrote error groups hold none until
// RebuildStats re-rolls them, which works while their raw runs are kept.
//
// A zero or inverted window, a WithHourly window longer than 7 days, and a
// Fingerprint that is not 16 lowercase hex characters are refused with a
// ValidationError naming window, with_hourly, or fingerprint, before anything
// is read.
func ErrorGroups(ctx context.Context, db *gorm.DB, p ErrorGroupsParams) (ErrorGroupsResult, error) {
	if db == nil {
		return ErrorGroupsResult{}, fmt.Errorf("flywheel: ErrorGroups: db is nil")
	}
	if p.From.IsZero() || p.To.IsZero() || !p.From.Before(p.To) {
		return ErrorGroupsResult{}, newValidationError("window", "requires From before To")
	}
	if p.Fingerprint != "" && !validFingerprint(p.Fingerprint) {
		return ErrorGroupsResult{}, newValidationError("fingerprint",
			fmt.Sprintf("%q is not %d lowercase hex characters", p.Fingerprint, fingerprintLen))
	}
	if p.WithHourly && p.To.Sub(p.From) > maxHourlyWindow {
		return ErrorGroupsResult{}, newValidationError("with_hourly",
			fmt.Sprintf("requires a window of at most %s", maxHourlyWindow))
	}

	progress, _, err := readStatsProgress(ctx, db)
	if err != nil {
		return ErrorGroupsResult{}, fmt.Errorf("flywheel: ErrorGroups: %w", err)
	}
	plan := planWindow(p.From, p.To, progress)
	if err := fitRawSpan(ctx, db, []*windowPlan{&plan}, progress, p.MaxRawSpan); err != nil {
		return ErrorGroupsResult{}, fmt.Errorf("flywheel: ErrorGroups: %w", err)
	}
	merged, err := windowErrorGroups(ctx, db, plan, p)
	if err != nil {
		return ErrorGroupsResult{}, fmt.Errorf("flywheel: ErrorGroups: %w", err)
	}

	groups := make([]*errorGroupAgg, 0, len(merged))
	for _, g := range merged {
		groups = append(groups, g)
	}
	sort.Slice(groups, func(i, j int) bool {
		if groups[i].Failures != groups[j].Failures {
			return groups[i].Failures > groups[j].Failures
		}
		if groups[i].Kind != groups[j].Kind {
			return groups[i].Kind < groups[j].Kind
		}
		return groups[i].Fingerprint < groups[j].Fingerprint
	})
	if limit := readLimit(p.Limit, defaultErrorGroupsLimit); len(groups) > limit {
		groups = groups[:limit]
	}
	if err := settleSightings(ctx, db, groups, p.To); err != nil {
		return ErrorGroupsResult{}, fmt.Errorf("flywheel: ErrorGroups: %w", err)
	}

	res := ErrorGroupsResult{Groups: make([]ErrorGroup, 0, len(groups)), Coverage: StatsCoverage{RawSpan: plan.rawSpan()}}
	if progress.covered() {
		res.Coverage.RolledFrom, res.Coverage.RolledThrough = progress.From, progress.To
	}
	res.Coverage.RawFrom, res.Coverage.RawTo = plan.tail[0], plan.tail[1]
	for _, g := range groups {
		eg := ErrorGroup{
			Kind: g.Kind, Fingerprint: g.Fingerprint, ErrorClass: g.ErrorClass,
			Failures: g.Failures, Discarded: g.Discarded, Retried: g.Failures - g.Discarded,
			FirstSeen: g.First.At, LastSeen: g.Last.At, FirstRunID: g.First.ID, LastRunID: g.Last.ID,
			SampleMessage: g.Sample, Message: NormalizeErrorMessage(g.Sample),
		}
		if p.WithHourly {
			for h := floorHour(p.From); h.Before(p.To); h = h.Add(time.Hour) {
				eg.Hourly = append(eg.Hourly, g.hourly[h.Unix()])
			}
		}
		res.Groups = append(res.Groups, eg)
	}
	return res, nil
}

// windowErrorGroups merges a planned window's error groups: each raw segment
// aggregated from job_runs, and the rolled hours read from
// job_error_groups_hourly, filtered by p's kind and fingerprint.
func windowErrorGroups(
	ctx context.Context, db *gorm.DB, plan windowPlan, p ErrorGroupsParams,
) (map[errorGroupKey]*errorGroupAgg, error) {
	merged := map[errorGroupKey]*errorGroupAgg{}
	add := func(key errorGroupKey, g *errorGroupAgg) {
		if p.Fingerprint != "" && key.Fingerprint != p.Fingerprint {
			return
		}
		cur, ok := merged[key]
		if !ok {
			cur = newErrorGroupAgg(key)
			merged[key] = cur
		}
		cur.merge(g)
	}
	for _, seg := range plan.raw {
		win, err := aggregateWindow(ctx, db, seg[0], seg[1], statsFilter{Kind: p.Kind})
		if err != nil {
			return nil, err
		}
		for key, g := range win.groups {
			add(key, g)
		}
	}
	if plan.rolledFrom.Before(plan.rolledTo) {
		if err := loadErrorGroupHours(ctx, db, plan.rolledFrom, plan.rolledTo, p.Kind, p.Fingerprint, add); err != nil {
			return nil, err
		}
	}
	return merged, nil
}

// loadErrorGroupHours reads the stored error groups of the hours in [from, to),
// filtered, and hands each row to add as a one-hour aggregate whose first and
// last runs carry the start of the hour until their finish times are read. It
// scans by hand, as loadRolledHours does, and is one primary-key range read.
func loadErrorGroupHours(
	ctx context.Context, db *gorm.DB, from, to time.Time, kind, fingerprint string,
	add func(errorGroupKey, *errorGroupAgg),
) error {
	query := `SELECT bucket_start_unix, kind, fingerprint, error_class, failures, discarded, first_run_id, ` +
		`last_run_id, sample_message FROM job_error_groups_hourly WHERE bucket_start_unix >= ? AND bucket_start_unix < ?`
	args := []any{from.Unix(), to.Unix()}
	if kind != "" {
		query += ` AND kind = ?`
		args = append(args, kind)
	}
	if fingerprint != "" {
		query += ` AND fingerprint = ?`
		args = append(args, fingerprint)
	}
	rows, err := db.WithContext(ctx).Raw(query+` ORDER BY bucket_start_unix`, args...).Rows()
	if err != nil {
		return fmt.Errorf("read error groups: %w", err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var (
			bucket      int64
			key         errorGroupKey
			g           errorGroupAgg
			first, last *string
		)
		if err := rows.Scan(&bucket, &key.Kind, &key.Fingerprint, &g.ErrorClass, &g.Failures, &g.Discarded,
			&first, &last, &g.Sample); err != nil {
			return fmt.Errorf("read error groups: scan: %w", err)
		}
		hour := unixHour(bucket)
		g.Kind, g.Fingerprint = key.Kind, key.Fingerprint
		g.First, g.Last = runRef{At: hour, ID: deref(first)}, runRef{At: hour, ID: deref(last)}
		g.hourly = map[int64]int64{bucket: g.Failures}
		add(key, &g)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("read error groups: %w", err)
	}
	return nil
}

// settleSightings finishes each group's first and last sightings: it probes the
// group's earliest retained rollup hour before to, then reads, in one statement,
// the finish time of every run named only by its rollup hour, and keeps the
// probed hour's first run when it is the earlier. A run retention has deleted
// keeps the start of its hour.
func settleSightings(ctx context.Context, db *gorm.DB, groups []*errorGroupAgg, to time.Time) error {
	probes := make([]runRef, len(groups))
	probed := make([]bool, len(groups))
	pending := map[string]struct{}{}
	for i, g := range groups {
		var rows []struct {
			BucketStartUnix int64
			FirstRunID      *string
		}
		if err := db.WithContext(ctx).Raw(`SELECT bucket_start_unix, first_run_id FROM job_error_groups_hourly `+
			`WHERE kind = ? AND fingerprint = ? AND bucket_start_unix < ? ORDER BY bucket_start_unix LIMIT 1`,
			g.Kind, g.Fingerprint, to.Unix()).Scan(&rows).Error; err != nil {
			return fmt.Errorf("probe first sighting: %w", err)
		}
		if len(rows) == 1 && deref(rows[0].FirstRunID) != "" {
			probes[i], probed[i] = runRef{At: unixHour(rows[0].BucketStartUnix), ID: *rows[0].FirstRunID}, true
			pending[probes[i].ID] = struct{}{}
		}
		for _, ref := range []runRef{g.First, g.Last} {
			if !ref.exact && ref.ID != "" {
				pending[ref.ID] = struct{}{}
			}
		}
	}
	finished, err := runFinishTimes(ctx, db, pending)
	if err != nil {
		return err
	}
	settle := func(ref *runRef) {
		if at, ok := finished[ref.ID]; ok && !ref.exact {
			ref.At, ref.exact = at, true
		}
	}
	for i, g := range groups {
		settle(&g.First)
		settle(&g.Last)
		if probed[i] {
			settle(&probes[i])
			if probes[i].before(g.First) {
				g.First = probes[i]
			}
		}
	}
	return nil
}

// runFinishTimes reads the finish time of each run in ids that is still kept,
// in UTC.
func runFinishTimes(ctx context.Context, db *gorm.DB, ids map[string]struct{}) (map[string]time.Time, error) {
	out := make(map[string]time.Time, len(ids))
	if len(ids) == 0 {
		return out, nil
	}
	list := make([]string, 0, len(ids))
	for id := range ids {
		list = append(list, id)
	}
	sort.Strings(list)
	var rows []jobRunRow
	if err := db.WithContext(ctx).Model(&jobRunRow{}).Select("id, finished_at").Where("id IN ?", list).
		Find(&rows).Error; err != nil {
		return nil, fmt.Errorf("read sighting times: %w", err)
	}
	for _, r := range rows {
		if r.FinishedAt != nil {
			out[r.ID] = r.FinishedAt.UTC()
		}
	}
	return out, nil
}

// deref returns *s, or "" for nil.
func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

// errorGroupKey keys an error group: the failures of one kind that share a
// fingerprint.
type errorGroupKey struct {
	Kind, Fingerprint string
}

// runRef names one failed run and when it finished. Two are ordered by (At, ID),
// so the first and last failure of a group are the same however its window is
// split. exact is false while At is only the start of the rollup hour the run
// finished in, as a stored error group records it.
type runRef struct {
	At    time.Time
	ID    string
	exact bool
}

// before reports whether r sorts before o.
func (r runRef) before(o runRef) bool {
	return r.At.Before(o.At) || (r.At.Equal(o.At) && r.ID < o.ID)
}

// errorGroupAgg is one error group's failures over a window: how many, how
// many discarded their job, the first and last of them, the last one's class
// and message, and how many fell in each UTC hour. First and Last are set once
// Failures is.
type errorGroupAgg struct {
	Kind, Fingerprint string
	// ErrorClass and Sample are the last failure's class and message, the
	// message cut at maxSampleMessage bytes.
	ErrorClass, Sample  string
	Failures, Discarded int64
	First, Last         runRef
	// hourly counts the failures per UTC hour, keyed by the hour's Unix start.
	hourly map[int64]int64
}

// newErrorGroupAgg returns an empty aggregate for key.
func newErrorGroupAgg(key errorGroupKey) *errorGroupAgg {
	return &errorGroupAgg{Kind: key.Kind, Fingerprint: key.Fingerprint, hourly: map[int64]int64{}}
}

// observe counts one failure into g.
func (g *errorGroupAgg) observe(ref runRef, class, message string, discarded bool) {
	if g.Failures == 0 || ref.before(g.First) {
		g.First = ref
	}
	if g.Failures == 0 || g.Last.before(ref) {
		g.Last, g.ErrorClass, g.Sample = ref, class, truncate(message, maxSampleMessage)
	}
	g.Failures++
	if discarded {
		g.Discarded++
	}
	g.hourly[floorHour(ref.At).Unix()]++
}

// merge adds other into g: counts and hours add, the first failure is the
// earlier, and the last failure — with its class and sample — the later.
func (g *errorGroupAgg) merge(other *errorGroupAgg) {
	if other.Failures == 0 {
		return
	}
	if g.Failures == 0 || other.First.before(g.First) {
		g.First = other.First
	}
	if g.Failures == 0 || g.Last.before(other.Last) {
		g.Last, g.ErrorClass, g.Sample = other.Last, other.ErrorClass, other.Sample
	}
	g.Failures += other.Failures
	g.Discarded += other.Discarded
	for h, n := range other.hourly {
		g.hourly[h] += n
	}
}

// errorGroupHourRows renders one hour's error groups as the rows the rollup
// stores, in key order, so every writer of the hour inserts them in the same
// sequence.
func errorGroupHourRows(
	bucket time.Time, groups map[errorGroupKey]*errorGroupAgg, rolledAt time.Time,
) []jobErrorGroupHourlyRow {
	out := make([]jobErrorGroupHourlyRow, 0, len(groups))
	for _, g := range groups {
		first, last := g.First.ID, g.Last.ID
		out = append(out, jobErrorGroupHourlyRow{
			BucketStartUnix: bucket.Unix(), Kind: g.Kind, Fingerprint: g.Fingerprint, ErrorClass: g.ErrorClass,
			Failures: g.Failures, Discarded: g.Discarded, FirstRunID: &first, LastRunID: &last,
			SampleMessage: g.Sample, RolledAt: rolledAt.UTC(),
		})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Kind != out[j].Kind {
			return out[i].Kind < out[j].Kind
		}
		return out[i].Fingerprint < out[j].Fingerprint
	})
	return out
}
