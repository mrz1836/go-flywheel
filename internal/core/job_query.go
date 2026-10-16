package core

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/mrz1836/go-foundation/models"
	"gorm.io/gorm"
)

// JobSort is the order QueryJobs lists jobs in. Every sort is broken by the job
// id, so a page boundary never falls between two rows it cannot tell apart.
type JobSort string

const (
	// JobSortNewest lists jobs by id, descending: UUIDv7 ids are insert order,
	// so this is newest first. It is the zero Sort.
	JobSortNewest JobSort = "newest"
	// JobSortOldest lists jobs by id, ascending: oldest first.
	JobSortOldest JobSort = "oldest"
	// JobSortFinished lists terminal jobs by finalized_at, then id, both
	// descending: the most recently finished first. Its States must be terminal
	// only.
	JobSortFinished JobSort = "finished"
	// JobSortScheduled lists waiting jobs by scheduled_at, then id, both
	// ascending: the next to become claimable first. Its States must be
	// non-terminal only.
	JobSortScheduled JobSort = "scheduled"
)

// MaxContainmentWindow is the widest created window a containment filter
// (JobQuery.ArgsContains, JobQuery.MetadataContains) or a text search
// (JobQuery.ErrorContains) may search.
const MaxContainmentWindow = 7 * 24 * time.Hour

// JobQuery filters, orders, and pages a QueryJobs read. Fields combine with
// AND; the values within one field combine with OR. The zero value lists the
// newest 50 jobs of every kind, queue, and state.
type JobQuery struct {
	// IDs matches these exact job ids, at most MaxReadLimit of them.
	IDs []string
	// States matches a job in any of these states.
	States []JobState
	// Kinds matches a job of any of these kinds.
	Kinds []string
	// Queues matches a job on any of these queues.
	Queues []string
	// Tags matches a job carrying any of these tags.
	Tags []string
	// ParentJobID matches the children of this job.
	ParentJobID string
	// PriorityMin and PriorityMax bound the job's priority, inclusively; nil
	// leaves that end open.
	PriorityMin *int
	PriorityMax *int
	// AttemptMin and AttemptMax bound the job's attempt counter, inclusively;
	// nil leaves that end open.
	AttemptMin *int
	AttemptMax *int
	// CreatedFrom and CreatedTo bound when the job was inserted, [CreatedFrom,
	// CreatedTo), at millisecond precision. They are applied to the id, whose
	// UUIDv7 prefix is its insert time, so the primary key serves them, and a job
	// inserted under a simulated clock is found where its id puts it. A zero end
	// is open.
	CreatedFrom time.Time
	CreatedTo   time.Time
	// ScheduledFrom and ScheduledTo bound scheduled_at, [ScheduledFrom,
	// ScheduledTo). A zero end is open.
	ScheduledFrom time.Time
	ScheduledTo   time.Time
	// FinalizedFrom and FinalizedTo bound finalized_at, [FinalizedFrom,
	// FinalizedTo); a job with no finalized_at never matches a set bound. A zero
	// end is open.
	FinalizedFrom time.Time
	FinalizedTo   time.Time
	// ArgsContains matches a job whose args contain this JSON object
	// (PostgreSQL's @>). It needs CreatedFrom, and a created window of at most
	// MaxContainmentWindow, CreatedTo zero reading as the context clock's now; on
	// SQLite it returns ErrUnsupportedDialect.
	ArgsContains json.RawMessage
	// MetadataContains matches a job whose metadata contains this JSON object,
	// under ArgsContains' rules. A NULL metadata never matches.
	MetadataContains json.RawMessage
	// ErrorClasses matches a job whose latest attempt failed with any of these
	// error classes. A job's latest attempt is the run of the attempt its counter
	// names (jobs.attempt), so a job never claimed, or whose current attempt is
	// still running, matches none of the latest-run filters.
	ErrorClasses []ErrorClass
	// Fingerprint matches a job whose latest attempt stored this error
	// fingerprint (see ErrorFingerprint): 16 lowercase hex characters. A run
	// written before the runtime stored fingerprints never matches.
	Fingerprint string
	// ErrorContains matches a job whose latest attempt's error message contains
	// this text, in any case; %, _, and \ in it match only themselves. It needs
	// CreatedFrom, and a created window of at most MaxContainmentWindow, under
	// ArgsContains' rules; on SQLite it returns ErrUnsupportedDialect.
	ErrorContains string
	// Sort is the order of the page; empty is JobSortNewest.
	Sort JobSort
	// Cursor is the NextCursor of the previous page; empty reads the first page.
	// It is bound to the sort that issued it.
	Cursor string
	// Limit caps the page: zero or negative selects 50, and a larger value than
	// MaxReadLimit reads MaxReadLimit.
	Limit int
}

// JobListItem is one job of a QueryJobs page: its JobView, its metadata as
// stored, and a summary of its current attempt's run. It never carries the args.
type JobListItem struct {
	JobView
	// Metadata is the job's metadata object as stored; {} when it is empty or
	// NULL.
	Metadata json.RawMessage `json:"metadata"`
	// LatestRun summarizes the run of the attempt the job names (jobs.attempt);
	// nil before the first claim, or when that claim wrote no run.
	LatestRun *RunSummary `json:"latest_run,omitempty"`
}

// RunSummary is the short form of a job's run a list shows.
type RunSummary struct {
	RunID      string `json:"run_id"`
	Attempt    int    `json:"attempt"`
	Outcome    string `json:"outcome"`
	ErrorClass string `json:"error_class,omitempty"`
	// Error is the run's error message up to its first line break, cut at 256
	// bytes on a rune boundary; empty when the run recorded none.
	Error      string     `json:"error,omitempty"`
	StartedAt  time.Time  `json:"started_at"`
	FinishedAt *time.Time `json:"finished_at,omitempty"`
	DurationMs *int       `json:"duration_ms,omitempty"`
}

// JobPage is one page of a QueryJobs read.
type JobPage struct {
	// Items is the page, in the query's sort order; empty, never nil, when no job
	// matches.
	Items []JobListItem `json:"items"`
	// NextCursor reads the next page when passed as JobQuery.Cursor with the same
	// sort; empty on the last page.
	NextCursor string `json:"next_cursor,omitempty"`
}

// defaultQueryLimit is a QueryJobs page when JobQuery.Limit is not positive.
const defaultQueryLimit = 50

// maxRunErrorBytes is the most of a run's error a RunSummary carries.
const maxRunErrorBytes = 256

// QueryJobs lists the jobs q matches, one page at a time, reading through db as
// given, with no transaction of its own. It is the read behind a filtered job
// list: every field of q narrows the match, a field's values widen it, and
// soft-deleted jobs never match. Each item carries the job's view, its metadata,
// and its current attempt's run in summary; no statement it runs loads args.
//
// # Sorts, and the index each wants
//
// JobSortNewest and JobSortOldest walk the primary key (UUIDv7 ids are insert
// order), and a created window is a range of it. A filter whose matches are
// common among the newest jobs reads a short stretch of that walk; one whose
// matches are rare there, or long past, walks far without its explorer index
// (ExplorerIndexSet: one kind, one queue, one state, or a tag on PostgreSQL)
// and reads its page straight off the index with one. JobSortFinished reads one
// range of jobs_finished per distinct state and merges them, as ListFinished
// does. JobSortScheduled sorts the jobs its states and filter select, which
// jobs_ready narrows to the claimable ones within a queue, so its cost grows
// with that backlog. A parent filter reads jobs_parent_state. No query needs an
// explorer index to be correct, only to be fast: without one, the host's
// statement timeout bounds the read. docs/BENCHMARKS.md measures each shape at
// a million jobs.
//
// The latest-run filters (ErrorClasses, Fingerprint, ErrorContains) are one
// EXISTS over each candidate's latest run, a probe of job_runs_job_attempt per
// job the other filters let through, so they are cheapest beside a created
// window, a kind, or a state that narrows the candidates first.
//
// # Pages
//
// A page holds at most q.Limit items (50 when zero, never more than
// MaxReadLimit). NextCursor, set only when another page exists, continues the
// same sort: the id breaks every tie, so no row is skipped or repeated across
// pages. A cursor is bound to its sort and not to the filter.
//
// # Refusals
//
// Before any statement runs, it returns a *ValidationError naming its field for
// an unknown state (states) or sort (sort); a minimum above its maximum
// (priority, attempt); an empty or inverted window (created, scheduled,
// finalized); more than MaxReadLimit ids (ids); JobSortFinished unless States is
// non-empty and terminal only, or JobSortScheduled unless it is non-empty and
// non-terminal only (states); a containment filter that is not a JSON object, or
// whose created window is open or wider than MaxContainmentWindow
// (args_contains, metadata_contains); an unknown error class (error_classes); a
// fingerprint that is not 16 lowercase hex characters (fingerprint); an
// ErrorContains whose created window is open or wider than MaxContainmentWindow
// (error_contains); and a cursor that is malformed or from another sort
// (cursor).
//
// # SQLite
//
// The containment filters and ErrorContains return ErrUnsupportedDialect. A tag
// filter reads json_each with no index. Scheduled and finalized windows, the
// time sorts, and their cursors compare as text, in the zone the jobs rows were
// written in, as ListFinished's do: rows written in another offset can order out
// of time order. PostgreSQL compares instants.
func QueryJobs(ctx context.Context, db *gorm.DB, q JobQuery) (JobPage, error) {
	if err := validateJobFilter(ctx, q, db.Name()); err != nil {
		return JobPage{}, err
	}
	sortBy, cursor, err := validateJobSort(q)
	if err != nil {
		return JobPage{}, err
	}
	limit := readLimit(q.Limit, defaultQueryLimit)

	var rows []jobListRow
	if sortBy == JobSortFinished {
		rows, err = finishedQueryRows(ctx, db, q, cursor, limit)
	} else {
		rows, err = queryRows(ctx, db, q, sortBy, cursor, limit)
	}
	if err != nil {
		return JobPage{}, fmt.Errorf("flywheel: query jobs: %w", err)
	}

	page := JobPage{Items: make([]JobListItem, 0, min(len(rows), limit))}
	if len(rows) > limit {
		next, err := encodeQueryCursor(cursorAfter(sortBy, rows[limit-1].Job))
		if err != nil {
			return JobPage{}, fmt.Errorf("flywheel: query jobs: %w", err)
		}
		page.NextCursor = next
		rows = rows[:limit]
	}
	for i := range rows {
		page.Items = append(page.Items, jobListItemFromRow(rows[i]))
	}
	return page, nil
}

// jobListRow is one row of a QueryJobs page: the job's view columns and its
// metadata, and its current attempt's run, every run column NULL when the job
// has none.
type jobListRow struct {
	Job           jobRow     `gorm:"embedded"`
	RunID         *string    `gorm:"column:run_id"`
	RunAttempt    *int       `gorm:"column:run_attempt"`
	RunOutcome    *string    `gorm:"column:run_outcome"`
	RunErrorClass *string    `gorm:"column:run_error_class"`
	RunError      *string    `gorm:"column:run_error"`
	RunStartedAt  *time.Time `gorm:"column:run_started_at"`
	RunFinishedAt *time.Time `gorm:"column:run_finished_at"`
	RunDurationMs *int       `gorm:"column:run_duration_ms"`
}

// jobListColumns is a QueryJobs page's select list: the JobView's columns and
// the metadata, each qualified with jobs. so the joined run's columns of the same
// name never capture it, and the run's summary. The error is cut in SQL to
// maxRunErrorBytes characters, so a page never carries a run's whole message.
func jobListColumns() []string {
	view := jobViewColumns()
	cols := make([]string, 0, len(view)+9)
	for _, c := range view {
		cols = append(cols, "jobs."+c)
	}
	return append(cols, "jobs.metadata",
		"r.id AS run_id", "r.attempt AS run_attempt", "r.outcome AS run_outcome",
		"r.error_class AS run_error_class",
		fmt.Sprintf("substr(r.error_message, 1, %d) AS run_error", maxRunErrorBytes),
		"r.started_at AS run_started_at", "r.finished_at AS run_finished_at", "r.duration_ms AS run_duration_ms")
}

// listStatement starts a page's statement: the jobs a filter narrows, each
// joined to the run of the attempt it names, the join ListRunning makes.
func listStatement(ctx context.Context, db *gorm.DB) *gorm.DB {
	return db.WithContext(ctx).Model(&jobRow{}).
		Select(jobListColumns()).
		Joins("LEFT JOIN job_runs r ON r.job_id = jobs.id AND r.attempt = jobs.attempt")
}

// queryRows reads one page of the newest, oldest, or scheduled sort, and one row
// more, which tells QueryJobs whether another page follows.
func queryRows(ctx context.Context, db *gorm.DB, q JobQuery, sortBy JobSort, c *queryCursor, limit int) ([]jobListRow, error) {
	stmt := applyJobFilter(listStatement(ctx, db), q, db.Name())
	switch sortBy {
	case JobSortOldest:
		if c != nil {
			stmt = stmt.Where("jobs.id > ?", c.ID)
		}
		stmt = stmt.Order("jobs.id ASC")
	case JobSortScheduled:
		if c != nil {
			stmt = stmt.Where("(jobs.scheduled_at > ? OR (jobs.scheduled_at = ? AND jobs.id > ?))", *c.Time, *c.Time, c.ID)
		}
		stmt = stmt.Order("jobs.scheduled_at ASC, jobs.id ASC")
	default:
		if c != nil {
			stmt = stmt.Where("jobs.id < ?", c.ID)
		}
		stmt = stmt.Order("jobs.id DESC")
	}
	var rows []jobListRow
	if err := stmt.Limit(limit + 1).Find(&rows).Error; err != nil {
		return nil, err
	}
	return rows, nil
}

// finishedQueryRows reads the finished sort: one range of jobs_finished per
// distinct state, each in page order and one row past the page, merged by
// finalized_at, then id, both descending. A state named twice is read once, or
// the merge would list its jobs twice. A single state IN (…) ORDER BY
// finalized_at would sort every terminal job instead.
func finishedQueryRows(ctx context.Context, db *gorm.DB, q JobQuery, c *queryCursor, limit int) ([]jobListRow, error) {
	var all []jobListRow
	for _, s := range distinctStates(q.States) {
		one := q
		one.States = []JobState{s}
		stmt := applyJobFilter(listStatement(ctx, db), one, db.Name()).Where("jobs.finalized_at IS NOT NULL")
		if c != nil {
			stmt = stmt.Where("(jobs.finalized_at < ? OR (jobs.finalized_at = ? AND jobs.id < ?))", *c.Time, *c.Time, c.ID)
		}
		var rows []jobListRow
		if err := stmt.Order("jobs.finalized_at DESC, jobs.id DESC").Limit(limit + 1).Find(&rows).Error; err != nil {
			return nil, fmt.Errorf("read %s page: %w", s, err)
		}
		all = append(all, rows...)
	}
	sort.Slice(all, func(i, j int) bool {
		a, b := all[i].Job.FinalizedAt, all[j].Job.FinalizedAt
		if !a.Equal(*b) {
			return a.After(*b)
		}
		return all[i].Job.ID > all[j].Job.ID
	})
	return all, nil
}

// cursorAfter is the cursor that continues sortBy past job.
func cursorAfter(sortBy JobSort, job jobRow) queryCursor {
	c := queryCursor{Sort: sortBy, ID: job.ID}
	switch sortBy {
	case JobSortFinished:
		c.Time = job.FinalizedAt
	case JobSortScheduled:
		at := job.ScheduledAt
		c.Time = &at
	}
	return c
}

// applyJobFilter narrows stmt, a statement on jobs, to the jobs q matches. Every
// column is qualified with jobs., so a statement that joins another table, or
// selects from one in a subquery, never resolves a name to it. q must have
// passed validateJobFilter.
//
// States, and the predicates a partial index carries, are spelled as SQL
// literals rather than bound: a planner may use a partial index only when it can
// prove the query's predicate implies the index's, which it cannot through a
// bound value (see liveStatesSQL). The states are validated JobState values, so
// inlining them carries no injection risk. Every value a caller supplies
// otherwise is bound, and the created window is bound as id values, so a host
// whose id column is a uuid compares it as one.
func applyJobFilter(stmt *gorm.DB, q JobQuery, dialect string) *gorm.DB {
	if states := distinctStates(q.States); len(states) == 1 {
		stmt = stmt.Where("jobs.state = '" + string(states[0]) + "'")
	} else if len(states) > 1 {
		quoted := make([]string, len(states))
		for i, s := range states {
			quoted[i] = "'" + string(s) + "'"
		}
		stmt = stmt.Where("jobs.state IN (" + strings.Join(quoted, ", ") + ")")
	}
	if len(q.IDs) > 0 {
		stmt = stmt.Where("jobs.id IN ?", q.IDs)
	}
	if len(q.Kinds) > 0 {
		stmt = stmt.Where("jobs.kind IN ?", q.Kinds)
	}
	if len(q.Queues) > 0 {
		stmt = stmt.Where("jobs.queue IN ?", q.Queues)
	}
	if q.ParentJobID != "" {
		stmt = stmt.Where("jobs.parent_job_id = ?", q.ParentJobID)
	}
	if q.PriorityMin != nil {
		stmt = stmt.Where("jobs.priority >= ?", *q.PriorityMin)
	}
	if q.PriorityMax != nil {
		stmt = stmt.Where("jobs.priority <= ?", *q.PriorityMax)
	}
	if q.AttemptMin != nil {
		stmt = stmt.Where("jobs.attempt >= ?", *q.AttemptMin)
	}
	if q.AttemptMax != nil {
		stmt = stmt.Where("jobs.attempt <= ?", *q.AttemptMax)
	}
	if !q.CreatedFrom.IsZero() {
		stmt = stmt.Where("jobs.id >= ?", idFloor(q.CreatedFrom))
	}
	if !q.CreatedTo.IsZero() {
		stmt = stmt.Where("jobs.id < ?", idFloor(q.CreatedTo))
	}
	if !q.ScheduledFrom.IsZero() {
		stmt = stmt.Where("jobs.scheduled_at >= ?", q.ScheduledFrom)
	}
	if !q.ScheduledTo.IsZero() {
		stmt = stmt.Where("jobs.scheduled_at < ?", q.ScheduledTo)
	}
	if !q.FinalizedFrom.IsZero() {
		stmt = stmt.Where("jobs.finalized_at >= ?", q.FinalizedFrom)
	}
	if !q.FinalizedTo.IsZero() {
		stmt = stmt.Where("jobs.finalized_at < ?", q.FinalizedTo)
	}
	if len(q.Tags) > 0 {
		stmt = applyTagFilter(stmt, q.Tags, dialect)
	}
	if len(q.ArgsContains) > 0 {
		stmt = stmt.Where("jobs.args @> CAST(? AS jsonb)", string(q.ArgsContains))
	}
	if len(q.MetadataContains) > 0 {
		stmt = stmt.Where("jobs.metadata @> CAST(? AS jsonb)", string(q.MetadataContains))
	}
	if len(q.ErrorClasses) > 0 || q.Fingerprint != "" || q.ErrorContains != "" {
		stmt = applyLatestRunFilter(stmt, q)
	}
	return stmt
}

// likeEscaper escapes the characters LIKE treats as special, so a text search
// matches the text as written under ESCAPE '\'.
//
//nolint:gochecknoglobals // a stateless replacer, built once
var likeEscaper = strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`)

// applyLatestRunFilter matches a job by the run of the attempt it names: one
// EXISTS whose every condition applies to that one row, found through
// job_runs_job_attempt, so a candidate costs one index probe and a job with no
// run matches nothing. Its alias, lr, is its own: a page's statement already
// joins the same run as r.
func applyLatestRunFilter(stmt *gorm.DB, q JobQuery) *gorm.DB {
	conds := []string{"lr.job_id = jobs.id", "lr.attempt = jobs.attempt"}
	var args []any
	if len(q.ErrorClasses) > 0 {
		classes := make([]string, len(q.ErrorClasses))
		for i, c := range q.ErrorClasses {
			classes[i] = string(c)
		}
		conds = append(conds, "lr.error_class IN ?")
		args = append(args, classes)
	}
	if q.Fingerprint != "" {
		conds = append(conds, "lr.error_fingerprint = ?")
		args = append(args, q.Fingerprint)
	}
	if q.ErrorContains != "" {
		conds = append(conds, `lr.error_message ILIKE ? ESCAPE '\'`)
		args = append(args, "%"+likeEscaper.Replace(q.ErrorContains)+"%")
	}
	return stmt.Where("EXISTS (SELECT 1 FROM job_runs lr WHERE "+strings.Join(conds, " AND ")+")", args...)
}

// applyTagFilter matches a job carrying any of tags. On PostgreSQL each tag is a
// one-element containment, the operator jobs_explore_tags' jsonb_path_ops serves,
// beside the index's own predicate as a literal. SQLite has no such index: it
// reads each candidate's tags with json_each.
func applyTagFilter(stmt *gorm.DB, tags []string, dialect string) *gorm.DB {
	if dialect == "postgres" {
		conds := make([]string, len(tags))
		args := make([]any, len(tags))
		for i, tag := range tags {
			one, _ := json.Marshal([]string{tag}) // a []string always encodes
			conds[i] = "jobs.tags @> CAST(? AS jsonb)"
			args[i] = string(one)
		}
		return stmt.Where("jobs.tags <> '[]'::jsonb").Where("("+strings.Join(conds, " OR ")+")", args...)
	}
	return stmt.Where("jobs.tags <> '[]'").
		Where("EXISTS (SELECT 1 FROM json_each(jobs.tags) WHERE json_each.value IN ?)", tags)
}

// distinctStates returns states with each repeat dropped, in order.
func distinctStates(states []JobState) []JobState {
	out := make([]JobState, 0, len(states))
	for _, s := range states {
		if !slices.Contains(out, s) {
			out = append(out, s)
		}
	}
	return out
}

// jobListItemFromRow builds a page item: the job's view, its metadata as stored
// ({} when empty or NULL), and its run's summary when it has one.
func jobListItemFromRow(r jobListRow) JobListItem {
	item := JobListItem{JobView: jobViewFromRow(r.Job), Metadata: json.RawMessage(`{}`)}
	if len(r.Job.Metadata) > 0 {
		item.Metadata = json.RawMessage(r.Job.Metadata)
	}
	if r.RunID == nil {
		return item
	}
	run := &RunSummary{
		RunID:      *r.RunID,
		Attempt:    derefInt(r.RunAttempt),
		Outcome:    derefString(r.RunOutcome),
		ErrorClass: derefString(r.RunErrorClass),
		Error:      runErrorLine(derefString(r.RunError)),
		FinishedAt: r.RunFinishedAt,
		DurationMs: r.RunDurationMs,
	}
	if r.RunStartedAt != nil {
		run.StartedAt = *r.RunStartedAt
	}
	item.LatestRun = run
	return item
}

// runErrorLine is a run's error as a list shows it: up to its first line break,
// cut at maxRunErrorBytes on a rune boundary.
func runErrorLine(s string) string {
	if i := strings.IndexAny(s, "\r\n"); i >= 0 {
		s = s[:i]
	}
	return truncate(s, maxRunErrorBytes)
}

// derefInt returns *p, or 0 when p is nil.
func derefInt(p *int) int {
	if p == nil {
		return 0
	}
	return *p
}

// validateJobFilter refuses a filter QueryJobs and CountJobs cannot run, before
// any statement does. Each refusal is a *ValidationError naming its field. A
// containment filter is validated before the dialect is checked, so a host
// learns the same field errors on both; only then does SQLite, which cannot
// express one, refuse it with ErrUnsupportedDialect.
func validateJobFilter(ctx context.Context, q JobQuery, dialect string) error {
	for _, s := range q.States {
		if !s.Valid() {
			return newValidationError("states", fmt.Sprintf("%q is not a job state", s))
		}
	}
	if q.PriorityMin != nil && q.PriorityMax != nil && *q.PriorityMin > *q.PriorityMax {
		return newValidationError("priority", "minimum is above the maximum")
	}
	if q.AttemptMin != nil && q.AttemptMax != nil && *q.AttemptMin > *q.AttemptMax {
		return newValidationError("attempt", "minimum is above the maximum")
	}
	windows := []struct {
		field    string
		from, to time.Time
	}{
		{"created", q.CreatedFrom, q.CreatedTo},
		{"scheduled", q.ScheduledFrom, q.ScheduledTo},
		{"finalized", q.FinalizedFrom, q.FinalizedTo},
	}
	for _, w := range windows {
		if !w.from.IsZero() && !w.to.IsZero() && !w.to.After(w.from) {
			return newValidationError(w.field, "window is empty or inverted: its end must be after its start")
		}
	}
	if len(q.IDs) > MaxReadLimit {
		return newValidationError("ids", fmt.Sprintf("has more than %d entries", MaxReadLimit))
	}
	for _, c := range q.ErrorClasses {
		if !c.Valid() {
			return newValidationError("error_classes", fmt.Sprintf("%q is not an error class", c))
		}
	}
	if q.Fingerprint != "" && !validFingerprint(q.Fingerprint) {
		return newValidationError("fingerprint",
			fmt.Sprintf("%q is not %d lowercase hex characters", q.Fingerprint, fingerprintLen))
	}
	if err := validateContainment(ctx, "args_contains", q.ArgsContains, q); err != nil {
		return err
	}
	if err := validateContainment(ctx, "metadata_contains", q.MetadataContains, q); err != nil {
		return err
	}
	if q.ErrorContains != "" {
		if err := validateSearchWindow(ctx, "error_contains", q); err != nil {
			return err
		}
	}
	if dialect != "postgres" && (len(q.ArgsContains) > 0 || len(q.MetadataContains) > 0) {
		return fmt.Errorf("flywheel: containment filters need PostgreSQL: %w", ErrUnsupportedDialect)
	}
	if dialect != "postgres" && q.ErrorContains != "" {
		return fmt.Errorf("flywheel: error_contains needs PostgreSQL: %w", ErrUnsupportedDialect)
	}
	return nil
}

// validateContainment refuses a containment filter that is not a JSON object, or
// that would search more than MaxContainmentWindow of created jobs (see
// validateSearchWindow). An empty value is no filter.
func validateContainment(ctx context.Context, field string, value json.RawMessage, q JobQuery) error {
	if len(value) == 0 {
		return nil
	}
	trimmed := bytes.TrimSpace(value)
	if len(trimmed) == 0 || trimmed[0] != '{' || !json.Valid(trimmed) {
		return newValidationError(field, "must be a JSON object")
	}
	return validateSearchWindow(ctx, field, q)
}

// validateSearchWindow refuses a filter, named field, that would search more
// than MaxContainmentWindow of created jobs: it needs CreatedFrom, and reads a
// zero CreatedTo as the context clock's now. The containment filters and
// ErrorContains share it.
func validateSearchWindow(ctx context.Context, field string, q JobQuery) error {
	if q.CreatedFrom.IsZero() {
		return newValidationError(field, "needs CreatedFrom: it searches a bounded created window")
	}
	to := q.CreatedTo
	if to.IsZero() {
		to = models.ClockFrom(ctx).Now(ctx)
	}
	if to.Sub(q.CreatedFrom) > MaxContainmentWindow {
		return newValidationError(field, "searches a created window wider than 7 days")
	}
	return nil
}

// validateJobSort resolves q's sort, empty reading as JobSortNewest, and decodes
// its cursor. It refuses an unknown sort, a time sort over states it cannot
// order, and a cursor that is malformed or that another sort issued. An empty
// States is refused for the finished and scheduled sorts: it means every state,
// and each of those sorts orders only one side of the terminal line.
func validateJobSort(q JobQuery) (JobSort, *queryCursor, error) {
	sortBy := q.Sort
	if sortBy == "" {
		sortBy = JobSortNewest
	}
	switch sortBy {
	case JobSortNewest, JobSortOldest:
	case JobSortFinished:
		if len(q.States) == 0 {
			return "", nil, newValidationError("states", "the finished sort needs terminal states to order")
		}
		for _, s := range q.States {
			if !isTerminalStateString(string(s)) {
				return "", nil, newValidationError("states",
					fmt.Sprintf("the finished sort orders terminal states only, and %q is not one", s))
			}
		}
	case JobSortScheduled:
		if len(q.States) == 0 {
			return "", nil, newValidationError("states", "the scheduled sort needs non-terminal states to order")
		}
		for _, s := range q.States {
			if isTerminalStateString(string(s)) {
				return "", nil, newValidationError("states",
					fmt.Sprintf("the scheduled sort orders non-terminal states only, and %q is terminal", s))
			}
		}
	default:
		return "", nil, newValidationError("sort",
			fmt.Sprintf("%q is not a sort: newest, oldest, finished, or scheduled", q.Sort))
	}
	if q.Cursor == "" {
		return sortBy, nil, nil
	}
	c, err := decodeQueryCursor(q.Cursor, sortBy)
	if err != nil {
		return "", nil, err
	}
	return sortBy, &c, nil
}

// queryCursorPrefix versions QueryJobs' cursor, so a later shape can tell its
// own cursors from this one's.
const queryCursorPrefix = "jq1."

// queryCursor is the key of the last row on a page: the sort that read it, the
// time that sort orders by (absent for the id sorts) as it was read, zone and
// all, and the id that breaks ties. It carries no filter: a cursor used with a
// changed filter reads that filter's rows past the key.
type queryCursor struct {
	Sort JobSort    `json:"s"`
	Time *time.Time `json:"t,omitempty"`
	ID   string     `json:"id"`
}

// encodeQueryCursor renders c as an opaque cursor: the prefix, then the key as
// base64url JSON without padding.
func encodeQueryCursor(c queryCursor) (string, error) {
	body, err := json.Marshal(c)
	if err != nil {
		return "", fmt.Errorf("encode cursor: %w", err)
	}
	return queryCursorPrefix + base64.RawURLEncoding.EncodeToString(body), nil
}

// decodeQueryCursor reads a cursor QueryJobs issued for sortBy, refusing another
// prefix, a body that does not decode, an empty id, a time sort's cursor without
// its time, and a cursor another sort issued.
func decodeQueryCursor(raw string, sortBy JobSort) (queryCursor, error) {
	malformed := newValidationError("cursor", "is malformed: pass the NextCursor of a previous page")
	body, ok := strings.CutPrefix(raw, queryCursorPrefix)
	if !ok {
		return queryCursor{}, malformed
	}
	data, err := base64.RawURLEncoding.DecodeString(body)
	if err != nil {
		return queryCursor{}, malformed
	}
	var c queryCursor
	if err := json.Unmarshal(data, &c); err != nil || c.ID == "" {
		return queryCursor{}, malformed
	}
	if c.Sort != sortBy {
		return queryCursor{}, newValidationError("cursor",
			fmt.Sprintf("was issued for another sort than %s", sortBy))
	}
	if (sortBy == JobSortFinished || sortBy == JobSortScheduled) && c.Time == nil {
		return queryCursor{}, malformed
	}
	return c, nil
}
