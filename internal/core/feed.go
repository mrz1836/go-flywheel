package core

import (
	"context"
	"encoding/base64"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/mrz1836/go-foundation/models"
	"gorm.io/gorm"
)

// DefaultFeedSettle is how much of the newest finish log ListRunFinishes holds
// back when ListRunFinishesParams.Settle is zero.
const DefaultFeedSettle = 5 * time.Second

// feedScanLimit is the most finish-log entries one ListRunFinishes page scans.
const feedScanLimit = 1000

// defaultFeedLimit is a ListRunFinishes page when Limit is not positive.
const defaultFeedLimit = 100

// feedCursorPrefix versions ListRunFinishes' cursor, so a later shape can tell
// its own cursors from this one's.
const feedCursorPrefix = "rf1."

// RunFinish is one finished run as the finish feed reports it: the finish-log
// entry and what its run recorded.
type RunFinish struct {
	FinishedAt time.Time `json:"finished_at"`
	RunID      string    `json:"run_id"`
	JobID      string    `json:"job_id"`
	// Kind and Queue are the run's, resolved through its job for a row an older
	// binary wrote without them.
	Kind    string `json:"kind"`
	Queue   string `json:"queue"`
	Attempt int    `json:"attempt"`
	Outcome string `json:"outcome"`
	// JobState is the state the attempt's finalize applied; empty when it was
	// superseded or the row predates the column.
	JobState   string `json:"job_state,omitempty"`
	ErrorClass string `json:"error_class,omitempty"`
	// DurationMs is nil for an attempt the lease sweep crashed.
	DurationMs *int `json:"duration_ms,omitempty"`
	// Superseded reports that the attempt's outcome was recorded but never
	// applied to its job.
	Superseded bool `json:"superseded"`
}

// ListRunFinishesParams configures a ListRunFinishes page. The zero value reads
// up to 100 finishes from the horizon on.
type ListRunFinishesParams struct {
	// After is the NextCursor of the previous page; empty starts at Since.
	After string
	// Since is where a read without a cursor starts, a finish at Since included;
	// zero starts at the horizon. It is ignored when After is set.
	Since time.Time
	// Settle is how much of the newest finish log a page holds back: a finish
	// newer than the context clock's now minus Settle waits for a later call.
	// Zero selects DefaultFeedSettle; a negative value is refused.
	Settle time.Duration
	// Kinds and Queues keep the finishes of any of these kinds and queues; empty
	// keeps every one.
	Kinds  []string
	Queues []string
	// Limit caps the finishes a page returns: zero or negative selects 100, and a
	// larger value than MaxReadLimit reads MaxReadLimit.
	Limit int
}

// RunFinishPage is one page of the finish feed.
type RunFinishPage struct {
	// Finishes is the page, ascending by finish time, then run id; empty, never
	// nil, when nothing new has settled.
	Finishes []RunFinish `json:"finishes"`
	// NextCursor is always set: pass it as After to poll for what finishes next.
	NextCursor string `json:"next_cursor"`
	// Horizon is the context clock's now minus Settle when the page was read: no
	// finish newer than it is on the page.
	Horizon time.Time `json:"horizon"`
}

// ListRunFinishes reads the finish log in order, one page at a time, through db
// as given and with no transaction of its own. It is the feed a console polls for
// what just finished: each call returns the finishes after its cursor, and its
// NextCursor is where the next call resumes, so every finish is read once.
//
// # The settle window
//
// A finalize stamps its run's finish before it commits, so a commit can land
// behind a reader that has already read past its finish time. A page therefore
// stops at the horizon, the context clock's now minus Settle (5 seconds by
// default): a finish younger than that waits for a later call, and a finalize
// that commits within Settle of its finish time is never passed by the cursor. A
// host whose commits can lag longer raises Settle.
//
// # Cost
//
// A page reads one range of job_run_finishes' primary key, at most 1,000
// entries, and each entry's run by primary key. Kinds and Queues are applied to
// those entries, and the cursor moves past every one scanned, so a filter that
// matches rarely returns a short or empty page rather than a long scan; poll
// again from its cursor. NextCursor follows the last finish returned when the
// page filled, else the last entry scanned, else the page's start. An entry
// whose run retention has deleted is scanned and skipped.
//
// # Refusals
//
// Before any statement runs, it returns a *ValidationError for a cursor that is
// not one this function issued (after) and for a negative Settle (settle).
func ListRunFinishes(ctx context.Context, db *gorm.DB, p ListRunFinishesParams) (RunFinishPage, error) {
	if p.Settle < 0 {
		return RunFinishPage{}, newValidationError("settle", "must not be negative")
	}
	settle := p.Settle
	if settle == 0 {
		settle = DefaultFeedSettle
	}
	horizon := models.ClockFrom(ctx).Now(ctx).Add(-settle).UTC()
	start := feedPos{At: horizon}
	switch {
	case p.After != "":
		pos, err := decodeFeedCursor(p.After)
		if err != nil {
			return RunFinishPage{}, err
		}
		start = pos
	case !p.Since.IsZero():
		start = feedPos{At: p.Since.UTC()}
	}
	limit := readLimit(p.Limit, defaultFeedLimit)

	entries, err := scanFinishes(ctx, db, start, horizon)
	if err != nil {
		return RunFinishPage{}, fmt.Errorf("flywheel: list run finishes: %w", err)
	}
	page := RunFinishPage{Finishes: make([]RunFinish, 0, min(len(entries), limit)), Horizon: horizon}
	next := start
	if len(entries) > 0 {
		last := entries[len(entries)-1]
		next = feedPos{At: last.FinishedAt, RunID: last.RunID}
	}
	for _, e := range entries {
		if !e.found || !feedMatches(p.Kinds, e.Kind) || !feedMatches(p.Queues, e.Queue) {
			continue
		}
		page.Finishes = append(page.Finishes, e.RunFinish)
		if len(page.Finishes) == limit {
			next = feedPos{At: e.FinishedAt, RunID: e.RunID}
			break
		}
	}
	page.NextCursor = encodeFeedCursor(next)
	return page, nil
}

// feedMatches reports whether value is any of want, an empty want matching all.
func feedMatches(want []string, value string) bool {
	return len(want) == 0 || slices.Contains(want, value)
}

// feedEntry is one scanned finish-log entry: the finish, and found false when
// its run is gone.
type feedEntry struct {
	RunFinish

	found bool
}

// feedScanSQL reads the entries after a position, up to the horizon, at most
// feedScanLimit of them, each with its run. The inner range walks the primary
// key alone: finished_at >= the position's time starts the range, which the OR
// that follows cannot do on its own, and the OR drops the entries at that time
// up to the position's run id. The LEFT JOIN keeps an entry whose run is gone,
// so the cursor can pass it.
const feedScanSQL = `SELECT f.finished_at, f.run_id, r.id, r.job_id, ` +
	resolvedKindExpr + ` AS kind, ` + resolvedQueueExpr + ` AS queue, ` +
	`r.attempt, r.outcome, r.job_state, r.error_class, r.duration_ms, ` +
	`CASE WHEN r.superseded THEN 1 ELSE 0 END AS sup ` +
	`FROM (SELECT finished_at, run_id FROM job_run_finishes ` +
	`WHERE finished_at >= ? AND (finished_at > ? OR (finished_at = ? AND run_id > ?)) AND finished_at <= ? ` +
	`ORDER BY finished_at, run_id LIMIT ?) f ` +
	`LEFT JOIN job_runs r ON r.id = f.run_id ` +
	`ORDER BY f.finished_at, f.run_id`

// scanFinishes runs feedScanSQL from start to horizon, both bound in UTC: the
// zone the finish log is written in, which SQLite compares as text.
func scanFinishes(ctx context.Context, db *gorm.DB, start feedPos, horizon time.Time) ([]feedEntry, error) {
	rows, err := db.WithContext(ctx).Raw(feedScanSQL, start.At, start.At, start.At, start.RunID, horizon, feedScanLimit).Rows()
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []feedEntry
	for rows.Next() {
		var (
			finished                        any
			runID                           string
			id, jobID, kind, queue, outcome *string
			jobState, errorClass            *string
			attempt, duration               *int
			superseded                      int64
		)
		if err := rows.Scan(&finished, &runID, &id, &jobID, &kind, &queue, &attempt, &outcome,
			&jobState, &errorClass, &duration, &superseded); err != nil {
			return nil, fmt.Errorf("scan: %w", err)
		}
		at, err := scannedTime(finished)
		if err != nil {
			return nil, err
		}
		e := feedEntry{RunFinish: RunFinish{FinishedAt: at, RunID: runID}, found: id != nil}
		if e.found {
			e.JobID = derefString(jobID)
			e.Kind = derefString(kind)
			e.Queue = derefString(queue)
			e.Attempt = derefInt(attempt)
			e.Outcome = derefString(outcome)
			e.JobState = derefString(jobState)
			e.ErrorClass = derefString(errorClass)
			e.Superseded = superseded != 0
			e.DurationMs = duration
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// feedPos is a position in the finish log: the entries after it are those with
// a later finish time, or the same one and a greater run id.
type feedPos struct {
	At    time.Time
	RunID string
}

// encodeFeedCursor renders p as an opaque cursor: the prefix, then the finish
// time in Unix nanoseconds and the run id, as unpadded base64url.
func encodeFeedCursor(p feedPos) string {
	body := strconv.FormatInt(p.At.UnixNano(), 10) + ":" + p.RunID
	return feedCursorPrefix + base64.RawURLEncoding.EncodeToString([]byte(body))
}

// decodeFeedCursor reads a cursor encodeFeedCursor issued, refusing any other.
func decodeFeedCursor(raw string) (feedPos, error) {
	malformed := newValidationError("after", "is malformed: pass the NextCursor of a previous page")
	body, ok := strings.CutPrefix(raw, feedCursorPrefix)
	if !ok {
		return feedPos{}, malformed
	}
	data, err := base64.RawURLEncoding.DecodeString(body)
	if err != nil {
		return feedPos{}, malformed
	}
	ns, runID, ok := strings.Cut(string(data), ":")
	if !ok {
		return feedPos{}, malformed
	}
	n, err := strconv.ParseInt(ns, 10, 64)
	if err != nil {
		return feedPos{}, malformed
	}
	return feedPos{At: time.Unix(0, n).UTC(), RunID: runID}, nil
}
