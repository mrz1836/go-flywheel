package core

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/mrz1836/go-foundation/models"
	"gorm.io/gorm"
)

// defaultNodeStartsLimit is a ListNodeStarts page's size when its Limit is zero.
const defaultNodeStartsLimit = 100

// nodeStartsCursorPrefix marks a ListNodeStarts cursor, so a cursor from
// another read is refused rather than misread.
const nodeStartsCursorPrefix = "ns1."

// NodeStart is one node start, read from a job_nodes row: the deploy event a
// registering Node's process start is. Every row is one, live or stopped.
type NodeStart struct {
	NodeID  string `json:"node_id"`
	Version string `json:"version,omitempty"`
	Commit  string `json:"commit,omitempty"`
	// ExecutorClass is the node's first runner's, from the row's runners; empty
	// when it ran none.
	ExecutorClass string    `json:"executor_class,omitempty"`
	StartedAt     time.Time `json:"started_at"`
}

// NodeStartsParams bounds a ListNodeStarts page.
type NodeStartsParams struct {
	// Since lists the starts at or after it; zero reads from the oldest row
	// kept.
	Since time.Time
	// Until lists the starts before it; zero reads to now, by the context's
	// clock.
	Until time.Time
	// After is the NextCursor of the previous page; empty reads from Since.
	After string
	// Limit caps the page. Zero (or negative) selects 100; above MaxReadLimit
	// reads MaxReadLimit.
	Limit int
}

// NodeStartsPage is one page of node starts, oldest first.
type NodeStartsPage struct {
	Starts []NodeStart `json:"starts"`
	// NextCursor reads the next page as NodeStartsParams.After; it is empty on
	// the last page.
	NextCursor string `json:"next_cursor,omitempty"`
}

// nodeStartsCursor is what a ListNodeStarts cursor encodes: the last start's
// started_at, as read, and its node id.
type nodeStartsCursor struct {
	StartedAt string `json:"t"`
	ID        string `json:"i"`
}

// ListNodeStarts pages through every node start whose started_at falls in
// [Since, Until), oldest first, by (started_at, id): one entry per job_nodes
// row, live or stopped, so a deploy marker can be drawn for every process that
// started in a window, past the one page ListNodes returns. A walk that follows
// NextCursor sees each row present when it began exactly once, never skipping
// or repeating one, and a row written behind the cursor meanwhile is not
// returned.
//
// It reads job_nodes alone, by its started_at, one row past the page to know
// whether another follows; the table holds one row per registering process
// start and is pruned by SchedulerConfig.NodeRetention, so a window reaches no
// further back than that retention (seven days by default), and the read adds
// no index. The window and the cursor compare started_at in UTC, as the
// registry writes it.
//
// An Until at or before a non-zero Since is refused with a *ValidationError
// naming "until", and a cursor this read did not issue with one naming
// "after".
func ListNodeStarts(ctx context.Context, db *gorm.DB, p NodeStartsParams) (NodeStartsPage, error) {
	if db == nil {
		return NodeStartsPage{}, fmt.Errorf("flywheel: ListNodeStarts: db is nil")
	}
	if !p.Since.IsZero() && !p.Until.IsZero() && !p.Until.After(p.Since) {
		return NodeStartsPage{}, newValidationError("until", "must be after since")
	}
	var after *nodeStartsCursor
	if p.After != "" {
		c, ok := decodeNodeStartsCursor(p.After)
		if !ok {
			return NodeStartsPage{}, newValidationError("after", "is not a cursor from ListNodeStarts")
		}
		after = &c
	}
	limit := p.Limit
	switch {
	case limit <= 0:
		limit = defaultNodeStartsLimit
	case limit > MaxReadLimit:
		limit = MaxReadLimit
	}
	until := p.Until
	if until.IsZero() {
		until = models.ClockFrom(ctx).Now(ctx)
	}

	q := db.WithContext(ctx).Model(&jobNodeRow{}).Select("id, version, commit_hash, runners, started_at").
		Where("started_at < ?", until.UTC())
	if !p.Since.IsZero() {
		q = q.Where("started_at >= ?", p.Since.UTC())
	}
	if after != nil {
		at, _ := time.Parse(time.RFC3339Nano, after.StartedAt) // decodeNodeStartsCursor parsed it
		at = at.UTC()
		q = q.Where("(started_at > ? OR (started_at = ? AND id > ?))", at, at, after.ID)
	}
	var rows []jobNodeRow
	if err := q.Order("started_at, id").Limit(limit + 1).Find(&rows).Error; err != nil {
		return NodeStartsPage{}, fmt.Errorf("flywheel: list node starts: %w", err)
	}

	page := NodeStartsPage{Starts: make([]NodeStart, 0, min(len(rows), limit))}
	for i, row := range rows {
		if i == limit {
			page.NextCursor = encodeNodeStartsCursor(rows[limit-1])
			break
		}
		start, err := nodeStartFromRow(row)
		if err != nil {
			return NodeStartsPage{}, fmt.Errorf("flywheel: list node starts: node %q: %w", row.ID, err)
		}
		page.Starts = append(page.Starts, start)
	}
	return page, nil
}

// nodeStartFromRow maps a job_nodes row to its NodeStart, taking the executor
// class from the first of its runners.
func nodeStartFromRow(row jobNodeRow) (NodeStart, error) {
	start := NodeStart{
		NodeID: row.ID, Version: row.Version, Commit: row.CommitHash, StartedAt: row.StartedAt.UTC(),
	}
	if len(row.Runners) > 0 {
		var runners []struct {
			ExecutorClass string `json:"executor_class"`
		}
		if err := json.Unmarshal(row.Runners, &runners); err != nil {
			return NodeStart{}, fmt.Errorf("decode runners: %w", err)
		}
		if len(runners) > 0 {
			start.ExecutorClass = runners[0].ExecutorClass
		}
	}
	return start, nil
}

// encodeNodeStartsCursor is the cursor that reads the starts after row: its
// started_at as read, to the nanosecond, and its id.
func encodeNodeStartsCursor(row jobNodeRow) string {
	raw, _ := json.Marshal(nodeStartsCursor{StartedAt: row.StartedAt.Format(time.RFC3339Nano), ID: row.ID}) // two strings always encode
	return nodeStartsCursorPrefix + base64.RawURLEncoding.EncodeToString(raw)
}

// decodeNodeStartsCursor reads a cursor encodeNodeStartsCursor wrote, reporting
// false for anything else.
func decodeNodeStartsCursor(s string) (nodeStartsCursor, bool) {
	body, ok := strings.CutPrefix(s, nodeStartsCursorPrefix)
	if !ok {
		return nodeStartsCursor{}, false
	}
	raw, err := base64.RawURLEncoding.DecodeString(body)
	if err != nil {
		return nodeStartsCursor{}, false
	}
	var c nodeStartsCursor
	if err := json.Unmarshal(raw, &c); err != nil || c.ID == "" {
		return nodeStartsCursor{}, false
	}
	if _, err := time.Parse(time.RFC3339Nano, c.StartedAt); err != nil {
		return nodeStartsCursor{}, false
	}
	return c, true
}
