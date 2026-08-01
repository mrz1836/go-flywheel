package core

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/mrz1836/go-foundation/models"
	"gorm.io/gorm"
)

// BatchProgress is the aggregate state of one parent's children, plus the
// parent's own state. It is the "how is this batch doing?" read: one call yields
// the per-state counts, the totals, and the age of the oldest pending child, so a
// caller renders a batch view without writing its own GROUP BY against the
// runtime's unexported rows.
type BatchProgress struct {
	// ParentJobID is the parent whose children this rolls up. In ProgressByKind it
	// is empty, because a kind is not a parent job.
	ParentJobID string `json:"parent_job_id"`
	// ParentState is the parent job's own state. It is empty when the parent row no
	// longer exists — pruned by retention, soft-deleted, or an unknown id — which is
	// how a caller tells "the parent is gone but its children remain" from "the
	// parent is still here". It is always empty in ProgressByKind.
	ParentState JobState `json:"parent_state"`
	// CountsByState is the child count per state; absent states are zero.
	CountsByState map[string]int `json:"counts_by_state"`
	// Total is the number of children (or, in ProgressByKind, jobs of the kind).
	Total int `json:"total"`
	// Terminal is the number of children that can no longer progress (succeeded,
	// cancelled, discarded); Pending is Total - Terminal, and includes running and
	// paused. A batch is complete when Pending is zero.
	Terminal int `json:"terminal"`
	Pending  int `json:"pending"`
	// OldestPendingAge is how long the least-recently-scheduled pending child has
	// been waiting (the sample instant minus its scheduled_at). It is zero when
	// nothing is pending. It is the signal a host anchors a batch deadline to: a
	// stalled-behind-backlog batch is distinguished from a healthy one by how old its
	// oldest pending child is, not by how long ago it was spawned — so a deadline
	// measured from spawn time would kill a healthy batch that is merely behind.
	//
	// It is populated only by Progress. ProgressMany and ProgressByKind leave it
	// zero: the oldest-pending read is a per-batch ordered scan that does not batch
	// across parents without either an O(total-pending) scan or a MIN() that is not
	// dialect-safe on SQLite, so the batched forms stay O(1) queries independent of
	// pending depth and a caller watching one batch's deadline calls Progress.
	OldestPendingAge time.Duration `json:"oldest_pending_age"`
}

// Progress returns the rollup for one parent, reading through db. It is up to
// three index-backed reads: the parent's own state (a primary-key lookup), the
// grouped child-count read (GROUP BY state, served index-only by jobs_parent_state
// with GORM's deleted_at IS NULL soft-delete scope), and — only when there is
// pending work — the oldest pending child's scheduled_at.
//
// An unknown or already-pruned parent is not an error: the children can outlive
// the parent row, so Progress returns the children's rollup with an empty
// ParentState rather than refusing. A caller distinguishing "unknown parent" from
// "known parent, no children" checks ParentState against Total.
func Progress(ctx context.Context, db *gorm.DB, parentJobID string) (BatchProgress, error) {
	now := models.ClockFrom(ctx).Now(ctx)
	bp := BatchProgress{ParentJobID: parentJobID, CountsByState: map[string]int{}}

	// The parent's own state, best-effort: a parent pruned by retention or
	// soft-deleted leaves its children pointing at an id no live row has, and that
	// is a reportable state (empty ParentState), not a failure.
	var parent jobRow
	switch err := db.WithContext(ctx).Select("state").Where("id = ?", parentJobID).First(&parent).Error; {
	case err == nil:
		bp.ParentState = JobState(parent.State)
	case errors.Is(err, gorm.ErrRecordNotFound):
		// Leave ParentState empty: the parent row is gone or the id is unknown.
	default:
		return BatchProgress{}, fmt.Errorf("flywheel: progress parent state: %w", err)
	}

	// Grouped child counts. Model(&jobRow{}) applies the deleted_at IS NULL scope,
	// which is both correct (a soft-deleted child is not part of the batch) and what
	// keeps the read on the jobs_parent_state partial index.
	var rows []struct {
		State string
		N     int
	}
	if err := db.WithContext(ctx).Model(&jobRow{}).
		Where("parent_job_id = ?", parentJobID).
		Select("state, count(*) as n").Group("state").Scan(&rows).Error; err != nil {
		return BatchProgress{}, fmt.Errorf("flywheel: progress counts: %w", err)
	}
	foldStateCounts(&bp, rows)

	// The age of the oldest pending child, read only when something is pending. It
	// copies SampleQueueHealth's oldest-ready pattern exactly: an ordered single-row
	// read of the real typed scheduled_at column, never MIN(scheduled_at), because
	// SQLite returns a bare aggregate as text and drops the column's datetime
	// affinity — which would fail the time scan on that dialect alone.
	if bp.Pending > 0 {
		var oldest time.Time
		res := db.WithContext(ctx).Model(&jobRow{}).
			Select("scheduled_at").
			Where("parent_job_id = ? AND state IN ?", parentJobID, nonTerminalStateStrings()).
			Order("scheduled_at asc").Limit(1).Scan(&oldest)
		if res.Error != nil {
			return BatchProgress{}, fmt.Errorf("flywheel: progress oldest pending: %w", res.Error)
		}
		if age := now.Sub(oldest); age > 0 {
			bp.OldestPendingAge = age
		}
	}

	return bp, nil
}

// ProgressMany returns rollups for several parents in a fixed number of reads —
// two, not one per parent — so a dashboard listing N batches does not issue N
// queries. It reads the parents' own states in one IN lookup and the children's
// counts in one GROUP BY parent_job_id, state.
//
// A requested parent with no children still gets an entry, with a zero Total,
// rather than being absent: that is what lets a caller distinguish "no children"
// (Total zero, ParentState set) from "unknown parent" (Total zero, ParentState
// empty). OldestPendingAge is left zero on every entry — see BatchProgress.
func ProgressMany(ctx context.Context, db *gorm.DB, parentJobIDs []string) (map[string]BatchProgress, error) {
	out := make(map[string]BatchProgress, len(parentJobIDs))
	for _, id := range parentJobIDs {
		out[id] = BatchProgress{ParentJobID: id, CountsByState: map[string]int{}}
	}
	if len(parentJobIDs) == 0 {
		return out, nil
	}

	// The parents' own states, in one lookup. A parent absent from the result is
	// gone or unknown and keeps its empty ParentState.
	var parents []struct {
		ID    string
		State string
	}
	if err := db.WithContext(ctx).Model(&jobRow{}).
		Where("id IN ?", parentJobIDs).
		Select("id, state").Scan(&parents).Error; err != nil {
		return nil, fmt.Errorf("flywheel: progress-many parent states: %w", err)
	}
	for _, p := range parents {
		bp := out[p.ID]
		bp.ParentState = JobState(p.State)
		out[p.ID] = bp
	}

	// The children's counts across every requested parent, in one GROUP BY.
	var rows []struct {
		ParentJobID string
		State       string
		N           int
	}
	if err := db.WithContext(ctx).Model(&jobRow{}).
		Where("parent_job_id IN ?", parentJobIDs).
		Select("parent_job_id, state, count(*) as n").
		Group("parent_job_id, state").Scan(&rows).Error; err != nil {
		return nil, fmt.Errorf("flywheel: progress-many counts: %w", err)
	}
	for _, r := range rows {
		bp := out[r.ParentJobID]
		bp.CountsByState[r.State] = r.N
		bp.Total += r.N
		if isTerminalStateString(r.State) {
			bp.Terminal += r.N
		}
		out[r.ParentJobID] = bp
	}
	for id, bp := range out {
		bp.Pending = bp.Total - bp.Terminal
		out[id] = bp
	}
	return out, nil
}

// ProgressByKind returns counts-by-state grouped by job kind — the parentless
// form of the rollup, for a host whose batches are defined by what the work is
// rather than by who spawned it. It is one GROUP BY kind, state across the
// requested kinds, and returns an entry for every requested kind including one
// with no jobs.
//
// ParentJobID and ParentState are empty on every entry (a kind is not a parent),
// and OldestPendingAge is zero — see BatchProgress.
//
// The kind filter is deliberately not index-served, exactly like Overview's kind
// filter: measured on a 1M table, the read plans the same whether or not a
// (kind, state) index exists, so none is added. It is an inspection query a human
// or a dashboard runs on demand, not a hot path a runner polls, so it does not
// justify widening the heavily-used jobs_state index or carrying a bespoke index
// for a single reader.
func ProgressByKind(ctx context.Context, db *gorm.DB, kinds []string) (map[string]BatchProgress, error) {
	out := make(map[string]BatchProgress, len(kinds))
	for _, k := range kinds {
		out[k] = BatchProgress{CountsByState: map[string]int{}}
	}
	if len(kinds) == 0 {
		return out, nil
	}

	var rows []struct {
		Kind  string
		State string
		N     int
	}
	if err := db.WithContext(ctx).Model(&jobRow{}).
		Where("kind IN ?", kinds).
		Select("kind, state, count(*) as n").
		Group("kind, state").Scan(&rows).Error; err != nil {
		return nil, fmt.Errorf("flywheel: progress-by-kind counts: %w", err)
	}
	for _, r := range rows {
		bp := out[r.Kind]
		bp.CountsByState[r.State] = r.N
		bp.Total += r.N
		if isTerminalStateString(r.State) {
			bp.Terminal += r.N
		}
		out[r.Kind] = bp
	}
	for k, bp := range out {
		bp.Pending = bp.Total - bp.Terminal
		out[k] = bp
	}
	return out, nil
}

// foldStateCounts folds a state→count grouping into bp: it fills CountsByState and
// accumulates Total, Terminal, and Pending. It is the shared reduction behind the
// single-parent Progress read.
func foldStateCounts(bp *BatchProgress, rows []struct {
	State string
	N     int
},
) {
	for _, r := range rows {
		bp.CountsByState[r.State] = r.N
		bp.Total += r.N
		if isTerminalStateString(r.State) {
			bp.Terminal += r.N
		}
	}
	bp.Pending = bp.Total - bp.Terminal
}

// isTerminalStateString reports whether a persisted state string is one of the
// terminal states (succeeded, cancelled, discarded). It is the string-keyed twin
// of TerminalStates, used to fold a grouped count without allocating a set per
// call.
func isTerminalStateString(s string) bool {
	switch JobState(s) {
	case StateSucceeded, StateCancelled, StateDiscarded:
		return true
	default:
		return false
	}
}

// ChildOutput is one terminal child of a parent, paired with the recorded output
// of its last attempt. It is the fold a barrier continuation reads: the barrier
// fires when the generation is complete, and this is how the continuation sees what
// the generation produced.
type ChildOutput struct {
	JobID string `json:"job_id"`
	// State is the child's final state — succeeded, cancelled, or discarded.
	State JobState `json:"state"`
	// Attempt is the attempt number of the recorded run this output came from. It is
	// zero for a child that never ran (a child cancelled before it was claimed has a
	// terminal state but no attempt and no output).
	Attempt int `json:"attempt"`
	// Output is the child's last attempt's recorded Result.Output, empty when the
	// child produced none or never ran.
	Output json.RawMessage `json:"output,omitempty"`
}

// ChildOutputs returns the recorded output of each terminal child of a parent,
// newest attempt per child, reading through db. It pages the terminal children by
// created_at (newest first) exactly as ListRuns pages a job's runs: p.Before is a
// created_at cursor (zero means newest) and a positive p.Limit caps the page; a
// zero p.Limit reads every terminal child, which is the fold a barrier continuation
// wants over a bounded generation.
//
// The outputs come from a second read keyed by the page's child ids rather than a
// SQL join, mirroring RecentFailures: the runs are loaded ordered by attempt, and
// the last row seen per child — the highest attempt — is the one that reached the
// terminal state. A terminal child with no recorded run (cancelled before it was
// claimed) still gets an entry, with a zero Attempt and an empty Output.
func ChildOutputs(ctx context.Context, db *gorm.DB, parentJobID string, p ListRunsParams) ([]ChildOutput, error) {
	query := db.WithContext(ctx).Model(&jobRow{}).
		Where("parent_job_id = ? AND state IN ?", parentJobID, terminalStateStrings())
	if !p.Before.IsZero() {
		query = query.Where("created_at < ?", p.Before)
	}
	if p.Limit > 0 {
		query = query.Limit(p.Limit)
	}
	var children []jobRow
	if err := query.Order("created_at desc, id desc").Find(&children).Error; err != nil {
		return nil, fmt.Errorf("flywheel: child outputs: %w", err)
	}
	if len(children) == 0 {
		return []ChildOutput{}, nil
	}

	ids := make([]string, len(children))
	for i := range children {
		ids[i] = children[i].ID
	}
	var runs []jobRunRow
	if err := db.WithContext(ctx).Model(&jobRunRow{}).
		Where("job_id IN ?", ids).
		Order("attempt asc").Find(&runs).Error; err != nil {
		return nil, fmt.Errorf("flywheel: child outputs runs: %w", err)
	}
	// attempt asc means the last write per child is the highest (final) attempt.
	latest := make(map[string]jobRunRow, len(runs))
	for i := range runs {
		latest[runs[i].JobID] = runs[i]
	}

	out := make([]ChildOutput, len(children))
	for i := range children {
		co := ChildOutput{JobID: children[i].ID, State: JobState(children[i].State)}
		if run, ok := latest[children[i].ID]; ok {
			co.Attempt = run.Attempt
			if len(run.Output) > 0 {
				co.Output = json.RawMessage(run.Output)
			}
		}
		out[i] = co
	}
	return out, nil
}
