package core

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"

	"gorm.io/gorm"
)

// defaultChildrenLimit is a ListChildren page when Limit is not positive.
const defaultChildrenLimit = 50

// childrenCursorPrefix versions ListChildren's cursor, so a later shape can
// tell its own cursors from this one's.
const childrenCursorPrefix = "jc1."

// ListChildrenParams configures a ListChildren page. The zero value lists a
// parent's newest 50 children in every state.
type ListChildrenParams struct {
	// States keeps the children in any of these states; empty keeps every one.
	States []JobState
	// Cursor is the NextCursor of the previous page; empty reads the first page.
	Cursor string
	// Limit caps the page: zero or negative selects 50, and a larger value than
	// MaxReadLimit reads MaxReadLimit.
	Limit int
}

// ListChildren lists a parent's children newest first by id, one page at a
// time, reading through db as given, with no transaction of its own. Each item
// is built as QueryJobs builds one: its view, its metadata, and its latest
// attempt's run in summary. Soft-deleted children never appear, and a parent
// with no children, or none that exists, reads an empty page.
//
// The page is one range of jobs_parent_state per call. NextCursor, set only
// when another page exists, continues the same parent's listing. Before any
// statement runs, it returns a *ValidationError for an unknown state (states)
// and for a cursor it did not issue (cursor).
func ListChildren(ctx context.Context, db *gorm.DB, parentJobID string, p ListChildrenParams) (JobPage, error) {
	for _, s := range p.States {
		if !s.Valid() {
			return JobPage{}, newValidationError("states", fmt.Sprintf("%q is not a job state", s))
		}
	}
	after := ""
	if p.Cursor != "" {
		id, err := decodeChildrenCursor(p.Cursor)
		if err != nil {
			return JobPage{}, err
		}
		after = id
	}
	limit := readLimit(p.Limit, defaultChildrenLimit)

	stmt := applyJobFilter(listStatement(ctx, db), JobQuery{States: p.States}, db.Name()).
		Where("jobs.parent_job_id = ?", parentJobID)
	if after != "" {
		stmt = stmt.Where("jobs.id < ?", after)
	}
	var rows []jobListRow
	if err := stmt.Order("jobs.id DESC").Limit(limit + 1).Find(&rows).Error; err != nil {
		return JobPage{}, fmt.Errorf("flywheel: list children: %w", err)
	}
	page := JobPage{Items: make([]JobListItem, 0, min(len(rows), limit))}
	if len(rows) > limit {
		rows = rows[:limit]
		page.NextCursor = childrenCursorPrefix + base64.RawURLEncoding.EncodeToString([]byte(rows[limit-1].Job.ID))
	}
	for i := range rows {
		page.Items = append(page.Items, jobListItemFromRow(rows[i]))
	}
	return page, nil
}

// decodeChildrenCursor reads the last id of the page a ListChildren cursor
// follows, refusing a cursor ListChildren did not issue.
func decodeChildrenCursor(raw string) (string, error) {
	malformed := newValidationError("cursor", "is malformed: pass the NextCursor of a previous page")
	body, ok := strings.CutPrefix(raw, childrenCursorPrefix)
	if !ok {
		return "", malformed
	}
	id, err := base64.RawURLEncoding.DecodeString(body)
	if err != nil || len(id) == 0 {
		return "", malformed
	}
	return string(id), nil
}

const (
	// DefaultTreeDepth is how many levels below its root JobTree lists when
	// JobTreeParams.MaxDepth is zero.
	DefaultTreeDepth = 5
	// MaxTreeDepth is the most levels below its root JobTree lists.
	MaxTreeDepth = 10
	// DefaultTreeNodes is how many nodes JobTree lists when
	// JobTreeParams.MaxNodes is zero.
	DefaultTreeNodes = 500
	// MaxTreeNodes is the most nodes JobTree lists.
	MaxTreeNodes = 2000
)

// JobTreeParams bounds a JobTree walk.
type JobTreeParams struct {
	// MaxDepth is how many levels below the root the walk lists: zero or
	// negative selects DefaultTreeDepth, and a larger value than MaxTreeDepth
	// reads MaxTreeDepth.
	MaxDepth int
	// MaxNodes is how many nodes the walk lists, the root included: zero or
	// negative selects DefaultTreeNodes, and a larger value than MaxTreeNodes
	// reads MaxTreeNodes.
	MaxNodes int
}

// JobTreeNode is one job of a tree: its view, its depth, how many children it
// has, and the children the walk listed.
type JobTreeNode struct {
	JobView
	// Depth is 0 for the root, 1 for its children, and so on.
	Depth int `json:"depth"`
	// ChildCount is every child the job has, listed or not, soft-deleted ones
	// aside.
	ChildCount int64 `json:"child_count"`
	// Children are the children the walk listed, newest first by id.
	Children []JobTreeNode `json:"children,omitempty"`
}

// JobTreeResult is a JobTree walk: the root, the nodes it listed, and whether a
// bound stopped it.
type JobTreeResult struct {
	Root JobTreeNode `json:"root"`
	// Nodes counts every node listed, the root included.
	Nodes int `json:"nodes"`
	// Truncated reports that a depth or node bound left a child unlisted.
	Truncated bool `json:"truncated"`
}

// JobTree walks a job's descendants breadth first, each node's children newest
// first by id, reading through db as given, with no transaction of its own. It
// lists at most p.MaxDepth levels below the root and p.MaxNodes nodes in all,
// sets Truncated when either bound leaves a child unlisted, and reports every
// listed node's full child count. Soft-deleted jobs are neither listed nor
// counted, and a job whose parent chain loops back on itself is listed once. An
// unknown root returns ErrJobNotFound.
//
// The walk reads one level at a time, the same statements on every dialect: a
// grouped count of the level's children, then the children that fit the node
// budget, which takes each parent's children in level order, the newest first.
// That is at most three statements a level, each bounded by the budget and
// served by jobs_parent_state, so a fan-out of any width costs what the bounds
// allow and never more.
func JobTree(ctx context.Context, db *gorm.DB, rootJobID string, p JobTreeParams) (JobTreeResult, error) {
	root, err := findJobView(ctx, db, rootJobID)
	if errors.Is(err, ErrJobNotFound) {
		return JobTreeResult{}, ErrJobNotFound
	}
	if err != nil {
		return JobTreeResult{}, fmt.Errorf("flywheel: job tree: %w", err)
	}
	maxDepth := treeBound(p.MaxDepth, DefaultTreeDepth, MaxTreeDepth)
	maxNodes := treeBound(p.MaxNodes, DefaultTreeNodes, MaxTreeNodes)

	w := treeWalk{nodes: []treeWalkNode{{view: root}}, listed: map[string]bool{root.ID: true}}
	level := []int{0}
	for depth := 0; len(level) > 0; depth++ {
		if level, err = w.descend(ctx, db, level, depth == maxDepth, maxNodes); err != nil {
			return JobTreeResult{}, fmt.Errorf("flywheel: job tree: %w", err)
		}
	}
	return JobTreeResult{Root: w.build(0, 0), Nodes: len(w.nodes), Truncated: w.truncated}, nil
}

// treeBound resolves a JobTree bound: def when v is not positive, and never
// more than maxV.
func treeBound(v, def, maxV int) int {
	if v <= 0 {
		return def
	}
	return min(v, maxV)
}

// treeWalk is a JobTree walk in progress: the nodes listed so far, in the order
// listed, each naming its children by index; the ids listed, so a looping chain
// is listed once; and whether a bound has cut the walk.
type treeWalk struct {
	nodes     []treeWalkNode
	listed    map[string]bool
	truncated bool
}

// treeWalkNode is one listed node: its view, its child count, and the indexes
// of the children listed under it.
type treeWalkNode struct {
	view  JobView
	count int64
	kids  []int
}

// descend counts the children of level's nodes and, unless last, lists the
// ones the node budget leaves room for, returning them as the next level.
func (w *treeWalk) descend(ctx context.Context, db *gorm.DB, level []int, last bool, maxNodes int) ([]int, error) {
	ids := make([]string, len(level))
	for i, n := range level {
		ids[i] = w.nodes[n].view.ID
	}
	counts, err := countChildren(ctx, db, ids)
	if err != nil {
		return nil, err
	}
	budget := int64(maxNodes - len(w.nodes))
	take := make(map[string]int64, len(level))
	var whole []string
	partial, partialTake := "", int64(0)
	for _, n := range level {
		id := w.nodes[n].view.ID
		c := counts[id]
		w.nodes[n].count = c
		if c == 0 {
			continue
		}
		if last {
			w.truncated = true
			continue
		}
		t := min(c, budget)
		if t < c {
			w.truncated = true
		}
		if t == 0 {
			continue
		}
		budget -= t
		take[id] = t
		if t == c {
			whole = append(whole, id)
		} else {
			partial, partialTake = id, t
		}
	}
	if len(take) == 0 {
		return nil, nil
	}
	children, err := readChildren(ctx, db, whole, partial, partialTake)
	if err != nil {
		return nil, err
	}
	var next []int
	for _, n := range level {
		id := w.nodes[n].view.ID
		kids := children[id]
		kids = kids[:min(int64(len(kids)), take[id])]
		for _, kid := range kids {
			if w.listed[kid.ID] {
				continue
			}
			w.listed[kid.ID] = true
			w.nodes = append(w.nodes, treeWalkNode{view: kid})
			w.nodes[n].kids = append(w.nodes[n].kids, len(w.nodes)-1)
			next = append(next, len(w.nodes)-1)
		}
	}
	return next, nil
}

// countChildren counts the children of each of parents, soft-deleted ones
// aside, in one grouped statement over jobs_parent_state.
func countChildren(ctx context.Context, db *gorm.DB, parents []string) (map[string]int64, error) {
	var rows []struct {
		ParentJobID string `gorm:"column:parent_job_id"`
		N           int64  `gorm:"column:n"`
	}
	if err := db.WithContext(ctx).Model(&jobRow{}).Select("parent_job_id, count(*) AS n").
		Where("parent_job_id IN ?", parents).Group("parent_job_id").Scan(&rows).Error; err != nil {
		return nil, fmt.Errorf("count children: %w", err)
	}
	out := make(map[string]int64, len(rows))
	for _, r := range rows {
		out[r.ParentJobID] = r.N
	}
	return out, nil
}

// readChildren reads the children of the parents taken whole in one statement,
// and the newest n children of the one taken in part, if any, in another, each
// parent's newest first.
func readChildren(ctx context.Context, db *gorm.DB, whole []string, partial string, n int64) (map[string][]JobView, error) {
	out := make(map[string][]JobView, len(whole)+1)
	add := func(rows []jobRow) {
		for i := range rows {
			v := jobViewFromRow(rows[i])
			out[v.ParentJobID] = append(out[v.ParentJobID], v)
		}
	}
	if len(whole) > 0 {
		var rows []jobRow
		if err := db.WithContext(ctx).Model(&jobRow{}).Select(jobViewColumns()).
			Where("parent_job_id IN ?", whole).Order("parent_job_id, id DESC").Find(&rows).Error; err != nil {
			return nil, fmt.Errorf("read children: %w", err)
		}
		add(rows)
	}
	if partial != "" {
		var rows []jobRow
		if err := db.WithContext(ctx).Model(&jobRow{}).Select(jobViewColumns()).
			Where("parent_job_id = ?", partial).Order("id DESC").Limit(int(n)).Find(&rows).Error; err != nil {
			return nil, fmt.Errorf("read children: %w", err)
		}
		add(rows)
	}
	return out, nil
}

// build renders the listed node i, at depth, and the nodes listed under it.
func (w *treeWalk) build(i, depth int) JobTreeNode {
	n := JobTreeNode{JobView: w.nodes[i].view, Depth: depth, ChildCount: w.nodes[i].count}
	for _, k := range w.nodes[i].kids {
		n.Children = append(n.Children, w.build(k, depth+1))
	}
	return n
}
