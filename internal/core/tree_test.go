package core

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/datatypes"
	"gorm.io/gorm"
)

// treeDay0 is when the tree fixtures' first job was inserted.
//
//nolint:gochecknoglobals // shared fixture instant
var treeDay0 = time.Date(2026, 9, 2, 0, 0, 0, 0, time.UTC)

// treeSeed is one job of a tree fixture: its name, its parent's name (none when
// empty), and its state (available when unset).
type treeSeed struct {
	name, parent string
	state        JobState
}

// seedTree writes each seed as a job whose id and created time are its position
// in seeds, a minute apart, so later seeds are newer and sort higher by id. A
// seed's parent names an earlier seed, or any id the map does not hold, used as
// written.
func seedTree(t *testing.T, db *gorm.DB, seeds []treeSeed) map[string]string {
	t.Helper()
	ids := make(map[string]string, len(seeds))
	for i, s := range seeds {
		at := treeDay0.Add(time.Duration(i) * time.Minute)
		ids[s.name] = idAt(at, i)
		state := s.state
		if state == "" {
			state = StateAvailable
		}
		row := jobRow{
			ID: ids[s.name], Kind: "tree." + s.name, Queue: defaultQueue, State: string(state), MaxAttempts: 25,
			CreatedAt: at, UpdatedAt: at, ScheduledAt: at,
		}
		if s.parent != "" {
			parent, ok := ids[s.parent]
			if !ok {
				parent = s.parent
			}
			row.ParentJobID = &parent
		}
		seedJob(t, db, row)
	}
	return ids
}

// childKeys returns the seed names of a page's items, in page order.
func childKeys(t *testing.T, ids map[string]string, page JobPage) []string {
	t.Helper()
	byID := make(map[string]string, len(ids))
	for k, v := range ids {
		byID[v] = k
	}
	out := make([]string, len(page.Items))
	for i, item := range page.Items {
		k, ok := byID[item.ID]
		require.True(t, ok, "an item the fixture did not seed: %s", item.ID)
		out[i] = k
	}
	return out
}

// TestListChildrenPagesNewestFirst proves ListChildren pages a parent's
// children newest first by id behind a jc1. cursor, filtered by state, each
// item carrying its metadata and its latest run as QueryJobs builds them; that
// a soft-deleted child and another parent's child are never listed; that an
// unknown parent reads an empty page; and that a bad state or cursor is refused.
func TestListChildrenPagesNewestFirst(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	ctx := context.Background()
	ids := seedTree(t, db, []treeSeed{
		{name: "p"},
		{name: "other"},
		{name: "c1", parent: "p", state: StateSucceeded},
		{name: "c2", parent: "p", state: StateRetryable},
		{name: "elsewhere", parent: "other"},
		{name: "c3", parent: "p", state: StateDiscarded},
		{name: "deleted", parent: "p"},
		{name: "c4", parent: "p", state: StateRunning},
		{name: "c5", parent: "p", state: StateRetryable},
	})
	require.NoError(t, db.Model(&jobRow{}).Where("id = ?", ids["deleted"]).Update("deleted_at", treeDay0).Error)
	require.NoError(t, db.Model(&jobRow{}).Where("id = ?", ids["c3"]).Updates(map[string]any{
		"attempt": 2, "metadata": datatypes.JSON(`{"batch":"b-1"}`),
	}).Error)
	for attempt, msg := range map[int]string{1: "first try", 2: "gave up"} {
		at := treeDay0.Add(time.Hour)
		class := string(ErrorPermanent)
		seedRun(t, db, jobRunRow{
			ID: fmt.Sprintf("%s-run-%d", ids["c3"], attempt), JobID: ids["c3"], Attempt: attempt, Kind: "tree.c3",
			ExecutorID: "exec", StartedAt: at, Outcome: string(OutcomeError), ErrorClass: &class, ErrorMessage: &msg,
			CreatedAt: at,
		})
	}

	var got []string
	p := ListChildrenParams{Limit: 2}
	for range 4 {
		page, err := ListChildren(ctx, db, ids["p"], p)
		require.NoError(t, err)
		require.LessOrEqual(t, len(page.Items), 2)
		got = append(got, childKeys(t, ids, page)...)
		if page.NextCursor == "" {
			break
		}
		assert.Regexp(t, `^jc1\.`, page.NextCursor)
		p.Cursor = page.NextCursor
	}
	assert.Equal(t, []string{"c5", "c4", "c3", "c2", "c1"}, got, "newest first, soft-deleted and other parents' children left out")

	all, err := ListChildren(ctx, db, ids["p"], ListChildrenParams{})
	require.NoError(t, err)
	require.Len(t, all.Items, 5)
	assert.Empty(t, all.NextCursor)
	c3 := all.Items[2]
	assert.Equal(t, ids["c3"], c3.ID)
	assert.JSONEq(t, `{"batch":"b-1"}`, string(c3.Metadata))
	require.NotNil(t, c3.LatestRun, "the item carries its latest run")
	assert.Equal(t, 2, c3.LatestRun.Attempt)
	assert.Equal(t, "gave up", c3.LatestRun.Error)
	assert.JSONEq(t, `{}`, string(all.Items[0].Metadata))
	assert.Nil(t, all.Items[0].LatestRun, "a child never claimed has no run")

	failed, err := ListChildren(ctx, db, ids["p"], ListChildrenParams{States: []JobState{StateRetryable, StateDiscarded}})
	require.NoError(t, err)
	assert.Equal(t, []string{"c5", "c3", "c2"}, childKeys(t, ids, failed))

	unknown, err := ListChildren(ctx, db, "no-such-parent", ListChildrenParams{})
	require.NoError(t, err)
	assert.NotNil(t, unknown.Items)
	assert.Empty(t, unknown.Items)
	assert.Empty(t, unknown.NextCursor)
	empty, err := ListChildren(ctx, db, "", ListChildrenParams{})
	require.NoError(t, err)
	assert.Empty(t, empty.Items, "an empty parent id names no parent")

	for name, c := range map[string]struct {
		p     ListChildrenParams
		field string
	}{
		"unknown_state":   {ListChildrenParams{States: []JobState{"lost"}}, "states"},
		"not_a_cursor":    {ListChildrenParams{Cursor: "nope"}, "cursor"},
		"bad_base64":      {ListChildrenParams{Cursor: "jc1.%%%"}, "cursor"},
		"empty_cursor_id": {ListChildrenParams{Cursor: "jc1."}, "cursor"},
		"a_query_cursor":  {ListChildrenParams{Cursor: "jq1.eyJzIjoibmV3ZXN0IiwiaWQiOiJ4In0"}, "cursor"},
	} {
		t.Run(name, func(t *testing.T) {
			session, rec := recordStatements(db)
			_, err := ListChildren(ctx, session, ids["p"], c.p)
			requireFieldRefusal(t, err, c.field)
			assert.Empty(t, rec.stmt, "a refusal runs no statement")
		})
	}
}

// treeWant is a tree written by hand: a node's seed name, its child count, and
// the children the walk lists, in order.
type treeWant struct {
	name  string
	count int64
	kids  []treeWant
}

// treeShape renders a walked node as a treeWant, checking each node's depth
// against its place in the tree.
func treeShape(t *testing.T, ids map[string]string, n JobTreeNode, depth int) treeWant {
	t.Helper()
	byID := make(map[string]string, len(ids))
	for k, v := range ids {
		byID[v] = k
	}
	name, ok := byID[n.ID]
	require.True(t, ok, "a node the fixture did not seed: %s", n.ID)
	assert.Equal(t, depth, n.Depth, "the depth of %s", name)
	out := treeWant{name: name, count: n.ChildCount}
	for _, c := range n.Children {
		out.kids = append(out.kids, treeShape(t, ids, c, depth+1))
	}
	return out
}

// leaf is a listed node with no listed children.
func leaf(name string, count int64) treeWant { return treeWant{name: name, count: count} }

// seedWide writes n children of parentID in one batch, each a millisecond after
// the last, so they sort by id in insert order.
func seedWide(t *testing.T, db *gorm.DB, parentID string, n int) {
	t.Helper()
	rows := make([]jobRow, n)
	for i := range rows {
		at := treeDay0.Add(time.Hour).Add(time.Duration(i) * time.Millisecond)
		parent := parentID
		rows[i] = jobRow{
			ID: idAt(at, i), Kind: "tree.wide", Queue: defaultQueue, State: string(StateAvailable), MaxAttempts: 25,
			CreatedAt: at, UpdatedAt: at, ScheduledAt: at, ParentJobID: &parent, Args: datatypes.JSON("{}"),
		}
	}
	require.NoError(t, db.CreateInBatches(rows, 200).Error)
}

// TestJobTreeStopsAtItsDepthAndNodeBounds proves JobTree lists a root and its
// descendants breadth first, each node's children newest first, within its depth
// and node bounds: a bound that leaves a child unlisted sets Truncated, every
// listed node counts all its children, the bounds clamp to their maximums, and
// an unknown root is ErrJobNotFound.
func TestJobTreeStopsAtItsDepthAndNodeBounds(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	ctx := context.Background()
	ids := seedTree(t, db, []treeSeed{
		{name: "r"},
		{name: "a", parent: "r"},
		{name: "b", parent: "r"},
		{name: "c", parent: "r"},
		{name: "b1", parent: "b"},
		{name: "b2", parent: "b"},
		{name: "w"},
		{name: "w1", parent: "w"},
		{name: "w2", parent: "w"},
		{name: "w3", parent: "w"},
		{name: "w4", parent: "w"},
		{name: "w5", parent: "w"},
		{name: "q"},
		{name: "q1", parent: "q"},
		{name: "q2", parent: "q1"},
		{name: "q3", parent: "q2"},
		{name: "q4", parent: "q3"},
		{name: "q5", parent: "q4"},
		{name: "q6", parent: "q5"},
		{name: "q7", parent: "q6"},
		{name: "q8", parent: "q7"},
		{name: "q9", parent: "q8"},
		{name: "q10", parent: "q9"},
		{name: "q11", parent: "q10"},
		{name: "q12", parent: "q11"},
		{name: "wide"},
	})
	walk := func(root string, p JobTreeParams) JobTreeResult {
		t.Helper()
		res, err := JobTree(ctx, db, ids[root], p)
		require.NoError(t, err)
		return res
	}

	full := walk("r", JobTreeParams{})
	assert.Equal(t, treeWant{name: "r", count: 3, kids: []treeWant{
		leaf("c", 0), {name: "b", count: 2, kids: []treeWant{leaf("b2", 0), leaf("b1", 0)}}, leaf("a", 0),
	}}, treeShape(t, ids, full.Root, 0))
	assert.Equal(t, 6, full.Nodes)
	assert.False(t, full.Truncated)

	shallow := walk("r", JobTreeParams{MaxDepth: 1})
	assert.Equal(t, treeWant{name: "r", count: 3, kids: []treeWant{leaf("c", 0), leaf("b", 2), leaf("a", 0)}},
		treeShape(t, ids, shallow.Root, 0), "the grandchildren are counted, not listed")
	assert.Equal(t, 4, shallow.Nodes)
	assert.True(t, shallow.Truncated)

	partial := walk("r", JobTreeParams{MaxNodes: 5})
	assert.Equal(t, treeWant{name: "r", count: 3, kids: []treeWant{
		leaf("c", 0), {name: "b", count: 2, kids: []treeWant{leaf("b2", 0)}}, leaf("a", 0),
	}}, treeShape(t, ids, partial.Root, 0), "the node budget takes the newest of a parent's children")
	assert.Equal(t, 5, partial.Nodes)
	assert.True(t, partial.Truncated)

	few := walk("w", JobTreeParams{MaxNodes: 3})
	assert.Equal(t, treeWant{name: "w", count: 5, kids: []treeWant{leaf("w5", 0), leaf("w4", 0)}}, treeShape(t, ids, few.Root, 0))
	assert.Equal(t, 3, few.Nodes)
	assert.True(t, few.Truncated)

	lone := walk("w1", JobTreeParams{})
	assert.Equal(t, leaf("w1", 0), treeShape(t, ids, lone.Root, 0))
	assert.Equal(t, 1, lone.Nodes)
	assert.False(t, lone.Truncated)

	deepest := func(n JobTreeNode) JobTreeNode {
		for len(n.Children) > 0 {
			require.Len(t, n.Children, 1)
			n = n.Children[0]
		}
		return n
	}
	byDefault := walk("q", JobTreeParams{})
	assert.Equal(t, DefaultTreeDepth+1, byDefault.Nodes, "zero walks DefaultTreeDepth levels")
	assert.True(t, byDefault.Truncated)
	clamped := walk("q", JobTreeParams{MaxDepth: 50, MaxNodes: 5000})
	assert.Equal(t, MaxTreeDepth+1, clamped.Nodes, "a depth past the maximum walks MaxTreeDepth levels")
	assert.True(t, clamped.Truncated)
	last := deepest(clamped.Root)
	assert.Equal(t, MaxTreeDepth, last.Depth)
	assert.Equal(t, ids["q10"], last.ID)
	assert.Equal(t, int64(1), last.ChildCount, "the last level still counts its children")

	seedWide(t, db, ids["wide"], MaxTreeNodes+100)
	wide := walk("wide", JobTreeParams{MaxDepth: 50, MaxNodes: 5000})
	assert.Equal(t, MaxTreeNodes, wide.Nodes, "a node bound past the maximum lists MaxTreeNodes")
	assert.Len(t, wide.Root.Children, MaxTreeNodes-1)
	assert.Equal(t, int64(MaxTreeNodes+100), wide.Root.ChildCount)
	assert.True(t, wide.Truncated)
	assert.Equal(t, DefaultTreeNodes, walk("wide", JobTreeParams{}).Nodes, "zero lists DefaultTreeNodes")

	_, err := JobTree(ctx, db, "no-such-job", JobTreeParams{})
	require.ErrorIs(t, err, ErrJobNotFound)
	require.NoError(t, db.Model(&jobRow{}).Where("id = ?", ids["w"]).Update("deleted_at", treeDay0).Error)
	_, err = JobTree(ctx, db, ids["w"], JobTreeParams{})
	require.ErrorIs(t, err, ErrJobNotFound, "a soft-deleted root is not found")
}

// treeDialectSeeds is the dialect fixture: a root whose children have children
// of their own, one with a grandchild, a soft-deleted child, and a parent chain
// that loops (x, y, z, with x's parent set to z after the seed).
//
//nolint:gochecknoglobals // shared fixture
var treeDialectSeeds = []treeSeed{
	{name: "r"},
	{name: "a", parent: "r"},
	{name: "b", parent: "r", state: StateSucceeded},
	{name: "gone", parent: "r"},
	{name: "c", parent: "r", state: StateRunning},
	{name: "a1", parent: "a", state: StateDiscarded},
	{name: "c1", parent: "c"},
	{name: "a2", parent: "a", state: StateRetryable},
	{name: "a2x", parent: "a2"},
	{name: "x"},
	{name: "y", parent: "x"},
	{name: "z", parent: "y"},
}

// seedTreeDialectFixture writes treeDialectSeeds, soft-deletes "gone", and
// closes the loop: x becomes z's child.
func seedTreeDialectFixture(t *testing.T, db *gorm.DB) map[string]string {
	t.Helper()
	ids := seedTree(t, db, treeDialectSeeds)
	// UpdateColumn writes the one column, so updated_at keeps the seeded time and
	// two databases seeded at different moments hold the same rows.
	require.NoError(t, db.Model(&jobRow{}).Where("id = ?", ids["gone"]).UpdateColumn("deleted_at", treeDay0).Error)
	require.NoError(t, db.Model(&jobRow{}).Where("id = ?", ids["x"]).UpdateColumn("parent_job_id", ids["z"]).Error)
	return ids
}

// treeDialectWalks are the walks the dialect fixture is read with.
//
//nolint:gochecknoglobals // shared fixture
var treeDialectWalks = []struct {
	name   string
	root   string
	params JobTreeParams
}{
	{"full", "r", JobTreeParams{}},
	{"a_partial_level", "r", JobTreeParams{MaxNodes: 6}},
	{"one_level", "r", JobTreeParams{MaxDepth: 1}},
	{"a_loop", "x", JobTreeParams{}},
}

// walkTreeDialectFixture walks the dialect fixture each way, every time in UTC
// so two dialects' results compare equal.
func walkTreeDialectFixture(t *testing.T, db *gorm.DB, ids map[string]string) map[string]JobTreeResult {
	t.Helper()
	out := make(map[string]JobTreeResult, len(treeDialectWalks))
	for _, w := range treeDialectWalks {
		res, err := JobTree(context.Background(), db, ids[w.root], w.params)
		require.NoError(t, err, w.name)
		res.Root = treeInUTC(res.Root)
		out[w.name] = res
	}
	return out
}

// treeInUTC returns n with every time in UTC.
func treeInUTC(n JobTreeNode) JobTreeNode {
	n.EnqueuedAt, n.ScheduledAt, n.UpdatedAt = n.EnqueuedAt.UTC(), n.ScheduledAt.UTC(), n.UpdatedAt.UTC()
	if n.FinalizedAt != nil {
		f := n.FinalizedAt.UTC()
		n.FinalizedAt = &f
	}
	for i := range n.Children {
		n.Children[i] = treeInUTC(n.Children[i])
	}
	return n
}

// TestJobTreeIsTheSameOnBothDialects proves the walk over the dialect fixture
// equals the tree written by hand: the order, the counts, a level the node
// budget cuts inside a parent, and a looping chain listed once.
func TestJobTreeIsTheSameOnBothDialects(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	ids := seedTreeDialectFixture(t, db)
	got := walkTreeDialectFixture(t, db, ids)

	want := map[string]struct {
		shape     treeWant
		nodes     int
		truncated bool
	}{
		"full": {treeWant{name: "r", count: 3, kids: []treeWant{
			{name: "c", count: 1, kids: []treeWant{leaf("c1", 0)}},
			leaf("b", 0),
			{name: "a", count: 2, kids: []treeWant{{name: "a2", count: 1, kids: []treeWant{leaf("a2x", 0)}}, leaf("a1", 0)}},
		}}, 8, false},
		"a_partial_level": {treeWant{name: "r", count: 3, kids: []treeWant{
			{name: "c", count: 1, kids: []treeWant{leaf("c1", 0)}},
			leaf("b", 0),
			{name: "a", count: 2, kids: []treeWant{leaf("a2", 1)}},
		}}, 6, true},
		"one_level": {treeWant{name: "r", count: 3, kids: []treeWant{leaf("c", 1), leaf("b", 0), leaf("a", 2)}}, 4, true},
		"a_loop": {treeWant{name: "x", count: 1, kids: []treeWant{
			{name: "y", count: 1, kids: []treeWant{leaf("z", 1)}},
		}}, 3, false},
	}
	for _, w := range treeDialectWalks {
		t.Run(w.name, func(t *testing.T) {
			res := got[w.name]
			assert.Equal(t, want[w.name].shape, treeShape(t, ids, res.Root, 0))
			assert.Equal(t, want[w.name].nodes, res.Nodes)
			assert.Equal(t, want[w.name].truncated, res.Truncated)
		})
	}
	assert.Equal(t, string(StateRunning), got["full"].Root.Children[0].State, "each node carries its job's view")
	assert.Equal(t, ids["r"], got["full"].Root.Children[0].ParentJobID)
}

// seedChildRun records one attempt of a fixture job, with output when given.
func seedChildRun(t *testing.T, db *gorm.DB, jobID string, attempt int, outcome RunOutcome, output string) {
	t.Helper()
	r := jobRunRow{
		ID: fmt.Sprintf("%s-run-%d", jobID, attempt), JobID: jobID, Attempt: attempt, Kind: "tree.child",
		ExecutorID: "exec", StartedAt: treeDay0.Add(time.Hour), Outcome: string(outcome), CreatedAt: treeDay0.Add(time.Hour),
	}
	if output != "" {
		r.Output = datatypes.JSON(output)
	}
	seedRun(t, db, r)
}

// TestChildOutputsCanIncludeUnfinishedChildren proves ChildOutputsWithOptions
// reads the children in the states it names, unfinished ones included, each
// with its latest recorded attempt and that attempt's output, and refuses an
// unknown state.
func TestChildOutputsCanIncludeUnfinishedChildren(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	ctx := context.Background()
	ids := seedTree(t, db, []treeSeed{
		{name: "p"},
		{name: "done", parent: "p", state: StateSucceeded},
		{name: "waiting", parent: "p", state: StateAvailable},
		{name: "retrying", parent: "p", state: StateRetryable},
		{name: "dropped", parent: "p", state: StateDiscarded},
		{name: "running", parent: "p", state: StateRunning},
	})
	seedChildRun(t, db, ids["done"], 1, OutcomeSuccess, `{"n":1}`)
	seedChildRun(t, db, ids["retrying"], 1, OutcomeError, `{"partial":true}`)
	seedChildRun(t, db, ids["running"], 1, OutcomeError, `{"try":1}`)
	seedChildRun(t, db, ids["running"], 2, OutcomeStarted, "")

	outs, err := ChildOutputsWithOptions(ctx, db, ids["p"], ChildOutputsOpts{
		States: []JobState{StateSucceeded, StateRetryable, StateRunning},
	})
	require.NoError(t, err)
	require.Len(t, outs, 3)
	assert.Equal(t, ChildOutput{JobID: ids["running"], State: StateRunning, Attempt: 2}, outs[0],
		"a running child reports its current attempt, which has no output yet")
	assert.Equal(t, ids["retrying"], outs[1].JobID)
	assert.Equal(t, StateRetryable, outs[1].State)
	assert.Equal(t, 1, outs[1].Attempt)
	assert.JSONEq(t, `{"partial":true}`, string(outs[1].Output))
	assert.Equal(t, ids["done"], outs[2].JobID)
	assert.JSONEq(t, `{"n":1}`, string(outs[2].Output))

	waiting, err := ChildOutputsWithOptions(ctx, db, ids["p"], ChildOutputsOpts{States: []JobState{StateAvailable}})
	require.NoError(t, err)
	assert.Equal(t, []ChildOutput{{JobID: ids["waiting"], State: StateAvailable}}, waiting, "a child never claimed has no attempt")

	paged, err := ChildOutputsWithOptions(ctx, db, ids["p"], ChildOutputsOpts{
		States: []JobState{StateSucceeded, StateRetryable, StateRunning}, Before: treeDay0.Add(5 * time.Minute), Limit: 1,
	})
	require.NoError(t, err)
	require.Len(t, paged, 1)
	assert.Equal(t, ids["retrying"], paged[0].JobID, "Before and Limit page the chosen states")

	_, err = ChildOutputsWithOptions(ctx, db, ids["p"], ChildOutputsOpts{States: []JobState{StateRunning, "lost"}})
	requireFieldRefusal(t, err, "states")
}

// TestChildOutputsKeepsItsContract proves ChildOutputs reads what it always
// has: terminal children only, newest first by created_at and id, its Before and
// Limit applied, every terminal child at a zero Limit, MaxReadLimit
// notwithstanding, and BeforeAttempt ignored; and that it equals
// ChildOutputsWithOptions with the terminal states.
func TestChildOutputsKeepsItsContract(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	ctx := context.Background()
	parent := "contract-parent"
	states := []JobState{StateSucceeded, StateAvailable, StateCancelled, StateRunning, StateDiscarded}
	rows := make([]jobRow, 0, 2*MaxReadLimit)
	for i := range 2 * MaxReadLimit {
		at := treeDay0.Add(time.Duration(i/2) * time.Minute) // pairs share a created_at
		pid := parent
		rows = append(rows, jobRow{
			ID: idAt(at, i), Kind: "tree.contract", Queue: defaultQueue, State: string(states[i%len(states)]),
			MaxAttempts: 25, CreatedAt: at, UpdatedAt: at, ScheduledAt: at, ParentJobID: &pid,
			Args: datatypes.JSON("{}"),
		})
	}
	require.NoError(t, db.CreateInBatches(rows, 200).Error)
	seedChildRun(t, db, rows[0].ID, 1, OutcomeError, `{"try":1}`)
	seedChildRun(t, db, rows[0].ID, 2, OutcomeSuccess, `{"try":2}`)

	all, err := ChildOutputs(ctx, db, parent, ListRunsParams{})
	require.NoError(t, err)
	terminal := 0
	for _, r := range rows {
		if isTerminalStateString(r.State) {
			terminal++
		}
	}
	require.Greater(t, terminal, MaxReadLimit, "the fixture holds more terminal children than a list page")
	require.Len(t, all, terminal, "a zero Limit reads every terminal child")
	for i, o := range all {
		assert.True(t, isTerminalStateString(string(o.State)), "only terminal children: %s", o.State)
		if i > 0 {
			assert.Greater(t, all[i-1].JobID, o.JobID, "newest first by created_at, then id")
		}
	}
	last := all[len(all)-1]
	assert.Equal(t, rows[0].ID, last.JobID)
	assert.Equal(t, 2, last.Attempt, "the latest attempt's output")
	assert.JSONEq(t, `{"try":2}`, string(last.Output))

	for name, p := range map[string]ListRunsParams{
		"every_child":     {},
		"a_limit":         {Limit: 7},
		"a_cursor":        {Before: treeDay0.Add(40 * time.Minute)},
		"both":            {Before: treeDay0.Add(40 * time.Minute), Limit: 3},
		"an_attempt_mark": {BeforeAttempt: 2, Limit: 5},
	} {
		t.Run(name, func(t *testing.T) {
			legacy, err := ChildOutputs(ctx, db, parent, p)
			require.NoError(t, err)
			opts, err := ChildOutputsWithOptions(ctx, db, parent, ChildOutputsOpts{Before: p.Before, Limit: p.Limit})
			require.NoError(t, err)
			explicit, err := ChildOutputsWithOptions(ctx, db, parent, ChildOutputsOpts{
				States: TerminalStates(), Before: p.Before, Limit: p.Limit,
			})
			require.NoError(t, err)
			assert.Equal(t, legacy, opts)
			assert.Equal(t, legacy, explicit)
			if p.Limit > 0 {
				assert.Len(t, legacy, p.Limit)
			}
			for _, o := range legacy {
				assert.True(t, isTerminalStateString(string(o.State)))
			}
		})
	}
}
