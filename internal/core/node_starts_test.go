package core

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/mrz1836/go-foundation/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/datatypes"
	"gorm.io/gorm"
)

// nodeStartsBase is when the node-start tests' oldest node started.
//
//nolint:gochecknoglobals // shared fixture
var nodeStartsBase = time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

// writeNodeStart writes one job_nodes row as a registering node does, started
// at at, stopped a day later when stopped is set, with the given runners.
func writeNodeStart(t *testing.T, db *gorm.DB, id string, at time.Time, stopped bool, runners ...NodeRunner) {
	t.Helper()
	if runners == nil {
		runners = []NodeRunner{}
	}
	raw, err := json.Marshal(runners)
	require.NoError(t, err)
	row := jobNodeRow{
		ID: id, ExecutorID: "executor-" + id, Version: "v1." + id, CommitHash: "c-" + id,
		StartedAt: at, HeartbeatAt: at.Add(time.Minute), Runners: datatypes.JSON(raw), Kinds: datatypes.JSON("[]"),
	}
	if stopped {
		end := at.Add(24 * time.Hour)
		row.StoppedAt = &end
	}
	require.NoError(t, db.Create(&row).Error)
}

// walkNodeStarts pages through ListNodeStarts with p until NextCursor is empty,
// returning every start in the order read. between, when set, runs after each
// page but the last.
func walkNodeStarts(t *testing.T, ctx context.Context, db *gorm.DB, p NodeStartsParams, between func()) []NodeStart { //nolint:revive // ctx-after-t matches the package's test helpers
	t.Helper()
	var all []NodeStart
	for range 1000 {
		page, err := ListNodeStarts(ctx, db, p)
		require.NoError(t, err)
		require.LessOrEqual(t, len(page.Starts), max(p.Limit, 1))
		all = append(all, page.Starts...)
		if page.NextCursor == "" {
			return all
		}
		require.True(t, strings.HasPrefix(page.NextCursor, "ns1."), "the cursor carries its prefix")
		p.After = page.NextCursor
		if between != nil {
			between()
		}
	}
	t.Fatal("the walk never reached its last page")
	return nil
}

// nodeStartIDs lists the node ids of starts.
func nodeStartIDs(starts []NodeStart) []string {
	ids := make([]string, len(starts))
	for i, s := range starts {
		ids[i] = s.NodeID
	}
	return ids
}

// nodeStartsSuite proves the walk: 250 node rows over 400 days, some stopped
// and ten sharing one start, are read in pages of seven, each row in the window
// exactly once, in (started_at, id) order, live and stopped alike; a row
// written mid-walk before the cursor is not returned, and none present when
// the walk began is skipped or repeated. db carries the current schema.
func nodeStartsSuite(t *testing.T, db *gorm.DB) {
	t.Helper()
	type start struct {
		id string
		at time.Time
	}
	var rows []start
	shared := nodeStartsBase.Add(200 * 24 * time.Hour)
	for i := range 250 {
		id := fmt.Sprintf("node-%03d", i)
		at := nodeStartsBase.Add(time.Duration(i) * 38 * time.Hour) // 250 starts over about 400 days
		if i >= 120 && i < 130 {
			at = shared
		}
		writeNodeStart(t, db, id, at, i%3 == 0)
		rows = append(rows, start{id, at})
	}
	slices.SortFunc(rows, func(a, b start) int {
		if c := a.at.Compare(b.at); c != 0 {
			return c
		}
		return strings.Compare(a.id, b.id)
	})
	ctx := clockCtx(context.Background(), models.NewFixedClock(nodeStartsBase.Add(500*24*time.Hour)))

	since, until := nodeStartsBase.Add(50*24*time.Hour), nodeStartsBase.Add(350*24*time.Hour)
	var want []string
	for _, r := range rows {
		if !r.at.Before(since) && r.at.Before(until) {
			want = append(want, r.id)
		}
	}
	got := walkNodeStarts(t, ctx, db, NodeStartsParams{Since: since, Until: until, Limit: 7}, nil)
	assert.Equal(t, want, nodeStartIDs(got), "every start in [Since, Until) once, in (started_at, id) order")
	for _, s := range got {
		assert.False(t, s.StartedAt.Before(since), s.NodeID)
		assert.True(t, s.StartedAt.Before(until), s.NodeID)
	}

	var everything []string
	for _, r := range rows {
		everything = append(everything, r.id)
	}
	late := 0
	got = walkNodeStarts(t, ctx, db, NodeStartsParams{Limit: 7}, func() {
		late++
		writeNodeStart(t, db, fmt.Sprintf("late-%03d", late), nodeStartsBase.Add(time.Hour), false)
	})
	assert.Equal(t, everything, nodeStartIDs(got),
		"a zero window reads every row kept; a row written mid-walk before the cursor is not returned")
}

// TestNodeStartsPageInStartOrderWithoutGapsOrRepeats runs the suite on SQLite,
// then proves the page sizes and what each entry carries.
func TestNodeStartsPageInStartOrderWithoutGapsOrRepeats(t *testing.T) {
	t.Parallel()

	t.Run("the walk", func(t *testing.T) {
		t.Parallel()
		nodeStartsSuite(t, newDB(t))
	})

	t.Run("a page holds 100 by default and at most 200", func(t *testing.T) {
		t.Parallel()
		db := newDB(t)
		for i := range 250 {
			writeNodeStart(t, db, fmt.Sprintf("node-%03d", i), nodeStartsBase.Add(time.Duration(i)*time.Minute), false)
		}
		ctx := clockCtx(context.Background(), models.NewFixedClock(nodeStartsBase.Add(24*time.Hour)))
		page, err := ListNodeStarts(ctx, db, NodeStartsParams{})
		require.NoError(t, err)
		assert.Len(t, page.Starts, 100)
		assert.NotEmpty(t, page.NextCursor)
		page, err = ListNodeStarts(ctx, db, NodeStartsParams{Limit: 500})
		require.NoError(t, err)
		assert.Len(t, page.Starts, MaxReadLimit)
		page, err = ListNodeStarts(ctx, db, NodeStartsParams{Limit: 50, After: page.NextCursor})
		require.NoError(t, err)
		assert.Len(t, page.Starts, 50)
		assert.Empty(t, page.NextCursor, "the last page has no cursor")

		page, err = ListNodeStarts(clockCtx(context.Background(), models.NewFixedClock(nodeStartsBase.Add(30*time.Second))),
			db, NodeStartsParams{})
		require.NoError(t, err)
		assert.Equal(t, []string{"node-000"}, nodeStartIDs(page.Starts), "a zero Until reads to now")
	})

	t.Run("each entry carries the node's release and first runner", func(t *testing.T) {
		t.Parallel()
		db := newDB(t)
		writeNodeStart(t, db, "node-a", nodeStartsBase, true,
			NodeRunner{Queues: []string{"default"}, ExecutorClass: "local", Concurrency: 4},
			NodeRunner{Queues: []string{"exports"}, ExecutorClass: "batch", Concurrency: 1})
		writeNodeStart(t, db, "node-b", nodeStartsBase.Add(time.Hour), false)
		ctx := clockCtx(context.Background(), models.NewFixedClock(nodeStartsBase.Add(24*time.Hour)))

		page, err := ListNodeStarts(ctx, db, NodeStartsParams{})
		require.NoError(t, err)
		require.Len(t, page.Starts, 2)
		assert.Equal(t, NodeStart{
			NodeID: "node-a", Version: "v1.node-a", Commit: "c-node-a", ExecutorClass: "local", StartedAt: nodeStartsBase,
		}, page.Starts[0], "a stopped node is a start too, with its first runner's class")
		assert.Empty(t, page.Starts[1].ExecutorClass, "a node with no runner has no class")
		assert.Equal(t, time.UTC, page.Starts[1].StartedAt.Location())
	})
}

// TestListNodeStartsRefusesABadWindowOrCursor proves the two refusals.
func TestListNodeStartsRefusesABadWindowOrCursor(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	ctx := context.Background()
	at := nodeStartsBase

	for _, tc := range []struct {
		name  string
		p     NodeStartsParams
		field string
	}{
		{"an Until before Since", NodeStartsParams{Since: at, Until: at.Add(-time.Second)}, "until"},
		{"an Until equal to Since", NodeStartsParams{Since: at, Until: at}, "until"},
		{"a cursor without its prefix", NodeStartsParams{After: "eyJ0IjoiMjAyNiJ9"}, "after"},
		{"a cursor that does not decode", NodeStartsParams{After: "ns1.!!!"}, "after"},
		{"a cursor of the wrong shape", NodeStartsParams{After: "ns1.bm90LWpzb24"}, "after"},
	} {
		_, err := ListNodeStarts(ctx, db, tc.p)
		var verr *ValidationError
		require.ErrorAs(t, err, &verr, tc.name)
		assert.Equal(t, tc.field, verr.Field, tc.name)
	}
}
