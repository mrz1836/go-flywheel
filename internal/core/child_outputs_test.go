package core

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestChildOutputsReturnsFinalOutputPerChild is A8: a completed generation yields
// one entry per child with its final state and its last attempt's recorded output.
func TestChildOutputsReturnsFinalOutputPerChild(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	d := NewSQLiteDriver(db)
	ctx := context.Background()

	parent := declareBarrierParent(t, d, db, ctx, "child", "finalize", 3)
	for i := range 3 {
		finalizeJob(t, d, ctx, claimOne(t, d, ctx), Result{Output: map[string]int{"n": i}}, nil)
	}

	outs, err := ChildOutputs(ctx, db, parent, ListRunsParams{})
	require.NoError(t, err)
	require.Len(t, outs, 3, "one entry per terminal child; the continuation is not terminal")

	seen := map[int]bool{}
	for _, o := range outs {
		assert.Equal(t, StateSucceeded, o.State)
		assert.Equal(t, 1, o.Attempt, "the recorded attempt is the one that completed")
		require.NotEmpty(t, o.Output)
		var payload struct{ N int }
		require.NoError(t, json.Unmarshal(o.Output, &payload))
		seen[payload.N] = true
	}
	assert.Len(t, seen, 3, "each child's own output is returned")
}

// TestChildOutputsCoversChildrenWithNoRun proves a terminal child that never ran —
// one cancelled before it was claimed — still gets an entry, with no output.
func TestChildOutputsCoversChildrenWithNoRun(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	d := NewSQLiteDriver(db)
	ctx := context.Background()

	parent := declareBarrierParent(t, d, db, ctx, "child", "finalize", 3)
	// One child runs and succeeds; the rest are cancelled before being claimed.
	finalizeJob(t, d, ctx, claimOne(t, d, ctx), Result{Output: map[string]string{"ok": "yes"}}, nil)
	res, err := CancelByParent(ctx, db, parent, ScopeOpts{})
	require.NoError(t, err)
	require.EqualValues(t, 2, res.Changed, "the two unclaimed children are cancelled")

	outs, err := ChildOutputs(ctx, db, parent, ListRunsParams{})
	require.NoError(t, err)
	require.Len(t, outs, 3, "all three children are terminal now")

	var withOutput, cancelled int
	for _, o := range outs {
		switch o.State {
		case StateSucceeded:
			withOutput++
			assert.NotEmpty(t, o.Output)
		case StateCancelled:
			cancelled++
			assert.Empty(t, o.Output, "a child cancelled before it ran has no output")
			assert.Zero(t, o.Attempt)
		}
	}
	assert.Equal(t, 1, withOutput)
	assert.Equal(t, 2, cancelled)
}

// TestChildOutputsExcludesPendingChildren proves an in-flight child is not returned:
// ChildOutputs is the fold over what the generation *produced*, so it is scoped to
// terminal children.
func TestChildOutputsExcludesPendingChildren(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	d := NewSQLiteDriver(db)
	ctx := context.Background()

	parent := declareBarrierParent(t, d, db, ctx, "child", "finalize", 3)
	finalizeJob(t, d, ctx, claimOne(t, d, ctx), Result{}, nil) // one done, two still available

	outs, err := ChildOutputs(ctx, db, parent, ListRunsParams{})
	require.NoError(t, err)
	assert.Len(t, outs, 1, "only the terminal child is folded; the pending ones are not")
}

// TestChildOutputsPagesByLimitAndCursor proves the ListRunsParams paging: Limit
// caps the page and Before walks it, newest first.
func TestChildOutputsPagesByLimitAndCursor(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	ctx := context.Background()

	// Seed five terminal children with increasing created_at, so the ordering is
	// deterministic without going through a driver.
	base := time.Now().UTC().Truncate(time.Second)
	p := "P"
	for i := range 5 {
		pid := p
		seedJob(t, db, jobRow{
			ID:          "c" + string(rune('a'+i)),
			Kind:        "child",
			State:       string(StateSucceeded),
			ParentJobID: &pid,
			CreatedAt:   base.Add(time.Duration(i) * time.Minute),
			ScheduledAt: base,
		})
	}

	first, err := ChildOutputs(ctx, db, p, ListRunsParams{Limit: 2})
	require.NoError(t, err)
	require.Len(t, first, 2, "Limit caps the page")
	assert.Equal(t, "ce", first[0].JobID, "newest child first")
	assert.Equal(t, "cd", first[1].JobID)

	next, err := ChildOutputs(ctx, db, p, ListRunsParams{Limit: 2, Before: base.Add(3 * time.Minute)})
	require.NoError(t, err)
	require.Len(t, next, 2, "the cursor walks to the next page")
	assert.Equal(t, "cc", next[0].JobID)
	assert.Equal(t, "cb", next[1].JobID)
}

func TestChildOutputsUnknownParentIsEmpty(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	outs, err := ChildOutputs(context.Background(), db, "nobody", ListRunsParams{})
	require.NoError(t, err)
	assert.Empty(t, outs)
}
