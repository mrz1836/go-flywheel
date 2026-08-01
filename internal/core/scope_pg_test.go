//go:build integration

package core

import (
	"context"
	"testing"
	"time"

	"github.com/mrz1836/go-foundation/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestParentScopedControlsPostgres proves the batched SELECT-then-UPDATE controls
// work against real PostgreSQL — the dialect where operator actions can genuinely
// run concurrently, and where the UPDATE's state re-guard is what keeps two callers
// from double-counting or resurrecting a terminated row.
func TestParentScopedControlsPostgres(t *testing.T) {
	t.Parallel()
	db := NewPostgresIsolatedDB(t)
	base := time.Now().UTC().Truncate(time.Second)
	finalized := base.Add(-time.Hour)
	ctx := models.WithClock(context.Background(), models.NewFixedClock(base))

	p := "P"
	seedChildrenBulk(t, db, p, 40, StateAvailable, base)
	seedChildrenBulk(t, db, p, 5, StateRunning, base)
	seedChild(t, db, "done", p, "leaf", StateSucceeded, base)
	require.NoError(t, db.Model(&jobRow{}).Where("id = ?", "done").Update("finalized_at", finalized).Error)

	// Pause holds the 40 claimable children in bounded batches.
	paused, err := PauseByParent(ctx, db, p, ScopeOpts{BatchSize: 10})
	require.NoError(t, err)
	assert.EqualValues(t, 40, paused.Changed)
	assert.EqualValues(t, 5, paused.SkippedRunning)
	assert.EqualValues(t, 1, paused.SkippedTerminal)
	assert.Equal(t, 4, paused.Batches, "40 children in batches of 10 is four transactions")
	assert.EqualValues(t, 40, countChildrenInState(t, db, p, StatePaused))

	// Resume returns them to available.
	resumed, err := ResumeByParent(ctx, db, p, ScopeOpts{})
	require.NoError(t, err)
	assert.EqualValues(t, 40, resumed.Changed)
	assert.EqualValues(t, 40, countChildrenInState(t, db, p, StateAvailable))

	// Cancel terminates the 40 and leaves the running and succeeded children alone.
	cancelled, err := CancelByParent(ctx, db, p, ScopeOpts{})
	require.NoError(t, err)
	assert.EqualValues(t, 40, cancelled.Changed)
	assert.EqualValues(t, 5, cancelled.SkippedRunning)
	assert.EqualValues(t, 1, cancelled.SkippedTerminal)
	assert.EqualValues(t, 40, countChildrenInState(t, db, p, StateCancelled))
	assert.EqualValues(t, 5, countChildrenInState(t, db, p, StateRunning), "running attempts are not interrupted")

	var done jobRow
	require.NoError(t, db.Where("id = ?", "done").First(&done).Error)
	assert.Equal(t, string(StateSucceeded), done.State, "a succeeded child is never clobbered")
	require.NotNil(t, done.FinalizedAt)
	assert.True(t, done.FinalizedAt.Equal(finalized), "its finalized_at is not restamped")
}
