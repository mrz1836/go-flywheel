//go:build integration

package flywheel

import (
	"context"
	"testing"
	"time"

	"github.com/mrz1836/go-foundation/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestProgressRollupsPostgres proves the rollups compute the same counts against
// real PostgreSQL, where the grouped reads and the parent_job_id IN / state IN
// binds render differently than on SQLite. It is the dialect the rollup's covering
// index actually serves.
func TestProgressRollupsPostgres(t *testing.T) {
	t.Parallel()
	db := NewPostgresIsolatedDB(t)
	base := time.Now().UTC().Truncate(time.Second)
	ctx := models.WithClock(context.Background(), models.NewFixedClock(base))

	seedJob(t, db, jobRow{ID: "P", Kind: "coordinator", State: string(StateRunning)})
	for i, st := range []JobState{
		StateSucceeded, StateSucceeded, StateAvailable, StateRunning, StateDiscarded,
	} {
		seedChild(t, db, "c"+string(rune('a'+i)), "P", "leaf", st, base.Add(-time.Duration(i)*time.Minute))
	}

	bp, err := Progress(ctx, db, "P")
	require.NoError(t, err)
	assert.Equal(t, StateRunning, bp.ParentState)
	assert.Equal(t, 5, bp.Total)
	assert.Equal(t, 3, bp.Terminal, "two succeeded plus one discarded")
	assert.Equal(t, 2, bp.Pending, "one available plus one running")
	assert.Positive(t, bp.OldestPendingAge, "the running child was scheduled in the past")

	many, err := ProgressMany(ctx, db, []string{"P", "unknown"})
	require.NoError(t, err)
	assert.Equal(t, 5, many["P"].Total)
	assert.Zero(t, many["unknown"].Total)
	assert.Empty(t, many["unknown"].ParentState)

	byKind, err := ProgressByKind(ctx, db, []string{"leaf", "coordinator"})
	require.NoError(t, err)
	assert.Equal(t, 5, byKind["leaf"].Total)
	assert.Equal(t, 1, byKind["coordinator"].Total)
}
