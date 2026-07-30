//go:build integration

package flywheel

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/mrz1836/go-foundation/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestReplayByParentAccountingPostgres runs the A4 accounting invariant against real
// PostgreSQL on a mixed cohort large enough to span several batches, where the
// UPDATE's state re-guard is what keeps a concurrent finalize from being
// double-counted or a terminal row resurrected. It proves the parity claim too: the
// buckets, the re-convergence, and the budget reset match the SQLite path.
func TestReplayByParentAccountingPostgres(t *testing.T) {
	t.Parallel()
	db := NewPostgresIsolatedDB(t)
	base := time.Now().UTC().Truncate(time.Second)
	fin := base.Add(-time.Hour)
	ctx := models.WithClock(context.Background(), models.NewFixedClock(base))

	p := "P"
	seedTerminalChildrenBulk(t, db, p, 2000, StateDiscarded, 5, fin)
	seedTerminalChildrenBulk(t, db, p, 300, StateSucceeded, 5, fin)
	seedChildrenBulk(t, db, p, 50, StateRunning, base)
	seedChildrenBulk(t, db, p, 100, StateAvailable, base) // neither terminal nor running

	res, err := ReplayByParent(ctx, db, p, ReplayOpts{
		RetryOpts: RetryOpts{ResetAttempts: true, Budget: 3}, BatchSize: 500,
	})
	require.NoError(t, err)

	assert.EqualValues(t, 2000, res.Changed, "only the discarded children are replayed")
	assert.EqualValues(t, 300, res.SkippedTerminal, "the succeeded children are terminal but untargeted")
	assert.EqualValues(t, 50, res.SkippedRunning, "a running attempt is never interrupted")
	assert.Equal(t, 4, res.Batches, "2000 discarded in batches of 500 is four transactions")

	universe := int64(2000 + 300 + 50)
	assert.EqualValues(t, universe, res.Changed+res.SkippedTerminal+res.SkippedRunning,
		"Changed + SkippedTerminal + SkippedRunning == the finished-or-running universe")

	assert.EqualValues(t, 0, countChildrenInState(t, db, p, StateDiscarded), "the cohort re-converged out of discarded")
	assert.EqualValues(t, 2100, countChildrenInState(t, db, p, StateAvailable), "2000 replayed + 100 already available")
	assert.EqualValues(t, 300, countChildrenInState(t, db, p, StateSucceeded), "no succeeded child re-ran")

	row := jobRowByID(t, db, "P-discarded-0")
	assert.Equal(t, 5, row.Attempt, "attempt is not rewound")
	assert.Equal(t, 8, row.MaxAttempts, "Budget 3 sets max_attempts = attempt 5 + 3")
}

// TestReplayByParentTouchesOnlyItsOwnChildrenPostgres proves the lineage scope: a
// replay of one parent leaves a sibling parent's identical cohort untouched.
func TestReplayByParentTouchesOnlyItsOwnChildrenPostgres(t *testing.T) {
	t.Parallel()
	db := NewPostgresIsolatedDB(t)
	base := time.Now().UTC().Truncate(time.Second)
	fin := base.Add(-time.Hour)
	ctx := context.Background()

	seedTerminalChildrenBulk(t, db, "P", 100, StateDiscarded, 3, fin)
	seedTerminalChildrenBulk(t, db, "Q", 100, StateDiscarded, 3, fin)

	res, err := ReplayByParent(ctx, db, "P", ReplayOpts{})
	require.NoError(t, err)
	assert.EqualValues(t, 100, res.Changed)
	assert.EqualValues(t, 0, countChildrenInState(t, db, "P", StateDiscarded), "P's children were replayed")
	assert.EqualValues(t, 100, countChildrenInState(t, db, "Q", StateDiscarded), "Q's children were left alone")
}

// TestReplayUnscopedByKindAndWindowPostgres proves the incident-shaped Replay:
// bounded by kind and a failure window, unscoped by lineage, across parents.
func TestReplayUnscopedByKindAndWindowPostgres(t *testing.T) {
	t.Parallel()
	db := NewPostgresIsolatedDB(t)
	base := time.Now().UTC().Truncate(time.Second)
	ctx := context.Background()

	within := base.Add(-10 * time.Minute)
	before := base.Add(-2 * time.Hour)
	seed := func(id, kind string, fin time.Time) {
		f := fin
		seedJob(t, db, jobRow{
			ID: id, Kind: kind, State: string(StateDiscarded),
			Attempt: 3, MaxAttempts: 3, FinalizedAt: &f, ScheduledAt: base,
		})
	}
	for i := range 40 {
		seed(fmt.Sprintf("scout-in-%d", i), "scout", within)
	}
	for i := range 10 {
		seed(fmt.Sprintf("scout-old-%d", i), "scout", before)
	}
	for i := range 25 {
		seed(fmt.Sprintf("weather-in-%d", i), "weather", within)
	}

	res, err := Replay(ctx, db, ReplayOpts{
		RetryOpts:   RetryOpts{ResetAttempts: true},
		Kinds:       []string{"scout"},
		FailedSince: base.Add(-time.Hour),
	})
	require.NoError(t, err)
	assert.EqualValues(t, 40, res.Changed, "only in-window scout failures are replayed")

	var scoutDiscarded, weatherDiscarded int64
	require.NoError(t, db.Model(&jobRow{}).
		Where("kind = ? AND state = ?", "scout", string(StateDiscarded)).Count(&scoutDiscarded).Error)
	require.NoError(t, db.Model(&jobRow{}).
		Where("kind = ? AND state = ?", "weather", string(StateDiscarded)).Count(&weatherDiscarded).Error)
	assert.EqualValues(t, 10, scoutDiscarded, "out-of-window scout failures are left discarded")
	assert.EqualValues(t, 25, weatherDiscarded, "the weather kind is untouched")
}
