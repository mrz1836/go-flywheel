package core

import (
	"context"
	"testing"
	"time"

	"github.com/mrz1836/go-foundation/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// assertQueueHealthDoesNotCountSucceededJobs seeds one job in every state, with
// most of them succeeded as a retained table's are, and asserts the health
// sample counts every state but succeeded. Shared by the SQLite and PostgreSQL
// suites.
func assertQueueHealthDoesNotCountSucceededJobs(t *testing.T, db *gorm.DB) {
	t.Helper()
	now := healthAnchor
	ctx := clockCtx(context.Background(), models.NewFixedClock(now))

	states := []JobState{
		StateAvailable, StateRunning, StateRetryable, StateScheduled, StatePaused,
		StateCancelled, StateDiscarded,
	}
	for _, st := range states {
		seedJob(t, db, jobRow{ID: "qs-" + string(st), Kind: "k", State: string(st), ScheduledAt: now.Add(time.Hour)})
	}
	for _, id := range []string{"qs-done-1", "qs-done-2", "qs-done-3"} {
		seedJob(t, db, jobRow{
			ID: id, Kind: "k", State: string(StateSucceeded),
			ScheduledAt: now.Add(-time.Hour), FinalizedAt: ptrTime(now.Add(-time.Hour)),
		})
	}

	qh, err := SampleQueueHealth(ctx, db)
	require.NoError(t, err)

	assert.NotContains(t, qh.CountsByState, string(StateSucceeded),
		"the sample does not count succeeded jobs, most of a retained table")
	for _, st := range states {
		assert.EqualValues(t, 1, qh.CountsByState[string(st)], "every other state is counted: %s", st)
	}
	assert.Len(t, qh.CountsByState, len(states))
	assert.EqualValues(t, 1, qh.InFlight, "the running count still doubles as in-flight")
}

func TestQueueHealthDoesNotCountSucceededJobs(t *testing.T) {
	t.Parallel()
	assertQueueHealthDoesNotCountSucceededJobs(t, newDB(t))
}
