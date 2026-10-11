package core

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// replayRunningParent is the parent the replay-running cases hang children on.
const replayRunningParent = "replay-running-parent"

// seedRunningCandidate writes an available job of kind "replay.running" on its
// own queue, as a child of replayRunningParent when child is set, so the test can
// claim exactly that job.
func seedRunningCandidate(t testing.TB, db *gorm.DB, id, queue string, child bool) {
	t.Helper()
	at := time.Now().Add(-time.Minute)
	row := jobRow{
		ID: id, Kind: "replay.running", Queue: queue, State: string(StateAvailable),
		MaxAttempts: 5, CreatedAt: at, UpdatedAt: at, ScheduledAt: at,
	}
	if child {
		parent := replayRunningParent
		row.ParentJobID = &parent
	}
	seedJob(t, db, row)
}

// requireStatesRefusal asserts err is the replay's refusal of a States that
// names running: a *ValidationError on states.
func requireStatesRefusal(t testing.TB, err error) {
	t.Helper()
	require.ErrorIs(t, err, ErrValidation)
	var ve *ValidationError
	require.True(t, errors.As(err, &ve), "the refusal is a *ValidationError, got %v", err)
	assert.Equal(t, "states", ve.Field)
}

// assertStillRunning asserts the job is running under the claim raw holds.
func assertStillRunning(t testing.TB, db *gorm.DB, raw RawJob) {
	t.Helper()
	var row jobRow
	require.NoError(t, db.Where("id = ?", raw.ID).First(&row).Error)
	assert.Equal(t, string(StateRunning), row.State, "the running attempt is left alone")
	require.NotNil(t, row.LeaseToken, "the attempt keeps its claim")
	assert.Equal(t, raw.LeaseToken, *row.LeaseToken)
}

// replayRunningCases are the ways a replay could reach a job whose attempt is
// still running.
func replayRunningCases() []struct {
	name string
	run  func(t *testing.T, ctx context.Context, d Driver, db *gorm.DB)
} {
	return []struct {
		name string
		run  func(t *testing.T, ctx context.Context, d Driver, db *gorm.DB)
	}{
		{
			name: "Replay naming running",
			run: func(t *testing.T, ctx context.Context, d Driver, db *gorm.DB) {
				seedRunningCandidate(t, db, "replay-run-a", "run-a", false)
				raw, _ := crashAttempt(t, ctx, d, "run-a")
				_, err := Replay(ctx, db, ReplayOpts{
					Kinds: []string{"replay.running"}, States: []JobState{StateRunning},
				})
				requireStatesRefusal(t, err)
				assertStillRunning(t, db, raw)
			},
		},
		{
			name: "ReplayByParent naming running",
			run: func(t *testing.T, ctx context.Context, d Driver, db *gorm.DB) {
				seedRunningCandidate(t, db, "replay-run-b", "run-b", true)
				raw, _ := crashAttempt(t, ctx, d, "run-b")
				_, err := ReplayByParent(ctx, db, replayRunningParent, ReplayOpts{
					States: []JobState{StateRunning},
				})
				requireStatesRefusal(t, err)
				assertStillRunning(t, db, raw)
			},
		},
		{
			name: "ReplayByParent naming cancelled skips a job whose attempt still runs",
			run: func(t *testing.T, ctx context.Context, d Driver, db *gorm.DB) {
				// One child is cancelled while its claimed attempt runs; its run is
				// still started. The other is cancelled before anything claimed it.
				seedRunningCandidate(t, db, "replay-run-c1", "run-c1", true)
				seedRunningCandidate(t, db, "replay-run-c2", "run-c2", true)
				raw, _ := crashAttempt(t, ctx, d, "run-c1")
				require.NoError(t, CancelJob(ctx, db, raw.ID))
				require.NoError(t, CancelJob(ctx, db, "replay-run-c2"))

				res, err := ReplayByParent(ctx, db, replayRunningParent, ReplayOpts{
					States: []JobState{StateCancelled},
				})
				require.NoError(t, err)
				assert.EqualValues(t, 1, res.Changed, "only the child with no attempt running is replayed")
				assert.EqualValues(t, 1, res.SkippedRunning, "the child whose attempt still runs is counted as running")
				assert.Zero(t, res.SkippedTerminal)
				assert.Equal(t, string(StateCancelled), jobRowByID(t, db, raw.ID).State,
					"a replay never starts a second attempt beside one still running")
				assert.Equal(t, string(StateAvailable), jobRowByID(t, db, "replay-run-c2").State)
			},
		},
	}
}

// replayRunningSuite runs every case on a fresh database from open.
func replayRunningSuite(t *testing.T, open dbOpener) {
	t.Helper()
	for _, tc := range replayRunningCases() {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			db := open(t)
			seedJob(t, db, jobRow{
				ID: replayRunningParent, Kind: "replay.parent", Queue: "parents",
				State: string(StateSucceeded), MaxAttempts: 1,
			})
			tc.run(t, context.Background(), driverForTest(t, db), db)
		})
	}
}

// TestReplayNeverRequeuesAJobWhoseAttemptStillRuns proves a replay never starts
// a job's work twice at once: a States naming running is refused before any
// write, and a cancelled job whose attempt is still running is left cancelled.
func TestReplayNeverRequeuesAJobWhoseAttemptStillRuns(t *testing.T) {
	t.Parallel()
	replayRunningSuite(t, sqliteOpener)
}

// TestReplayLeavesARunningJobAloneEvenWithAllowRunning proves a replay ignores
// AllowRunning: a States naming running is refused before any write.
func TestReplayLeavesARunningJobAloneEvenWithAllowRunning(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	ctx := context.Background()
	d := NewSQLiteDriver(db)
	raw, _ := runningJob(t, ctx, d, db, "replay-allowed")

	_, err := Replay(ctx, db, ReplayOpts{
		RetryOpts: RetryOpts{AllowRunning: true, Force: true},
		Kinds:     []string{"crash.loop"},
		States:    []JobState{StateRunning},
	})
	requireStatesRefusal(t, err)
	assertStillRunning(t, db, raw)
}
