package core

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/mrz1836/go-foundation/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/datatypes"
	"gorm.io/gorm"
)

// readLimitRows is how many rows each list read's fixture holds: more than
// MaxReadLimit, so a read that honors an oversized limit, or reads everything at
// a zero one, returns more than the bound.
const readLimitRows = 250

// readLimitAnchor is the clock instant the read-limit fixtures are written and
// read against, so SlowRuns' default 24-hour window covers every seeded run.
//
//nolint:gochecknoglobals // shared deterministic test anchor
var readLimitAnchor = time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC)

// seedListReadFixture writes readLimitRows discarded jobs, each with one
// finished, failed run in the finish log; readLimitRows running jobs; and one
// job with readLimitRows attempts. It returns the id of that last job.
func seedListReadFixture(t *testing.T, db *gorm.DB) string {
	t.Helper()
	ctx := clockCtx(context.Background(), models.NewFixedClock(readLimitAnchor))

	finished := make([]jobRow, readLimitRows)
	running := make([]jobRow, readLimitRows)
	for i := range readLimitRows {
		at := readLimitAnchor.Add(-time.Duration(i+1) * time.Minute)
		finished[i] = jobRow{
			ID: fmt.Sprintf("lim-done-%03d", i), Kind: "lim", Queue: "default", State: string(StateDiscarded),
			Args: datatypes.JSON("{}"), Metadata: datatypes.JSON("{}"), Tags: datatypes.JSON("[]"),
			Attempt: 1, MaxAttempts: 1,
			ScheduledAt: at, FinalizedAt: ptrTime(at), CreatedAt: at, UpdatedAt: at,
		}
		running[i] = jobRow{
			ID: fmt.Sprintf("lim-run-%03d", i), Kind: "lim", Queue: "default", State: string(StateRunning),
			Args: datatypes.JSON("{}"), Metadata: datatypes.JSON("{}"), Tags: datatypes.JSON("[]"),
			Attempt: 1, MaxAttempts: 3,
			ScheduledAt: at, LeasedUntil: ptrTime(readLimitAnchor.Add(time.Hour)), CreatedAt: at, UpdatedAt: at,
		}
	}
	require.NoError(t, db.CreateInBatches(finished, 50).Error)
	require.NoError(t, db.CreateInBatches(running, 50).Error)

	for i := range readLimitRows {
		finishedAt := finished[i].ScheduledAt
		_, err := SeedRun(ctx, db, RunSeed{
			JobID: finished[i].ID, Attempt: 1, ExecutorID: "lim", Outcome: OutcomeError,
			StartedAt: finishedAt.Add(-time.Duration(i+1) * time.Second), FinishedAt: &finishedAt,
			ErrorMessage: "boom",
		})
		require.NoError(t, err)
	}

	const manyRuns = "lim-many-runs"
	seedJob(t, db, jobRow{
		ID: manyRuns, Kind: "lim", Queue: "default", State: string(StateRetryable),
		Attempt: readLimitRows, MaxAttempts: readLimitRows + 1, ScheduledAt: readLimitAnchor, CreatedAt: readLimitAnchor,
	})
	runs := make([]jobRunRow, readLimitRows)
	for i := range readLimitRows {
		at := readLimitAnchor.Add(time.Duration(i) * time.Second)
		runs[i] = jobRunRow{
			ID: fmt.Sprintf("lim-attempt-%03d", i+1), JobID: manyRuns, Attempt: i + 1, ExecutorID: "lim",
			StartedAt: at, Outcome: string(OutcomeError), CreatedAt: at,
		}
	}
	require.NoError(t, db.CreateInBatches(runs, 50).Error)
	return manyRuns
}

// assertEveryListReadReturnsAtMost200 asks each list read for far more rows than
// MaxReadLimit (200), over a fixture holding more than that, and expects exactly
// the bound back. Shared by the SQLite and PostgreSQL suites.
func assertEveryListReadReturnsAtMost200(t *testing.T, db *gorm.DB) {
	t.Helper()
	require.Equal(t, 200, MaxReadLimit, "the documented bound")
	manyRuns := seedListReadFixture(t, db)
	ctx := clockCtx(context.Background(), models.NewFixedClock(readLimitAnchor))
	const oversized = 1_000_000

	reads := []struct {
		name string
		read func() (int, error)
	}{
		{"ListJobs", func() (int, error) {
			out, err := ListJobs(ctx, db, ListJobsParams{Limit: oversized})
			return len(out), err
		}},
		{"ListFinished", func() (int, error) {
			out, err := ListFinished(ctx, db, ListFinishedParams{Limit: oversized})
			return len(out), err
		}},
		{"ListRunning", func() (int, error) {
			out, err := ListRunning(ctx, db, ListRunningParams{Limit: oversized})
			return len(out), err
		}},
		{"SlowRuns", func() (int, error) {
			out, err := SlowRuns(ctx, db, SlowRunsParams{Limit: oversized})
			return len(out), err
		}},
		{"RecentFailures", func() (int, error) {
			out, err := RecentFailures(ctx, db, RecentFailuresParams{Limit: oversized})
			return len(out), err
		}},
		{"ListRuns", func() (int, error) {
			out, err := ListRuns(ctx, db, manyRuns, ListRunsParams{Limit: oversized})
			return len(out), err
		}},
	}
	for _, r := range reads {
		t.Run(r.name, func(t *testing.T) {
			n, err := r.read()
			require.NoError(t, err)
			assert.Equal(t, MaxReadLimit, n, "%s returns at most MaxReadLimit rows however large the limit", r.name)
		})
	}
}

// assertListRunsWithoutALimitReadsAtMost200 reads a job with more attempts than
// MaxReadLimit (200) at a zero and a negative limit, and expects the newest page
// of MaxReadLimit runs.
func assertListRunsWithoutALimitReadsAtMost200(t *testing.T, db *gorm.DB) {
	t.Helper()
	manyRuns := seedListReadFixture(t, db)
	ctx := context.Background()

	for _, limit := range []int{0, -1} {
		runs, err := ListRuns(ctx, db, manyRuns, ListRunsParams{Limit: limit})
		require.NoError(t, err)
		require.Equal(t, MaxReadLimit, len(runs), "a limit of %d reads the newest page of MaxReadLimit runs, not every attempt", limit)
		assert.Equal(t, readLimitRows, runs[0].Attempt, "the page starts at the newest attempt")
		assert.Equal(t, readLimitRows-MaxReadLimit+1, runs[len(runs)-1].Attempt, "and ends MaxReadLimit attempts later")
	}

	next, err := ListRuns(ctx, db, manyRuns, ListRunsParams{BeforeAttempt: readLimitRows - MaxReadLimit + 1})
	require.NoError(t, err)
	assert.Equal(t, readLimitRows-MaxReadLimit, len(next), "BeforeAttempt pages past the default page to the rest")
}

func TestEveryListReadReturnsAtMost200(t *testing.T) {
	t.Parallel()
	assertEveryListReadReturnsAtMost200(t, newDB(t))
}

func TestListRunsWithoutALimitReadsAtMost200(t *testing.T) {
	t.Parallel()
	assertListRunsWithoutALimitReadsAtMost200(t, newDB(t))
}

// TestChildOutputsWithoutALimitStillReadsEveryTerminalChild guards the one read
// a zero limit leaves whole: a barrier continuation folds its whole generation,
// which BarrierMaxChildren bounds, so ChildOutputs reads every terminal child.
func TestChildOutputsWithoutALimitStillReadsEveryTerminalChild(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	ctx := context.Background()
	const parent = "lim-parent"
	seedJob(t, db, jobRow{ID: parent, Kind: "lim", State: string(StateSucceeded), ScheduledAt: readLimitAnchor})

	children := make([]jobRow, readLimitRows)
	for i := range readLimitRows {
		at := readLimitAnchor.Add(time.Duration(i) * time.Second)
		p := parent
		children[i] = jobRow{
			ID: fmt.Sprintf("lim-child-%03d", i), Kind: "lim", Queue: "default", State: string(StateSucceeded),
			Args: datatypes.JSON("{}"), Metadata: datatypes.JSON("{}"), Tags: datatypes.JSON("[]"),
			ParentJobID: &p, Attempt: 1, MaxAttempts: 1,
			ScheduledAt: at, FinalizedAt: ptrTime(at), CreatedAt: at, UpdatedAt: at,
		}
	}
	require.NoError(t, db.CreateInBatches(children, 50).Error)

	outs, err := ChildOutputs(ctx, db, parent, ListRunsParams{})
	require.NoError(t, err)
	assert.Equal(t, readLimitRows, len(outs), "a zero limit reads every terminal child of the generation")
}
