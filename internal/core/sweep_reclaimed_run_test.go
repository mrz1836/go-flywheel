package core

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// reclaimedRunSuite crashes a job's second attempt while its first still runs:
// the first was claimed and wrote its run, a retry with AllowRunning re-queued
// the job, and the second attempt was claimed in turn. The sweep that reclaims
// the second attempt crashes that attempt's run alone. The first attempt's run
// stays started until the first attempt returns, and its late finalize records
// it as superseded, so one reclaim counts one crash and one discard however the
// first attempt ends.
func reclaimedRunSuite(t *testing.T, db *gorm.DB) {
	t.Helper()
	ctx := context.Background()
	d := driverForTest(t, db)
	rec := &sweepRecorder{}
	s := newSchedulerCfg(t, SchedulerConfig{DB: db, Client: NewClient(db), Driver: d, Observer: rec})
	from := floorHour(time.Now()).Add(-time.Hour)
	to := from.Add(4 * time.Hour)

	id := seedCrashJob(t, db, "reclaimed-run", "reclaimed-run", 2)
	first, firstRun := crashAttempt(t, ctx, d, "reclaimed-run")
	require.NoError(t, RetryJobWithOptions(ctx, db, id, RetryOpts{AllowRunning: true}))
	_, secondRun := crashAttempt(t, ctx, d, "reclaimed-run")
	sweepPastTheLease(t, s)

	assert.Equal(t, string(StateDiscarded), jobRowByID(t, db, id).State, "the reclaimed attempt was the job's last")
	runs := runsOf(t, db, id)
	require.Len(t, runs, 2)
	require.Equal(t, firstRun, runs[0].ID)
	require.Equal(t, secondRun, runs[1].ID)
	assert.Equal(t, string(OutcomeCrashed), runs[1].Outcome, "the reclaimed attempt's run is crashed")
	require.NotNil(t, runs[1].JobState)
	assert.Equal(t, string(StateDiscarded), *runs[1].JobState, "the crashed run records the discard")
	assert.Equal(t, string(OutcomeStarted), runs[0].Outcome, "the older attempt still runs, so its run is its own to finish")
	assert.Nil(t, runs[0].JobState, "the sweep wrote nothing on the older attempt's run")
	assert.Nil(t, runs[0].FinishedAt, "the older attempt's run has not finished")

	require.Len(t, rec.events, 1)
	assert.Equal(t, 1, rec.events[0].Discarded)
	require.Len(t, rec.events[0].Jobs, 1)
	assert.Equal(t, secondRun, rec.events[0].Jobs[0].RunID, "the event names the reclaimed attempt's run")

	assertOneCrashDiscard(t, db, from, to, 0, "raw, before the late finalize")

	// The first attempt returns. Its claim went with the retry, so its finalize
	// is superseded: it records the attempt and leaves the job as it is.
	out, err := d.Finalize(ctx, first, firstRun, Result{}, nil, time.Now())
	require.NoError(t, err)
	assert.True(t, out.Superseded, "the first attempt no longer holds the claim")
	assert.Equal(t, string(StateDiscarded), jobRowByID(t, db, id).State, "a superseded finalize changes nothing on the job")

	assertOneCrashDiscard(t, db, from, to, 1, "raw, after the late finalize")
	rollupAll(t, db, to.Add(time.Hour))
	assertOneCrashDiscard(t, db, from, to, 1, "rolled up")
}

// reclaimedRunWithoutAStubSuite reclaims a second attempt whose process died
// before it wrote its run, while the first attempt's run is still started. The
// sweep crashes no run: the reclaimed attempt has none, and the older one is not
// the sweep's to end.
func reclaimedRunWithoutAStubSuite(t *testing.T, db *gorm.DB) {
	t.Helper()
	ctx := context.Background()
	d := driverForTest(t, db)
	rec := &sweepRecorder{}
	s := newSchedulerCfg(t, SchedulerConfig{DB: db, Client: NewClient(db), Driver: d, Observer: rec})

	id := seedCrashJob(t, db, "reclaimed-bare", "reclaimed-bare", 2)
	_, firstRun := crashAttempt(t, ctx, d, "reclaimed-bare")
	require.NoError(t, RetryJobWithOptions(ctx, db, id, RetryOpts{AllowRunning: true}))
	batch, err := d.Dequeue(ctx, []string{"reclaimed-bare"}, AnyClass, true, 1, crashLease)
	require.NoError(t, err)
	require.Len(t, batch, 1, "the second attempt is claimed, and dies before it writes its run")
	sweepPastTheLease(t, s)

	assert.Equal(t, string(StateDiscarded), jobRowByID(t, db, id).State, "the reclaimed attempt was the job's last")
	require.Len(t, rec.events, 1)
	require.Len(t, rec.events[0].Jobs, 1)
	assert.Empty(t, rec.events[0].Jobs[0].RunID, "the reclaimed attempt wrote no run, so the sweep names none")

	runs := runsOf(t, db, id)
	require.Len(t, runs, 1)
	assert.Equal(t, firstRun, runs[0].ID)
	assert.Equal(t, string(OutcomeStarted), runs[0].Outcome, "the older attempt's run is left to it")
	assert.Nil(t, runs[0].JobState)
}

// assertOneCrashDiscard reads the window's stats for the crash tests' kind and
// checks they count one crashed attempt and one discard, with superseded
// attempts beside them and nothing counted as a retry.
func assertOneCrashDiscard(t *testing.T, db *gorm.DB, from, to time.Time, superseded int, served string) {
	t.Helper()
	res, err := Stats(context.Background(), db, StatsParams{From: from, To: to})
	require.NoError(t, err)
	ks := mustKind(t, res, "crash.loop")
	assert.EqualValues(t, 1, ks.Outcomes.Crashed, "%s: one reclaim is one crashed attempt", served)
	assert.EqualValues(t, 1, ks.Discarded, "%s: one reclaim is at most one discard", served)
	assert.EqualValues(t, superseded, ks.Superseded, "%s: the older attempt is counted as superseded once it returns", served)
	assert.Zero(t, ks.Retries, "%s: nothing ran again", served)
}

// TestSweepCrashesOnlyTheRunOfTheReclaimedAttempt proves the sweep ends the run
// of the attempt it reclaimed and no other, so a job re-queued while an attempt
// still ran counts one crash and one discard when its next attempt crashes.
func TestSweepCrashesOnlyTheRunOfTheReclaimedAttempt(t *testing.T) {
	t.Parallel()

	t.Run("the older attempt's run is left to it", func(t *testing.T) {
		t.Parallel()
		reclaimedRunSuite(t, newDB(t))
	})
	t.Run("a reclaimed attempt with no run names none", func(t *testing.T) {
		t.Parallel()
		reclaimedRunWithoutAStubSuite(t, newDB(t))
	})
}
