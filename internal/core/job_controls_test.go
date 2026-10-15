package core

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/mrz1836/go-foundation/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// singleJobControl is one of the five single-job controls, named as its call,
// with the state it acts on.
type singleJobControl struct {
	name string
	acts JobState
	run  func(ctx context.Context, db *gorm.DB, id string) error
}

// singleJobControls returns the five controls, each called with an argument it
// accepts: a new time for RescheduleJob, priority 5 for SetJobPriority.
func singleJobControls() []singleJobControl {
	later := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	return []singleJobControl{
		{name: "RescheduleJob", acts: StateAvailable, run: func(ctx context.Context, db *gorm.DB, id string) error {
			return RescheduleJob(ctx, db, id, later)
		}},
		{name: "SetJobPriority", acts: StateAvailable, run: func(ctx context.Context, db *gorm.DB, id string) error {
			return SetJobPriority(ctx, db, id, 5)
		}},
		{name: "PauseJob", acts: StateAvailable, run: PauseJob},
		{name: "ResumeJob", acts: StatePaused, run: ResumeJob},
		{name: "DeleteJob", acts: StateDiscarded, run: DeleteJob},
	}
}

// seedControlJob writes one job of kind leaf in state, created, updated, and
// scheduled at, in the driver's own zone, and finalized at then when state is
// terminal.
func seedControlJob(t testing.TB, db *gorm.DB, id string, state JobState, at time.Time) {
	t.Helper()
	row := jobRow{
		ID: id, Kind: "leaf", State: string(state), Attempt: 1, MaxAttempts: 5,
		CreatedAt: at, UpdatedAt: at, ScheduledAt: at,
	}
	if slices.Contains(TerminalStates(), state) {
		row.FinalizedAt = &at
	}
	seedJob(t, db, row)
}

// unscopedJobRow reads a whole jobs row, soft-deleted or not.
func unscopedJobRow(t *testing.T, db *gorm.DB, id string) jobRow {
	t.Helper()
	var row jobRow
	require.NoError(t, db.Unscoped().Model(&jobRow{}).Where("id = ?", id).First(&row).Error)
	return row
}

// assertSingleJobControlsRefuseARunningJob runs each single-job control, one
// subtest per control, on jobs it must refuse: a running job (ErrJobRunning), a
// terminal one (ErrJobTerminal; DeleteJob deletes those), a missing and a
// soft-deleted one (ErrJobNotFound), and any job under an actor the history
// can't record (a *ValidationError, before anything is read). Every refusal
// leaves the row exactly as it was and writes no entry.
func assertSingleJobControlsRefuseARunningJob(t *testing.T, db *gorm.DB) {
	t.Helper()
	ctx := WithActor(context.Background(), auditOperator)
	d := driverForTest(t, db)
	at := time.Now().Add(-time.Hour)

	for _, c := range singleJobControls() {
		t.Run(c.name, func(t *testing.T) {
			raw, _ := runningJob(t, context.Background(), d, db, "running-"+c.name)
			before := jobRowByID(t, db, raw.ID)
			require.ErrorIs(t, c.run(ctx, db, raw.ID), ErrJobRunning, "a running job is never changed")
			assert.Equal(t, before, jobRowByID(t, db, raw.ID), "the running job's row is unchanged, column by column")
			assert.EqualValues(t, 1, runCount(t, db, raw.ID), "the running attempt's run is kept")
			assert.Empty(t, auditRows(t, db, raw.ID))

			if c.name != "DeleteJob" {
				for _, state := range TerminalStates() {
					id := fmt.Sprintf("%s-%s", state, c.name)
					seedControlJob(t, db, id, state, at)
					before := jobRowByID(t, db, id)
					require.ErrorIs(t, c.run(ctx, db, id), ErrJobTerminal, "a %s job", state)
					assert.Equal(t, before, jobRowByID(t, db, id), "a %s job is unchanged", state)
					assert.Empty(t, auditRows(t, db, id))
				}
			}

			require.ErrorIs(t, c.run(ctx, db, "no-such-job"), ErrJobNotFound)
			gone := "soft-deleted-" + c.name
			seedControlJob(t, db, gone, c.acts, at)
			require.NoError(t, db.Delete(&jobRow{}, "id = ?", gone).Error)
			before = unscopedJobRow(t, db, gone)
			require.ErrorIs(t, c.run(ctx, db, gone), ErrJobNotFound, "a soft-deleted job is not found")
			assert.Equal(t, before, unscopedJobRow(t, db, gone), "the soft-deleted row is left as it was")
			assert.Empty(t, auditRows(t, db, gone))

			waiting := "actor-" + c.name
			seedControlJob(t, db, waiting, c.acts, at)
			before = jobRowByID(t, db, waiting)
			for _, bad := range invalidActors() {
				badCtx := WithActor(context.Background(), bad.actor)
				assertRefusedActor(t, c.run(badCtx, db, waiting), bad)
				assertRefusedActor(t, c.run(badCtx, db, "no-such-job"), bad)
			}
			assert.Equal(t, before, jobRowByID(t, db, waiting), "a refused actor changes nothing")
			assert.Empty(t, auditRows(t, db, waiting))
		})
	}
	assert.Zero(t, countRows(t, db, &jobAuditRow{}), "no refusal writes an entry")
}

// TestSingleJobControlsRefuseARunningJob proves no single-job control changes a
// running job, a terminal one it can't act on, a missing or soft-deleted one,
// or any job under an actor the history can't record, and that each says why.
func TestSingleJobControlsRefuseARunningJob(t *testing.T) {
	t.Parallel()
	assertSingleJobControlsRefuseARunningJob(t, newDB(t))
}

// TestDeleteRefusesALiveJob proves DeleteJob deletes only a finished job: a
// waiting job is refused with ErrJobNotTerminal, a running one with
// ErrJobRunning, and a missing or soft-deleted one with ErrJobNotFound, each
// left as it was with no entry.
func TestDeleteRefusesALiveJob(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	ctx := WithActor(context.Background(), auditOperator)
	at := time.Now().Add(-time.Hour)

	for _, state := range []JobState{StateAvailable, StateRetryable, StateScheduled, StatePaused} {
		id := "live-" + string(state)
		seedControlJob(t, db, id, state, at)
		before := jobRowByID(t, db, id)
		require.ErrorIs(t, DeleteJob(ctx, db, id), ErrJobNotTerminal, "a %s job", state)
		assert.Equal(t, before, jobRowByID(t, db, id), "a %s job is unchanged", state)
		assert.Empty(t, auditRows(t, db, id))
	}
	assert.EqualError(t, ErrJobNotTerminal, "flywheel: job has not reached a terminal state")

	raw, _ := runningJob(t, context.Background(), driverForTest(t, db), db, "live-running")
	require.ErrorIs(t, DeleteJob(ctx, db, raw.ID), ErrJobRunning)
	assert.Equal(t, string(StateRunning), jobState(t, db, raw.ID))
	assert.EqualValues(t, 1, runCount(t, db, raw.ID))

	require.ErrorIs(t, DeleteJob(ctx, db, "no-such-job"), ErrJobNotFound)
	seedControlJob(t, db, "live-soft-deleted", StateSucceeded, at)
	require.NoError(t, db.Delete(&jobRow{}, "id = ?", "live-soft-deleted").Error)
	require.ErrorIs(t, DeleteJob(ctx, db, "live-soft-deleted"), ErrJobNotFound)
	assert.Equal(t, string(StateSucceeded), unscopedJobRow(t, db, "live-soft-deleted").State,
		"a soft-deleted job is not hard-deleted")
	assert.Zero(t, countRows(t, db, &jobAuditRow{}))
}

// TestDeleteRemovesRunsAndFinishEntriesAndKeepsTheHistory proves DeleteJob
// removes a finished job with its runs and their finish-log entries, keeps the
// job's history and adds its own entry, and that a barrier the deleted job
// declared goes with its row and never fires.
func TestDeleteRemovesRunsAndFinishEntriesAndKeepsTheHistory(t *testing.T) {
	t.Parallel()

	t.Run("runs, finish-log entries, and history", func(t *testing.T) {
		t.Parallel()
		db := newDB(t)
		ctx := WithActor(context.Background(), auditOperator)
		at := time.Date(2026, 1, 1, 9, 0, 0, 0, time.UTC)

		// A job that failed once, was retried by an operator, and was discarded on
		// its second attempt; and a neighbor whose rows must stay.
		for _, id := range []string{"del-1", "del-neighbor"} {
			seedJob(t, db, jobRow{ID: id, Kind: "report.build", State: string(StateRetryable), Attempt: 1, ScheduledAt: at})
			fin := at.Add(time.Minute)
			seedRun(t, db, jobRunRow{
				ID: id + "-run-1", JobID: id, Attempt: 1, Kind: "report.build", Queue: "default",
				ExecutorID: "x", Outcome: string(OutcomeError), StartedAt: at, FinishedAt: &fin,
			})
		}
		require.NoError(t, RetryJob(clockCtx(ctx, models.NewFixedClock(at.Add(90*time.Second))), db, "del-1"))
		for _, id := range []string{"del-1", "del-neighbor"} {
			fin := at.Add(2 * time.Minute)
			seedRun(t, db, jobRunRow{
				ID: id + "-run-2", JobID: id, Attempt: 2, Kind: "report.build", Queue: "default",
				ExecutorID: "x", Outcome: string(OutcomeError), StartedAt: at, FinishedAt: &fin,
			})
			require.NoError(t, db.Model(&jobRow{}).Where("id = ?", id).Updates(map[string]any{
				"state": string(StateDiscarded), "attempt": 2, "finalized_at": fin,
			}).Error)
		}
		require.NoError(t, db.Exec(fmt.Sprintf(finishLogInsert, "job_runs", "")).Error)
		for _, run := range []string{"del-1-run-1", "del-1-run-2", "del-neighbor-run-1", "del-neighbor-run-2"} {
			require.Len(t, finishEntries(t, db, run), 1, "the fixture logs %s's finish", run)
		}

		deletedAt := time.Date(2026, 1, 2, 9, 0, 0, 0, time.UTC)
		require.NoError(t, DeleteJob(clockCtx(ctx, models.NewFixedClock(deletedAt)), db, "del-1"))

		var left int64
		require.NoError(t, db.Unscoped().Model(&jobRow{}).Where("id = ?", "del-1").Count(&left).Error)
		assert.Zero(t, left, "the job is hard-deleted")
		assert.Zero(t, runCount(t, db, "del-1"), "both runs are deleted")
		assert.Empty(t, finishEntries(t, db, "del-1-run-1"), "the first run's finish-log entry is deleted")
		assert.Empty(t, finishEntries(t, db, "del-1-run-2"), "the second run's finish-log entry is deleted")

		assert.Equal(t, string(StateDiscarded), jobState(t, db, "del-neighbor"), "another job is untouched")
		assert.EqualValues(t, 2, runCount(t, db, "del-neighbor"))
		assert.Len(t, finishEntries(t, db, "del-neighbor-run-1"), 1)
		assert.Len(t, finishEntries(t, db, "del-neighbor-run-2"), 1)

		page, err := ListAudit(context.Background(), db, ListAuditParams{TargetType: AuditTargetJob, TargetID: "del-1"})
		require.NoError(t, err)
		require.Len(t, page.Entries, 2, "the earlier entry is kept, and the delete adds one")
		deleted, retried := page.Entries[0], page.Entries[1]
		assert.Equal(t, AuditRetried, retried.Action, "the history outlives the job")
		assert.Equal(t, AuditDeleted, deleted.Action)
		assert.Equal(t, AuditTargetJob, deleted.TargetType)
		assert.Equal(t, auditOperator, deleted.Actor)
		assert.True(t, deleted.CreatedAt.Equal(deletedAt), "stamped with the control's clock reading")
		var details map[string]any
		require.NoError(t, json.Unmarshal(deleted.Details, &details))
		assert.Equal(t, map[string]any{"from_state": "discarded", "kind": "report.build", "state": "discarded"}, details)
	})

	t.Run("a declared barrier goes with the row", func(t *testing.T) {
		t.Parallel()
		db := newDB(t)
		d := NewSQLiteDriver(db)
		ctx := context.Background()

		parent := declareBarrierParent(t, d, db, ctx, "child", "rollup", 1)
		require.Equal(t, string(StateSucceeded), jobState(t, db, parent))
		var child jobRow
		require.NoError(t, db.Where("parent_job_id = ?", parent).First(&child).Error)

		require.NoError(t, DeleteJob(ctx, db, parent))
		assert.Zero(t, runCount(t, db, parent), "the parent's run is deleted with it")
		require.NoError(t, CancelJob(ctx, db, child.ID), "the child's cancel finds no parent")

		assert.EqualValues(t, 0, countJobsOfKind(t, db, "rollup"), "no continuation is enqueued")
		assert.Equal(t, string(StateCancelled), jobState(t, db, child.ID))
		require.NotNil(t, jobRowByID(t, db, child.ID).ParentJobID, "the child keeps its parent id")
	})
}

// TestRescheduleMovesOnlyScheduledAt proves RescheduleJob moves a waiting job's
// scheduled_at, as given, and changes nothing else but updated_at; that a past
// time makes a job claimable at once; and that the zero time is refused.
func TestRescheduleMovesOnlyScheduledAt(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	now := time.Date(2026, 1, 1, 8, 0, 0, 0, time.UTC)
	ctx := clockCtx(WithActor(context.Background(), auditOperator), models.NewFixedClock(now))
	from := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)
	to := from.Add(time.Hour)

	for _, state := range []JobState{StateAvailable, StateRetryable, StateScheduled, StatePaused} {
		id := "resched-" + string(state)
		seedControlJob(t, db, id, state, from)
		before := jobRowByID(t, db, id)

		require.NoError(t, RescheduleJob(ctx, db, id, to))
		after := jobRowByID(t, db, id)
		assert.Equal(t, string(state), after.State, "the job keeps its state")
		assert.True(t, after.ScheduledAt.Equal(to), "scheduled_at is the new time: %s", after.ScheduledAt)
		assert.True(t, after.UpdatedAt.Equal(now), "updated_at is the control's clock reading")
		want := before
		want.ScheduledAt, want.UpdatedAt = after.ScheduledAt, after.UpdatedAt
		assert.Equal(t, want, after, "nothing else changes")

		row := onlyAuditRow(t, db, id)
		assert.Equal(t, string(AuditRescheduled), row.Action)
		assert.Equal(t, map[string]any{
			"from_state": string(state), "from": "2026-01-01T10:00:00Z", "to": "2026-01-01T11:00:00Z",
		}, auditDetails(t, row))
	}

	// A time in the past makes a job claimable at once. The times are in the
	// driver's own zone, as an insert's would be, and the job has a queue of its
	// own, since the jobs above are already due.
	ahead := time.Now().Add(time.Hour)
	seedJob(t, db, jobRow{
		ID: "resched-later", Kind: "leaf", Queue: "later", State: string(StateAvailable),
		MaxAttempts: 5, CreatedAt: ahead, UpdatedAt: ahead, ScheduledAt: ahead,
	})
	d := NewSQLiteDriver(db)
	batch, err := d.Dequeue(context.Background(), []string{"later"}, AnyClass, true, 1, time.Minute)
	require.NoError(t, err)
	require.Empty(t, batch, "a job scheduled an hour ahead is not claimable yet")
	require.NoError(t, RescheduleJob(context.Background(), db, "resched-later", time.Now().Add(-time.Hour)))
	batch, err = d.Dequeue(context.Background(), []string{"later"}, AnyClass, true, 1, time.Minute)
	require.NoError(t, err)
	require.Len(t, batch, 1, "rescheduled into the past, the job is claimable at once")
	assert.Equal(t, "resched-later", batch[0].ID)

	seedControlJob(t, db, "resched-zero", StateAvailable, from)
	before := jobRowByID(t, db, "resched-zero")
	err = RescheduleJob(ctx, db, "resched-zero", time.Time{})
	var ve *ValidationError
	require.ErrorAs(t, err, &ve)
	require.ErrorIs(t, err, ErrValidation)
	assert.Equal(t, "at", ve.Field)
	require.ErrorAs(t, RescheduleJob(ctx, db, "no-such-job", time.Time{}), &ve, "refused before the job is read")
	assert.Equal(t, before, jobRowByID(t, db, "resched-zero"), "the zero time changes nothing")
	assert.Empty(t, auditRows(t, db, "resched-zero"))
}

// TestSetJobPriorityChangesOnlyThePriority proves SetJobPriority changes a
// waiting job's priority and updated_at and nothing else, that the same
// priority changes nothing, and that a priority below 1 is refused.
func TestSetJobPriorityChangesOnlyThePriority(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	now := time.Date(2026, 1, 1, 8, 0, 0, 0, time.UTC)
	ctx := clockCtx(WithActor(context.Background(), auditOperator), models.NewFixedClock(now))
	at := time.Date(2026, 1, 1, 6, 0, 0, 0, time.UTC)

	for _, state := range []JobState{StateAvailable, StateRetryable, StateScheduled, StatePaused} {
		id := "prio-" + string(state)
		seedControlJob(t, db, id, state, at)
		before := jobRowByID(t, db, id)
		require.Equal(t, 100, before.Priority, "a job's default priority")

		require.NoError(t, SetJobPriority(ctx, db, id, 5))
		after := jobRowByID(t, db, id)
		assert.Equal(t, 5, after.Priority)
		assert.True(t, after.UpdatedAt.Equal(now), "updated_at is the control's clock reading")
		want := before
		want.Priority, want.UpdatedAt = after.Priority, after.UpdatedAt
		assert.Equal(t, want, after, "nothing else changes")
		row := onlyAuditRow(t, db, id)
		assert.Equal(t, string(AuditReprioritized), row.Action)
		assert.Equal(t, map[string]any{"from_state": string(state), "from": float64(100), "to": float64(5)},
			auditDetails(t, row))

		later := clockCtx(ctx, models.NewFixedClock(now.Add(time.Hour)))
		require.NoError(t, SetJobPriority(later, db, id, 5), "the same priority")
		assert.Equal(t, after, jobRowByID(t, db, id), "the same priority changes nothing, updated_at included")
		assert.Len(t, auditRows(t, db, id), 1, "and records nothing")
	}

	seedControlJob(t, db, "prio-low", StateAvailable, at)
	before := jobRowByID(t, db, "prio-low")
	for _, priority := range []int{0, -1} {
		err := SetJobPriority(ctx, db, "prio-low", priority)
		var ve *ValidationError
		require.ErrorAs(t, err, &ve, "priority %d", priority)
		require.ErrorIs(t, err, ErrValidation)
		assert.Equal(t, "priority", ve.Field)
		require.ErrorAs(t, SetJobPriority(ctx, db, "no-such-job", priority), &ve, "refused before the job is read")
	}
	assert.Equal(t, before, jobRowByID(t, db, "prio-low"), "a refused priority changes nothing")
	assert.Empty(t, auditRows(t, db, "prio-low"))
}

// TestResumeKeepsScheduledAt proves ResumeJob returns a paused job to available
// with its scheduled_at kept, so a job paused while deferred stays deferred and
// no runner claims it early.
func TestResumeKeepsScheduledAt(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	now := time.Now()
	ctx := clockCtx(WithActor(context.Background(), auditOperator), models.NewFixedClock(now))
	ahead := now.Add(time.Hour)
	seedControlJob(t, db, "resume-1", StatePaused, ahead)
	before := jobRowByID(t, db, "resume-1")

	require.NoError(t, ResumeJob(ctx, db, "resume-1"))
	after := jobRowByID(t, db, "resume-1")
	assert.Equal(t, string(StateAvailable), after.State)
	assert.True(t, after.ScheduledAt.Equal(before.ScheduledAt), "scheduled_at is kept: %s", after.ScheduledAt)
	assert.True(t, after.UpdatedAt.Equal(now), "updated_at is the control's clock reading")
	want := before
	want.State, want.UpdatedAt = after.State, after.UpdatedAt
	assert.Equal(t, want, after, "nothing else changes")
	row := onlyAuditRow(t, db, "resume-1")
	assert.Equal(t, string(AuditResumed), row.Action)
	assert.Equal(t, map[string]any{"from_state": "paused"}, auditDetails(t, row))

	batch, err := NewSQLiteDriver(db).Dequeue(context.Background(), []string{"default"}, AnyClass, true, 1, time.Minute)
	require.NoError(t, err)
	assert.Empty(t, batch, "a job resumed an hour before its time is not claimed early")
}

// TestPauseAndResumeAreIdempotent proves PauseJob holds a claimable job and
// does nothing to one already paused, and ResumeJob does nothing to a job that
// isn't paused: a no-op changes nothing, updated_at included, and writes no
// entry.
func TestPauseAndResumeAreIdempotent(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	now := time.Date(2026, 1, 1, 8, 0, 0, 0, time.UTC)
	ctx := clockCtx(WithActor(context.Background(), auditOperator), models.NewFixedClock(now))
	later := clockCtx(ctx, models.NewFixedClock(now.Add(time.Hour)))
	at := time.Date(2026, 1, 1, 6, 0, 0, 0, time.UTC)

	for _, state := range []JobState{StateAvailable, StateRetryable, StateScheduled} {
		id := "pause-" + string(state)
		seedControlJob(t, db, id, state, at)
		before := jobRowByID(t, db, id)

		require.NoError(t, PauseJob(ctx, db, id))
		paused := jobRowByID(t, db, id)
		assert.Equal(t, string(StatePaused), paused.State)
		assert.True(t, paused.UpdatedAt.Equal(now), "updated_at is the control's clock reading")
		want := before
		want.State, want.UpdatedAt = paused.State, paused.UpdatedAt
		assert.Equal(t, want, paused, "nothing else changes")
		row := onlyAuditRow(t, db, id)
		assert.Equal(t, string(AuditPaused), row.Action)
		assert.Equal(t, map[string]any{"from_state": string(state)}, auditDetails(t, row))

		require.NoError(t, PauseJob(later, db, id), "pausing a paused job")
		assert.Equal(t, paused, jobRowByID(t, db, id), "a second pause changes nothing, updated_at included")
		assert.Len(t, auditRows(t, db, id), 1, "and records nothing")

		resumeID := "resume-" + string(state)
		seedControlJob(t, db, resumeID, state, at)
		before = jobRowByID(t, db, resumeID)
		require.NoError(t, ResumeJob(later, db, resumeID), "resuming a %s job", state)
		assert.Equal(t, before, jobRowByID(t, db, resumeID), "resuming a job that isn't paused changes nothing")
		assert.Empty(t, auditRows(t, db, resumeID))
	}

	seedControlJob(t, db, "pause-twice", StatePaused, at)
	require.NoError(t, ResumeJob(ctx, db, "pause-twice"))
	resumed := jobRowByID(t, db, "pause-twice")
	require.NoError(t, ResumeJob(later, db, "pause-twice"), "resuming a resumed job")
	assert.Equal(t, resumed, jobRowByID(t, db, "pause-twice"))
	assert.Len(t, auditRows(t, db, "pause-twice"), 1)
}
