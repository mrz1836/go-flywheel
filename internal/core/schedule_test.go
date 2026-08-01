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

func TestUpsertPeriodicInsertsIntervalDefinition(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	base := time.Now().UTC().Truncate(time.Second)
	ctx := models.WithClock(context.Background(), models.NewFixedClock(base))

	require.NoError(t, UpsertPeriodic(ctx, db, PeriodicSpec{
		Slug: "p-interval", Kind: "test.k", Every: time.Minute, Active: true,
	}))

	views, err := ListPeriodics(ctx, db)
	require.NoError(t, err)
	require.Len(t, views, 1)
	assert.Equal(t, "p-interval", views[0].Slug)
	assert.Equal(t, "test.k", views[0].Kind)
	assert.Equal(t, "periodic", views[0].Queue, "queue defaults to periodic")
	assert.Equal(t, 60, views[0].IntervalSeconds)
	assert.Empty(t, views[0].Cron)
	assert.True(t, views[0].Active)
	assert.True(t, views[0].NextRunAt.After(base), "a fresh schedule does not fire immediately")
}

func TestUpsertPeriodicInsertsCronDefinition(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	ctx := context.Background()

	require.NoError(t, UpsertPeriodic(ctx, db, PeriodicSpec{
		Slug: "p-cron", Kind: "test.k", Cron: "*/5 * * * *", Queue: "q", Active: true,
		ArgsTemplate: []byte(`{"x":1}`),
	}))

	views, err := ListPeriodics(ctx, db)
	require.NoError(t, err)
	require.Len(t, views, 1)
	assert.Equal(t, "*/5 * * * *", views[0].Cron)
	assert.Zero(t, views[0].IntervalSeconds)
	assert.Equal(t, "q", views[0].Queue)
}

func TestUpsertPeriodicUpdatesExistingBySlug(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	ctx := context.Background()

	require.NoError(t, UpsertPeriodic(ctx, db, PeriodicSpec{Slug: "p", Kind: "k1", Every: time.Minute, Active: true}))
	require.NoError(t, UpsertPeriodic(ctx, db, PeriodicSpec{Slug: "p", Kind: "k2", Every: time.Minute, Active: false}))

	views, err := ListPeriodics(ctx, db)
	require.NoError(t, err)
	require.Len(t, views, 1, "re-upserting the same slug updates rather than duplicates")
	assert.Equal(t, "k2", views[0].Kind, "the kind is updated")
	assert.False(t, views[0].Active, "the active flag is updated")
}

func TestUpsertPeriodicSwitchesScheduleType(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	ctx := context.Background()

	require.NoError(t, UpsertPeriodic(ctx, db, PeriodicSpec{Slug: "p", Kind: "k", Every: time.Minute, Active: true}))
	require.NoError(t, UpsertPeriodic(ctx, db, PeriodicSpec{Slug: "p", Kind: "k", Cron: "0 * * * *", Active: true}))

	views, err := ListPeriodics(ctx, db)
	require.NoError(t, err)
	require.Len(t, views, 1)
	assert.Equal(t, "0 * * * *", views[0].Cron, "the cron expression replaces the interval")
	assert.Zero(t, views[0].IntervalSeconds, "the interval is cleared when switching to cron")
}

func TestUpsertPeriodicRejectsInvalidSpecs(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	ctx := context.Background()

	tests := map[string]PeriodicSpec{
		"missing slug":     {Kind: "k", Every: time.Minute},
		"missing kind":     {Slug: "s", Every: time.Minute},
		"neither schedule": {Slug: "s", Kind: "k"},
		"both schedules":   {Slug: "s", Kind: "k", Every: time.Minute, Cron: "* * * * *"},
		"malformed cron":   {Slug: "s", Kind: "k", Cron: "not a cron"},
	}
	for name, spec := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			require.Error(t, UpsertPeriodic(ctx, db, spec))
		})
	}
}

func TestUpsertPeriodicThenSchedulerFiresWhenDue(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	base := time.Now().UTC().Truncate(time.Second)

	// Upsert at base: next_run_at becomes base + 60s.
	require.NoError(t, UpsertPeriodic(
		models.WithClock(context.Background(), models.NewFixedClock(base)), db,
		PeriodicSpec{Slug: "fires", Kind: "test.success", Every: time.Minute, Active: true},
	))

	// At base+90s the definition is due; a tick fires it through the same insert
	// core the Scheduler uses.
	fireCtx := models.WithClock(context.Background(), models.NewFixedClock(base.Add(90*time.Second)))
	n, err := newScheduler(t, db).Tick(fireCtx)
	require.NoError(t, err)
	assert.Positive(t, n, "the upserted periodic fires once due")
	assert.Positive(t, jobCount(t, db, "test.success"))
}

func TestRetryJobActiveJobReturnsToAvailable(t *testing.T) {
	t.Parallel()

	tests := map[string]JobState{
		"available": StateAvailable,
		"running":   StateRunning,
		"retryable": StateRetryable,
		"scheduled": StateScheduled,
		"paused":    StatePaused,
	}

	for name, state := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			db := newDB(t)
			base := time.Now().UTC().Truncate(time.Second)
			leased := base.Add(time.Minute)
			finalized := base.Add(-time.Hour)
			token := "tok"
			ctx := models.WithClock(context.Background(), models.NewFixedClock(base))

			seedJob(t, db, jobRow{
				ID: "j", Kind: "k", State: string(state),
				LeasedUntil: &leased, LeaseToken: &token, FinalizedAt: &finalized,
			})

			require.NoError(t, RetryJob(ctx, db, "j"))

			row := jobRowByID(t, db, "j")
			assert.Equal(t, string(StateAvailable), row.State)
			assert.Nil(t, row.LeasedUntil, "retry releases the lease")
			assert.Nil(t, row.LeaseToken, "retry clears the fence so a running attempt cannot finalize over the re-run")
			assert.Nil(t, row.FinalizedAt, "retry clears the finalization stamp")
			assert.True(t, row.ScheduledAt.Equal(base), "the job is scheduled for now so it is re-claimed at once")
		})
	}
}

// TestRetryJobTerminalJobIsRefused is A4: a terminal job — a succeeded one above
// all — is left exactly as it is and ErrJobTerminal is returned, so a "retry now"
// can never silently re-run finished work. It fails against the earlier unguarded
// RetryJob, which returned any job to available.
func TestRetryJobTerminalJobIsRefused(t *testing.T) {
	t.Parallel()

	tests := map[string]JobState{
		"succeeded": StateSucceeded,
		"cancelled": StateCancelled,
		"discarded": StateDiscarded,
	}

	for name, state := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			db := newDB(t)
			base := time.Now().UTC().Truncate(time.Second)
			finalized := base.Add(-time.Hour)
			ctx := models.WithClock(context.Background(), models.NewFixedClock(base))

			seedJob(t, db, jobRow{
				ID: "j-terminal", Kind: "k", State: string(state), FinalizedAt: &finalized,
			})

			require.ErrorIs(t, RetryJob(ctx, db, "j-terminal"), ErrJobTerminal)

			row := jobRowByID(t, db, "j-terminal")
			assert.Equal(t, string(state), row.State, "a terminal job keeps its recorded outcome")
			require.NotNil(t, row.FinalizedAt)
			assert.True(t, row.FinalizedAt.Equal(finalized), "finalized_at is not cleared")
		})
	}
}

// TestRetryJobForceRerunsTerminalJob proves the escape hatch: Force retries a
// terminal job deliberately, returning even a succeeded one to available.
func TestRetryJobForceRerunsTerminalJob(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	ctx := context.Background()

	seedJob(t, db, jobRow{ID: "j-done", Kind: "k", State: string(StateSucceeded)})
	require.NoError(t, RetryJobWithOptions(ctx, db, "j-done", RetryOpts{Force: true}))
	assert.Equal(t, string(StateAvailable), jobState(t, db, "j-done"),
		"Force re-runs a succeeded job")
}

// TestRetryJobResetAttemptsRestoresBudgetAsHeadroom is A1: a job discarded at
// attempt == max_attempts is retried with ResetAttempts, which restores a real
// budget by raising max_attempts rather than rewinding attempt. Budget 0 (and a
// negative Budget) restores the original budget — max_attempts becomes
// attempt + old max_attempts — and a positive Budget grants exactly that many
// attempts of headroom. attempt is never written, so it stays monotonic and the
// next job_runs row cannot collide with the recorded failures (see the pg test for
// that invariant under the real claim path).
func TestRetryJobResetAttemptsRestoresBudgetAsHeadroom(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		budget          int
		wantMaxAttempts int
	}{
		"budget zero restores the original 25 as headroom": {budget: 0, wantMaxAttempts: 50},
		"budget three grants exactly three":                {budget: 3, wantMaxAttempts: 28},
		"negative budget behaves like zero":                {budget: -5, wantMaxAttempts: 50},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			db := newDB(t)
			base := time.Now().UTC().Truncate(time.Second)
			finalized := base.Add(-time.Hour)
			ctx := models.WithClock(context.Background(), models.NewFixedClock(base))

			seedJob(t, db, jobRow{
				ID: "j", Kind: "k", State: string(StateDiscarded),
				Attempt: 25, MaxAttempts: 25, FinalizedAt: &finalized,
			})

			require.NoError(t, RetryJobWithOptions(ctx, db, "j", RetryOpts{
				Force: true, ResetAttempts: true, Budget: tt.budget,
			}))

			row := jobRowByID(t, db, "j")
			assert.Equal(t, string(StateAvailable), row.State)
			assert.Equal(t, 25, row.Attempt, "attempt is never rewound — it is the job_runs audit key")
			assert.Equal(t, tt.wantMaxAttempts, row.MaxAttempts,
				"the budget is restored as headroom above the current attempt")
			assert.Nil(t, row.FinalizedAt, "the discard's finalization is cleared")
			assert.True(t, row.ScheduledAt.Equal(base), "with no Delay the job is immediately claimable")
		})
	}
}

// TestRetryJobResetAttemptsWithoutForceOnANonTerminalJob proves ResetAttempts is not
// coupled to Force: a still-retryable job (below its budget) can have its headroom
// topped up without forcing a terminal-state retry.
func TestRetryJobResetAttemptsWithoutForceOnANonTerminalJob(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	ctx := context.Background()

	seedJob(t, db, jobRow{ID: "j", Kind: "k", State: string(StateRetryable), Attempt: 3, MaxAttempts: 5})
	require.NoError(t, RetryJobWithOptions(ctx, db, "j", RetryOpts{ResetAttempts: true, Budget: 10}))

	row := jobRowByID(t, db, "j")
	assert.Equal(t, string(StateAvailable), row.State)
	assert.Equal(t, 3, row.Attempt, "attempt is untouched")
	assert.Equal(t, 13, row.MaxAttempts, "max_attempts becomes attempt + Budget")
}

// TestRetryJobDelaySchedulesIntoTheFuture proves Delay defers the re-run: the job
// returns to available but with a future scheduled_at, which the claim gates on.
func TestRetryJobDelaySchedulesIntoTheFuture(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	base := time.Now().UTC().Truncate(time.Second)
	ctx := models.WithClock(context.Background(), models.NewFixedClock(base))

	seedJob(t, db, jobRow{ID: "j", Kind: "k", State: string(StateRetryable)})
	require.NoError(t, RetryJobWithOptions(ctx, db, "j", RetryOpts{Delay: 10 * time.Minute}))

	row := jobRowByID(t, db, "j")
	assert.Equal(t, string(StateAvailable), row.State, "the state advances to available")
	assert.True(t, row.ScheduledAt.Equal(base.Add(10*time.Minute)),
		"Delay defers the re-run without a state change, gated by the claim's scheduled_at <= now")
}

// TestRetryJobWithoutResetAttemptsLeavesTheBudgetAlone pins the default: a plain
// retry grants no new headroom, so a job discarded at its ceiling is re-claimable
// but re-discards on that one attempt. This is the one-more-try shape ResetAttempts
// exists to fix, asserted so a regression that silently reset the budget is caught.
func TestRetryJobWithoutResetAttemptsLeavesTheBudgetAlone(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	ctx := context.Background()

	seedJob(t, db, jobRow{
		ID: "j", Kind: "k", State: string(StateDiscarded), Attempt: 25, MaxAttempts: 25,
	})
	require.NoError(t, RetryJobWithOptions(ctx, db, "j", RetryOpts{Force: true}))

	row := jobRowByID(t, db, "j")
	assert.Equal(t, 25, row.Attempt, "attempt is untouched")
	assert.Equal(t, 25, row.MaxAttempts, "max_attempts is untouched — no free retry without ResetAttempts")
}

func TestRetryJobMissingReturnsErrJobNotFound(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	require.ErrorIs(t, RetryJob(context.Background(), db, "nope"), ErrJobNotFound)
	require.ErrorIs(t, RetryJobWithOptions(context.Background(), db, "nope", RetryOpts{Force: true}), ErrJobNotFound,
		"Force skips the state guard, so a miss is a missing job, never a terminal one")
}

func TestRetryJobSoftDeletedReturnsErrJobNotFound(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	ctx := context.Background()

	seedJob(t, db, jobRow{ID: "j-gone", Kind: "k", State: string(StateAvailable)})
	require.NoError(t, db.Delete(&jobRow{}, "id = ?", "j-gone").Error)

	// A soft-deleted job is invisible to the guarded update and the classifying
	// count alike, so it reads as missing rather than terminal — even under Force.
	require.ErrorIs(t, RetryJob(ctx, db, "j-gone"), ErrJobNotFound)
	require.ErrorIs(t, RetryJobWithOptions(ctx, db, "j-gone", RetryOpts{Force: true}), ErrJobNotFound)
}

func TestCancelJobMovesToCancelled(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	ctx := context.Background()

	seedJob(t, db, jobRow{ID: "j-active", Kind: "k", State: string(StateAvailable)})
	require.NoError(t, CancelJob(ctx, db, "j-active"))
	assert.Equal(t, string(StateCancelled), jobState(t, db, "j-active"))
}

func TestCancelJobMissingReturnsErrJobNotFound(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	require.ErrorIs(t, CancelJob(context.Background(), db, "nope"), ErrJobNotFound)
}

// jobRowByID reads a whole jobs row. The cancel tests assert on nullable time
// columns (finalized_at, leased_until), which jobState's bare Scan into a string
// cannot express — that helper only works because state is NOT NULL.
func jobRowByID(t *testing.T, db *gorm.DB, jobID string) jobRow {
	t.Helper()
	var row jobRow
	require.NoError(t, db.Model(&jobRow{}).Where("id = ?", jobID).First(&row).Error)
	return row
}

func TestCancelJobTerminalJobIsRefused(t *testing.T) {
	t.Parallel()

	tests := map[string]JobState{
		"succeeded": StateSucceeded,
		"cancelled": StateCancelled,
		"discarded": StateDiscarded,
	}

	for name, state := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			db := newDB(t)
			base := time.Now().UTC().Truncate(time.Second)
			finalized := base.Add(-time.Hour)
			ctx := models.WithClock(context.Background(), models.NewFixedClock(base))

			seedJob(t, db, jobRow{
				ID: "j-terminal", Kind: "k", State: string(state), FinalizedAt: &finalized,
			})

			require.ErrorIs(t, CancelJob(ctx, db, "j-terminal"), ErrJobTerminal)

			row := jobRowByID(t, db, "j-terminal")
			assert.Equal(t, string(state), row.State, "a terminal job keeps its recorded outcome")
			require.NotNil(t, row.FinalizedAt)
			assert.True(t, row.FinalizedAt.Equal(finalized), "finalized_at is not restamped")
		})
	}
}

func TestCancelJobActiveJobMovesToCancelled(t *testing.T) {
	t.Parallel()

	tests := map[string]JobState{
		"available": StateAvailable,
		"running":   StateRunning,
		"retryable": StateRetryable,
		"scheduled": StateScheduled,
	}

	for name, state := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			db := newDB(t)
			base := time.Now().UTC().Truncate(time.Second)
			leased := base.Add(time.Minute)
			ctx := models.WithClock(context.Background(), models.NewFixedClock(base))

			seedJob(t, db, jobRow{
				ID: "j-active", Kind: "k", State: string(state), LeasedUntil: &leased,
			})

			require.NoError(t, CancelJob(ctx, db, "j-active"))

			row := jobRowByID(t, db, "j-active")
			assert.Equal(t, string(StateCancelled), row.State)
			require.NotNil(t, row.FinalizedAt)
			assert.True(t, row.FinalizedAt.Equal(base), "finalized_at is stamped from the clock")
			assert.Nil(t, row.LeasedUntil, "cancelling releases the lease")
		})
	}
}

func TestCancelJobSoftDeletedReturnsErrJobNotFound(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	ctx := context.Background()

	seedJob(t, db, jobRow{ID: "j-gone", Kind: "k", State: string(StateAvailable)})
	require.NoError(t, db.Delete(&jobRow{}, "id = ?", "j-gone").Error)

	// A soft-deleted job is invisible to the guarded update and to the classifying
	// count alike, so it reads as missing rather than terminal.
	require.ErrorIs(t, CancelJob(ctx, db, "j-gone"), ErrJobNotFound)
}

func TestSetPeriodicActiveToggles(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	ctx := context.Background()

	require.NoError(t, UpsertPeriodic(ctx, db, PeriodicSpec{Slug: "p", Kind: "k", Every: time.Minute, Active: true}))

	require.NoError(t, SetPeriodicActive(ctx, db, "p", false))
	views, err := ListPeriodics(ctx, db)
	require.NoError(t, err)
	require.Len(t, views, 1)
	assert.False(t, views[0].Active, "the schedule is deactivated but preserved")

	require.NoError(t, SetPeriodicActive(ctx, db, "p", true))
	views, err = ListPeriodics(ctx, db)
	require.NoError(t, err)
	assert.True(t, views[0].Active, "the schedule is reactivated")
}

func TestSetPeriodicActiveMissingReturnsErrPeriodicNotFound(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	require.ErrorIs(t, SetPeriodicActive(context.Background(), db, "nope", false), ErrPeriodicNotFound)
}

func TestDeletePeriodicRemoves(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	ctx := context.Background()

	require.NoError(t, UpsertPeriodic(ctx, db, PeriodicSpec{Slug: "p", Kind: "k", Every: time.Minute, Active: true}))
	require.NoError(t, DeletePeriodic(ctx, db, "p"))

	views, err := ListPeriodics(ctx, db)
	require.NoError(t, err)
	assert.Empty(t, views, "the schedule is removed")
}

func TestDeletePeriodicMissingReturnsErrPeriodicNotFound(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	require.ErrorIs(t, DeletePeriodic(context.Background(), db, "nope"), ErrPeriodicNotFound)
}
