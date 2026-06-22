package flywheel

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUpsertPeriodicInsertsIntervalDefinition(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	base := time.Now().UTC().Truncate(time.Second)
	ctx := WithClock(context.Background(), NewFixedClock(base))

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
		WithClock(context.Background(), NewFixedClock(base)), db,
		PeriodicSpec{Slug: "fires", Kind: "test.success", Every: time.Minute, Active: true},
	))

	// At base+90s the definition is due; a tick fires it through the same insert
	// core the Scheduler uses.
	fireCtx := WithClock(context.Background(), NewFixedClock(base.Add(90*time.Second)))
	n, err := NewScheduler(db, NewClient(db)).Tick(fireCtx)
	require.NoError(t, err)
	assert.Positive(t, n, "the upserted periodic fires once due")
	assert.Positive(t, jobCount(t, db, "test.success"))
}

func TestRetryJobReturnsTerminalJobToAvailable(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	ctx := context.Background()

	seedJob(t, db, jobRow{ID: "j-discarded", Kind: "k", State: string(StateDiscarded)})
	require.NoError(t, RetryJob(ctx, db, "j-discarded"))
	assert.Equal(t, string(StateAvailable), jobState(t, db, "j-discarded"))
}

func TestRetryJobMissingReturnsErrJobNotFound(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	require.ErrorIs(t, RetryJob(context.Background(), db, "nope"), ErrJobNotFound)
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
