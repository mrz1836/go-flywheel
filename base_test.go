package flywheel

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestJobRowBeforeCreateAppliesDefaults proves a jobs row created with only a
// Kind lands with the canonical queue/priority/state/run_on/max_attempts
// defaults and a non-zero scheduled_at — the producer defaults now live in the
// row's own lifecycle hook.
func TestJobRowBeforeCreateAppliesDefaults(t *testing.T) {
	t.Parallel()
	db := newDB(t)

	row := jobRow{Kind: "test.defaults"}
	require.NoError(t, db.Create(&row).Error)

	assert.NotEmpty(t, row.ID)
	assert.Equal(t, "default", row.Queue)
	assert.Equal(t, 100, row.Priority)
	assert.Equal(t, 25, row.MaxAttempts)
	assert.Equal(t, string(StateAvailable), row.State)
	assert.Equal(t, string(AnyClass), row.ExecutorClass, "the default executor class is the wildcard")
	assert.False(t, row.ScheduledAt.IsZero())
	assert.False(t, row.CreatedAt.IsZero())
	assert.JSONEq(t, "[]", string(row.Tags))
	assert.JSONEq(t, "{}", string(row.Args))
	assert.JSONEq(t, "{}", string(row.Metadata))
}

// TestJobRowBeforeCreateRejectsBlankKind proves a kind-less job is rejected with
// a ValidationError.
func TestJobRowBeforeCreateRejectsBlankKind(t *testing.T) {
	t.Parallel()
	db := newDB(t)

	err := db.Create(&jobRow{}).Error
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrValidation)
}

// TestJobRowBeforeCreateRejectsUnknownState proves an unrecognized state literal
// is rejected rather than silently persisted.
func TestJobRowBeforeCreateRejectsUnknownState(t *testing.T) {
	t.Parallel()
	db := newDB(t)

	err := db.Create(&jobRow{Kind: "k", State: "invented"}).Error
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrValidation)
}

// TestJobRowSoftDeleteScopesQueries proves gorm.DeletedAt soft-deletes a job:
// the default query hides it while Unscoped still sees it.
func TestJobRowSoftDeleteScopesQueries(t *testing.T) {
	t.Parallel()
	db := newDB(t)

	row := jobRow{Kind: "test.softdelete"}
	require.NoError(t, db.Create(&row).Error)

	require.NoError(t, db.Where("id = ?", row.ID).Delete(&jobRow{}).Error)

	var visible int64
	require.NoError(t, db.Model(&jobRow{}).Where("id = ?", row.ID).Count(&visible).Error)
	assert.EqualValues(t, 0, visible, "a soft-deleted job is hidden from default queries")

	var all int64
	require.NoError(t, db.Unscoped().Model(&jobRow{}).Where("id = ?", row.ID).Count(&all).Error)
	assert.EqualValues(t, 1, all, "Unscoped still sees the soft-deleted row")
}

// TestJobRunRowBeforeCreateRequiresMandatoryFields proves the append-only audit
// row enforces its required identity fields.
func TestJobRunRowBeforeCreateRequiresMandatoryFields(t *testing.T) {
	t.Parallel()
	db := newDB(t)

	// Missing JobID.
	err := db.Create(&jobRunRow{ExecutorClass: "local", ExecutorID: "h1", Outcome: "started"}).Error
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrValidation)

	// A fully specified run row mints its ID and defaults the timestamps.
	run := jobRunRow{JobID: NewID(), Attempt: 1, ExecutorClass: "local", ExecutorID: "h1", Outcome: "started"}
	require.NoError(t, db.Create(&run).Error)
	assert.NotEmpty(t, run.ID)
	assert.False(t, run.StartedAt.IsZero())
	assert.False(t, run.CreatedAt.IsZero())
}

// TestJobPeriodicRowBeforeSaveRejectsBothCronAndInterval proves the
// exactly-one-of-schedule invariant rejects a row carrying both.
func TestJobPeriodicRowBeforeSaveRejectsBothCronAndInterval(t *testing.T) {
	t.Parallel()
	db := newDB(t)

	cron := "*/5 * * * *"
	interval := 30
	err := db.Create(&jobPeriodicRow{
		Slug: "both", Kind: "k", NextRunAt: time.Now().UTC(),
		CronExpr: &cron, IntervalSeconds: &interval,
	}).Error
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrValidation)
}

// TestJobPeriodicRowBeforeSaveRejectsNeitherCronNorInterval proves a row with no
// schedule is rejected.
func TestJobPeriodicRowBeforeSaveRejectsNeitherCronNorInterval(t *testing.T) {
	t.Parallel()
	db := newDB(t)

	err := db.Create(&jobPeriodicRow{Slug: "neither", Kind: "k", NextRunAt: time.Now().UTC()}).Error
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrValidation)
}

// TestJobPeriodicRowBeforeSaveRejectsZeroNextRunAt proves next_run_at is required.
func TestJobPeriodicRowBeforeSaveRejectsZeroNextRunAt(t *testing.T) {
	t.Parallel()
	db := newDB(t)

	interval := 30
	err := db.Create(&jobPeriodicRow{Slug: "zerot", Kind: "k", IntervalSeconds: &interval}).Error
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrValidation)
}

// TestJobPeriodicRowBeforeSaveAppliesDefaultsAndStampsUpdatedAt proves a valid
// periodic lands with the queue/args defaults and a stamped UpdatedAt.
func TestJobPeriodicRowBeforeSaveAppliesDefaultsAndStampsUpdatedAt(t *testing.T) {
	t.Parallel()
	db := newDB(t)

	now := time.Now().UTC().Truncate(time.Second)
	ctx := WithClock(context.Background(), NewFixedClock(now))

	interval := 30
	row := jobPeriodicRow{Slug: "defaults", Kind: "k", NextRunAt: now, IntervalSeconds: &interval}
	require.NoError(t, db.WithContext(ctx).Create(&row).Error)

	assert.NotEmpty(t, row.ID)
	assert.Equal(t, "periodic", row.Queue)
	assert.JSONEq(t, "{}", string(row.ArgsTemplate))
	assert.Equal(t, now, row.UpdatedAt.UTC())
	assert.False(t, row.CreatedAt.IsZero())
}

// TestJobPeriodicPartialUpdateBypassesValidation proves the scheduler's
// bare-model column update (Model(&jobPeriodicRow{}).Updates(...)) is not
// tripped by the BeforeSave invariant — the guard skips identity-less updates.
func TestJobPeriodicPartialUpdateBypassesValidation(t *testing.T) {
	t.Parallel()
	db := newDB(t)

	now := time.Now().UTC()
	interval := 30
	row := jobPeriodicRow{Slug: "advance", Kind: "k", NextRunAt: now, IntervalSeconds: &interval}
	require.NoError(t, db.Create(&row).Error)

	// Advance next_run_at via a column map on a bare model, exactly as the
	// scheduler's fire() does. This must not raise a validation error.
	next := now.Add(time.Minute)
	require.NoError(t, db.Model(&jobPeriodicRow{}).
		Where("id = ?", row.ID).
		Updates(map[string]any{"next_run_at": next, "updated_at": now}).Error)

	var got jobPeriodicRow
	require.NoError(t, db.Where("id = ?", row.ID).First(&got).Error)
	assert.WithinDuration(t, next, got.NextRunAt, time.Second)
}
