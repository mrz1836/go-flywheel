package core

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// retentionStatusRules are the status tests' rules: discarded jobs kept 90
// days, a noisy kind kept 7, and a kind no job has, kept 1.
//
//nolint:gochecknoglobals // shared fixture
var retentionStatusRules = []RetentionRule{
	{State: StateDiscarded, MaxAge: 90 * retentionDay},
	{Kind: "noisy", MaxAge: 7 * retentionDay},
	{Kind: "silent", MaxAge: retentionDay},
}

// retentionStatusSuite proves what RetentionStatus reports, through the
// database open returns: a row per rule in the order given, then the default;
// each row's cutoff, before and under the stats-rollup hold; the oldest job
// and the due count of only the jobs the row decides; and the same refusals as
// retention.
func retentionStatusSuite(t *testing.T, open func(t *testing.T) *gorm.DB) {
	t.Helper()
	seed := func(t *testing.T) *gorm.DB {
		t.Helper()
		db := open(t)
		seedAged(t, db, "k-succeeded-40d", "k", StateSucceeded, 40*retentionDay)
		seedAged(t, db, "k-succeeded-10d", "k", StateSucceeded, 10*retentionDay)
		seedAged(t, db, "k-cancelled-50d", "k", StateCancelled, 50*retentionDay)
		seedAged(t, db, "k-discarded-40d", "k", StateDiscarded, 40*retentionDay)
		seedAged(t, db, "k-discarded-100d", "k", StateDiscarded, 100*retentionDay)
		seedAged(t, db, "noisy-succeeded-10d", "noisy", StateSucceeded, 10*retentionDay)
		seedAged(t, db, "noisy-succeeded-3d", "noisy", StateSucceeded, 3*retentionDay)
		// Both the discarded rule and the noisy rule match it; the kind rule
		// decides, so the discarded row neither counts it nor reports it.
		seedAged(t, db, "noisy-discarded-120d", "noisy", StateDiscarded, 120*retentionDay)
		seedJob(t, db, jobRow{ID: "running", Kind: "k", State: string(StateRunning)})
		return db
	}
	ago := func(d time.Duration) time.Time { return retentionNow.Add(-d) }
	assertRow := func(t *testing.T, row, want RetentionStatusRow) {
		t.Helper()
		assert.Equal(t, want.Kind, row.Kind)
		assert.Equal(t, want.State, row.State)
		assert.Equal(t, want.MaxAge, row.MaxAge)
		assert.True(t, want.Cutoff.Equal(row.Cutoff), "%s/%s: cutoff %s, want %s", row.Kind, row.State, row.Cutoff, want.Cutoff)
		if want.Oldest == nil {
			assert.Nil(t, row.Oldest, "%s/%s decides no finished job", row.Kind, row.State)
		} else if assert.NotNil(t, row.Oldest, "%s/%s", row.Kind, row.State) {
			assert.True(t, want.Oldest.Equal(*row.Oldest), "%s/%s: oldest %s, want %s", row.Kind, row.State, *row.Oldest, *want.Oldest)
		}
		assert.Equal(t, want.Due, row.Due, "%s/%s", row.Kind, row.State)
	}
	at := func(d time.Duration) *time.Time {
		v := ago(d)
		return &v
	}

	t.Run("a row per rule in order, then the default", func(t *testing.T) {
		t.Parallel()
		db := seed(t)
		rows, err := RetentionStatus(fixedClockCtx(retentionNow), db, 30*retentionDay, retentionStatusRules)
		require.NoError(t, err)
		require.Len(t, rows, 4)
		assertRow(t, rows[0], RetentionStatusRow{
			State: StateDiscarded, MaxAge: 90 * retentionDay, Cutoff: ago(90 * retentionDay),
			Oldest: at(100 * retentionDay), Due: JobCount{N: 1},
		})
		assertRow(t, rows[1], RetentionStatusRow{
			Kind: "noisy", MaxAge: 7 * retentionDay, Cutoff: ago(7 * retentionDay),
			Oldest: at(120 * retentionDay), Due: JobCount{N: 2},
		})
		assertRow(t, rows[2], RetentionStatusRow{
			Kind: "silent", MaxAge: retentionDay, Cutoff: ago(retentionDay),
		})
		assertRow(t, rows[3], RetentionStatusRow{
			MaxAge: 30 * retentionDay, Cutoff: ago(30 * retentionDay),
			Oldest: at(50 * retentionDay), Due: JobCount{N: 2},
		})
	})

	t.Run("without rules every job falls to the default row", func(t *testing.T) {
		t.Parallel()
		db := seed(t)
		rows, err := RetentionStatus(fixedClockCtx(retentionNow), db, 30*retentionDay, nil)
		require.NoError(t, err)
		require.Len(t, rows, 1)
		assertRow(t, rows[0], RetentionStatusRow{
			MaxAge: 30 * retentionDay, Cutoff: ago(30 * retentionDay),
			Oldest: at(120 * retentionDay), Due: JobCount{N: 5},
		})
	})

	t.Run("the stats-rollup hold caps each cutoff once the rollup covers hours", func(t *testing.T) {
		t.Parallel()
		db := seed(t)
		watermark := ago(60 * retentionDay).Truncate(time.Hour)
		writeStatsWatermark(t, db, watermark)
		rows, err := RetentionStatus(fixedClockCtx(retentionNow), db, 30*retentionDay, retentionStatusRules)
		require.NoError(t, err)
		require.Len(t, rows, 4)
		assert.True(t, ago(90*retentionDay).Equal(rows[0].Cutoff), "a cutoff before the hold is its own")
		for _, row := range rows[1:] {
			assert.True(t, watermark.Equal(row.Cutoff), "%s/%s is held at the watermark", row.Kind, row.State)
		}
		assert.Equal(t, JobCount{N: 1}, rows[1].Due, "only the noisy job before the hold is due")
		assert.Equal(t, JobCount{}, rows[3].Due, "no default job is before the hold")
		assert.True(t, at(50*retentionDay).Equal(*rows[3].Oldest), "the oldest kept job does not move with the cutoff")
	})

	t.Run("bad input is refused as retention refuses it", func(t *testing.T) {
		t.Parallel()
		db := open(t)
		ctx := context.Background()
		var verr *ValidationError
		_, err := RetentionStatus(ctx, db, 30*retentionDay, []RetentionRule{{State: StateRunning, MaxAge: retentionDay}})
		require.ErrorAs(t, err, &verr)
		assert.Equal(t, "rules[0].state", verr.Field)
		_, err = RetentionStatus(ctx, db, 0, nil)
		require.ErrorAs(t, err, &verr)
		assert.Equal(t, "max_age", verr.Field)
		_, err = RetentionStatus(ctx, db, -time.Hour, nil)
		require.ErrorAs(t, err, &verr)
		assert.Equal(t, "max_age", verr.Field)
	})
}

// TestRetentionStatusReportsTheOldestRetainedJob runs the suite on SQLite.
func TestRetentionStatusReportsTheOldestRetainedJob(t *testing.T) {
	t.Parallel()
	retentionStatusSuite(t, func(t *testing.T) *gorm.DB {
		t.Helper()
		return newDB(t)
	})
}
