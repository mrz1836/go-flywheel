package core

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// limiterStateSuite grants concurrency holds and spends a rate bucket through
// DBLimiters, adds an expired hold behind them, and reads the state back: live
// holds are summed and their soonest expiry reported, an expired hold counts
// nothing, a bucket reports its tokens as stored, and nothing is written.
func limiterStateSuite(t *testing.T, db *gorm.DB) {
	t.Helper()
	ctx := context.Background()
	t0 := time.Date(2026, 3, 2, 12, 0, 0, 0, time.UTC)

	holds, err := NewDBLimiter(db, DBLimiterConfig{MaxConcurrent: 4, HoldTTL: time.Minute})
	require.NoError(t, err)
	first, err := holds.Acquire(publishedAt(ctx, t0), "source:a", 2)
	require.NoError(t, err)
	require.Equal(t, 2, first.N)
	second, err := holds.Acquire(publishedAt(ctx, t0.Add(10*time.Second)), "source:a", 1)
	require.NoError(t, err)
	require.Equal(t, 1, second.N)
	// Acquire deletes a resource's expired holds, so the expired one is written
	// after the grants.
	require.NoError(t, db.Create(&limiterHoldRow{
		Token: "lapsed-hold", Resource: "source:a", N: 1, ExpiresAt: t0.Add(-time.Second),
	}).Error)
	// A resource whose only hold has lapsed holds nothing.
	_, err = holds.Acquire(publishedAt(ctx, t0.Add(-2*time.Minute)), "source:b", 1)
	require.NoError(t, err)

	rate, err := NewDBLimiter(db, DBLimiterConfig{Rate: 10, Interval: time.Minute})
	require.NoError(t, err)
	spent, err := rate.Acquire(publishedAt(ctx, t0), "source:rate", 3)
	require.NoError(t, err)
	require.Equal(t, 3, spent.N)

	var before int64
	require.NoError(t, db.Model(&limiterHoldRow{}).Count(&before).Error)

	infos, err := ListLimiters(publishedAt(ctx, t0.Add(20*time.Second)), db)
	require.NoError(t, err)
	require.Len(t, infos, 3)

	a := infos[0]
	assert.Equal(t, "source:a", a.Resource, "sorted by resource")
	assert.EqualValues(t, 3, a.Held, "the live holds are summed and the lapsed one counts nothing")
	require.NotNil(t, a.NextHoldExpiry)
	assertSameInstant(t, t0.Add(time.Minute), *a.NextHoldExpiry, "the soonest live hold's expiry")

	b := infos[1]
	assert.Equal(t, "source:b", b.Resource)
	assert.Zero(t, b.Held, "a resource whose holds have lapsed holds nothing")
	assert.Nil(t, b.NextHoldExpiry)

	r := infos[2]
	assert.Equal(t, "source:rate", r.Resource)
	assert.Equal(t, 7, r.Tokens, "a bucket's tokens as stored")
	assertSameInstant(t, t0, r.UpdatedAt, "and when it was stored")
	assert.Zero(t, r.Held)
	assert.Nil(t, r.NextHoldExpiry)

	var after int64
	require.NoError(t, db.Model(&limiterHoldRow{}).Count(&after).Error)
	assert.Equal(t, before, after, "the read deletes no hold, the lapsed ones included")
	var lapsed int64
	require.NoError(t, db.Model(&limiterHoldRow{}).Where("token = ?", "lapsed-hold").Count(&lapsed).Error)
	assert.EqualValues(t, 1, lapsed)
}

// TestListLimitersReportsLiveHoldsAndTheNextExpiry proves ListLimiters reports
// each bucket's stored tokens, the permits its live holds take, and the
// soonest live hold's expiry, without writing; and that it returns at most
// MaxReadLimit buckets, the first by resource.
func TestListLimitersReportsLiveHoldsAndTheNextExpiry(t *testing.T) {
	t.Parallel()

	t.Run("holds and tokens", func(t *testing.T) {
		t.Parallel()
		limiterStateSuite(t, newDB(t))
	})

	t.Run("at most MaxReadLimit buckets", func(t *testing.T) {
		t.Parallel()
		db := newDB(t)
		at := time.Date(2026, 3, 2, 12, 0, 0, 0, time.UTC)
		const total = MaxReadLimit + 50
		rows := make([]limiterBucketRow, total)
		for i := range rows {
			rows[i] = limiterBucketRow{
				Resource: fmt.Sprintf("bucket-%03d", total-1-i), Tokens: i, RefilledAt: at, UpdatedAt: at,
			}
		}
		require.NoError(t, db.Create(&rows).Error)

		infos, err := ListLimiters(context.Background(), db)
		require.NoError(t, err)
		require.Len(t, infos, MaxReadLimit)
		for i, info := range infos {
			assert.Equal(t, fmt.Sprintf("bucket-%03d", i), info.Resource)
		}
	})

	t.Run("a nil database", func(t *testing.T) {
		t.Parallel()
		_, err := ListLimiters(context.Background(), nil)
		require.Error(t, err)
	})
}
