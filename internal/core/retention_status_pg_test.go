//go:build integration

package core

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// TestRetentionStatusReportsTheOldestRetainedJobPostgres runs the suite on
// PostgreSQL, and proves a row's due count stops at DefaultCountCap with the
// planner's estimate past it.
func TestRetentionStatusReportsTheOldestRetainedJobPostgres(t *testing.T) {
	t.Parallel()
	retentionStatusSuite(t, func(t *testing.T) *gorm.DB {
		t.Helper()
		return NewPostgresIsolatedDB(t)
	})

	t.Run("a due count past the cap reads capped, with the estimate", func(t *testing.T) {
		t.Parallel()
		db := NewPostgresIsolatedDB(t)
		seedTerminalJobsBulkPG(t, db, DefaultCountCap+1, retentionNow.Add(-100*retentionDay))
		require.NoError(t, db.Exec("ANALYZE jobs").Error)
		rows, err := RetentionStatus(fixedClockCtx(retentionNow), db, 30*retentionDay, nil)
		require.NoError(t, err)
		require.Len(t, rows, 1)
		assert.EqualValues(t, DefaultCountCap, rows[0].Due.N)
		assert.True(t, rows[0].Due.Capped)
		assert.GreaterOrEqual(t, rows[0].Due.Estimate, int64(DefaultCountCap+1))
	})
}
