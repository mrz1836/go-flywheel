//go:build integration

package core

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// TestEveryControlRecordsOneEntryPerChangedJobPostgres mirrors the entries each
// control writes on PostgreSQL, where every control locks the rows it changes.
func TestEveryControlRecordsOneEntryPerChangedJobPostgres(t *testing.T) {
	t.Parallel()
	assertEveryControlRecordsOneEntryPerChangedJob(t, NewPostgresIsolatedDB(t))
}

// TestAControlRecordsTheStateTheJobWasInWhenItChangedPostgres proves a control
// records the state it changed the job from, read under its row lock: a cancel
// that waits on an uncommitted retry sees the retry's result once it commits,
// and records available, not the retryable it would have read without the lock.
func TestAControlRecordsTheStateTheJobWasInWhenItChangedPostgres(t *testing.T) {
	t.Parallel()
	db := NewPostgresIsolatedDB(t)
	ctx := context.Background()
	seedJob(t, db, jobRow{ID: "race-1", Kind: "leaf", State: string(StateRetryable), ScheduledAt: time.Now().UTC()})

	holder := db.Begin()
	require.NoError(t, holder.Error)
	t.Cleanup(func() { holder.Rollback() })
	require.NoError(t, RetryJob(ctx, holder, "race-1"), "the retry's change is held uncommitted")

	done := make(chan error, 1)
	go func() { done <- CancelJob(ctx, db, "race-1") }()
	awaitBlockedBy(t, db, backendPID(t, holder), "the cancel waits on the retry's row lock")
	require.NoError(t, holder.Commit().Error)

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(30 * time.Second):
		t.Fatal("the cancel did not finish")
	}
	assert.Equal(t, string(StateCancelled), jobState(t, db, "race-1"))
	rows := auditRows(t, db, "race-1")
	require.Len(t, rows, 2)
	byAction := map[string]jobAuditRow{rows[0].Action: rows[0], rows[1].Action: rows[1]}
	assert.Equal(t, "retryable", auditDetails(t, byAction[string(AuditRetried)])["from_state"])
	assert.Equal(t, "available", auditDetails(t, byAction[string(AuditCancelled)])["from_state"],
		"the cancel records the state it changed the job from")
}

// TestBatchControlsRefuseADialectorWithoutReturningPostgres proves a batch that
// can't read back the ids its update changed fails and rolls back rather than
// record less than it changed.
func TestBatchControlsRefuseADialectorWithoutReturningPostgres(t *testing.T) {
	t.Parallel()
	db := NewPostgresIsolatedDB(t)
	var schema string
	require.NoError(t, db.Raw("SELECT current_schema()").Scan(&schema).Error)
	plain, err := gorm.Open(postgres.New(postgres.Config{
		DSN: withSearchPath(requirePostgresDSN(t), schema), WithoutReturning: true,
	}), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	require.NoError(t, err)
	t.Cleanup(func() {
		if sqlDB, dbErr := plain.DB(); dbErr == nil {
			_ = sqlDB.Close()
		}
	})
	seedChildrenBulk(t, db, "NR", 3, StateAvailable, time.Now().UTC())

	_, err = CancelByParent(context.Background(), plain, "NR", ScopeOpts{})
	require.ErrorContains(t, err, "returned none of their ids")
	assert.EqualValues(t, 3, countChildrenInState(t, db, "NR", StateAvailable), "no child changed")
	assert.Zero(t, countRows(t, db, &jobAuditRow{}), "and no entry was written")
}

// TestListAuditPagesNeverSkipOrRepeatPostgres mirrors the keyset paging on
// PostgreSQL, whose timestamps round-trip at microseconds.
func TestListAuditPagesNeverSkipOrRepeatPostgres(t *testing.T) {
	t.Parallel()
	assertListAuditPagesNeverSkipOrRepeat(t, NewPostgresIsolatedDB(t))
}
