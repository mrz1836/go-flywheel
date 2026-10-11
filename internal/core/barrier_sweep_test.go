package core

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// crashDiscardBarrierSuite gives a barrier parent two single-attempt children:
// one succeeds, and the other's process dies mid-attempt. The sweep that
// discards the crashed child makes it the generation's last terminal child, so
// the sweep fires the barrier, once.
func crashDiscardBarrierSuite(t *testing.T, db *gorm.DB) {
	t.Helper()
	d := driverForTest(t, db)
	s := crashScheduler(t, db, d)
	ctx := context.Background()
	parentID := declareBarrierParent(t, d, db, ctx, "child", "finalize", 2)
	require.NoError(t, db.Model(&jobRow{}).Where("parent_job_id = ?", parentID).
		Update("max_attempts", 1).Error)

	finalizeJob(t, d, ctx, claimOne(t, d, ctx), Result{}, nil)
	crashed, _ := crashAttempt(t, ctx, d, "default")
	assertBarrierContinuations(t, db, parentID, 0)

	sweepPastTheLease(t, s)
	assert.Equal(t, string(StateDiscarded), jobRowByID(t, db, crashed.ID).State,
		"the crash spent the child's only attempt")
	assertBarrierContinuations(t, db, parentID, 1)

	// A second sweep finds nothing to reclaim and enqueues nothing more.
	sweepPastTheLease(t, s)
	assertBarrierContinuations(t, db, parentID, 1)
}

// TestCrashDiscardedChildFiresItsParentsBarrier proves a child the sweep
// discards counts toward its parent's barrier like any terminal child.
func TestCrashDiscardedChildFiresItsParentsBarrier(t *testing.T) {
	t.Parallel()
	crashDiscardBarrierSuite(t, newDB(t))
}
