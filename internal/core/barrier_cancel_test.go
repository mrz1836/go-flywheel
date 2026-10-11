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

// pendingChildIDs returns a parent's children of kind "child" in state, in id
// order.
func pendingChildIDs(t testing.TB, db *gorm.DB, parentID string, state JobState) []string {
	t.Helper()
	var ids []string
	require.NoError(t, db.Model(&jobRow{}).
		Where("parent_job_id = ? AND kind = ? AND state = ?", parentID, "child", string(state)).
		Order("id").Pluck("id", &ids).Error)
	return ids
}

// assertBarrierContinuations checks how many continuations of kind "finalize"
// the parent's barrier enqueued. One means it fired exactly once: the
// continuation is keyed on the parent, joins the parent's generation, waits to
// run, and the parent's barrier is cleared. Zero means it is still armed.
func assertBarrierContinuations(t testing.TB, db *gorm.DB, parentID string, want int64) {
	t.Helper()
	require.EqualValues(t, want, countJobsOfKind(t, db, "finalize"), "continuations enqueued")

	var parent jobRow
	require.NoError(t, db.Where("id = ?", parentID).First(&parent).Error)
	if want == 0 {
		assert.NotNil(t, parent.BarrierKind, "a barrier that has not fired stays armed")
		return
	}
	assert.Nil(t, parent.BarrierKind, "the barrier is cleared once it fires")

	var cont jobRow
	require.NoError(t, db.Where("kind = ?", "finalize").First(&cont).Error)
	require.NotNil(t, cont.UniqueKey)
	assert.Equal(t, "barrier:"+parentID, *cont.UniqueKey)
	require.NotNil(t, cont.ParentJobID)
	assert.Equal(t, parentID, *cont.ParentJobID)
	assert.Equal(t, string(StateAvailable), cont.State, "the continuation waits to run")
}

// cancelBarrierCase is one way a cancel can make children terminal outside a
// finalize, and how many continuations it must leave behind.
type cancelBarrierCase struct {
	name     string
	children int
	act      func(t *testing.T, ctx context.Context, d Driver, db *gorm.DB, parentID string)
	want     int64
}

// cancelBarrierCases are the cancel paths that end a generation, and one that
// does not.
func cancelBarrierCases() []cancelBarrierCase {
	return []cancelBarrierCase{
		{
			name:     "CancelByParent with no child running",
			children: 3,
			act: func(t *testing.T, ctx context.Context, d Driver, db *gorm.DB, parentID string) {
				finalizeJob(t, d, ctx, claimOne(t, d, ctx), Result{}, nil) // one child succeeds first
				res, err := CancelByParent(ctx, db, parentID, ScopeOpts{})
				require.NoError(t, err)
				assert.EqualValues(t, 2, res.Changed, "the two pending children are cancelled")
				assert.EqualValues(t, 1, res.SkippedTerminal, "the succeeded child is left as it was")
			},
			want: 1,
		},
		{
			name:     "CancelByParent whose last batch is full",
			children: 2,
			act: func(t *testing.T, ctx context.Context, _ Driver, db *gorm.DB, parentID string) {
				res, err := CancelByParent(ctx, db, parentID, ScopeOpts{BatchSize: 2})
				require.NoError(t, err)
				assert.EqualValues(t, 2, res.Changed,
					"the call cancels the two children and never the continuation it enqueued")
				assert.Equal(t, 1, res.Batches)
			},
			want: 1,
		},
		{
			name:     "CancelJob on the last pending child",
			children: 3,
			act: func(t *testing.T, ctx context.Context, d Driver, db *gorm.DB, parentID string) {
				finalizeJob(t, d, ctx, claimOne(t, d, ctx), Result{}, nil)
				finalizeJob(t, d, ctx, claimOne(t, d, ctx), Result{}, permanentErr())
				last := pendingChildIDs(t, db, parentID, StateAvailable)
				require.Len(t, last, 1)
				require.NoError(t, CancelJob(ctx, db, last[0]))
			},
			want: 1,
		},
		{
			name:     "CancelJob on a running child that is the last",
			children: 3,
			act: func(t *testing.T, ctx context.Context, d Driver, db *gorm.DB, parentID string) {
				finalizeJob(t, d, ctx, claimOne(t, d, ctx), Result{}, nil)
				finalizeJob(t, d, ctx, claimOne(t, d, ctx), Result{}, nil)
				running := claimOne(t, d, ctx)
				runID := models.NewID()
				require.NoError(t, d.InsertRunStub(ctx, runID, running, time.Now(), "local", "exec"))

				require.NoError(t, CancelJob(ctx, db, running.ID))
				assertBarrierContinuations(t, db, parentID, 1)

				// The cancelled attempt finishes afterwards. Its finalize is
				// superseded, so it enqueues nothing more.
				out, err := d.Finalize(ctx, running, runID, Result{}, nil, time.Now())
				require.NoError(t, err)
				assert.True(t, out.Superseded, "the cancel took the attempt's claim")
				assert.Equal(t, StateCancelled, out.State)
			},
			want: 1,
		},
		{
			name:     "CancelJob on one of two pending children",
			children: 2,
			act: func(t *testing.T, ctx context.Context, _ Driver, db *gorm.DB, parentID string) {
				ids := pendingChildIDs(t, db, parentID, StateAvailable)
				require.Len(t, ids, 2)
				require.NoError(t, CancelJob(ctx, db, ids[0]))
			},
			want: 0,
		},
	}
}

// cancelBarrierSuite runs every cancel case on a fresh database from open.
func cancelBarrierSuite(t *testing.T, open dbOpener) {
	t.Helper()
	for _, tc := range cancelBarrierCases() {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			db := open(t)
			d := driverForTest(t, db)
			ctx := context.Background()
			parentID := declareBarrierParent(t, d, db, ctx, "child", "finalize", tc.children)

			tc.act(t, ctx, d, db, parentID)
			assertBarrierContinuations(t, db, parentID, tc.want)
		})
	}
}

// TestCancellingTheLastPendingChildFiresItsParentsBarrier proves a barrier runs
// its continuation when a cancel, not a finalize, makes the last pending child
// terminal: CancelByParent, including a batch that fills exactly, and CancelJob,
// including on a running child. A cancel that leaves a child pending fires
// nothing.
func TestCancellingTheLastPendingChildFiresItsParentsBarrier(t *testing.T) {
	t.Parallel()
	cancelBarrierSuite(t, sqliteOpener)
}
