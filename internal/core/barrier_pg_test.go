//go:build integration

package core

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// TestBarrierFiresExactlyOnceUnderConcurrentChaosPostgres is A7: a barrier over
// 100 children where 20 retry once, 5 are discarded, 3 are cancelled, and one
// attempt is superseded — the whole generation drained by concurrent workers —
// enqueues the continuation exactly once, keyed on the parent, across 50
// repetitions. It is the test the FOR UPDATE serialization in fireBarrierIfComplete
// exists for: without it, two children finalizing at once could each count the
// other as pending and the barrier would never fire.
func TestBarrierFiresExactlyOnceUnderConcurrentChaosPostgres(t *testing.T) {
	t.Parallel()
	db := NewPostgresIsolatedDB(t)

	const (
		reps     = 50
		children = 100
		workers  = 8
	)
	for rep := range reps {
		runBarrierChaosRep(t, db, rep, children, workers)
	}
}

// runBarrierChaosRep drives one repetition: it clears the tables, declares a
// barrier over `children` children (continuation on its own queue so the drain
// never claims it), drains them concurrently through a mix of outcomes, and asserts
// the continuation landed exactly once.
func runBarrierChaosRep(t *testing.T, db *gorm.DB, rep, children, workers int) {
	t.Helper()
	ctx := context.Background()
	require.NoError(t, db.Exec(`DELETE FROM job_runs`).Error)
	require.NoError(t, db.Exec(`DELETE FROM jobs`).Error)
	d := NewPostgresDriver(db)

	// Declare the barrier: a parent on the default queue fanning out `children`
	// children, with the continuation routed to its own queue.
	seedJob(t, db, jobRow{
		ID: "P", Kind: "coordinator", Queue: "default", State: string(StateAvailable),
		MaxAttempts: 5, ScheduledAt: time.Now().Add(-time.Minute),
	})
	parent := claimOneFrom(t, d, ctx, "default")
	finalizeJob(t, d, ctx, parent, Result{
		FollowUps: barrierFollowUps("child", children),
		Barrier:   &Barrier{Kind: "finalize", Queue: "cont"},
	}, nil)

	tags := chaosOutcomeTags(children, rep)
	var nextTag atomic.Int64
	var seen sync.Map // childID -> struct{}: a re-claimed child (retry/supersede) succeeds

	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			drainBarrierChildren(t, d, db, ctx, tags, &nextTag, &seen)
		}()
	}
	wg.Wait()

	// Every child reached a terminal state, and the continuation landed exactly once.
	var pending int64
	require.NoError(t, db.Model(&jobRow{}).
		Where("parent_job_id = ? AND kind = ? AND state NOT IN ?", "P", "child", terminalStateStrings()).
		Count(&pending).Error)
	require.Zero(t, pending, "rep %d: every child must reach a terminal state", rep)

	var conts []jobRow
	require.NoError(t, db.Where("kind = ?", "finalize").Find(&conts).Error)
	require.Len(t, conts, 1, "rep %d: the barrier fires exactly once under concurrency", rep)
	require.NotNil(t, conts[0].UniqueKey)
	assert.Equal(t, "barrier:P", *conts[0].UniqueKey, "rep %d: the continuation is barrier-keyed", rep)
	require.NotNil(t, conts[0].ParentJobID)
	assert.Equal(t, "P", *conts[0].ParentJobID)
}

// drainBarrierChildren is one worker's loop: it claims children and finalizes each
// through its assigned outcome, exiting when no child is claimable and none is
// pending. It is the concurrent finalize path the barrier's exactly-once guarantee
// is stressed on.
//
//nolint:revive // ctx-as-second-arg matches the testing.TB-first helper convention
func drainBarrierChildren(
	t *testing.T, d Driver, db *gorm.DB, ctx context.Context,
	tags []string, nextTag *atomic.Int64, seen *sync.Map,
) {
	t.Helper()
	for {
		batch, err := d.Dequeue(ctx, []string{"default"}, AnyClass, true, 1, time.Minute)
		require.NoError(t, err)
		if len(batch) == 0 {
			// Nothing claimable. If no child is still pending, this worker is done;
			// otherwise a retrying or just-reset child is briefly out of the ready set.
			var pending int64
			require.NoError(t, db.Model(&jobRow{}).
				Where("parent_job_id = ? AND kind = ? AND state NOT IN ?", "P", "child", terminalStateStrings()).
				Count(&pending).Error)
			if pending == 0 {
				return
			}
			time.Sleep(time.Millisecond)
			continue
		}
		child := batch[0]
		if _, replayed := seen.LoadOrStore(child.ID, struct{}{}); replayed {
			// A child claimed a second time (it retried or was superseded) now succeeds.
			finalizeJob(t, d, ctx, child, Result{}, nil)
			continue
		}
		applyChaosOutcome(t, d, db, ctx, child, tags[int(nextTag.Add(1)-1)])
	}
}

// applyChaosOutcome finalizes child through the outcome its tag names.
//
//nolint:revive // ctx-as-second-arg matches the testing.TB-first helper convention
func applyChaosOutcome(t *testing.T, d Driver, db *gorm.DB, ctx context.Context, child RawJob, tag string) {
	t.Helper()
	switch tag {
	case "retry":
		finalizeJob(t, d, ctx, child, Result{},
			&classifiedError{cause: errRetry, class: ErrorTransient, retryDelay: time.Nanosecond})
	case "discard":
		finalizeJob(t, d, ctx, child, Result{}, permanentErr())
	case "cancel":
		finalizeJob(t, d, ctx, child, Result{Cancel: true}, nil)
	case "supersede":
		// Reclaim the child under the attempt: clear its token so the finalize is
		// superseded (and must not fire the barrier), then return it to available so
		// the drain re-claims and completes it.
		require.NoError(t, db.Model(&jobRow{}).Where("id = ?", child.ID).Update("lease_token", nil).Error)
		out := finalizeJob(t, d, ctx, child, Result{}, nil)
		require.True(t, out.Superseded, "a cleared token supersedes the finalize")
		require.NoError(t, db.Model(&jobRow{}).Where("id = ?", child.ID).Updates(map[string]any{
			"state": string(StateAvailable), "leased_until": nil, "lease_token": nil,
			"scheduled_at": time.Now().Add(-time.Second),
		}).Error)
	default: // "succeed"
		finalizeJob(t, d, ctx, child, Result{}, nil)
	}
}

// errRetry is the transient error the chaos retries use.
//
//nolint:err113 // a fixed sentinel for the test's transient failures
var errRetry = errTestTransient()

func errTestTransient() error { return &staticErr{"transient chaos"} }

type staticErr struct{ s string }

func (e *staticErr) Error() string { return e.s }

// chaosOutcomeTags builds the 100-outcome plan for a repetition — 71 succeed, 20
// retry, 5 discard, 3 cancel, 1 supersede — rotated by rep so the outcome order,
// and therefore which child finalizes last, varies run to run.
func chaosOutcomeTags(n, rep int) []string {
	tags := make([]string, 0, n)
	add := func(tag string, count int) {
		for range count {
			tags = append(tags, tag)
		}
	}
	add("retry", 20)
	add("discard", 5)
	add("cancel", 3)
	add("supersede", 1)
	add("succeed", n-len(tags))
	// Rotate deterministically by rep (no rand: the harness forbids it and a rotation
	// is enough to vary the last-finalizer across repetitions).
	shift := rep % n
	return append(tags[shift:], tags[:shift]...)
}
