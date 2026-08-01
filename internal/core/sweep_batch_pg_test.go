//go:build integration

package core

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// TestSweepReclaimsADeepBacklogInBoundedBatchesPostgres is the bounds half of
// the mass-expiry acceptance, seeded rather than produced by expiring live
// leases.
//
// The distinction is the point. Only a *held* lease can be expired, and the
// in-flight set is capped by the connection budget, so no live scenario reaches
// a backlog this deep — the chaos scenario's population is ~16-32 by
// construction. A backlog of 200k is what an executor pool leaves behind when
// it dies, and it is reachable here only by writing the rows directly.
//
// Before this change the sweep did not merely take longer at this depth: it
// built one UPDATE binding a parameter per row, and PostgreSQL's extended
// protocol rejects a statement carrying more than 65,535 of them. The unbounded
// sweep failed outright at 65,532 rows, which made a deep backlog permanently
// unrecoverable rather than slow.
func TestSweepReclaimsADeepBacklogInBoundedBatchesPostgres(t *testing.T) {
	t.Parallel()
	db := NewPostgresIsolatedDB(t)
	now := time.Now().UTC().Truncate(time.Second)

	const backlog = 200_000
	seedExpiredLeasesBulkPG(t, db, backlog, now)

	d := NewPostgresDriverWithOptions(db, DriverOpts{SweepBatchSize: 1000})
	reclaimed, err := d.Sweep(context.Background(), now)
	require.NoError(t, err, "a backlog past the 65,535-parameter limit must still sweep")
	assert.Equal(t, backlog, reclaimed)

	var running int64
	require.NoError(t, db.Model(&jobRow{}).
		Where("state = ?", string(StateRunning)).Count(&running).Error)
	assert.EqualValues(t, 0, running, "the whole backlog is drained")

	var tokened int64
	require.NoError(t, db.Model(&jobRow{}).Where("lease_token IS NOT NULL").Count(&tokened).Error)
	assert.EqualValues(t, 0, tokened, "every reclaimed job released its fence token")
}

// TestSweepSkipsRowsLockedByAConcurrentSweepPostgres proves FOR UPDATE SKIP
// LOCKED does what it is there for: two sweeps running at once take disjoint
// batches and their union is complete, rather than one blocking on the other's
// row locks for a whole transaction.
//
// It does not make two schedulers a supported deployment — the scheduler is a
// singleton by design. It makes an accidental second one a throughput cost
// instead of an outage.
func TestSweepSkipsRowsLockedByAConcurrentSweepPostgres(t *testing.T) {
	t.Parallel()
	db := NewPostgresIsolatedDB(t)
	now := time.Now().UTC().Truncate(time.Second)

	const backlog = 20_000
	seedExpiredLeasesBulkPG(t, db, backlog, now)

	a := NewPostgresDriverWithOptions(db, DriverOpts{SweepBatchSize: 500})
	b := NewPostgresDriverWithOptions(db, DriverOpts{SweepBatchSize: 500})

	var wg sync.WaitGroup
	counts := make([]int, 2)
	errs := make([]error, 2)
	start := make(chan struct{})

	for i, d := range []Driver{a, b} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			counts[i], errs[i] = d.Sweep(context.Background(), now)
		}()
	}
	close(start)
	wg.Wait()

	require.NoError(t, errs[0])
	require.NoError(t, errs[1])

	// Disjoint: a row reclaimed by one sweep is not counted by the other, so the
	// totals sum to the backlog exactly. Double-counting here would mean the two
	// sweeps reclaimed overlapping sets, which is what SKIP LOCKED prevents.
	assert.Equal(t, backlog, counts[0]+counts[1],
		"the two sweeps reclaim disjoint sets whose union is the whole backlog")

	var running int64
	require.NoError(t, db.Model(&jobRow{}).
		Where("state = ?", string(StateRunning)).Count(&running).Error)
	assert.EqualValues(t, 0, running, "nothing is left behind by either sweep")

	// Both made progress. A sweep that blocked for the other's whole transaction
	// would finish with zero while its sibling took everything.
	assert.Positive(t, counts[0], "both sweeps make progress rather than one blocking")
	assert.Positive(t, counts[1], "both sweeps make progress rather than one blocking")
}

// TestFenceSweepClearsTheTokenPostgres mirrors the SQLite fence assertion on the
// dialect whose reclaim is a hand-written statement.
//
// It is the regression gate for the single most dangerous line in the sweep
// rewrite. The SQLite path builds its update from reclaimUpdate, so the token
// clear is shared; the PostgreSQL path spells the SET list out in raw SQL, and
// an omitted `lease_token = NULL` there would return the job to available while
// leaving the expired attempt able to finalize over the next claim — silently
// reopening the double-execution window the fence closes.
func TestFenceSweepClearsTheTokenPostgres(t *testing.T) {
	t.Parallel()
	db := NewPostgresIsolatedDB(t)
	d := NewPostgresDriver(db)
	ctx := context.Background()

	seedClaimable(t, db, 1)
	batch, err := d.Dequeue(ctx, []string{"default"}, AnyClass, true, 1, time.Millisecond)
	require.NoError(t, err)
	require.Len(t, batch, 1)
	require.NotNil(t, leaseToken(t, db, batch[0].ID), "the claim stamped a token")

	reclaimed, err := d.Sweep(ctx, time.Now().Add(time.Second))
	require.NoError(t, err)
	assert.Equal(t, 1, reclaimed)
	assert.Nil(t, leaseToken(t, db, batch[0].ID), "a reclaimed job holds no token")

	var state string
	require.NoError(t, db.Table("jobs").Select("state").
		Where("id = ?", batch[0].ID).Scan(&state).Error)
	assert.Equal(t, string(StateAvailable), state)
}

// TestSweepLeavesSoftDeletedJobsAlonePostgres pins the condition the raw
// statement has to carry by hand.
//
// The SQLite reclaim runs through Model(&jobRow{}) and inherits GORM's
// soft-delete scope for free. A raw statement inherits nothing, so without an
// explicit `deleted_at IS NULL` this dialect would reclaim soft-deleted running
// jobs and SQLite would not — a dialect divergence introduced by the very
// rewrite meant to make the dialects explicit.
func TestSweepLeavesSoftDeletedJobsAlonePostgres(t *testing.T) {
	t.Parallel()
	db := NewPostgresIsolatedDB(t)
	now := time.Now().UTC().Truncate(time.Second)
	seedExpiredLeasesBulkPG(t, db, 3, now)
	require.NoError(t, db.Where("id = ?", "bulk-expired-0").Delete(&jobRow{}).Error)

	d := NewPostgresDriver(db)
	reclaimed, err := d.Sweep(context.Background(), now)
	require.NoError(t, err)
	assert.Equal(t, 2, reclaimed, "a soft-deleted running job is not reclaimed")
}

// seedExpiredLeasesBulkPG writes n running jobs with expired leases, plus their
// started run stubs, in one server-side statement per table.
//
// A row-at-a-time seed of 200k jobs through GORM would dominate the test's
// runtime and measure the seeder rather than the sweep.
func seedExpiredLeasesBulkPG(t *testing.T, db *gorm.DB, n int, now time.Time) {
	t.Helper()
	expired := now.Add(-time.Hour)

	require.NoError(t, db.Exec(`
		INSERT INTO jobs (id, created_at, updated_at, metadata, kind, queue, args, priority,
		                  state, attempt, max_attempts, scheduled_at, leased_until, lease_token,
		                  executor_class, tags)
		SELECT 'bulk-expired-' || g, ?, ?, '{}'::jsonb, 'sweep.batch', 'default', '{}'::jsonb, 100,
		       'running', 1, 25, ?, ?, 'token-' || g, '', '[]'::jsonb
		FROM generate_series(0, ?) AS g`,
		expired, expired, expired, expired, n-1).Error)

	require.NoError(t, db.Exec(`
		INSERT INTO job_runs (id, job_id, attempt, executor_class, executor_id, started_at,
		                      outcome, enqueued_children, created_at)
		SELECT 'bulk-run-' || g, 'bulk-expired-' || g, 1, 'local', 'exec-1', ?, ?, 0, ?
		FROM generate_series(0, ?) AS g`,
		expired, string(OutcomeStarted), expired, n-1).Error)
}
