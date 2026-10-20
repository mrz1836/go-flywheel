//go:build integration

package core

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// progressHOTWrites is how many progress writes the HOT test waits for: fifty
// heartbeats' worth, enough versions of one row to fill its page more than
// once, so the test also covers the pruning that makes room for the next.
const progressHOTWrites = 50

// TestProgressWritesKeepTheRunUpdateHOTPostgres proves the heartbeat's progress
// write stays a heap-only update on PostgreSQL: no index covers the columns it
// sets, so across fifty writes, and the finalize after them, every job_runs
// update in the test's schema is HOT.
//
// It does not run in parallel. A HOT update needs room on the row's page, which
// pruning the row's dead versions makes, and a long transaction in another test
// holds pruning back; run alone, nothing but this test touches the database
// while it measures.
func TestProgressWritesKeepTheRunUpdateHOTPostgres(t *testing.T) {
	db := NewPostgresIsolatedDB(t)
	writes := countProgressWrites(t, db)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	release := make(chan struct{})
	w := &progressWorker{work: func(_ context.Context, job *Job[progressArgs]) (Result, error) {
		for i := int64(1); ; i++ {
			job.SetProgress(RunProgress{Message: "copying", Counters: map[string]int64{"rows": i}})
			select {
			case <-release:
				return Result{}, nil
			case <-time.After(time.Millisecond):
			}
		}
	}}
	_, err := Insert(ctx, NewClient(db), progressArgs{}, InsertOpts{})
	require.NoError(t, err)
	wait := drainInBackground(t, ctx, progressRunner(t, db, NewPostgresDriver(db), w))

	require.Eventually(t, func() bool { return writes.Load() >= progressHOTWrites },
		30*time.Second, 10*time.Millisecond, "the heartbeat writes the changing progress")
	close(release)
	wait()

	// The finalize is the one other update of the run. The statistics reach
	// pg_stat_user_tables a little after each backend goes idle, so poll until
	// they count every update the test made.
	want := writes.Load() + 1
	var upd, hot int64
	require.Eventually(t, func() bool {
		upd, hot = runUpdateStats(t, db)
		return upd >= want
	}, 15*time.Second, 100*time.Millisecond, "the statistics count every update")
	t.Logf("job_runs: %d updates, %d of them HOT", upd, hot)
	assert.Equal(t, upd, hot, "every job_runs update is a heap-only update")
}

// runUpdateStats reads the test schema's job_runs update counts.
func runUpdateStats(t *testing.T, db *gorm.DB) (updates, hotUpdates int64) {
	t.Helper()
	var row struct {
		Upd int64
		Hot int64
	}
	require.NoError(t, db.Raw(`SELECT n_tup_upd AS upd, n_tup_hot_upd AS hot FROM pg_stat_user_tables `+
		`WHERE schemaname = current_schema() AND relname = 'job_runs'`).Scan(&row).Error)
	return row.Upd, row.Hot
}
