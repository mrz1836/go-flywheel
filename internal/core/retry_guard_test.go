package core

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// runningJob seeds one job on its own queue, claims it, and commits its run
// stub, leaving its attempt running. It returns the claim and the run id.
//
//nolint:revive // ctx-as-second-arg matches the testing.TB-first helper convention
func runningJob(t testing.TB, ctx context.Context, d Driver, db *gorm.DB, id string) (RawJob, string) {
	t.Helper()
	seedCrashJob(t, db, id, "retry-"+id, 5)
	return crashAttempt(t, ctx, d, "retry-"+id)
}

// retryRefusesRunningSuite proves a running job is refused, with or without
// Force or ResetAttempts, and that the refusal writes nothing.
func retryRefusesRunningSuite(t *testing.T, db *gorm.DB) {
	t.Helper()
	ctx := context.Background()
	d := driverForTest(t, db)
	raw, _ := runningJob(t, ctx, d, db, "guarded")
	before := jobRowByID(t, db, raw.ID)

	require.ErrorIs(t, RetryJob(ctx, db, raw.ID), ErrJobRunning)
	for _, opts := range []RetryOpts{
		{Force: true},
		{ResetAttempts: true, Budget: 3},
		{Delay: time.Minute},
		{Force: true, ResetAttempts: true},
	} {
		require.ErrorIs(t, RetryJobWithOptions(ctx, db, raw.ID, opts), ErrJobRunning, "opts %+v", opts)
	}

	after := jobRowByID(t, db, raw.ID)
	assert.Equal(t, string(StateRunning), after.State)
	require.NotNil(t, after.LeaseToken, "the running attempt keeps its claim")
	assert.Equal(t, raw.LeaseToken, *after.LeaseToken)
	assert.Equal(t, before.LeasedUntil, after.LeasedUntil, "the lease is untouched")
	assert.Equal(t, before.MaxAttempts, after.MaxAttempts)
	assert.True(t, before.ScheduledAt.Equal(after.ScheduledAt))
	assert.True(t, before.UpdatedAt.Equal(after.UpdatedAt), "the refusal writes nothing")
}

// TestRetryRefusesARunningJob proves a retry never starts a second copy of a job
// whose attempt is running: it returns ErrJobRunning and leaves the job and its
// lease as they were.
func TestRetryRefusesARunningJob(t *testing.T) {
	t.Parallel()
	retryRefusesRunningSuite(t, newDB(t))
}

// TestAllowRunningRequeuesARunningJob proves the opt-in keeps the re-queue for a
// caller that asks for it: the job returns to available with its claim cleared,
// and the running attempt's finalize is superseded.
func TestAllowRunningRequeuesARunningJob(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	ctx := context.Background()
	d := NewSQLiteDriver(db)
	raw, runID := runningJob(t, ctx, d, db, "allowed")

	require.NoError(t, RetryJobWithOptions(ctx, db, raw.ID, RetryOpts{AllowRunning: true}))
	row := jobRowByID(t, db, raw.ID)
	assert.Equal(t, string(StateAvailable), row.State)
	assert.Nil(t, row.LeaseToken, "the running attempt's claim is gone")
	assert.Nil(t, row.LeasedUntil)

	out, err := d.Finalize(ctx, raw, runID, Result{}, nil, time.Now())
	require.NoError(t, err)
	assert.True(t, out.Superseded, "the running attempt's result is discarded")
	assert.Equal(t, StateAvailable, out.State)
}

// forceRetrySuite proves Force re-runs a terminal job and never one whose
// attempt still runs: a running job, or one cancelled while its attempt runs,
// until that attempt has returned or AllowRunning is set.
func forceRetrySuite(t *testing.T, open dbOpener) {
	t.Helper()
	force := RetryOpts{Force: true}
	for _, tc := range []struct {
		name string
		run  func(t *testing.T, ctx context.Context, d Driver, db *gorm.DB)
	}{
		{
			name: "a running job is refused",
			run: func(t *testing.T, ctx context.Context, d Driver, db *gorm.DB) {
				raw, _ := runningJob(t, ctx, d, db, "force-running")
				require.ErrorIs(t, RetryJobWithOptions(ctx, db, raw.ID, force), ErrJobRunning)
				assert.Equal(t, string(StateRunning), jobRowByID(t, db, raw.ID).State)
			},
		},
		{
			name: "a succeeded job runs again",
			run: func(t *testing.T, ctx context.Context, d Driver, db *gorm.DB) {
				seedCrashJob(t, db, "force-done", "retry-force-done", 5)
				finalizeJob(t, d, ctx, claimOneFrom(t, d, ctx, "retry-force-done"), Result{}, nil)
				require.Equal(t, string(StateSucceeded), jobRowByID(t, db, "force-done").State)

				require.ErrorIs(t, RetryJob(ctx, db, "force-done"), ErrJobTerminal, "without Force it is refused")
				require.NoError(t, RetryJobWithOptions(ctx, db, "force-done", force))
				assert.Equal(t, string(StateAvailable), jobRowByID(t, db, "force-done").State)
			},
		},
		{
			name: "a job cancelled while its attempt runs is refused",
			run: func(t *testing.T, ctx context.Context, d Driver, db *gorm.DB) {
				raw, _ := runningJob(t, ctx, d, db, "force-cancelled")
				require.NoError(t, CancelJob(ctx, db, raw.ID))

				require.ErrorIs(t, RetryJobWithOptions(ctx, db, raw.ID, force), ErrJobRunning,
					"its attempt still runs, so a re-run would overlap it")
				require.ErrorIs(t, RetryJob(ctx, db, raw.ID), ErrJobTerminal, "without Force it is terminal first")
				assert.Equal(t, string(StateCancelled), jobRowByID(t, db, raw.ID).State)
			},
		},
		{
			name: "a job cancelled while its attempt runs is re-queued with AllowRunning",
			run: func(t *testing.T, ctx context.Context, d Driver, db *gorm.DB) {
				raw, _ := runningJob(t, ctx, d, db, "force-allowed")
				require.NoError(t, CancelJob(ctx, db, raw.ID))

				require.NoError(t, RetryJobWithOptions(ctx, db, raw.ID, RetryOpts{Force: true, AllowRunning: true}))
				row := jobRowByID(t, db, raw.ID)
				assert.Equal(t, string(StateAvailable), row.State)
				assert.Nil(t, row.LeaseToken)
				assert.Nil(t, row.FinalizedAt)
			},
		},
		{
			name: "a cancelled job runs again once its attempt has returned",
			run: func(t *testing.T, ctx context.Context, d Driver, db *gorm.DB) {
				raw, runID := runningJob(t, ctx, d, db, "force-returned")
				require.NoError(t, CancelJob(ctx, db, raw.ID))
				out, err := d.Finalize(ctx, raw, runID, Result{}, nil, time.Now())
				require.NoError(t, err)
				require.True(t, out.Superseded, "the late finalize records the attempt and changes nothing")

				require.NoError(t, RetryJobWithOptions(ctx, db, raw.ID, force))
				assert.Equal(t, string(StateAvailable), jobRowByID(t, db, raw.ID).State)
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			db := open(t)
			tc.run(t, context.Background(), driverForTest(t, db), db)
		})
	}
}

// TestForceRetriesATerminalJobButNotARunningOne proves Force means "terminal
// too", never "running too".
func TestForceRetriesATerminalJobButNotARunningOne(t *testing.T) {
	t.Parallel()
	forceRetrySuite(t, sqliteOpener)
}
