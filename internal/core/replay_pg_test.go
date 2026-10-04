//go:build integration

package core

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/mrz1836/go-foundation/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// TestReplayByParentAccountingPostgres runs the A4 accounting invariant against real
// PostgreSQL on a mixed cohort large enough to span several batches, where the
// UPDATE's state re-guard is what keeps a concurrent finalize from being
// double-counted or a terminal row resurrected. It proves the parity claim too: the
// buckets, the re-convergence, and the budget reset match the SQLite path.
func TestReplayByParentAccountingPostgres(t *testing.T) {
	t.Parallel()
	db := NewPostgresIsolatedDB(t)
	base := time.Now().UTC().Truncate(time.Second)
	fin := base.Add(-time.Hour)
	ctx := models.WithClock(context.Background(), models.NewFixedClock(base))

	p := "P"
	seedTerminalChildrenBulk(t, db, p, 2000, StateDiscarded, 5, fin)
	seedTerminalChildrenBulk(t, db, p, 300, StateSucceeded, 5, fin)
	seedChildrenBulk(t, db, p, 50, StateRunning, base)
	seedChildrenBulk(t, db, p, 100, StateAvailable, base) // neither terminal nor running

	res, err := ReplayByParent(ctx, db, p, ReplayOpts{
		RetryOpts: RetryOpts{ResetAttempts: true, Budget: 3}, BatchSize: 500,
	})
	require.NoError(t, err)

	assert.EqualValues(t, 2000, res.Changed, "only the discarded children are replayed")
	assert.EqualValues(t, 300, res.SkippedTerminal, "the succeeded children are terminal but untargeted")
	assert.EqualValues(t, 50, res.SkippedRunning, "a running attempt is never interrupted")
	assert.Equal(t, 4, res.Batches, "2000 discarded in batches of 500 is four transactions")

	universe := int64(2000 + 300 + 50)
	assert.EqualValues(t, universe, res.Changed+res.SkippedTerminal+res.SkippedRunning,
		"Changed + SkippedTerminal + SkippedRunning == the finished-or-running universe")

	assert.EqualValues(t, 0, countChildrenInState(t, db, p, StateDiscarded), "the cohort re-converged out of discarded")
	assert.EqualValues(t, 2100, countChildrenInState(t, db, p, StateAvailable), "2000 replayed + 100 already available")
	assert.EqualValues(t, 300, countChildrenInState(t, db, p, StateSucceeded), "no succeeded child re-ran")

	row := jobRowByID(t, db, "P-discarded-0")
	assert.Equal(t, 5, row.Attempt, "attempt is not rewound")
	assert.Equal(t, 8, row.MaxAttempts, "Budget 3 sets max_attempts = attempt 5 + 3")
}

// TestReplayByParentTouchesOnlyItsOwnChildrenPostgres proves the lineage scope: a
// replay of one parent leaves a sibling parent's identical cohort untouched.
func TestReplayByParentTouchesOnlyItsOwnChildrenPostgres(t *testing.T) {
	t.Parallel()
	db := NewPostgresIsolatedDB(t)
	base := time.Now().UTC().Truncate(time.Second)
	fin := base.Add(-time.Hour)
	ctx := context.Background()

	seedTerminalChildrenBulk(t, db, "P", 100, StateDiscarded, 3, fin)
	seedTerminalChildrenBulk(t, db, "Q", 100, StateDiscarded, 3, fin)

	res, err := ReplayByParent(ctx, db, "P", ReplayOpts{})
	require.NoError(t, err)
	assert.EqualValues(t, 100, res.Changed)
	assert.EqualValues(t, 0, countChildrenInState(t, db, "P", StateDiscarded), "P's children were replayed")
	assert.EqualValues(t, 100, countChildrenInState(t, db, "Q", StateDiscarded), "Q's children were left alone")
}

// TestReplayUnscopedByKindAndWindowPostgres proves the incident-shaped Replay:
// bounded by kind and a failure window, unscoped by lineage, across parents.
func TestReplayUnscopedByKindAndWindowPostgres(t *testing.T) {
	t.Parallel()
	db := NewPostgresIsolatedDB(t)
	base := time.Now().UTC().Truncate(time.Second)
	ctx := context.Background()

	within := base.Add(-10 * time.Minute)
	before := base.Add(-2 * time.Hour)
	seed := func(id, kind string, fin time.Time) {
		f := fin
		seedJob(t, db, jobRow{
			ID: id, Kind: kind, State: string(StateDiscarded),
			Attempt: 3, MaxAttempts: 3, FinalizedAt: &f, ScheduledAt: base,
		})
	}
	for i := range 40 {
		seed(fmt.Sprintf("scout-in-%d", i), "scout", within)
	}
	for i := range 10 {
		seed(fmt.Sprintf("scout-old-%d", i), "scout", before)
	}
	for i := range 25 {
		seed(fmt.Sprintf("weather-in-%d", i), "weather", within)
	}

	res, err := Replay(ctx, db, ReplayOpts{
		RetryOpts:   RetryOpts{ResetAttempts: true},
		Kinds:       []string{"scout"},
		FailedSince: base.Add(-time.Hour),
	})
	require.NoError(t, err)
	assert.EqualValues(t, 40, res.Changed, "only in-window scout failures are replayed")

	var scoutDiscarded, weatherDiscarded int64
	require.NoError(t, db.Model(&jobRow{}).
		Where("kind = ? AND state = ?", "scout", string(StateDiscarded)).Count(&scoutDiscarded).Error)
	require.NoError(t, db.Model(&jobRow{}).
		Where("kind = ? AND state = ?", "weather", string(StateDiscarded)).Count(&weatherDiscarded).Error)
	assert.EqualValues(t, 10, scoutDiscarded, "out-of-window scout failures are left discarded")
	assert.EqualValues(t, 25, weatherDiscarded, "the weather kind is untouched")
}

// TestStaggerDistributesUniformlyPostgres is A5 against real PostgreSQL — the same
// deterministic decile assertion as the SQLite path, proving the CASE-based
// scheduled_at placement reads identically on both dialects.
func TestStaggerDistributesUniformlyPostgres(t *testing.T) {
	t.Parallel()
	assertStaggerDeciles(t, NewPostgresIsolatedDB(t))
}

// TestReplayKeepsOneSiblingPerActiveKeyPostgres is the sibling case on
// PostgreSQL, where a second live job on a key raises 23505.
func TestReplayKeepsOneSiblingPerActiveKeyPostgres(t *testing.T) {
	t.Parallel()
	runKeyedReplayCase(t, NewPostgresIsolatedDB, assertReplayKeepsOneSiblingPerActiveKey)
}

// TestReplaySkipsAJobWhoseActiveKeyIsHeldPostgres is the live-holder case on
// PostgreSQL.
func TestReplaySkipsAJobWhoseActiveKeyIsHeldPostgres(t *testing.T) {
	t.Parallel()
	runKeyedReplayCase(t, NewPostgresIsolatedDB, assertReplaySkipsAJobWhoseActiveKeyIsHeld)
}

// TestReplayOfALiveJobIsNotBlockedByItsOwnKeyPostgres is the live-state replay
// on PostgreSQL.
func TestReplayOfALiveJobIsNotBlockedByItsOwnKeyPostgres(t *testing.T) {
	t.Parallel()
	runKeyedReplayCase(t, NewPostgresIsolatedDB, assertReplayOfALiveJobIsNotBlockedByItsOwnKey)
}

// TestReplayKeepsOneJobPerActiveKeyAcrossBatchesPostgres is the multi-batch case
// on PostgreSQL.
func TestReplayKeepsOneJobPerActiveKeyAcrossBatchesPostgres(t *testing.T) {
	t.Parallel()
	runKeyedReplayCase(t, NewPostgresIsolatedDB, assertReplayKeepsOneJobPerActiveKeyAcrossBatches)
}

// TestReplayAccountsForAKeyedCohortPostgres is the mixed keyed cohort's
// accounting on PostgreSQL.
func TestReplayAccountsForAKeyedCohortPostgres(t *testing.T) {
	t.Parallel()
	runKeyedReplayCase(t, NewPostgresIsolatedDB, assertReplayAccountsForAKeyedCohort)
}

// TestReplayWindowsAKeyedCohortPostgres is the windowed keyed cohort on
// PostgreSQL.
func TestReplayWindowsAKeyedCohortPostgres(t *testing.T) {
	t.Parallel()
	runKeyedReplayCase(t, NewPostgresIsolatedDB, assertReplayWindowsAKeyedCohort)
}

// TestStaggerKeepsSlotsAcrossActiveKeySkipsPostgres is the staggered keyed
// cohort on PostgreSQL.
func TestStaggerKeepsSlotsAcrossActiveKeySkipsPostgres(t *testing.T) {
	t.Parallel()
	runKeyedReplayCase(t, NewPostgresIsolatedDB, assertStaggerKeepsSlotsAcrossActiveKeySkips)
}

// TestReplayUpdateRechecksTheActiveKeyPostgres is the UPDATE's key guard on
// PostgreSQL, where its absence fails the batch with 23505.
func TestReplayUpdateRechecksTheActiveKeyPostgres(t *testing.T) {
	t.Parallel()
	assertReplayUpdateRechecksTheActiveKey(t, NewPostgresIsolatedDB(t))
}

// newTranslateErrorPostgresDB is newTranslateErrorDB's PostgreSQL mirror: a
// second handle on a fresh isolated schema, opened with TranslateError, which
// substitutes gorm.ErrDuplicatedKey for the driver's 23505.
func newTranslateErrorPostgresDB(t *testing.T) *gorm.DB {
	t.Helper()
	var schema string
	require.NoError(t, NewPostgresIsolatedDB(t).Raw("SELECT current_schema()").Scan(&schema).Error)
	db, err := gorm.Open(postgres.Open(withSearchPath(requirePostgresDSN(t), schema)), &gorm.Config{
		TranslateError: true,
		Logger:         logger.Default.LogMode(logger.Silent),
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		if sqlDB, dbErr := db.DB(); dbErr == nil {
			_ = sqlDB.Close()
		}
	})
	return db
}

// keyRaceDBsPostgres mirrors keyRaceDBs on PostgreSQL: a lost key race reaches
// the replay as the driver's 23505, or as gorm.ErrDuplicatedKey under
// TranslateError.
func keyRaceDBsPostgres() []keyRaceDB {
	return []keyRaceDB{
		{name: "driver error", open: func(t *testing.T) *gorm.DB { return NewPostgresIsolatedDB(t) }},
		{name: "TranslateError", open: newTranslateErrorPostgresDB, translated: true},
	}
}

// TestReplayRetriesABatchThatLosesAKeyRacePostgres and its bounded twin run the
// SQLite retry tests' forced key race on PostgreSQL, where the forcing statement
// aborts the batch's transaction and the error is 23505, or gorm.ErrDuplicatedKey
// under TranslateError.
func TestReplayRetriesABatchThatLosesAKeyRacePostgres(t *testing.T) {
	t.Parallel()
	for _, tc := range keyRaceDBsPostgres() {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assertReplayRetriesALostKeyRace(t, tc.open(t))
		})
	}
}

func TestReplayStopsRetryingAKeyRaceItKeepsLosingPostgres(t *testing.T) {
	t.Parallel()
	for _, tc := range keyRaceDBsPostgres() {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assertReplayStopsRetryingALostKeyRace(t, tc.open(t), tc.translated)
		})
	}
}

// replayOutcome is what a replay run in the background returned.
type replayOutcome struct {
	res ScopeResult
	err error
}

// replayInBackground runs ReplayByParent over parent P on its own goroutine, for
// a test that must act while the replay waits on a lock.
func replayInBackground(ctx context.Context, db *gorm.DB) <-chan replayOutcome {
	done := make(chan replayOutcome, 1)
	go func() {
		res, err := ReplayByParent(ctx, db, "P", ReplayOpts{})
		done <- replayOutcome{res: res, err: err}
	}()
	return done
}

// awaitReplay returns the background replay's outcome, failing the test if it
// does not finish.
func awaitReplay(t *testing.T, done <-chan replayOutcome) replayOutcome {
	t.Helper()
	select {
	case out := <-done:
		return out
	case <-time.After(30 * time.Second):
		t.Fatal("the replay did not finish")
		return replayOutcome{}
	}
}

// backendPID returns the PostgreSQL backend pid serving tx.
func backendPID(t *testing.T, tx *gorm.DB) int64 {
	t.Helper()
	var pid int64
	require.NoError(t, tx.Raw("SELECT pg_backend_pid()").Scan(&pid).Error)
	return pid
}

// awaitBlockedBy waits until some backend is waiting on a lock the backend pid
// holds: the barrier a lock-ordering test advances on.
func awaitBlockedBy(t *testing.T, db *gorm.DB, pid int64, msg string) {
	t.Helper()
	require.Eventually(t, func() bool {
		var waiting int64
		err := db.Raw("SELECT count(*) FROM pg_stat_activity WHERE ? = ANY(pg_blocking_pids(pid))", pid).
			Scan(&waiting).Error
		return err == nil && waiting > 0
	}, 10*time.Second, 5*time.Millisecond, msg)
}

// TestReplayRetriesABatchAConcurrentEnqueueRacesPostgres races a real enqueue
// against a replay's UPDATE, the one window the UPDATE's guard cannot close. The
// enqueue's row is written but not committed, so the replay's SELECT and its
// guard both read the key as free; the UPDATE then waits on the index entry, and
// the enqueue's commit fails it with 23505 — gorm.ErrDuplicatedKey when the
// replay's handle has TranslateError. The replay reruns the batch, reads the
// committed holder, and leaves the job terminal, so the caller sees only the
// outcome.
func TestReplayRetriesABatchAConcurrentEnqueueRacesPostgres(t *testing.T) {
	t.Parallel()
	for _, tc := range keyRaceDBsPostgres() {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			db := tc.open(t)
			ctx := context.Background()
			seedKeyedJob(t, db, "P-1", "leaf", "P", StateDiscarded, "K")
			seedKeyedJob(t, db, "P-2", "leaf", "P", StateDiscarded, "")

			enqueue := db.Begin()
			require.NoError(t, enqueue.Error)
			t.Cleanup(func() { enqueue.Rollback() }) // releases a waiting replay if the test fails first
			pid := backendPID(t, enqueue)
			holder, err := Enqueue(ctx, NewClient(db), "other", []byte(`{}`),
				InsertOpts{UniqueActiveKey: "K", Tx: enqueue})
			require.NoError(t, err)

			done := replayInBackground(ctx, db)
			awaitBlockedBy(t, db, pid, "the replay's UPDATE waits on the uncommitted holder")
			require.NoError(t, enqueue.Commit().Error)

			out := awaitReplay(t, done)
			require.NoError(t, out.err, "the lost race is retried, not returned")
			assert.EqualValues(t, 1, out.res.Changed, "the unkeyed job goes live")
			assert.EqualValues(t, 1, out.res.SkippedActiveKey, "the rerun reads the committed holder")
			assert.Equal(t, string(StateDiscarded), jobState(t, db, "P-1"))
			assert.Equal(t, string(StateAvailable), jobState(t, db, holder), "the enqueued job keeps the key")
		})
	}
}

// TestReplayRetriesABatchThatLosesAKeyDeadlockPostgres drives the deadlock form
// of the key race. The batch covers P-1 (key K1) and P-2 (K2). A host
// transaction enqueues K2 and leaves it uncommitted, so the replay's UPDATE takes
// K1's index entry and then waits on K2; the host then enqueues K1 and waits on
// the replay. PostgreSQL breaks the cycle by failing the replay, which has waited
// longest, with 40P01. Its batch rolled back whole, so the replay reruns it: the
// rerun waits on the host's uncommitted keys, the host's commit fails it with
// 23505, and the next rerun reads both holders and leaves both jobs terminal.
func TestReplayRetriesABatchThatLosesAKeyDeadlockPostgres(t *testing.T) {
	t.Parallel()
	db := NewPostgresIsolatedDB(t)
	ctx := context.Background()
	c := NewClient(db)
	seedKeyedJob(t, db, "P-1", "leaf", "P", StateDiscarded, "K1")
	seedKeyedJob(t, db, "P-2", "leaf", "P", StateDiscarded, "K2")

	host := db.Begin()
	require.NoError(t, host.Error)
	t.Cleanup(func() { host.Rollback() }) // releases a waiting replay if the test fails first
	pid := backendPID(t, host)
	k2, err := Enqueue(ctx, c, "other", []byte(`{}`), InsertOpts{UniqueActiveKey: "K2", Tx: host})
	require.NoError(t, err)

	done := replayInBackground(ctx, db)
	awaitBlockedBy(t, db, pid, "the replay's UPDATE takes K1, then waits on the host's K2")

	// The host now takes K1 and waits on the replay: a cycle. The replay is the
	// victim, so the host's insert lands once the replay's batch rolls back.
	type enqueued struct {
		id  string
		err error
	}
	hostDone := make(chan enqueued, 1)
	go func() {
		id, enqErr := Enqueue(ctx, c, "other", []byte(`{}`), InsertOpts{UniqueActiveKey: "K1", Tx: host})
		hostDone <- enqueued{id: id, err: enqErr}
	}()
	var k1 enqueued
	select {
	case k1 = <-hostDone:
	case <-time.After(30 * time.Second):
		t.Fatal("the host's enqueue did not finish")
	}
	require.NoError(t, k1.err, "PostgreSQL fails the replay, which waited first, not the host")
	// The rerun reads both keys as free, since the host has not committed, and its
	// UPDATE waits on the host again; the commit then fails it with 23505, and the
	// rerun after that reads both holders.
	awaitBlockedBy(t, db, pid, "the rerun's UPDATE waits on the host's uncommitted keys")
	require.NoError(t, host.Commit().Error)

	out := awaitReplay(t, done)
	require.NoError(t, out.err, "the deadlock is retried, not returned")
	assert.Zero(t, out.res.Changed)
	assert.EqualValues(t, 2, out.res.SkippedActiveKey, "the rerun reads both committed holders")
	assert.Equal(t, string(StateDiscarded), jobState(t, db, "P-1"))
	assert.Equal(t, string(StateDiscarded), jobState(t, db, "P-2"))
	assert.Equal(t, string(StateAvailable), jobState(t, db, k1.id), "the host keeps K1")
	assert.Equal(t, string(StateAvailable), jobState(t, db, k2), "the host keeps K2")
}

// TestReplayHolderLookupReachesTheActiveKeyIndexPostgres proves the holder lookup
// can probe jobs_unique_active_key under a generic plan, the plan a cached
// prepared statement settles on, where only literals prove a partial index's
// predicate. It explains the two statements the replay actually sends, recorded
// as GORM built them, with the sequential scan disabled.
func TestReplayHolderLookupReachesTheActiveKeyIndexPostgres(t *testing.T) {
	t.Parallel()
	db := NewPostgresIsolatedDB(t)
	seedKeyedJob(t, db, "P-1", "leaf", "P", StateDiscarded, "K")
	issued := recordHolderLookups(t, db)

	_, err := ReplayByParent(context.Background(), db, "P", ReplayOpts{})
	require.NoError(t, err)
	require.Len(t, *issued, 2, "the batch's SELECT and its UPDATE carry the holder lookup")
	for i, st := range *issued {
		plan := genericPlan(t, db, fmt.Sprintf("replay_stmt_%d", i), st)
		t.Logf("%s\n%s", st.sql, plan)
		assert.Contains(t, plan, "jobs_unique_active_key", "%s\n%s", st.sql, plan)
		assert.NotContains(t, plan, "Seq Scan on jobs holder", "%s\n%s", st.sql, plan)
	}
}

// genericPlan returns the generic plan of a recorded statement: it prepares the
// statement as sent, $n parameters and all, and explains its execution with
// plan_cache_mode forcing the generic plan, so no bound value can reach the
// planner. The sequential scan is disabled, the transaction rolls back, and the
// prepared statement is deallocated, so nothing outlives the probe.
func genericPlan(t *testing.T, db *gorm.DB, name string, st holderLookup) string {
	t.Helper()
	args := strings.TrimSuffix(strings.Repeat("NULL, ", len(st.vars)), ", ")
	var plan string
	err := db.Transaction(func(tx *gorm.DB) error {
		for _, stmt := range []string{
			`SET LOCAL enable_seqscan = off`,
			`SET LOCAL plan_cache_mode = force_generic_plan`,
			"PREPARE " + name + " AS " + st.sql,
		} {
			if err := tx.Exec(stmt).Error; err != nil {
				return err
			}
		}
		var lines []string
		if err := tx.Raw("EXPLAIN EXECUTE " + name + "(" + args + ")").Scan(&lines).Error; err != nil {
			return err
		}
		plan = strings.Join(lines, "\n")
		if err := tx.Exec("DEALLOCATE " + name).Error; err != nil {
			return err
		}
		return gorm.ErrInvalidTransaction // roll back: neither setting may persist
	})
	require.ErrorIs(t, err, gorm.ErrInvalidTransaction)
	return plan
}
