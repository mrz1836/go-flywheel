package flywheel

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// realworld_scenarios_test.go models a sports / data-intelligence ingestion
// platform on top of flywheel: periodic ingestion, fan-out DAGs, retries with
// backoff, rate-limit handling via snooze, crash recovery, idempotent enqueue,
// the outbox pattern, observability, and retention. Every scenario drives
// through the public API on an in-memory SQLite database.

// --- shared real-world test helpers -----------------------------------------

// rwRunner builds a single-concurrency SQLite Runner that claims every executor
// class on the default and periodic queues, with a tight poll interval so
// backoff/snooze scenarios drain fast. The optional mutators tweak the config
// (backoff base, observer, default timeout, ...).
func rwRunner(t testing.TB, db *gorm.DB, reg *Registry, mutators ...func(*RunnerConfig)) *Runner {
	t.Helper()
	cfg := RunnerConfig{
		DB:            db,
		Driver:        NewSQLiteDriver(db),
		Registry:      reg,
		Queues:        []string{"default", "periodic"},
		ExecutorClass: "local",
		ClaimAnyClass: true,
		Concurrency:   1,
		PollInterval:  2 * time.Millisecond,
	}
	for _, m := range mutators {
		m(&cfg)
	}
	r, err := NewRunner(cfg)
	if err != nil {
		t.Fatalf("rwRunner: %v", err)
	}
	return r
}

// recordObserver captures the lifecycle events the Runner emits so a scenario can
// reconcile observed counters against the driven outcomes.
type recordObserver struct {
	mu       sync.Mutex
	claimed  int
	starts   int
	finishes []FinishEvent
	retries  []RetryEvent
}

func (o *recordObserver) OnClaim(_ context.Context, ev ClaimEvent) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.claimed += ev.Claimed
}

func (o *recordObserver) OnStart(_ context.Context, _ JobEvent) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.starts++
}

func (o *recordObserver) OnFinish(_ context.Context, ev FinishEvent) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.finishes = append(o.finishes, ev)
}

func (o *recordObserver) OnRetry(_ context.Context, ev RetryEvent) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.retries = append(o.retries, ev)
}

func (o *recordObserver) outcomeCount(outcome RunOutcome) int {
	o.mu.Lock()
	defer o.mu.Unlock()
	n := 0
	for _, f := range o.finishes {
		if f.Outcome == outcome {
			n++
		}
	}
	return n
}

// --- ingestion fan-out DAG ---------------------------------------------------

type slateArgs struct {
	Games int `json:"games"`
}

func (slateArgs) Kind() string { return "rw.fetch-slate" }

type boxscoreArgs struct {
	Game    int `json:"game"`
	Players int `json:"players"`
}

func (boxscoreArgs) Kind() string { return "rw.fetch-boxscore" }

type playerStatArgs struct {
	Game   int `json:"game"`
	Player int `json:"player"`
}

func (playerStatArgs) Kind() string { return "rw.extract-player-stat" }

// slateWorker fans out one boxscore job per game.
type slateWorker struct{ playersPerGame int }

func (*slateWorker) Kind() string { return "rw.fetch-slate" }
func (w *slateWorker) Work(_ context.Context, job *Job[slateArgs]) (Result, error) {
	followUps := make([]FollowUp, job.Args.Games)
	for i := range followUps {
		followUps[i] = FollowUp{
			Kind:   "rw.fetch-boxscore",
			Args:   boxscoreArgs{Game: i, Players: w.playersPerGame},
			Parent: true,
		}
	}
	return Result{FollowUps: followUps}, nil
}

// boxscoreWorker fans out one player-stat job per player.
type boxscoreWorker struct{}

func (*boxscoreWorker) Kind() string { return "rw.fetch-boxscore" }
func (*boxscoreWorker) Work(_ context.Context, job *Job[boxscoreArgs]) (Result, error) {
	followUps := make([]FollowUp, job.Args.Players)
	for i := range followUps {
		followUps[i] = FollowUp{
			Kind:   "rw.extract-player-stat",
			Args:   playerStatArgs{Game: job.Args.Game, Player: i},
			Parent: true,
		}
	}
	return Result{FollowUps: followUps}, nil
}

type playerStatWorker struct{ ran atomic.Int32 }

func (*playerStatWorker) Kind() string { return "rw.extract-player-stat" }
func (w *playerStatWorker) Work(_ context.Context, _ *Job[playerStatArgs]) (Result, error) {
	w.ran.Add(1)
	return Result{}, nil
}

// TestRealWorldIngestionFanOutDAG drives a three-level ingestion DAG
// (slate → boxscore → player-stat) to completion and asserts the whole tree
// reaches succeeded, parent linkage is correct, and the enqueued_children audit
// matches the fan-out.
func TestRealWorldIngestionFanOutDAG(t *testing.T) {
	t.Parallel()
	db := newDB(t)

	const games = 3
	const playersPerGame = 4

	reg := NewRegistry()
	Register(reg, &slateWorker{playersPerGame: playersPerGame})
	Register(reg, &boxscoreWorker{})
	leaf := &playerStatWorker{}
	Register(reg, leaf)
	r := newRunner(t, db, reg)

	ctx := context.Background()
	slateID, err := Insert(ctx, NewClient(db), slateArgs{Games: games}, InsertOpts{})
	require.NoError(t, err)

	runToIdle(t, ctx, r)

	// The whole DAG succeeded.
	const wantTotal = 1 + games + games*playersPerGame
	overview, err := Overview(ctx, db, OverviewParams{})
	require.NoError(t, err)
	assert.Equal(t, wantTotal, overview.Total, "every node of the DAG exists")
	assert.Equal(t, wantTotal, overview.CountsByState[string(StateSucceeded)], "every node succeeded")
	assert.EqualValues(t, games*playersPerGame, leaf.ran.Load(), "every leaf ran exactly once")

	// Parent linkage: each boxscore points at the slate; each player-stat points
	// at a boxscore.
	boxscores, err := ListJobs(ctx, db, ListJobsParams{Kind: "rw.fetch-boxscore", Limit: 100})
	require.NoError(t, err)
	require.Len(t, boxscores, games)
	for _, b := range boxscores {
		assert.Equal(t, slateID, b.ParentJobID, "each boxscore is parented to the slate")
	}
	boxscoreIDs := make(map[string]bool, len(boxscores))
	for _, b := range boxscores {
		boxscoreIDs[b.ID] = true
	}

	stats, err := ListJobs(ctx, db, ListJobsParams{Kind: "rw.extract-player-stat", Limit: 100})
	require.NoError(t, err)
	require.Len(t, stats, games*playersPerGame)
	for _, s := range stats {
		assert.Truef(t, boxscoreIDs[s.ParentJobID], "player-stat %s is parented to a boxscore", s.ID)
	}

	// enqueued_children audit: the slate enqueued `games`, each boxscore enqueued
	// `playersPerGame`.
	slateRuns, err := ListRuns(ctx, db, slateID, ListRunsParams{})
	require.NoError(t, err)
	require.Len(t, slateRuns, 1)
	assert.Equal(t, games, enqueuedChildren(t, db, slateRuns[0].ID), "the slate run records its fan-out width")
	for _, b := range boxscores {
		runs, runErr := ListRuns(ctx, db, b.ID, ListRunsParams{})
		require.NoError(t, runErr)
		require.Len(t, runs, 1)
		assert.Equal(t, playersPerGame, enqueuedChildren(t, db, runs[0].ID), "each boxscore run records its fan-out width")
	}
}

// enqueuedChildren reads the enqueued_children audit column for a run.
func enqueuedChildren(t *testing.T, db *gorm.DB, runID string) int {
	t.Helper()
	var n int
	require.NoError(t, db.Table("job_runs").Select("enqueued_children").Where("id = ?", runID).Scan(&n).Error)
	return n
}

// --- periodic polling --------------------------------------------------------

// TestRealWorldPeriodicPolling drives UpsertPeriodic + Scheduler.Tick on a fixed
// clock across interval and cron specs, the redundant-tick no-op, and the
// backfill cap.
func TestRealWorldPeriodicPolling(t *testing.T) {
	t.Parallel()

	t.Run("interval-fires-and-is-idempotent", func(t *testing.T) {
		t.Parallel()
		db := newDB(t)
		sched := NewScheduler(db, NewClient(db))
		base := time.Now().UTC().Truncate(time.Second)

		// A fresh schedule seeds next_run_at = base + Every, so it does not fire
		// immediately at base.
		require.NoError(t, UpsertPeriodic(clockCtx(context.Background(), NewFixedClock(base)),
			db, PeriodicSpec{Slug: "ingest", Kind: "rw.periodic.interval", Every: time.Minute, Active: true}))

		atBase, err := sched.Tick(clockCtx(context.Background(), NewFixedClock(base)))
		require.NoError(t, err)
		assert.Zero(t, atBase, "nothing is due at the seed instant")

		// Advance past the first fire.
		due := clockCtx(context.Background(), NewFixedClock(base.Add(90*time.Second)))
		fired, err := sched.Tick(due)
		require.NoError(t, err)
		assert.Positive(t, fired, "the due interval fires")
		assert.Positive(t, jobCount(t, db, "rw.periodic.interval"))

		// A redundant tick at the same instant is a no-op (bucketed unique_key).
		again, err := sched.Tick(due)
		require.NoError(t, err)
		assert.Zero(t, again, "a repeat tick at the same instant enqueues nothing")
		assert.EqualValues(t, fired, jobCount(t, db, "rw.periodic.interval"))
	})

	t.Run("cron-fires", func(t *testing.T) {
		t.Parallel()
		db := newDB(t)
		sched := NewScheduler(db, NewClient(db))
		base := time.Now().UTC().Truncate(time.Minute)

		require.NoError(t, UpsertPeriodic(clockCtx(context.Background(), NewFixedClock(base)),
			db, PeriodicSpec{Slug: "cron-ingest", Kind: "rw.periodic.cron", Cron: "*/1 * * * *", Active: true}))

		due := clockCtx(context.Background(), NewFixedClock(base.Add(150*time.Second)))
		fired, err := sched.Tick(due)
		require.NoError(t, err)
		assert.Positive(t, fired, "an every-minute cron with slack fires")
		assert.Positive(t, jobCount(t, db, "rw.periodic.cron"))
	})

	t.Run("backfill-cap-bounds-catchup", func(t *testing.T) {
		t.Parallel()
		db := newDB(t)
		sched := NewSchedulerWithConfig(SchedulerConfig{DB: db, Client: NewClient(db), BackfillCap: 10})
		base := time.Now().UTC().Truncate(time.Second)

		require.NoError(t, UpsertPeriodic(clockCtx(context.Background(), NewFixedClock(base)),
			db, PeriodicSpec{Slug: "lagging", Kind: "rw.periodic.backfill", Every: time.Minute, Active: true}))

		// Tick an hour later: ~60 missed buckets, capped at 10.
		fired, err := sched.Tick(clockCtx(context.Background(), NewFixedClock(base.Add(time.Hour))))
		require.NoError(t, err)
		assert.Equal(t, 10, fired, "catch-up is bounded by the backfill cap")
		assert.EqualValues(t, 10, jobCount(t, db, "rw.periodic.backfill"))
	})
}

// --- rate-limit handling via snooze -----------------------------------------

type rateLimitArgs struct{ V string }

func (rateLimitArgs) Kind() string { return "rw.rate-limited" }

// rateLimitWorker simulates a 429: it snoozes the first `limited` attempts, then
// succeeds.
type rateLimitWorker struct {
	limited int32
	calls   atomic.Int32
}

func (*rateLimitWorker) Kind() string { return "rw.rate-limited" }
func (w *rateLimitWorker) Work(_ context.Context, _ *Job[rateLimitArgs]) (Result, error) {
	if w.calls.Add(1) <= w.limited {
		d := time.Millisecond
		return Result{Snooze: &d}, nil
	}
	return Result{}, nil
}

// TestRealWorldRateLimitSnooze models a worker that hits a rate limit and snoozes
// repeatedly before succeeding. The job must reschedule (never advance toward
// discarded) and retry headroom must be preserved.
func TestRealWorldRateLimitSnooze(t *testing.T) {
	t.Parallel()
	db := newDB(t)

	const limited = 5
	const startMaxAttempts = 4

	obs := &recordObserver{}
	w := &rateLimitWorker{limited: limited}
	reg := NewRegistry()
	Register(reg, w)
	r := rwRunner(t, db, reg, func(c *RunnerConfig) { c.Observer = obs })

	id, err := Insert(context.Background(), NewClient(db), rateLimitArgs{V: "x"},
		InsertOpts{MaxAttempts: startMaxAttempts})
	require.NoError(t, err)

	runToIdle(t, context.Background(), r)

	assert.Equal(t, string(StateSucceeded), jobState(t, db, id), "the job survives the rate limit and succeeds")
	assert.EqualValues(t, limited+1, w.calls.Load())

	// max_attempts rose by exactly the snooze count; retry headroom preserved.
	var maxAttempts, attempt int
	require.NoError(t, db.Table("jobs").Select("max_attempts").Where("id = ?", id).Scan(&maxAttempts).Error)
	require.NoError(t, db.Table("jobs").Select("attempt").Where("id = ?", id).Scan(&attempt).Error)
	assert.Equal(t, startMaxAttempts+limited, maxAttempts, "each snooze raised max_attempts by one")
	assert.LessOrEqual(t, attempt, maxAttempts, "the snooze never erodes retry headroom")

	// A snooze is not a retry: no OnRetry events fired.
	assert.Empty(t, obs.retries, "a snooze is not a retry — it does not advance toward discard")
	assert.Equal(t, limited, obs.outcomeCount(OutcomeSnooze), "every snooze was observed as a snooze outcome")
	assert.Equal(t, 1, obs.outcomeCount(OutcomeSuccess), "the final attempt succeeded")
}

// --- transient failure + exponential backoff --------------------------------

type transientArgs struct{ V string }

func (transientArgs) Kind() string { return "rw.transient" }

// transientWorker fails (transient) the first failuresBefore attempts then
// succeeds. It supplies no NextRetry, so the runner's exponential backoff drives
// the delay.
type transientWorker struct {
	failuresBefore int32
	calls          atomic.Int32
}

func (*transientWorker) Kind() string { return "rw.transient" }
func (w *transientWorker) Work(_ context.Context, _ *Job[transientArgs]) (Result, error) {
	if w.calls.Add(1) <= w.failuresBefore {
		return Result{}, assert.AnError
	}
	return Result{}, nil
}

// TestRealWorldTransientBackoff fails a worker a few times then succeeds, and
// asserts the attempt accounting, one audit row per attempt, growing retry
// delays, and a final success.
func TestRealWorldTransientBackoff(t *testing.T) {
	t.Parallel()
	db := newDB(t)

	const failures = 3

	obs := &recordObserver{}
	w := &transientWorker{failuresBefore: failures}
	reg := NewRegistry()
	Register(reg, w)
	// A small backoff base keeps the exponential ladder (5ms, 10ms, 20ms) fast
	// while still growing.
	r := rwRunner(t, db, reg, func(c *RunnerConfig) {
		c.Observer = obs
		c.RetryBackoffBase = 5 * time.Millisecond
	})

	id, err := Insert(context.Background(), NewClient(db), transientArgs{V: "x"}, InsertOpts{})
	require.NoError(t, err)

	runToIdle(t, context.Background(), r)

	assert.Equal(t, string(StateSucceeded), jobState(t, db, id))
	assert.EqualValues(t, failures+1, w.calls.Load(), "failures then one success")
	assert.EqualValues(t, failures+1, runCount(t, db, id), "one job_runs row per attempt — append-only audit")

	// Attempts are strictly monotonic 1..failures+1 across the audit rows.
	var attempts []int
	require.NoError(t, db.Table("job_runs").Where("job_id = ?", id).Order("attempt").Pluck("attempt", &attempts).Error)
	require.Len(t, attempts, failures+1)
	for i, a := range attempts {
		assert.Equal(t, i+1, a, "attempt numbers are strictly monotonic")
	}

	// OnRetry fired once per failure with a non-decreasing delay (exponential
	// backoff with bounded jitter).
	obs.mu.Lock()
	retries := append([]RetryEvent(nil), obs.retries...)
	obs.mu.Unlock()
	require.Len(t, retries, failures, "one retry observed per transient failure")
	for i := 1; i < len(retries); i++ {
		assert.GreaterOrEqual(t, retries[i].Delay, retries[i-1].Delay, "the backoff delay grows across retries")
		assert.Equal(t, ErrorTransient, retries[i].ErrorClass, "a default failure is classified transient")
	}
}

// --- permanent / validation errors ------------------------------------------

type classifiedArgs struct {
	Subject string `json:"subject"`
}

func (classifiedArgs) Kind() string { return "rw.classified" }

// classifyingWorker fails with a fixed message and reports a fixed error class.
type classifyingWorker struct {
	kind  string
	class ErrorClass
	msg   string
}

func (w *classifyingWorker) Kind() string              { return w.kind }
func (w *classifyingWorker) Classify(error) ErrorClass { return w.class }
func (w *classifyingWorker) Work(context.Context, *Job[classifiedArgs]) (Result, error) {
	return Result{}, &simpleError{msg: w.msg}
}

// simpleError is a minimal error type for the classified-error scenarios.
type simpleError struct{ msg string }

func (e *simpleError) Error() string { return e.msg }

// TestRealWorldPermanentAndValidationErrorsDiscard asserts that permanent and
// validation classes discard on the first attempt and surface through
// RecentFailures with their final error.
func TestRealWorldPermanentAndValidationErrorsDiscard(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name  string
		kind  string
		class ErrorClass
		msg   string
	}{
		{name: "permanent", kind: "rw.classified", class: ErrorPermanent, msg: "upstream gone"},
		{name: "validation", kind: "rw.classified", class: ErrorValidation, msg: "bad payload"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			db := newDB(t)
			reg := NewRegistry()
			Register(reg, &classifyingWorker{kind: tc.kind, class: tc.class, msg: tc.msg})
			r := newRunner(t, db, reg)

			ctx := context.Background()
			id, err := Insert(ctx, NewClient(db), classifiedArgs{Subject: tc.name}, InsertOpts{})
			require.NoError(t, err)

			runToIdle(t, ctx, r)

			assert.Equal(t, string(StateDiscarded), jobState(t, db, id), "a %s error discards immediately", tc.name)
			assert.EqualValues(t, 1, runCount(t, db, id), "no retries for a terminal error class")

			failures, err := RecentFailures(ctx, db, RecentFailuresParams{})
			require.NoError(t, err)
			require.Len(t, failures, 1)
			assert.Equal(t, id, failures[0].JobID)
			assert.Equal(t, string(tc.class), failures[0].ErrorClass, "the final error class is surfaced")
			assert.Equal(t, tc.msg, failures[0].ErrorMessage, "the final error message is surfaced")
		})
	}
}

// --- timeout precedence ------------------------------------------------------

type timeoutPrecedenceArgs struct{}

func (timeoutPrecedenceArgs) Kind() string { return "rw.timeout-precedence" }

// timeoutPrecedenceWorker parks on the first attempt until its ctx is cancelled,
// then succeeds. Its per-kind Timeouter is long (10s); a short per-job timeout
// must win, so the attempt times out quickly instead of hanging.
type timeoutPrecedenceWorker struct{ calls atomic.Int32 }

func (*timeoutPrecedenceWorker) Kind() string                       { return "rw.timeout-precedence" }
func (*timeoutPrecedenceWorker) Timeout() time.Duration             { return 10 * time.Second }
func (*timeoutPrecedenceWorker) NextRetry(error, int) time.Duration { return time.Millisecond }
func (w *timeoutPrecedenceWorker) Work(ctx context.Context, _ *Job[timeoutPrecedenceArgs]) (Result, error) {
	if w.calls.Add(1) == 1 {
		<-ctx.Done()
		return Result{}, ctx.Err()
	}
	return Result{}, nil
}

// TestRealWorldTimeoutPrecedence confirms a per-job InsertOpts.Timeout overrides
// both a long per-kind Timeouter and the runner's DefaultTimeout: the attempt
// times out (and is classified ErrorTimeout) in milliseconds, then retries to
// success.
func TestRealWorldTimeoutPrecedence(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	reg := NewRegistry()
	w := &timeoutPrecedenceWorker{}
	Register(reg, w)
	// Both the per-kind Timeouter (10s) and the runner DefaultTimeout (10s) are
	// long; the 20ms per-job timeout must win, or this test would hang.
	r := rwRunner(t, db, reg, func(c *RunnerConfig) { c.DefaultTimeout = 10 * time.Second })

	ctx := context.Background()
	id, err := Insert(ctx, NewClient(db), timeoutPrecedenceArgs{}, InsertOpts{Timeout: 20 * time.Millisecond})
	require.NoError(t, err)

	done := make(chan struct{})
	go func() {
		runToIdle(t, ctx, r)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the per-job timeout did not override the longer per-kind/default timeout")
	}

	assert.Equal(t, string(StateSucceeded), jobState(t, db, id), "the job times out once, then succeeds")
	assert.EqualValues(t, 2, w.calls.Load())

	var firstOutcome, firstClass string
	require.NoError(t, db.Table("job_runs").Select("outcome").Where("job_id = ?", id).Order("attempt").Limit(1).Scan(&firstOutcome).Error)
	require.NoError(t, db.Table("job_runs").Select("error_class").Where("job_id = ?", id).Order("attempt").Limit(1).Scan(&firstClass).Error)
	assert.Equal(t, string(OutcomeTimeout), firstOutcome, "the first attempt records a timeout outcome")
	assert.Equal(t, string(ErrorTimeout), firstClass, "a deadline overrides the worker classifier")
}

// --- crash recovery (lease expiry) ------------------------------------------

type recoverArgs struct{ V string }

func (recoverArgs) Kind() string { return "rw.recover" }

type recoverWorker struct{ ran atomic.Int32 }

func (*recoverWorker) Kind() string { return "rw.recover" }
func (w *recoverWorker) Work(_ context.Context, _ *Job[recoverArgs]) (Result, error) {
	w.ran.Add(1)
	return Result{}, nil
}

// TestRealWorldCrashRecoveryLeaseExpiry simulates an executor that claims a job
// and dies mid-flight: the lease expires, a Sweep reclaims the job to available
// and crashes the stale stub, and a fresh runner reruns it to success — no lost
// work, no double-finalize.
func TestRealWorldCrashRecoveryLeaseExpiry(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	driver := NewSQLiteDriver(db)
	base := time.Now().UTC().Truncate(time.Second)

	reg := NewRegistry()
	w := &recoverWorker{}
	Register(reg, w)

	// Insert under the same fixed clock the crash-claim uses, so scheduled_at is
	// exactly base and the claim is deterministic regardless of wall-clock drift
	// or stored-time precision.
	ctx := context.Background()
	claimCtx := clockCtx(ctx, NewFixedClock(base))
	id, err := Insert(claimCtx, NewClient(db), recoverArgs{V: "x"}, InsertOpts{})
	require.NoError(t, err)

	// A "crashed" executor: claim the job and write its run stub, then never
	// finalize.
	batch, err := driver.Dequeue(claimCtx, []string{"default"}, "local", true, 1, 30*time.Second)
	require.NoError(t, err)
	require.Len(t, batch, 1)
	require.NoError(t, driver.InsertRunStub(claimCtx, NewID(), batch[0], base, "local", "deadbeef:1"))
	assert.Equal(t, string(StateRunning), jobState(t, db, id), "the job is leased to the now-dead executor")

	// The lease expires; sweep reclaims it and crashes the stale stub.
	sweepCtx := clockCtx(ctx, NewFixedClock(base.Add(time.Hour)))
	sched := NewScheduler(db, NewClient(db))
	reclaimed, err := sched.Sweep(sweepCtx)
	require.NoError(t, err)
	assert.Equal(t, 1, reclaimed, "the expired lease is reclaimed")
	assert.Equal(t, string(StateAvailable), jobState(t, db, id), "the job is back to available")

	// A fresh runner reruns it to success, driven on a fixed clock past the lease
	// so the whole test stays in consistent UTC (the stored scheduled_at is base;
	// a wall-clock runner in a non-UTC zone would compare it inconsistently).
	r := rwRunner(t, db, reg)
	runToIdle(t, sweepCtx, r)

	assert.Equal(t, string(StateSucceeded), jobState(t, db, id), "the reclaimed job completes")
	assert.EqualValues(t, 1, w.ran.Load(), "the work ran exactly once — the crash claim never executed the worker")

	// Two audit rows: the crashed stub and the successful rerun. No lost work.
	var crashed, succeeded int64
	require.NoError(t, db.Table("job_runs").Where("job_id = ? AND outcome = ?", id, string(OutcomeCrashed)).Count(&crashed).Error)
	require.NoError(t, db.Table("job_runs").Where("job_id = ? AND outcome = ?", id, string(OutcomeSuccess)).Count(&succeeded).Error)
	assert.EqualValues(t, 1, crashed, "the stale stub is marked crashed")
	assert.EqualValues(t, 1, succeeded, "the rerun is recorded as a success")
}

// TestRealWorldSupersededFinalizeIsNoDoubleFinalize covers the superseded path:
// a job cancelled out from under a running attempt stays cancelled when the
// attempt finalizes, the attempt is still audited, and no follow-ups fire.
func TestRealWorldSupersededFinalizeIsNoDoubleFinalize(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	driver := NewSQLiteDriver(db)
	base := time.Now().UTC().Truncate(time.Second)
	ctx := clockCtx(context.Background(), NewFixedClock(base))

	id, err := Insert(ctx, NewClient(db), recoverArgs{V: "x"}, InsertOpts{})
	require.NoError(t, err)

	batch, err := driver.Dequeue(ctx, []string{"default"}, "local", true, 1, 30*time.Second)
	require.NoError(t, err)
	require.Len(t, batch, 1)
	runID := NewID()
	require.NoError(t, driver.InsertRunStub(ctx, runID, batch[0], base, "local", "exec-1"))

	// The job is cancelled out from under the running attempt.
	require.NoError(t, CancelJob(ctx, db, id))

	// The worker finishes successfully with a follow-up, but the finalize is
	// superseded by the cancel.
	result := Result{FollowUps: []FollowUp{{Kind: "rw.recover.child", Args: recoverArgs{V: "child"}}}}
	require.NoError(t, driver.Finalize(ctx, batch[0], runID, result, nil, base.Add(time.Second)))

	assert.Equal(t, string(StateCancelled), jobState(t, db, id), "the cancel is not overwritten by the finishing worker")
	assert.EqualValues(t, 1, runCount(t, db, id), "the attempt is still audited exactly once")
	var children int64
	require.NoError(t, db.Table("jobs").Where("kind = ?", "rw.recover.child").Count(&children).Error)
	assert.Zero(t, children, "a superseded finalize enqueues no follow-ups")
}

// --- idempotent enqueue ------------------------------------------------------

// TestRealWorldIdempotentEnqueue asserts UniqueKey enforces forever-idempotency
// and UniqueActiveKey frees up once the job reaches a terminal state.
func TestRealWorldIdempotentEnqueue(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	reg := NewRegistry()
	Register(reg, &successWorker{})
	client := NewClient(db)
	ctx := context.Background()

	// UniqueKey: the first insert wins; every duplicate resolves to
	// ErrAlreadyEnqueued.
	const dupes = 20
	firstID, err := Insert(ctx, client, successArgs{V: "slate"}, InsertOpts{UniqueKey: "slate-2026-06-23"})
	require.NoError(t, err)
	for range dupes {
		_, dupErr := Insert(ctx, client, successArgs{V: "slate"}, InsertOpts{UniqueKey: "slate-2026-06-23"})
		require.ErrorIs(t, dupErr, ErrAlreadyEnqueued, "a duplicate unique key is a clean already-enqueued")
	}
	assert.EqualValues(t, 1, jobCount(t, db, "test.success"), "exactly one job lands for the unique key")

	// UniqueActiveKey: a second insert collides while the first is active.
	activeID, err := Insert(ctx, client, successArgs{V: "subject"}, InsertOpts{UniqueActiveKey: "subject-42"})
	require.NoError(t, err)
	_, err = Insert(ctx, client, successArgs{V: "subject"}, InsertOpts{UniqueActiveKey: "subject-42"})
	require.ErrorIs(t, err, ErrAlreadyEnqueued, "only one in-flight job per active key")

	// Drain to terminal; the active key frees up.
	r := newRunner(t, db, reg)
	runToIdle(t, ctx, r)
	require.Equal(t, string(StateSucceeded), jobState(t, db, activeID))

	reactivatedID, err := Insert(ctx, client, successArgs{V: "subject"}, InsertOpts{UniqueActiveKey: "subject-42"})
	require.NoError(t, err, "a terminal job frees its active key for a fresh run")
	assert.NotEqual(t, activeID, reactivatedID, "the re-enqueue is a brand new job")
	assert.NotEqual(t, firstID, reactivatedID)
}

// --- outbox pattern ----------------------------------------------------------

// TestRealWorldOutboxPattern asserts that an Insert on a caller transaction lands
// only when the transaction commits.
func TestRealWorldOutboxPattern(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	reg := NewRegistry()
	Register(reg, &successWorker{})
	client := NewClient(db)
	ctx := context.Background()

	// Rollback: the job never appears.
	rolledBack := db.Begin()
	require.NoError(t, rolledBack.Error)
	rollbackID, err := Insert(ctx, client, successArgs{V: "rollback"}, InsertOpts{Tx: rolledBack})
	require.NoError(t, err)
	require.NoError(t, rolledBack.Rollback().Error)
	_, err = FindJob(ctx, db, rollbackID)
	assert.ErrorIs(t, err, ErrJobNotFound, "a rolled-back outbox insert leaves no job")

	// Commit: the job appears and runs.
	committed := db.Begin()
	require.NoError(t, committed.Error)
	commitID, err := Insert(ctx, client, successArgs{V: "commit"}, InsertOpts{Tx: committed})
	require.NoError(t, err)
	require.NoError(t, committed.Commit().Error)

	view, err := FindJob(ctx, db, commitID)
	require.NoError(t, err, "a committed outbox insert is visible")
	assert.Equal(t, string(StateAvailable), view.State)

	r := newRunner(t, db, reg)
	runToIdle(t, ctx, r)
	assert.Equal(t, string(StateSucceeded), jobState(t, db, commitID), "the committed job runs")
}

// --- observability -----------------------------------------------------------

// TestRealWorldObservabilityCountersReconcile drives a mix of outcomes through a
// recording observer and reconciles the observed counters against the driven
// state.
func TestRealWorldObservabilityCountersReconcile(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	ctx := context.Background()

	obs := &recordObserver{}
	reg := NewRegistry()
	Register(reg, &successWorker{})                                                            // succeeds once
	Register(reg, &transientWorker{failuresBefore: 2})                                         // 2 failures + 1 success
	Register(reg, &classifyingWorker{kind: "rw.obs.perm", class: ErrorPermanent, msg: "nope"}) // discards once
	r := rwRunner(t, db, reg, func(c *RunnerConfig) {
		c.Observer = obs
		c.RetryBackoffBase = time.Millisecond
	})

	client := NewClient(db)
	_, err := Insert(ctx, client, successArgs{V: "a"}, InsertOpts{})
	require.NoError(t, err)
	_, err = Insert(ctx, client, transientArgs{V: "b"}, InsertOpts{})
	require.NoError(t, err)
	_, err = Enqueue(ctx, client, "rw.obs.perm", []byte(`{"subject":"c"}`), InsertOpts{})
	require.NoError(t, err)

	runToIdle(t, ctx, r)

	// Attempts: success(1) + transient(3) + permanent(1) = 5.
	const wantAttempts = 5
	obs.mu.Lock()
	starts, claimed, finishCount, retryCount := obs.starts, obs.claimed, len(obs.finishes), len(obs.retries)
	obs.mu.Unlock()
	assert.Equal(t, wantAttempts, starts, "OnStart fires once per attempt")
	assert.Equal(t, wantAttempts, finishCount, "OnFinish fires once per attempt")
	assert.GreaterOrEqual(t, claimed, wantAttempts, "every attempt was claimed at least once")
	assert.Equal(t, 2, retryCount, "two transient failures produced two retries")

	assert.Equal(t, 2, obs.outcomeCount(OutcomeSuccess), "two jobs succeeded")
	assert.Equal(t, 3, obs.outcomeCount(OutcomeError), "two transient + one permanent error outcomes")

	// The observer's view reconciles with the persisted overview.
	overview, err := Overview(ctx, db, OverviewParams{})
	require.NoError(t, err)
	assert.Equal(t, 2, overview.CountsByState[string(StateSucceeded)])
	assert.Equal(t, 1, overview.CountsByState[string(StateDiscarded)])

	runs, err := CountRuns(ctx, db)
	require.NoError(t, err)
	assert.EqualValues(t, wantAttempts, runs, "the audit log has one row per attempt")
}

// TestRealWorldQueueHealthGauges asserts SampleQueueHealth and Overview report
// the driven queue depth: ready, in-flight, scheduled-ahead, and oldest-ready
// lag.
func TestRealWorldQueueHealthGauges(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	client := NewClient(db)
	base := time.Now().UTC().Truncate(time.Second)
	ctx := clockCtx(context.Background(), NewFixedClock(base))

	// Three ready jobs, scheduled in the past so the oldest has measurable lag.
	const ready = 3
	for i := range ready {
		past := base.Add(-time.Duration(i+1) * time.Minute)
		_, err := Insert(ctx, client, successArgs{V: "ready"}, InsertOpts{ScheduleAt: &past})
		require.NoError(t, err)
	}
	// One job scheduled in the future (claimable but not yet due).
	future := base.Add(time.Hour)
	_, err := Insert(ctx, client, successArgs{V: "future"}, InsertOpts{ScheduleAt: &future})
	require.NoError(t, err)
	// One running job (in flight).
	seedJob(t, db, jobRow{
		ID: NewID(), Kind: "test.success", State: string(StateRunning),
		Attempt: 1, MaxAttempts: 25, CreatedAt: base, UpdatedAt: base, ScheduledAt: base, LeasedUntil: &future,
	})

	qh, err := SampleQueueHealth(ctx, db)
	require.NoError(t, err)
	assert.EqualValues(t, ready, qh.Ready, "three jobs are claimable and due")
	assert.EqualValues(t, 1, qh.InFlight, "one job is running")
	assert.EqualValues(t, 1, qh.ScheduledAhead, "one claimable job is not yet due")
	assert.Equal(t, 3*time.Minute, qh.OldestReadyAge, "the oldest ready job has been claimable for three minutes")
	assert.Equal(t, base, qh.SampledAt)

	overview, err := Overview(ctx, db, OverviewParams{})
	require.NoError(t, err)
	assert.Equal(t, ready+2, overview.Total, "overview counts every live job")
	assert.Equal(t, ready+1, overview.CountsByState[string(StateAvailable)])
	assert.Equal(t, 1, overview.CountsByState[string(StateRunning)])
}

// --- retention ---------------------------------------------------------------

// TestRealWorldRetentionRemovesTerminalJobs asserts DeleteFinishedJobs removes
// terminal jobs and their runs while leaving non-terminal jobs untouched.
func TestRealWorldRetentionRemovesTerminalJobs(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	reg := NewRegistry()
	Register(reg, &successWorker{})
	client := NewClient(db)
	base := time.Now().UTC().Truncate(time.Second)
	ctx := clockCtx(context.Background(), NewFixedClock(base))

	// One job that runs to succeeded (terminal, finalized at base).
	finishedID, err := Insert(ctx, client, successArgs{V: "done"}, InsertOpts{})
	require.NoError(t, err)
	r := rwRunner(t, db, reg)
	runToIdle(t, ctx, r)
	require.Equal(t, string(StateSucceeded), jobState(t, db, finishedID))
	require.EqualValues(t, 1, runCount(t, db, finishedID))

	// One non-terminal job scheduled far in the future, so it stays available.
	future := base.Add(time.Hour)
	pendingID, err := Insert(ctx, client, successArgs{V: "pending"}, InsertOpts{ScheduleAt: &future})
	require.NoError(t, err)

	// Prune everything finalized before base+1m.
	deleted, err := DeleteFinishedJobs(ctx, db, base.Add(time.Minute))
	require.NoError(t, err)
	assert.EqualValues(t, 1, deleted, "exactly the one terminal job is pruned")

	_, err = FindJob(ctx, db, finishedID)
	assert.ErrorIs(t, err, ErrJobNotFound, "the terminal job is gone")
	assert.EqualValues(t, 0, runCount(t, db, finishedID), "its audit rows are gone too")

	view, err := FindJob(ctx, db, pendingID)
	require.NoError(t, err, "the non-terminal job is untouched")
	assert.Equal(t, string(StateAvailable), view.State)
}

// --- worker panic ------------------------------------------------------------

// TestRealWorldWorkerPanicIsRecovered asserts a panicking worker is recovered
// into a transient error, the runner survives, the failure is audited, and the
// retry completes.
func TestRealWorldWorkerPanicIsRecovered(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	reg := NewRegistry()
	w := &panicWorker{}
	Register(reg, w)
	r := rwRunner(t, db, reg, func(c *RunnerConfig) { c.RetryBackoffBase = time.Millisecond })

	ctx := context.Background()
	id, err := Insert(ctx, NewClient(db), panicArgs{V: "x"}, InsertOpts{})
	require.NoError(t, err)

	runToIdle(t, ctx, r)

	assert.Equal(t, string(StateSucceeded), jobState(t, db, id), "the runner survives the panic and the retry succeeds")
	assert.EqualValues(t, 2, w.calls.Load(), "panic on attempt 1, success on attempt 2")

	// The panicking attempt was audited as a transient error mentioning the panic.
	var class, msg string
	require.NoError(t, db.Table("job_runs").Select("error_class").Where("job_id = ?", id).Order("attempt").Limit(1).Scan(&class).Error)
	require.NoError(t, db.Table("job_runs").Select("error_message").Where("job_id = ?", id).Order("attempt").Limit(1).Scan(&msg).Error)
	assert.Equal(t, string(ErrorTransient), class, "a bare panic defaults to a transient classification")
	assert.True(t, strings.Contains(msg, "panicked"), "the recovered panic is recorded in the audit trail")
}
