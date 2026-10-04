package core

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/mrz1836/go-foundation/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// runner_unknown_kind_test.go covers what a Runner does with a claimed job of a
// kind its Registry does not register: defer it, a free snooze, while the job is
// younger than RunnerConfig.UnknownKindGrace, so a runner of the release that
// does register the kind can take it, and discard it once the window has passed.
//
// The database-backed tests run on a SimulatedClock and drive one claim at a time
// through pollOnce. A FixedClock will not do: both drivers decide what is
// claimable from the context clock, so a deferred job under a frozen clock never
// becomes claimable again.

// unknownKindT0 anchors the database-backed cases. The zone is deliberately not
// UTC, like writePathT0: the window is a duration between two instants, and no
// zone may leak into it.
//
//nolint:gochecknoglobals // shared fixture instant
var unknownKindT0 = time.Date(2026, 3, 8, 6, 30, 0, 0, time.FixedZone("UTC-5", -5*3600))

const (
	// stockGrace is the UnknownKindGrace a Runner applies when the field is left
	// zero, which every database-backed case here does.
	stockGrace = 15 * time.Minute
	// stockWindowDeferrals is how many times a job no runner registers is deferred
	// across stockGrace when every claim lands the instant the job is due: six
	// claims climb the ladder (1+2+4+8+16+32s, 63s), thirteen more wait a minute
	// each (to 843s), and a last one waits the 57s left to the edge, where the
	// next claim discards. 21 run rows in all.
	stockWindowDeferrals = 20
	// orphanKind is a kind no Registry in this file registers.
	orphanKind = "deploy.new_kind"
	// unknownKindMessage is ErrUnknownKind's text, as the audit row records it.
	unknownKindMessage = "jobs: unknown job kind"
	// deferWarnMsg and discardErrMsg are the two log lines the Runner writes.
	deferWarnMsg  = "jobs: deferring jobs of a kind this runner does not register"
	discardErrMsg = "jobs: discarded a job of a kind this runner does not register"
)

// unknownKindFixture is one SQLite database, a SimulatedClock starting at
// unknownKindT0, and a log recorder every runner it builds writes to.
type unknownKindFixture struct {
	db   *gorm.DB
	clk  *models.SimulatedClock
	logs *recordingHandler
}

func newUnknownKindFixture(t *testing.T) *unknownKindFixture {
	t.Helper()
	return &unknownKindFixture{
		db:   newDB(t),
		clk:  models.NewSimulatedClock(unknownKindT0),
		logs: &recordingHandler{},
	}
}

// ctx carries the fixture's clock, so every claim, finalize, and insert reads it.
func (f *unknownKindFixture) ctx() context.Context {
	return clockCtx(context.Background(), f.clk)
}

// now is the fixture clock's current instant.
func (f *unknownKindFixture) now() time.Time { return f.clk.Now(context.Background()) }

// enqueue inserts one job of kind at the clock's current instant.
func (f *unknownKindFixture) enqueue(t *testing.T, kind string, opts InsertOpts) string {
	t.Helper()
	id, err := Enqueue(f.ctx(), NewClient(f.db), kind, []byte(`{}`), opts)
	require.NoError(t, err)
	return id
}

// runner builds a SQLite Runner over the fixture's database that logs to its
// recorder.
func (f *unknownKindFixture) runner(t *testing.T, reg *Registry, mutators ...func(*RunnerConfig)) *Runner {
	t.Helper()
	return rwRunner(t, f.db, reg, append([]func(*RunnerConfig){func(c *RunnerConfig) {
		c.Logger = slog.New(f.logs)
	}}, mutators...)...)
}

// pollOne claims and settles exactly one job at the clock's current instant.
func (f *unknownKindFixture) pollOne(t *testing.T, r *Runner) {
	t.Helper()
	n, err := r.pollOnce(f.ctx())
	require.NoError(t, err)
	require.Equal(t, 1, n, "one job was claimable at %s", f.now())
}

// loadJobRow reads one jobs row back.
func loadJobRow(t *testing.T, db *gorm.DB, id string) jobRow {
	t.Helper()
	var row jobRow
	require.NoError(t, db.Where("id = ?", id).First(&row).Error)
	return row
}

// runsOf returns a job's run rows in attempt order.
func runsOf(t *testing.T, db *gorm.DB, jobID string) []jobRunRow {
	t.Helper()
	var rows []jobRunRow
	require.NoError(t, db.Where("job_id = ?", jobID).Order("attempt").Find(&rows).Error)
	return rows
}

// logsFor returns every record logged at level with msg.
func logsFor(h *recordingHandler, level slog.Level, msg string) []map[string]any {
	var out []map[string]any
	for _, rec := range h.records() {
		if rec["msg"] == msg && rec["level"] == level.String() {
			out = append(out, rec)
		}
	}
	return out
}

// requireDeferralRun asserts a run row is a deferral: a snooze that applied
// scheduled and carries ErrUnknownKind's message with no error class.
func requireDeferralRun(t *testing.T, run jobRunRow) {
	t.Helper()
	assert.Equal(t, string(OutcomeSnooze), run.Outcome)
	require.NotNil(t, run.JobState)
	assert.Equal(t, string(StateScheduled), *run.JobState)
	require.NotNil(t, run.ErrorMessage, "the deferral records why the job was handed back")
	assert.Equal(t, unknownKindMessage, *run.ErrorMessage)
	assert.Nil(t, run.ErrorClass, "a snooze is not a failure, so it carries no class")
	assert.False(t, run.Superseded)
}

// requireDiscardRun asserts a run row is the discard: a permanent ErrUnknownKind.
func requireDiscardRun(t *testing.T, run jobRunRow) {
	t.Helper()
	assert.Equal(t, string(OutcomeError), run.Outcome)
	require.NotNil(t, run.JobState)
	assert.Equal(t, string(StateDiscarded), *run.JobState)
	require.NotNil(t, run.ErrorMessage)
	assert.Equal(t, unknownKindMessage, *run.ErrorMessage)
	require.NotNil(t, run.ErrorClass)
	assert.Equal(t, string(ErrorPermanent), *run.ErrorClass)
}

// requireDeferredResults asserts a Driver double was handed n finalizes and
// that each was a deferral: a Result carrying a positive snooze. The doubles
// report a fixed outcome whatever they are handed, so without it a test of a
// deferral would pass as well over a discard.
func requireDeferredResults(t *testing.T, results []Result, n int) {
	t.Helper()
	require.Len(t, results, n)
	for i, res := range results {
		require.NotNil(t, res.Snooze, "finalize %d is a deferral", i)
		assert.Positive(t, *res.Snooze, "finalize %d", i)
	}
}

// youngOrphan is a claimed RawJob of an unregistered kind enqueued just now, for
// the Driver doubles, which serve a fixed batch rather than reading a database.
func youngOrphan(id string) RawJob {
	return RawJob{
		ID: id, Kind: orphanKind, Queue: "default", Args: []byte(`{}`),
		Attempt: 1, MaxAttempts: 25, CreatedAt: time.Now(), ScheduledAt: time.Now(),
	}
}

// --- defer and discard ---------------------------------------------------------

// TestRunnerDefersAYoungJobOfAnUnregisteredKind is the core of the fix: a runner
// that does not register a job's kind hands a young job back instead of
// discarding it, and the hand-back is a snooze in every column that matters.
func TestRunnerDefersAYoungJobOfAnUnregisteredKind(t *testing.T) {
	t.Parallel()
	f := newUnknownKindFixture(t)
	id := f.enqueue(t, orphanKind, InsertOpts{MaxAttempts: 3})

	claimAt := f.clk.Advance(5 * time.Second)
	f.pollOne(t, f.runner(t, NewRegistry()))

	row := loadJobRow(t, f.db, id)
	assert.Equal(t, string(StateScheduled), row.State, "the job is handed back, not discarded")
	assert.Equal(t, 1, row.Attempt)
	assert.Equal(t, 4, row.MaxAttempts, "the snooze raises max_attempts, so no attempt is spent")
	assert.Equal(t, time.Second, row.ScheduledAt.Sub(claimAt), "the first deferral is the ladder's first rung")
	assert.Nil(t, row.LeaseToken, "the claim is released")
	assert.Nil(t, row.LeasedUntil)
	assert.Nil(t, row.FinalizedAt, "a deferred job is not finished")

	runs := runsOf(t, f.db, id)
	require.Len(t, runs, 1)
	requireDeferralRun(t, runs[0])
}

// TestRunnerCapsTheLastDeferralAtTheWindowEdge proves the delay is capped at what
// is left of the window, so the job is decided on the window's edge rather than a
// ladder rung past it, and that the edge itself discards.
func TestRunnerCapsTheLastDeferralAtTheWindowEdge(t *testing.T) {
	t.Parallel()
	f := newUnknownKindFixture(t)
	id := f.enqueue(t, orphanKind, InsertOpts{})
	r := f.runner(t, NewRegistry())
	edge := unknownKindT0.Add(stockGrace)

	// 300ms before the edge, the ladder's first rung (a second) is longer than
	// what is left.
	f.clk.Set(edge.Add(-300 * time.Millisecond))
	f.pollOne(t, r)
	row := loadJobRow(t, f.db, id)
	require.Equal(t, string(StateScheduled), row.State)
	assert.True(t, row.ScheduledAt.Equal(edge), "the last deferral lands exactly on the edge, got %s", row.ScheduledAt)

	f.clk.Set(edge)
	f.pollOne(t, r)
	assert.Equal(t, string(StateDiscarded), jobState(t, f.db, id), "a job exactly grace old is discarded")

	runs := runsOf(t, f.db, id)
	require.Len(t, runs, 2)
	requireDeferralRun(t, runs[0])
	requireDiscardRun(t, runs[1])
}

// TestRunnerDiscardsAnUnregisteredKindOnceTheWindowHasPassed covers the other side
// of the window: a job older than the grace is discarded on its first claim, and
// the discard, silent before the grace existed, is logged as an error.
func TestRunnerDiscardsAnUnregisteredKindOnceTheWindowHasPassed(t *testing.T) {
	t.Parallel()
	f := newUnknownKindFixture(t)
	id := f.enqueue(t, orphanKind, InsertOpts{Queue: "default"})

	f.clk.Set(unknownKindT0.Add(stockGrace + time.Second))
	f.pollOne(t, f.runner(t, NewRegistry()))

	assert.Equal(t, string(StateDiscarded), jobState(t, f.db, id))
	runs := runsOf(t, f.db, id)
	require.Len(t, runs, 1)
	requireDiscardRun(t, runs[0])

	discards := logsFor(f.logs, slog.LevelError, discardErrMsg)
	require.Len(t, discards, 1, "every discard is logged")
	assert.Equal(t, orphanKind, discards[0]["kind"])
	assert.Equal(t, id, discards[0]["job_id"])
	assert.Equal(t, "default", discards[0]["queue"])
	assert.EqualValues(t, 1, discards[0]["attempt"])
	assert.Equal(t, stockGrace, discards[0]["grace"])
	assert.Equal(t, stockGrace+time.Second, discards[0]["age"])
	assert.Empty(t, logsFor(f.logs, slog.LevelWarn, deferWarnMsg), "nothing was deferred")
}

// walkWindow claims job id each time it falls due, starting at the clock's
// current instant, until a claim discards it, and returns how many claims
// deferred it. On every deferral it requires that the job kept the headroom it
// was enqueued with (budget) and is due again no later than edge.
func (f *unknownKindFixture) walkWindow(t *testing.T, r *Runner, id string, budget int, edge time.Time) int {
	t.Helper()
	for deferrals := 0; deferrals < 100; deferrals++ {
		f.pollOne(t, r)
		row := loadJobRow(t, f.db, id)
		if row.State != string(StateScheduled) {
			return deferrals
		}
		require.Equal(t, budget, row.MaxAttempts-row.Attempt, "deferral %d spent no attempt", deferrals+1)
		require.False(t, row.ScheduledAt.After(edge), "deferral %d lands inside the window", deferrals+1)
		f.clk.Set(row.ScheduledAt)
	}
	t.Fatal("still deferring after 100 claims")
	return 0
}

// TestRunnerDeferralsSpendNoAttemptsAndEndAtTheEdge walks one orphaned job through
// the whole window, claim after claim, with a retry budget of two. Every deferral
// leaves the budget where it was, every one lands inside the window, and the job
// is discarded on the edge at an attempt past its original budget, so it is the
// window, not the budget, that ends it.
func TestRunnerDeferralsSpendNoAttemptsAndEndAtTheEdge(t *testing.T) {
	t.Parallel()
	f := newUnknownKindFixture(t)
	const budget = 2
	id := f.enqueue(t, orphanKind, InsertOpts{MaxAttempts: budget})
	r := f.runner(t, NewRegistry())
	edge := unknownKindT0.Add(stockGrace)

	deferrals := f.walkWindow(t, r, id, budget, edge)

	row := loadJobRow(t, f.db, id)
	require.Equal(t, string(StateDiscarded), row.State, "the window ends the deferrals")
	assert.True(t, f.now().Equal(edge), "the discard happens on the edge, at %s", f.now())
	assert.Equal(t, stockWindowDeferrals, deferrals)
	assert.Greater(t, row.Attempt, budget, "the job outlived its original budget")

	runs := runsOf(t, f.db, id)
	require.Len(t, runs, deferrals+1, "one run row per claim")
	for _, run := range runs[:deferrals] {
		requireDeferralRun(t, run)
	}
	requireDiscardRun(t, runs[deferrals])
}

// TestRunnerDeferralCostIgnoresRetrySettings pins the write cost of a job no
// runner registers: 21 run rows across the default window, the discard included,
// whatever the retry ladder is set to. On the retry ladder, a one-second ceiling
// would re-claim the job every second (about 900 rows), and a thirty-minute one
// would park it far past the point a runner of the new release could take it.
func TestRunnerDeferralCostIgnoresRetrySettings(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name          string
		base, ceiling time.Duration
	}{
		{"the default retry ladder", 0, 0},
		{"a one-second retry ceiling", time.Millisecond, time.Second},
		{"a thirty-minute retry ceiling", 30 * time.Second, 30 * time.Minute},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			f := newUnknownKindFixture(t)
			id := f.enqueue(t, orphanKind, InsertOpts{})
			r := f.runner(t, NewRegistry(), func(c *RunnerConfig) {
				c.RetryBackoffBase = tc.base
				c.MaxRetryBackoff = tc.ceiling
			})
			edge := unknownKindT0.Add(stockGrace)

			deferrals := f.walkWindow(t, r, id, defaultMaxAttempts, edge)

			assert.Equal(t, stockWindowDeferrals, deferrals)
			assert.Len(t, runsOf(t, f.db, id), stockWindowDeferrals+1, "one run row per claim")
			assert.True(t, f.now().Equal(edge), "the discard happens on the edge, at %s", f.now())
			assert.Equal(t, string(StateDiscarded), jobState(t, f.db, id))
		})
	}
}

// TestRunnerRollingDeployNewRunnerRunsAJobAnOldRunnerDeferred is issue #61's own
// scenario: during a rolling deploy a runner of the old release claims a job of a
// kind only the new release registers. It defers the job instead of discarding
// it, and a runner of the new release then runs it.
func TestRunnerRollingDeployNewRunnerRunsAJobAnOldRunnerDeferred(t *testing.T) {
	t.Parallel()
	f := newUnknownKindFixture(t)
	id, err := Insert(f.ctx(), NewClient(f.db), successArgs{V: "new"}, InsertOpts{})
	require.NoError(t, err)

	old := f.runner(t, NewRegistry())
	f.pollOne(t, old)
	require.Equal(t, string(StateScheduled), jobState(t, f.db, id), "the old runner hands the job back")

	w := &successWorker{}
	reg := NewRegistry()
	Register(reg, w)
	f.clk.Advance(time.Minute)
	f.pollOne(t, f.runner(t, reg))

	row := loadJobRow(t, f.db, id)
	assert.Equal(t, string(StateSucceeded), row.State, "the new runner runs it")
	assert.EqualValues(t, 1, w.calls.Load())
	assert.Equal(t, 2, row.Attempt)
	assert.Equal(t, defaultMaxAttempts+1, row.MaxAttempts,
		"the deferral spent nothing: the headroom is a first-attempt success's")

	runs := runsOf(t, f.db, id)
	require.Len(t, runs, 2)
	requireDeferralRun(t, runs[0])
	assert.Equal(t, string(OutcomeSuccess), runs[1].Outcome)
	assert.Equal(t, 2, runs[1].Attempt)
}

// --- logging and observers -----------------------------------------------------

// TestRunnerWarnsOncePerUnregisteredKind proves the deferral warning is per kind,
// not per claim: a deploy's worth of deferrals of one kind logs one line, and a
// second kind logs its own.
func TestRunnerWarnsOncePerUnregisteredKind(t *testing.T) {
	t.Parallel()
	f := newUnknownKindFixture(t)
	// A second apart, so the claim order, which ties on scheduled_at, is the
	// enqueue order.
	a1 := f.enqueue(t, "deploy.kind_a", InsertOpts{})
	f.clk.Advance(time.Second)
	f.enqueue(t, "deploy.kind_a", InsertOpts{})
	bAt := f.clk.Advance(time.Second)
	b := f.enqueue(t, "deploy.kind_b", InsertOpts{Queue: "periodic"})
	r := f.runner(t, NewRegistry())

	// Three claims defer all three jobs; a minute later, three more defer them again.
	for range 2 {
		for range 3 {
			f.pollOne(t, r)
		}
		f.clk.Advance(time.Minute)
	}

	warns := logsFor(f.logs, slog.LevelWarn, deferWarnMsg)
	require.Len(t, warns, 2, "one warning per kind, however many deferrals")
	byKind := map[any]map[string]any{warns[0]["kind"]: warns[0], warns[1]["kind"]: warns[1]}
	require.Contains(t, byKind, "deploy.kind_a")
	require.Contains(t, byKind, "deploy.kind_b")

	for _, tc := range []struct {
		kind, jobID, queue string
		enqueuedAt         time.Time
	}{
		{"deploy.kind_a", a1, "default", unknownKindT0}, // the first job deferred is the one named
		{"deploy.kind_b", b, "periodic", bAt},
	} {
		w := byKind[tc.kind]
		assert.Equal(t, tc.jobID, w["job_id"], tc.kind)
		assert.Equal(t, tc.queue, w["queue"], tc.kind)
		assert.Equal(t, stockGrace, w["grace"], tc.kind)
		discardAt, ok := w["discard_at"].(time.Time)
		require.True(t, ok, "discard_at is a time")
		assert.True(t, discardAt.Equal(tc.enqueuedAt.Add(stockGrace)),
			"%s: discard_at is the enqueue plus the grace, got %s", tc.kind, discardAt)
	}
	assert.Empty(t, logsFor(f.logs, slog.LevelError, discardErrMsg))
}

// TestRunnerWarnsOncePerKindUnderConcurrentDispatch pins the warn-once guard under
// concurrent dispatch: eight slots deferring the same kind at once log one line.
// Run it under -race.
func TestRunnerWarnsOncePerKindUnderConcurrentDispatch(t *testing.T) {
	t.Parallel()
	const n = 8
	batch := make([]RawJob, n)
	for i := range batch {
		batch[i] = youngOrphan(models.NewID())
	}
	fd := &fakeDriver{batch: batch}
	logs := &recordingHandler{}
	r, err := NewRunner(RunnerConfig{
		DB: newDB(t), Driver: fd, Registry: NewRegistry(), Queues: []string{"default"},
		ExecutorClass: "local", Concurrency: n, Logger: slog.New(logs),
	})
	require.NoError(t, err)

	claimed, err := r.pollOnce(context.Background())
	require.NoError(t, err)
	require.Equal(t, n, claimed)

	assert.Len(t, logsFor(logs, slog.LevelWarn, deferWarnMsg), 1, "one warning across eight concurrent deferrals")
	requireDeferredResults(t, fd.finalizedResults(), n)
}

// TestRunnerWarnsAgainForALaterEpisode covers a long-lived runner: it warned
// about a kind during a deploy, and days later a rollback starts deferring the
// kind again. Once the grace has passed since the last warning, the next
// deferral warns again, and the episode after that is quiet again.
func TestRunnerWarnsAgainForALaterEpisode(t *testing.T) {
	t.Parallel()
	f := newUnknownKindFixture(t)
	first := f.enqueue(t, orphanKind, InsertOpts{})
	r := f.runner(t, NewRegistry())
	f.pollOne(t, r)

	// Inside the grace of that warning: quiet.
	f.clk.Advance(time.Minute)
	f.pollOne(t, r)
	require.Len(t, logsFor(f.logs, slog.LevelWarn, deferWarnMsg), 1)

	// Days later, a new job of the kind.
	f.clk.Set(unknownKindT0.Add(72 * time.Hour))
	second := f.enqueue(t, orphanKind, InsertOpts{})
	f.pollOne(t, r) // the first job, long past its window: discarded
	f.pollOne(t, r) // the second: deferred, and warned about
	assert.Equal(t, string(StateDiscarded), jobState(t, f.db, first))
	assert.Equal(t, string(StateScheduled), jobState(t, f.db, second))

	// A minute later, inside the new warning's grace: quiet again.
	f.clk.Advance(time.Minute)
	f.pollOne(t, r)

	warns := logsFor(f.logs, slog.LevelWarn, deferWarnMsg)
	require.Len(t, warns, 2, "one warning per episode")
	assert.Equal(t, first, warns[0]["job_id"])
	assert.Equal(t, second, warns[1]["job_id"])
}

// TestDispatchUnknownKindFinalizeErrorPropagates covers a deferral whose finalize
// fails: nothing was persisted, so the error reaches the caller and there is no
// log line and no observer event describing an outcome the database does not hold.
func TestDispatchUnknownKindFinalizeErrorPropagates(t *testing.T) {
	t.Parallel()
	finalizeErr := errors.New("finalize down")
	fd := &fakeDriver{batch: []RawJob{youngOrphan("j")}, finalizeErr: finalizeErr}
	logs := &recordingHandler{}
	obs := &recordingObserver{}
	r, err := NewRunner(RunnerConfig{
		DB: newDB(t), Driver: fd, Registry: NewRegistry(), Queues: []string{"default"},
		ExecutorClass: "local", Observer: obs, Logger: slog.New(logs),
	})
	require.NoError(t, err)

	_, err = r.pollOnce(context.Background())
	require.ErrorIs(t, err, finalizeErr)
	requireDeferredResults(t, fd.finalizedResults(), 1)

	assert.Empty(t, logsFor(logs, slog.LevelWarn, deferWarnMsg))
	assert.Empty(t, logsFor(logs, slog.LevelError, discardErrMsg))
	_, starts, finishes, retries := obs.snapshot()
	assert.Empty(t, starts)
	assert.Empty(t, finishes, "a failed finalize persisted nothing to report")
	assert.Empty(t, retries)
	assert.Empty(t, obs.snapshotSupersedes())
}

// TestObserverUnknownKindDeferralIsASnoozeWithNoStart pins what an observer sees
// of a deferral: no OnStart, since no worker ran, then one OnFinish reporting a
// snooze that carries ErrUnknownKind with no error class, and no OnRetry, since a
// snooze is not a retry.
func TestObserverUnknownKindDeferralIsASnoozeWithNoStart(t *testing.T) {
	t.Parallel()
	f := newUnknownKindFixture(t)
	id := f.enqueue(t, orphanKind, InsertOpts{})
	obs := &recordingObserver{}
	f.pollOne(t, f.runner(t, NewRegistry(), func(c *RunnerConfig) { c.Observer = obs }))

	_, starts, finishes, retries := obs.snapshot()
	assert.Empty(t, starts, "no worker ran")
	require.Len(t, finishes, 1)
	assert.Equal(t, id, finishes[0].JobID)
	assert.Equal(t, OutcomeSnooze, finishes[0].Outcome)
	assert.Empty(t, string(finishes[0].ErrorClass), "a snooze carries no error class")
	require.ErrorIs(t, finishes[0].Err, ErrUnknownKind, "the event says why the job was handed back")
	assert.Empty(t, retries, "a snooze is not a retry")
	assert.Empty(t, obs.snapshotSupersedes())
}

// TestObserverSupersededUnknownKindDeferralFiresOnSupersede covers a deferral that
// lost its claim before it finalized, cancelled underneath it: the finalize
// applied nothing, so the observer sees a supersede, not a finish, and the Runner
// does not warn about a deferral that never took effect.
func TestObserverSupersededUnknownKindDeferralFiresOnSupersede(t *testing.T) {
	t.Parallel()
	pd := &permitDriver{
		batch:   []RawJob{youngOrphan("j")},
		outcome: FinalizeOutcome{Superseded: true, State: StateCancelled, RunOutcome: OutcomeSnooze},
	}
	logs := &recordingHandler{}
	obs := &recordingObserver{}
	r, err := NewRunner(RunnerConfig{
		DB: newDB(t), Driver: pd, Registry: NewRegistry(), Queues: []string{"default"},
		ExecutorClass: "local", Observer: obs, Logger: slog.New(logs),
	})
	require.NoError(t, err)

	claimed, err := r.pollOnce(context.Background())
	require.NoError(t, err)
	require.Equal(t, 1, claimed)
	requireDeferredResults(t, pd.finalizedResults(), 1)

	supersedes := obs.snapshotSupersedes()
	require.Len(t, supersedes, 1)
	assert.Equal(t, StateCancelled, supersedes[0].State)
	_, starts, finishes, _ := obs.snapshot()
	assert.Empty(t, starts)
	assert.Empty(t, finishes, "a superseded finalize is not a finish")
	assert.Empty(t, logsFor(logs, slog.LevelWarn, deferWarnMsg), "a deferral that took no effect is not announced")
}

// --- the window ------------------------------------------------------------------

// TestUnknownKindGraceResolution covers the three cases of the window rule.
func TestUnknownKindGraceResolution(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name   string
		config time.Duration
		want   time.Duration
	}{
		{"zero selects fifteen minutes", 0, 15 * time.Minute},
		{"explicit is kept", 30 * time.Minute, 30 * time.Minute},
		{"explicit below the default is kept", time.Second, time.Second},
		{"negative disables", -1, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			r := &Runner{cfg: RunnerConfig{UnknownKindGrace: tc.config}}
			assert.Equal(t, tc.want, r.unknownKindGrace())
		})
	}
}

// TestUnknownKindDeferral is the decision table: defer while the job is younger
// than the window, by the deferral's own ladder (a second, doubling per attempt)
// capped at a minute and at what is left of the window; discard on the edge,
// past it, with the window disabled, or with no enqueue time to measure from.
func TestUnknownKindDeferral(t *testing.T) {
	t.Parallel()
	const grace = 15 * time.Minute
	created := unknownKindT0

	for _, tc := range []struct {
		name         string
		createdAt    time.Time
		now          time.Time
		attempt      int
		grace        time.Duration
		wantDelay    time.Duration
		wantDeferred bool
	}{
		{"the first claim defers a second", created, created, 1, grace, time.Second, true},
		{"each claim doubles the delay", created, created.Add(time.Minute), 3, grace, 4 * time.Second, true},
		{"the sixth claim waits 32 seconds", created, created.Add(time.Minute), 6, grace, 32 * time.Second, true},
		{"the ladder is capped at one minute", created, created.Add(time.Minute), 7, grace, time.Minute, true},
		{"a late attempt stays at the cap", created, created.Add(time.Minute), 1000, grace, time.Minute, true},
		{"an attempt below one is the first rung", created, created, 0, grace, time.Second, true},
		{"the delay is capped at the remaining window", created, created.Add(grace - 10*time.Second), 7, grace, 10 * time.Second, true},
		{"a nanosecond before the edge defers by a nanosecond", created, created.Add(grace - 1), 1, grace, 1, true},
		{"exactly on the edge discards", created, created.Add(grace), 1, grace, 0, false},
		{"past the edge discards", created, created.Add(grace + time.Minute), 1, grace, 0, false},
		{"a disabled window discards", created, created, 1, 0, 0, false},
		{"a negative window discards", created, created, 1, -time.Minute, 0, false},
		{"no enqueue time discards", time.Time{}, created, 1, grace, 0, false},
		{"a short window caps below the ladder", created, created, 1, 500 * time.Millisecond, 500 * time.Millisecond, true},
		{"a future enqueue counts as age zero", created.Add(time.Hour), created, 7, grace, time.Minute, true},
		{"a future enqueue never defers past the window", created.Add(time.Hour), created, 7, 30 * time.Second, 30 * time.Second, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			delay, deferred := unknownKindDeferral(tc.createdAt, tc.now, tc.attempt, tc.grace)
			assert.Equal(t, tc.wantDeferred, deferred)
			assert.Equal(t, tc.wantDelay, delay)
		})
	}
}

// FuzzUnknownKindDeferral checks the decision's invariants over arbitrary
// instants, attempts, and windows: it defers exactly when an anchored job is
// younger than a positive window, and a deferral is the ladder's rung for the
// attempt cut to what is left of the window, so it is positive, never longer than
// the one-minute cap or the window, and never lands past the window's edge.
func FuzzUnknownKindDeferral(f *testing.F) {
	f.Add(int64(0), int64(time.Minute), int64(15*time.Minute), 1, false)
	f.Add(int64(0), int64(15*time.Minute), int64(15*time.Minute), 7, false)
	f.Add(int64(time.Hour), int64(0), int64(30*time.Second), 3, false)
	f.Add(int64(0), int64(0), int64(0), 1, true)
	f.Add(int64(-1), int64(1<<62), int64(1<<62), -5, false)
	f.Add(int64(0), int64(1), int64(time.Minute), 1<<30, false)

	f.Fuzz(func(t *testing.T, createdNs, nowNs, graceNs int64, attempt int, noAnchor bool) {
		grace := time.Duration(graceNs)
		now := time.Unix(0, nowNs)
		createdAt := time.Unix(0, createdNs)
		if noAnchor {
			createdAt = time.Time{}
		}

		delay, deferred := unknownKindDeferral(createdAt, now, attempt, grace)

		age := max(now.Sub(createdAt), 0)
		wantDeferred := grace > 0 && !createdAt.IsZero() && age < grace
		require.Equal(t, wantDeferred, deferred, "age %s, grace %s", age, grace)
		if !deferred {
			require.Zero(t, delay)
			return
		}
		rung := expBackoff(unknownKindDeferralBase, maxUnknownKindDeferral, attempt)
		require.Equal(t, min(rung, grace-age), delay, "the rung for attempt %d, cut to the window", attempt)
		require.Positive(t, delay)
		require.LessOrEqual(t, delay, maxUnknownKindDeferral)
		require.LessOrEqual(t, delay, grace)
	})
}

// TestRunUntilIdleWaitsOutTheWindowThenDiscards runs the real loop on the real
// clock with a short window: RunUntilIdle keeps polling while the job is
// deferred, returns once the window has passed and the job is discarded, and
// does not discard it early.
func TestRunUntilIdleWaitsOutTheWindowThenDiscards(t *testing.T) {
	t.Parallel()
	// Wide enough that a loaded -race run still makes its first claim well inside
	// it: the ladder defers a second, then the half second left, then discards.
	const grace = 1500 * time.Millisecond
	db := newDB(t)
	logs := &recordingHandler{}
	r := rwRunner(t, db, NewRegistry(), func(c *RunnerConfig) {
		c.UnknownKindGrace = grace
		c.Logger = slog.New(logs)
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	// The startup schema probe runs now rather than inside the window.
	require.NoError(t, r.checkSchema(ctx))
	before := time.Now()
	id, err := Enqueue(ctx, NewClient(db), orphanKind, []byte(`{}`), InsertOpts{})
	require.NoError(t, err)

	require.NoError(t, r.RunUntilIdle(ctx))
	assert.GreaterOrEqual(t, time.Since(before), grace, "the job was kept for the whole window")

	assert.Equal(t, string(StateDiscarded), jobState(t, db, id))
	runs := runsOf(t, db, id)
	require.GreaterOrEqual(t, len(runs), 2, "deferred at least once before the discard")
	last := len(runs) - 1
	for _, run := range runs[:last] {
		requireDeferralRun(t, run)
	}
	requireDiscardRun(t, runs[last])
	assert.Len(t, logsFor(logs, slog.LevelWarn, deferWarnMsg), 1)
	assert.Len(t, logsFor(logs, slog.LevelError, discardErrMsg), 1)
}
