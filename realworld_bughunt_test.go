package flywheel

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// realworld_bughunt_test.go holds targeted defect probes. Each probe is written
// to FAIL if the hypothesized bug exists; once a bug is confirmed and fixed in
// source, the probe stays on as its regression test.
//
// Confirmed and fixed:
//   - B1: truncate split multi-byte runes (driver.go) — now rune-aware.
//   - B2: Scheduler.Tick starved later definitions after one fire error
//     (scheduler.go) — now isolates per definition and aggregates with
//     errors.Join.
//
// Probed and found safe-by-design:
//   - B3: UpsertPeriodic already validates cron syntax up front.
//   - B4: a snooze never advances a job toward discarded (max_attempts is raised).
//   - B5: expBackoff is monotonic, capped, and never negative/overflowed.

// --- B1: UTF-8-splitting error truncation -----------------------------------

type b1Args struct{ V string }

func (b1Args) Kind() string { return "rw.bughunt.b1" }

// b1Worker fails permanently with a fixed, oversized error message so the
// finalize path runs the message through truncate before storing it.
type b1Worker struct{ msg string }

func (*b1Worker) Kind() string              { return "rw.bughunt.b1" }
func (*b1Worker) Classify(error) ErrorClass { return ErrorPermanent }
func (w *b1Worker) Work(_ context.Context, _ *Job[b1Args]) (Result, error) {
	return Result{}, errors.New(w.msg)
}

// TestBugHuntB1TruncateIsRuneAware drives a worker whose error message exceeds
// maxErrorMessage with a multi-byte rune straddling the byte boundary. The
// stored message must remain valid UTF-8 and its error_payload JSON consistent.
//
// Before the fix, truncate did a raw byte slice (s[:n]) that split the rune,
// producing invalid UTF-8 in the audit trail (and, on a real Postgres deploy,
// a failed text insert that wedges the job).
func TestBugHuntB1TruncateIsRuneAware(t *testing.T) {
	t.Parallel()
	db := newDB(t)

	// 4095 ASCII bytes + a 3-byte rune places byte index 4096 in the MIDDLE of
	// the rune, so a byte-slice cut at maxErrorMessage splits it.
	msg := strings.Repeat("a", maxErrorMessage-1) + "世" + strings.Repeat("b", 16)
	require.Greater(t, len(msg), maxErrorMessage)

	reg := NewRegistry()
	Register(reg, &b1Worker{msg: msg})
	r := newRunner(t, db, reg)

	id, err := Insert(context.Background(), NewClient(db), b1Args{V: "x"}, InsertOpts{MaxAttempts: 1})
	require.NoError(t, err)

	runToIdle(t, context.Background(), r)
	require.Equal(t, string(StateDiscarded), jobState(t, db, id), "a permanent error discards immediately")

	runs, err := ListRuns(context.Background(), db, id, ListRunsParams{})
	require.NoError(t, err)
	require.Len(t, runs, 1)
	require.NotNil(t, runs[0].Error, "the failed attempt records its error message")

	stored := *runs[0].Error
	assert.True(t, utf8.ValidString(stored), "the stored error message must remain valid UTF-8 after truncation")
	assert.LessOrEqual(t, len(stored), maxErrorMessage, "the stored message respects the byte cap")
	assert.True(t, strings.HasPrefix(msg, stored), "truncation only drops a suffix, never rewrites bytes")

	// The error_payload JSON must round-trip and carry the identical message.
	var rawRun jobRunRow
	require.NoError(t, db.Table("job_runs").Where("job_id = ?", id).First(&rawRun).Error)
	require.NotEmpty(t, rawRun.ErrorPayload, "error_payload is written alongside error_message")
	var payload map[string]string
	require.NoError(t, json.Unmarshal(rawRun.ErrorPayload, &payload), "error_payload must be valid JSON")
	assert.Equal(t, stored, payload["message"], "error_payload.message matches the stored error_message")
	assert.True(t, utf8.ValidString(payload["message"]), "the payload message is valid UTF-8")
}

// TestBugHuntB1TruncateUnitRuneBoundary exercises truncate directly across rune
// boundaries so the helper's contract is locked in independent of the driver.
func TestBugHuntB1TruncateUnitRuneBoundary(t *testing.T) {
	t.Parallel()

	// "héllo": 'é' is two bytes (0xC3 0xA9), so byte indices 2 and 3 fall inside it.
	const s = "héllo"
	for n := 0; n <= len(s); n++ {
		got := truncate(s, n)
		assert.LessOrEqualf(t, len(got), n, "truncate(%q, %d) must not exceed the byte cap", s, n)
		assert.Truef(t, utf8.ValidString(got), "truncate(%q, %d) = %q must be valid UTF-8", s, n, got)
		assert.Truef(t, strings.HasPrefix(s, got), "truncate(%q, %d) = %q must be a prefix of the input", s, n, got)
	}

	assert.Equal(t, s, truncate(s, len(s)), "a string at the cap is returned unchanged")
	assert.Equal(t, "h", truncate(s, 2), "a cut inside 'é' backs off to the previous rune boundary")
}

// --- B2: periodic tick starvation -------------------------------------------

// TestBugHuntB2TickIsolatesBadDefinition installs one definition that errors on
// fire (an invalid cron, written past validation via raw SQL) ordered BEFORE two
// healthy interval definitions. A single Tick must still enqueue the healthy
// definitions and report the bad one's error.
//
// Before the fix, Tick returned on the first fire error, so every definition
// scanned after a broken one was starved on every tick — one bad schedule wedged
// the whole periodic system.
func TestBugHuntB2TickIsolatesBadDefinition(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	sched := NewScheduler(db, NewClient(db))

	now := time.Now().UTC().Truncate(time.Second)
	ctx := clockCtx(context.Background(), NewFixedClock(now))
	due := now.Add(-time.Minute)

	// Insertion order is the scan order (rowid). The broken definition is first,
	// so a fail-fast Tick would never reach the healthy ones behind it.
	installPeriodicCron(t, db, "bad-cron", "rw.b2.bad", "not a valid cron", due)
	installPeriodic(t, db, "healthy-a", "rw.b2.a", due, true)
	installPeriodic(t, db, "healthy-b", "rw.b2.b", due, true)

	enqueued, err := sched.Tick(ctx)
	require.Error(t, err, "the invalid cron definition still surfaces its error")

	assert.Positive(t, jobCount(t, db, "rw.b2.a"), "a healthy definition must fire despite an earlier broken one")
	assert.Positive(t, jobCount(t, db, "rw.b2.b"), "every healthy definition must fire, not just the first")
	assert.Positive(t, enqueued, "the tick reports the jobs the healthy definitions enqueued")
	assert.Zero(t, jobCount(t, db, "rw.b2.bad"), "the broken definition enqueues nothing")
}

// --- B3: invalid cron rejected at write time (safe-by-design) ----------------

// TestBugHuntB3UpsertRejectsInvalidCron confirms UpsertPeriodic validates cron
// syntax at write time, turning a latent fire-time failure into an enqueue-time
// error. This hypothesis was found safe-by-design — PeriodicSpec.validate already
// parses the cron — so this stays as a guard, not a fix.
func TestBugHuntB3UpsertRejectsInvalidCron(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	ctx := context.Background()

	err := UpsertPeriodic(ctx, db, PeriodicSpec{
		Slug:   "b3-bad",
		Kind:   "rw.b3",
		Cron:   "not a valid cron",
		Active: true,
	})
	require.Error(t, err, "an invalid cron must be rejected at upsert, not persisted")

	views, listErr := ListPeriodics(ctx, db)
	require.NoError(t, listErr)
	assert.Empty(t, views, "no periodic row is persisted when validation fails")

	// A well-formed cron on the same slug is accepted.
	require.NoError(t, UpsertPeriodic(ctx, db, PeriodicSpec{
		Slug:   "b3-good",
		Kind:   "rw.b3",
		Cron:   "*/5 * * * *",
		Active: true,
	}))
}

// --- B4: snooze is truly free ------------------------------------------------

type b4Args struct{ V string }

func (b4Args) Kind() string { return "rw.bughunt.b4" }

// b4Worker snoozes snoozes-many times, then succeeds.
type b4Worker struct {
	snoozes int32
	calls   atomic.Int32
}

func (*b4Worker) Kind() string { return "rw.bughunt.b4" }
func (w *b4Worker) Work(_ context.Context, _ *Job[b4Args]) (Result, error) {
	if w.calls.Add(1) <= w.snoozes {
		d := time.Millisecond
		return Result{Snooze: &d}, nil
	}
	return Result{}, nil
}

// TestBugHuntB4SnoozeNeverDiscards snoozes a job far more times than its
// MaxAttempts and asserts it never lands in discarded and ultimately succeeds —
// a snooze raises max_attempts rather than consuming an attempt, so retry
// headroom is preserved exactly. Safe-by-design; this guards the guarantee.
func TestBugHuntB4SnoozeNeverDiscards(t *testing.T) {
	t.Parallel()
	db := newDB(t)

	const snoozes = 50
	const startMaxAttempts = 3

	w := &b4Worker{snoozes: snoozes}
	reg := NewRegistry()
	Register(reg, w)
	r := rwRunner(t, db, reg, func(c *RunnerConfig) { c.RetryBackoffBase = time.Millisecond })

	id, err := Insert(context.Background(), NewClient(db), b4Args{V: "x"},
		InsertOpts{MaxAttempts: startMaxAttempts})
	require.NoError(t, err)

	runToIdle(t, context.Background(), r)

	assert.Equal(t, string(StateSucceeded), jobState(t, db, id), "50 snoozes then success — never discarded")
	assert.EqualValues(t, snoozes+1, w.calls.Load(), "every snooze ran plus the final success")

	var maxAttempts int
	require.NoError(t, db.Table("jobs").Select("max_attempts").Where("id = ?", id).Scan(&maxAttempts).Error)
	assert.Equal(t, startMaxAttempts+snoozes, maxAttempts, "each snooze raised max_attempts by one")

	// Every snooze attempt recorded a snooze outcome; the final one a success.
	var snoozeRuns int64
	require.NoError(t, db.Table("job_runs").
		Where("job_id = ? AND outcome = ?", id, string(OutcomeSnooze)).Count(&snoozeRuns).Error)
	assert.EqualValues(t, snoozes, snoozeRuns, "one snooze outcome per snooze")
}

// --- B5: backoff edges -------------------------------------------------------

// TestBugHuntB5ExpBackoffEdges tables expBackoff over attempt edge cases and
// asserts it is monotonic, capped at the max, and never negative or overflowed.
// Safe-by-design; this guards the ladder's contract.
func TestBugHuntB5ExpBackoffEdges(t *testing.T) {
	t.Parallel()

	const base = time.Second
	const maxDelay = time.Minute

	t.Run("table", func(t *testing.T) {
		t.Parallel()
		cases := []struct {
			attempt int
			want    time.Duration
		}{
			{attempt: -5, want: base}, // attempt < 1 normalizes to 1
			{attempt: 0, want: base},
			{attempt: 1, want: base},
			{attempt: 2, want: 2 * base},
			{attempt: 3, want: 4 * base},
			{attempt: 7, want: maxDelay},         // 64s would exceed 60s — capped
			{attempt: 1_000_000, want: maxDelay}, // large attempt returns the cap, no overflow
		}
		for _, tc := range cases {
			assert.Equalf(t, tc.want, expBackoff(base, maxDelay, tc.attempt),
				"expBackoff(base, max, %d)", tc.attempt)
		}
	})

	t.Run("monotonic-and-bounded", func(t *testing.T) {
		t.Parallel()
		var prev time.Duration
		for attempt := 0; attempt <= 64; attempt++ {
			d := expBackoff(base, maxDelay, attempt)
			assert.Positivef(t, d, "delay is never zero or negative (attempt %d)", attempt)
			assert.LessOrEqualf(t, d, maxDelay, "delay is capped at max (attempt %d)", attempt)
			assert.GreaterOrEqualf(t, d, prev, "delay is monotonic non-decreasing (attempt %d)", attempt)
			prev = d
		}
	})
}

// --- B6: large args/output round-trip & deep DAG -----------------------------

type b6Args struct {
	Blob  string `json:"blob"`
	Items []int  `json:"items"`
}

func (b6Args) Kind() string { return "rw.bughunt.b6" }

type b6Output struct {
	Echo  string `json:"echo"`
	Count int    `json:"count"`
}

// b6Worker records the args it received and returns a large structured output.
type b6Worker struct {
	gotBlobLen  int
	gotItemsLen int
}

func (*b6Worker) Kind() string { return "rw.bughunt.b6" }
func (w *b6Worker) Work(_ context.Context, job *Job[b6Args]) (Result, error) {
	w.gotBlobLen = len(job.Args.Blob)
	w.gotItemsLen = len(job.Args.Items)
	return Result{Output: b6Output{Echo: job.Args.Blob, Count: len(job.Args.Items)}}, nil
}

// TestBugHuntB6LargeArgsAndOutputRoundTrip enqueues a job with a large JSON args
// blob and a worker returning a large structured output, then asserts both
// round-trip faithfully through the store.
func TestBugHuntB6LargeArgsAndOutputRoundTrip(t *testing.T) {
	t.Parallel()
	db := newDB(t)

	const blobLen = 100_000
	const itemCount = 2_000
	items := make([]int, itemCount)
	for i := range items {
		items[i] = i
	}
	args := b6Args{Blob: strings.Repeat("x", blobLen), Items: items}

	w := &b6Worker{}
	reg := NewRegistry()
	Register(reg, w)
	r := newRunner(t, db, reg)

	id, err := Insert(context.Background(), NewClient(db), args, InsertOpts{})
	require.NoError(t, err)

	runToIdle(t, context.Background(), r)
	require.Equal(t, string(StateSucceeded), jobState(t, db, id))

	assert.Equal(t, blobLen, w.gotBlobLen, "the worker received the full args blob")
	assert.Equal(t, itemCount, w.gotItemsLen, "the worker received every args item")

	runs, err := ListRuns(context.Background(), db, id, ListRunsParams{})
	require.NoError(t, err)
	require.Len(t, runs, 1)
	require.NotEmpty(t, runs[0].Output, "the structured output is stored")

	var out b6Output
	require.NoError(t, json.Unmarshal(runs[0].Output, &out), "stored output is valid JSON")
	assert.Equal(t, itemCount, out.Count, "output count survives the round-trip")
	assert.Len(t, out.Echo, blobLen, "the large echoed blob survives the round-trip")
}

type chainArgs struct {
	Depth int `json:"depth"`
}

func (chainArgs) Kind() string { return "rw.bughunt.chain" }

// chainWorker recurses one follow-up of itself per level until Depth reaches 0.
type chainWorker struct{ ran atomic.Int32 }

func (*chainWorker) Kind() string { return "rw.bughunt.chain" }
func (w *chainWorker) Work(_ context.Context, job *Job[chainArgs]) (Result, error) {
	w.ran.Add(1)
	if job.Args.Depth > 0 {
		return Result{FollowUps: []FollowUp{{
			Kind:   "rw.bughunt.chain",
			Args:   chainArgs{Depth: job.Args.Depth - 1},
			Parent: true,
		}}}, nil
	}
	return Result{}, nil
}

// TestBugHuntB6DeepFollowUpChain drives a long A→B→C→… follow-up chain to confirm
// no depth-related breakage: every level runs once and reaches succeeded.
func TestBugHuntB6DeepFollowUpChain(t *testing.T) {
	t.Parallel()
	db := newDB(t)

	const depth = 40

	w := &chainWorker{}
	reg := NewRegistry()
	Register(reg, w)
	r := newRunner(t, db, reg)

	_, err := Insert(context.Background(), NewClient(db), chainArgs{Depth: depth}, InsertOpts{})
	require.NoError(t, err)

	runToIdle(t, context.Background(), r)

	assert.EqualValues(t, depth+1, w.ran.Load(), "every level of the chain ran exactly once")

	overview, err := Overview(context.Background(), db, OverviewParams{Kind: "rw.bughunt.chain"})
	require.NoError(t, err)
	assert.Equal(t, depth+1, overview.CountsByState[string(StateSucceeded)], "every chained job succeeded")
	assert.Equal(t, depth+1, overview.Total, "the chain produced exactly depth+1 jobs")
}
