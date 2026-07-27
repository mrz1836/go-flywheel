package flywheel

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// tightenPollLadder shrinks a runner's poll ladder to 1 ms / 4 ms, which
// saturates on the third consecutive failure.
//
// It exists so a test that asserts on a *persistent* poll failure runs in
// milliseconds. RunUntilIdle's give-up rule is ladder saturation, and at the
// shipped defaults — 100 ms doubling to 30 s — that is ten attempts spread over
// roughly 51 seconds.
func tightenPollLadder(r *Runner) {
	r.cfg.PollInterval = time.Millisecond
	r.cfg.MaxPollBackoff = 4 * time.Millisecond
}

// recordingHandler captures the structured attributes of every log record, so a
// test can assert on the log's cadence and content rather than on its absence.
type recordingHandler struct {
	mu   sync.Mutex
	logs []map[string]any
}

func (*recordingHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h *recordingHandler) Handle(_ context.Context, rec slog.Record) error {
	entry := map[string]any{"msg": rec.Message, "level": rec.Level.String()}
	rec.Attrs(func(a slog.Attr) bool {
		entry[a.Key] = a.Value.Any()
		return true
	})

	h.mu.Lock()
	defer h.mu.Unlock()
	h.logs = append(h.logs, entry)
	return nil
}

func (h *recordingHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *recordingHandler) WithGroup(string) slog.Handler      { return h }

// records returns a copy of what has been logged so far.
func (h *recordingHandler) records() []map[string]any {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]map[string]any(nil), h.logs...)
}

// --- FR-04-07 / FR-04-08: the ladder itself ---------------------------------

// TestPollBackoffGrowsExponentiallyAndCapsAtMaxPollBackoff pins the ladder's
// shape: each rung doubles, jitter stays inside ±25%, and saturation is reported
// on the first rung that reaches the ceiling — the fact RunUntilIdle's give-up
// rule is built on.
func TestPollBackoffGrowsExponentiallyAndCapsAtMaxPollBackoff(t *testing.T) {
	t.Parallel()
	const base = 100 * time.Millisecond
	const ceiling = 800 * time.Millisecond

	var b pollBackoff
	for i, want := range []time.Duration{base, 2 * base, 4 * base, ceiling, ceiling, ceiling} {
		delay, saturated := b.next(base, ceiling)

		low := time.Duration(float64(want) * (1 - backoffJitterSpread/2))
		high := time.Duration(float64(want) * (1 + backoffJitterSpread/2))
		assert.GreaterOrEqualf(t, delay, low, "failure %d: %s is below the jitter floor for %s", i+1, delay, want)
		assert.LessOrEqualf(t, delay, high, "failure %d: %s is above the jitter ceiling for %s", i+1, delay, want)

		// 100ms doubles to 800ms on the fourth rung: ceil(log2(8)) + 1 == 4.
		assert.Equalf(t, i+1 >= 4, saturated, "failure %d reported the wrong saturation", i+1)
	}
}

// TestPollBackoffResetsAfterASuccessfulPoll proves a single success returns the
// ladder to its first rung, rather than decaying it.
func TestPollBackoffResetsAfterASuccessfulPoll(t *testing.T) {
	t.Parallel()
	const base = 100 * time.Millisecond
	const ceiling = 800 * time.Millisecond

	var b pollBackoff
	for range 4 {
		_, _ = b.next(base, ceiling)
	}
	require.Equal(t, 4, b.failures, "the ladder climbed")

	b.reset()
	delay, saturated := b.next(base, ceiling)
	assert.False(t, saturated, "the ladder is back at the bottom")
	assert.LessOrEqual(t, delay, time.Duration(float64(base)*(1+backoffJitterSpread/2)),
		"the next delay is one base interval again, not the ceiling")
}

// TestNewRunnerDefaultsAndFloorsMaxPollBackoff covers both halves of the
// resolution: the zero default, and the floor that keeps the ladder from
// saturating on its first rung.
func TestNewRunnerDefaultsAndFloorsMaxPollBackoff(t *testing.T) {
	t.Parallel()
	tests := map[string]struct {
		poll, maxPoll, want time.Duration
	}{
		"zero takes the default":          {0, 0, defaultMaxPollBackoff},
		"an explicit ceiling is kept":     {0, 5 * time.Second, 5 * time.Second},
		"a ceiling below the floor rises": {10 * time.Second, time.Second, 20 * time.Second},
		"a ceiling equal to the interval": {100 * time.Millisecond, 100 * time.Millisecond, 200 * time.Millisecond},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			db := newDB(t)
			r, err := NewRunner(RunnerConfig{
				DB: db, Driver: NewSQLiteDriver(db), Registry: NewRegistry(),
				Queues: []string{"default"}, PollInterval: tc.poll, MaxPollBackoff: tc.maxPoll,
			})
			require.NoError(t, err)
			assert.Equal(t, tc.want, r.cfg.MaxPollBackoff)
			assert.GreaterOrEqual(t, r.cfg.MaxPollBackoff, 2*r.cfg.PollInterval,
				"the ceiling always leaves a rung to climb")
		})
	}
}

// --- FR-04-09: bounded logging during an outage -----------------------------

// TestPollErrorLoggingFollowsTheBackoffCadence is A5's logging half, asserted as
// a number rather than narrated.
//
// The separation is what makes it meaningful: at a 1 ms poll interval a 500 ms
// outage would produce roughly 500 attempts and 500 error lines. Under the ladder
// the same window costs a low double-digit count, and every attempt carries its
// own consecutive-failure ordinal.
func TestPollErrorLoggingFollowsTheBackoffCadence(t *testing.T) {
	t.Parallel()
	rec := &recordingHandler{}
	fd := &fakeDriver{dequeueErr: errors.New("database is down")}
	r := newFakeRunner(t, fd, 1)
	r.cfg.PollInterval = time.Millisecond
	r.cfg.MaxPollBackoff = 100 * time.Millisecond
	r.cfg.Logger = slog.New(rec)

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, r.Run(ctx), context.DeadlineExceeded, "Run never gives up; only its context ends it")

	attempts := fd.dequeueCalls()
	logs := rec.records()

	require.Positive(t, attempts, "the loop did poll")
	assert.Less(t, attempts, 40,
		"attempts are ladder-spaced: ~500 would mean the loop polled at PollInterval throughout")
	assert.Len(t, logs, attempts, "one log line per failed attempt, not one per poll interval")

	for i, entry := range logs {
		assert.Equal(t, "jobs: poll failed", entry["msg"])
		assert.EqualValues(t, i+1, entry["consecutive_failures"],
			"each line names its place on the ladder")
		assert.NotEmpty(t, entry["backoff"], "each line names the delay it is about to take")
	}
}

// TestRunKeepsPollingThroughAPersistentClaimFailure is the rest of A5: Run does
// not give up, the attempts it makes are bounded, and the first success after
// recovery puts it straight back on the normal interval.
func TestRunKeepsPollingThroughAPersistentClaimFailure(t *testing.T) {
	t.Parallel()

	t.Run("a permanent failure is retried forever, on the ladder", func(t *testing.T) {
		t.Parallel()
		fd := &fakeDriver{dequeueErr: errors.New("connection refused")}
		r := newFakeRunner(t, fd, 1)
		r.cfg.PollInterval = 2 * time.Millisecond
		r.cfg.MaxPollBackoff = 64 * time.Millisecond
		r.cfg.Logger = slog.New(&recordingHandler{})

		ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
		defer cancel()
		require.ErrorIs(t, r.Run(ctx), context.DeadlineExceeded)

		attempts := fd.dequeueCalls()
		assert.Greater(t, attempts, 2, "it kept trying rather than bailing out")
		assert.Less(t, attempts, 30,
			"the ladder bounded the attempts; at PollInterval throughout there would be ~200")
	})

	t.Run("recovery restores the normal interval immediately", func(t *testing.T) {
		t.Parallel()
		// Three failures put the ladder at its fourth rung; the fourth claim
		// succeeds and must reset it, after which the job is claimed and run.
		fd := &fakeDriver{
			dequeueErr:      errors.New("failing over"),
			dequeueFailures: 3,
			batch:           []RawJob{{ID: "a", Kind: "test.success", Args: []byte(`{}`)}},
		}
		r := newFakeRunner(t, fd, 1)
		tightenPollLadder(r)
		r.cfg.MaxPollBackoff = 64 * time.Millisecond // a ceiling the 3 failures cannot reach
		r.cfg.Logger = slog.New(&recordingHandler{})

		require.NoError(t, r.RunUntilIdle(context.Background()),
			"the drain completed across the outage")
		assert.Equal(t, 1, fd.finalized, "the job ran once the database came back")
	})
}

// --- FR-04-10: RunUntilIdle's tolerance and its bound -----------------------

// TestRunUntilIdleRetriesATransientPollFailureThenCompletes is A6's first half:
// a blip inside the ladder's budget is absorbed rather than returned.
func TestRunUntilIdleRetriesATransientPollFailureThenCompletes(t *testing.T) {
	t.Parallel()
	fd := &fakeDriver{
		dequeueErr:      errors.New("connection reset"),
		dequeueFailures: 2,
		batch:           []RawJob{{ID: "a", Kind: "test.success", Args: []byte(`{}`)}},
	}
	r := newFakeRunner(t, fd, 1)
	r.cfg.PollInterval = time.Millisecond
	r.cfg.MaxPollBackoff = 32 * time.Millisecond // saturates on failure 6, well past 2
	r.cfg.Logger = slog.New(&recordingHandler{})

	require.NoError(t, r.RunUntilIdle(context.Background()),
		"two transient poll failures do not abort an invocation with budget left")
	assert.Equal(t, 1, fd.finalized, "the queue was actually drained")
}

// TestRunUntilIdleGivesUpWhenThePollBackoffLadderSaturates is the other half,
// and it asserts the exact attempt count rather than "eventually".
//
// The bound is the ladder, not the context: at 1 ms doubling to 4 ms the ladder
// saturates on the third failure — ceil(log2(4)) + 1 — so a caller that passed no
// deadline at all still gets an answer.
func TestRunUntilIdleGivesUpWhenThePollBackoffLadderSaturates(t *testing.T) {
	t.Parallel()
	boom := errors.New("database is gone")
	fd := &fakeDriver{dequeueErr: boom}
	r := newFakeRunner(t, fd, 1)
	tightenPollLadder(r)
	r.cfg.Logger = slog.New(&recordingHandler{})

	err := r.RunUntilIdle(context.Background())
	require.ErrorIs(t, err, boom, "the give-up wraps the failure that caused it")
	assert.ErrorContains(t, err, "poll failed 3 consecutive times")
	assert.Equal(t, 3, fd.dequeueCalls(),
		"exactly ceil(log2(MaxPollBackoff/PollInterval)) + 1 attempts were made")
}

// TestRunUntilIdleReturnsPromptlyOnContextCancelDuringBackoff is A6's liveness
// half: a cancel landing mid-backoff is not held until the rung expires.
func TestRunUntilIdleReturnsPromptlyOnContextCancelDuringBackoff(t *testing.T) {
	t.Parallel()
	fd := &fakeDriver{dequeueErr: errors.New("database is gone")}
	r := newFakeRunner(t, fd, 1)
	// A long first rung and a far ceiling: without honoring the cancel this would
	// sit for two full seconds before even reaching its give-up rule.
	r.cfg.PollInterval = 2 * time.Second
	r.cfg.MaxPollBackoff = time.Minute
	r.cfg.Logger = slog.New(&recordingHandler{})

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()

	start := time.Now()
	err := r.RunUntilIdle(ctx)
	elapsed := time.Since(start)

	require.ErrorIs(t, err, context.Canceled)
	assert.Less(t, elapsed, time.Second, "the cancel cut the backoff short rather than waiting it out")
}
