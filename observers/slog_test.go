package observers

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	flywheel "github.com/mrz1836/go-flywheel"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// capturingHandler is a slog.Handler test double that records every emitted
// record (above its enabled level), so a test can assert the message, level, and
// attributes a SlogObserver logs without parsing formatted text.
type capturingHandler struct {
	mu      sync.Mutex
	level   slog.Level
	records []slog.Record
}

func (h *capturingHandler) Enabled(_ context.Context, l slog.Level) bool { return l >= h.level }

func (h *capturingHandler) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.records = append(h.records, r.Clone())
	return nil
}

func (h *capturingHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *capturingHandler) WithGroup(string) slog.Handler      { return h }

func (h *capturingHandler) snapshot() []slog.Record {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]slog.Record(nil), h.records...)
}

// attrsOf collects a record's attributes into a key->value map for assertion.
func attrsOf(r slog.Record) map[string]slog.Value {
	out := map[string]slog.Value{}
	r.Attrs(func(a slog.Attr) bool {
		out[a.Key] = a.Value
		return true
	})
	return out
}

func TestSlogObserverLogsRoutineEventsAtDebug(t *testing.T) {
	t.Parallel()
	h := &capturingHandler{level: slog.LevelDebug}
	obs := NewSlog(slog.New(h))
	ctx := context.Background()

	obs.OnClaim(ctx, flywheel.ClaimEvent{ExecutorClass: "local", Queues: []string{"default"}, Claimed: 3})
	obs.OnStart(ctx, flywheel.JobEvent{JobID: "j1", RunID: "r1", Kind: "k", Queue: "q", Attempt: 1})
	obs.OnFinish(ctx, flywheel.FinishEvent{
		JobEvent:    flywheel.JobEvent{JobID: "j1", RunID: "r1", Kind: "k", Queue: "q", Attempt: 1},
		Outcome:     flywheel.OutcomeSuccess,
		State:       flywheel.StateSucceeded,
		MaxAttempts: 5,
		Duration:    2 * time.Second,
	})
	obs.OnRetry(ctx, flywheel.RetryEvent{
		JobEvent: flywheel.JobEvent{JobID: "j1", Kind: "k"}, NextAttempt: 2, Delay: time.Second, ErrorClass: flywheel.ErrorTransient,
	})

	records := h.snapshot()
	require.Len(t, records, 4, "every lifecycle event is logged")
	for _, r := range records {
		assert.Equal(t, slog.LevelDebug, r.Level, "routine events log at debug so info-level serve stays quiet")
	}

	claim := attrsOf(records[0])
	assert.Equal(t, "flywheel: jobs claimed", records[0].Message)
	assert.Equal(t, "local", claim["executor_class"].String())
	assert.EqualValues(t, 3, claim["claimed"].Int64())

	start := attrsOf(records[1])
	assert.Equal(t, "flywheel: job started", records[1].Message)
	assert.Equal(t, "j1", start["job_id"].String())
	assert.Equal(t, "k", start["kind"].String())

	finish := attrsOf(records[2])
	assert.Equal(t, "flywheel: job finished", records[2].Message)
	assert.Equal(t, "success", finish["outcome"].String())
	assert.Equal(t, "r1", finish["run_id"].String())
	assert.Equal(t, "succeeded", finish["job_state"].String())
	assert.EqualValues(t, 5, finish["max_attempts"].Int64())

	retry := attrsOf(records[3])
	assert.Equal(t, "flywheel: job retry scheduled", records[3].Message)
	assert.EqualValues(t, 2, retry["next_attempt"].Int64())
}

// TestSlogObserverFailedAttemptLogsAtWarn proves a failed attempt the job will
// retry survives an info-level handler at warn, with everything an operator
// needs to act on it: the error, its class, the attempt against the budget,
// and the job's resulting state. A timeout and a crash are failures too.
func TestSlogObserverFailedAttemptLogsAtWarn(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		outcome flywheel.RunOutcome
		class   flywheel.ErrorClass
	}{
		{flywheel.OutcomeError, flywheel.ErrorTransient},
		{flywheel.OutcomeTimeout, flywheel.ErrorTimeout},
		{flywheel.OutcomeCrashed, flywheel.ErrorTransient},
	} {
		t.Run(string(tc.outcome), func(t *testing.T) {
			t.Parallel()
			h := &capturingHandler{level: slog.LevelInfo}
			NewSlog(slog.New(h)).OnFinish(context.Background(), flywheel.FinishEvent{
				JobEvent:    flywheel.JobEvent{JobID: "j1", RunID: "r2", Kind: "k", Queue: "q", Attempt: 2},
				Outcome:     tc.outcome,
				ErrorClass:  tc.class,
				Err:         errors.New("upstream refused the request"),
				State:       flywheel.StateRetryable,
				MaxAttempts: 4,
				Duration:    time.Second,
			})

			records := h.snapshot()
			require.Len(t, records, 1, "a failed attempt survives an info-level handler")
			assert.Equal(t, slog.LevelWarn, records[0].Level)
			assert.Equal(t, "flywheel: job attempt failed", records[0].Message)

			attrs := attrsOf(records[0])
			assert.Equal(t, "j1", attrs["job_id"].String())
			assert.Equal(t, "r2", attrs["run_id"].String())
			assert.Equal(t, "k", attrs["kind"].String())
			assert.Equal(t, "q", attrs["queue"].String())
			assert.Equal(t, string(tc.outcome), attrs["outcome"].String())
			assert.EqualValues(t, 2, attrs["attempt"].Int64())
			assert.EqualValues(t, 4, attrs["max_attempts"].Int64())
			assert.Equal(t, "retryable", attrs["job_state"].String())
			assert.Equal(t, string(tc.class), attrs["error_class"].String())
			assert.Equal(t, "upstream refused the request", attrs["error"].String(),
				"the worker error text is on the line, so nobody has to dig it out of job_runs")
		})
	}
}

// TestSlogObserverDiscardedJobLogsAtError proves a failure that ended the job —
// its budget spent, or an error that is never retried — logs at error, since
// the job's work is lost until it is replayed.
func TestSlogObserverDiscardedJobLogsAtError(t *testing.T) {
	t.Parallel()

	for name, ev := range map[string]flywheel.FinishEvent{
		"budget spent": {
			JobEvent: flywheel.JobEvent{JobID: "j1", RunID: "r4", Kind: "k", Queue: "q", Attempt: 4},
			Outcome:  flywheel.OutcomeError, ErrorClass: flywheel.ErrorTransient,
			Err: errors.New("still refused"), State: flywheel.StateDiscarded, MaxAttempts: 4,
		},
		"permanent error": {
			JobEvent: flywheel.JobEvent{JobID: "j2", RunID: "r1", Kind: "k", Queue: "q", Attempt: 1},
			Outcome:  flywheel.OutcomeError, ErrorClass: flywheel.ErrorPermanent,
			Err: errors.New("bad input"), State: flywheel.StateDiscarded, MaxAttempts: 4,
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			h := &capturingHandler{level: slog.LevelWarn}
			NewSlog(slog.New(h)).OnFinish(context.Background(), ev)

			records := h.snapshot()
			require.Len(t, records, 1, "a discard survives a warn-level handler")
			assert.Equal(t, slog.LevelError, records[0].Level)
			assert.Equal(t, "flywheel: job failed and was discarded", records[0].Message)

			attrs := attrsOf(records[0])
			assert.Equal(t, ev.JobID, attrs["job_id"].String())
			assert.Equal(t, "discarded", attrs["job_state"].String())
			assert.Equal(t, string(ev.ErrorClass), attrs["error_class"].String())
			assert.Equal(t, ev.Err.Error(), attrs["error"].String())
			assert.EqualValues(t, ev.Attempt, attrs["attempt"].Int64())
			assert.EqualValues(t, 4, attrs["max_attempts"].Int64())
		})
	}
}

// TestSlogObserverRoutineFinishesStayQuietAtInfo: a success, a snooze — even
// one carrying an error, such as a job of a kind this runner does not register,
// deferred for one that does — and a cancel are routine, so an info-level
// handler sees none of them.
func TestSlogObserverRoutineFinishesStayQuietAtInfo(t *testing.T) {
	t.Parallel()
	h := &capturingHandler{level: slog.LevelInfo}
	obs := NewSlog(slog.New(h))
	ctx := context.Background()
	job := flywheel.JobEvent{JobID: "j1", RunID: "r1", Kind: "k", Queue: "q", Attempt: 1}

	obs.OnFinish(ctx, flywheel.FinishEvent{JobEvent: job, Outcome: flywheel.OutcomeSuccess, State: flywheel.StateSucceeded})
	obs.OnFinish(ctx, flywheel.FinishEvent{
		JobEvent: job, Outcome: flywheel.OutcomeSnooze, State: flywheel.StateScheduled, Err: flywheel.ErrUnknownKind,
	})
	obs.OnFinish(ctx, flywheel.FinishEvent{JobEvent: job, Outcome: flywheel.OutcomeCancelled, State: flywheel.StateCancelled})

	assert.Empty(t, h.snapshot(), "routine finishes log at debug")
}

// TestSlogObserverFailedFinishWithoutStateLogsAtWarn covers an event built by
// hand, with no resulting state or budget: a failure still logs at warn, and
// the line carries no job_state or max_attempts it does not know.
func TestSlogObserverFailedFinishWithoutStateLogsAtWarn(t *testing.T) {
	t.Parallel()
	h := &capturingHandler{level: slog.LevelInfo}
	NewSlog(slog.New(h)).OnFinish(context.Background(), flywheel.FinishEvent{
		JobEvent: flywheel.JobEvent{JobID: "j1", Kind: "k", Queue: "q", Attempt: 1},
		Outcome:  flywheel.OutcomeError, ErrorClass: flywheel.ErrorTransient, Err: errors.New("boom"),
	})

	records := h.snapshot()
	require.Len(t, records, 1)
	assert.Equal(t, slog.LevelWarn, records[0].Level)
	assert.Equal(t, "flywheel: job attempt failed", records[0].Message)
	attrs := attrsOf(records[0])
	_, hasState := attrs["job_state"]
	_, hasBudget := attrs["max_attempts"]
	assert.False(t, hasState, "no state is claimed when the event carries none")
	assert.False(t, hasBudget, "no budget is claimed when the event carries none")
	assert.Equal(t, "boom", attrs["error"].String())
}

func TestSlogObserverFinishSuccessOmitsErrorAttrs(t *testing.T) {
	t.Parallel()
	h := &capturingHandler{level: slog.LevelDebug}
	NewSlog(slog.New(h)).OnFinish(context.Background(), flywheel.FinishEvent{
		JobEvent: flywheel.JobEvent{JobID: "j1", Kind: "k", Queue: "q"},
		Outcome:  flywheel.OutcomeSuccess,
		Duration: time.Second,
	})

	records := h.snapshot()
	require.Len(t, records, 1)
	attrs := attrsOf(records[0])
	_, hasClass := attrs["error_class"]
	_, hasErr := attrs["error"]
	assert.False(t, hasClass, "a success carries no error_class attr")
	assert.False(t, hasErr, "a success carries no error attr")
}

func TestSlogObserverDebugSuppressedAtInfoLevel(t *testing.T) {
	t.Parallel()
	h := &capturingHandler{level: slog.LevelInfo} // info: debug events filter out
	NewSlog(slog.New(h)).OnStart(context.Background(), flywheel.JobEvent{JobID: "j1", Kind: "k"})
	assert.Empty(t, h.snapshot(), "at info level the debug lifecycle events are suppressed")
}

// TestSlogObserverSupersedeLogsAtWarn: a supersede is not routine, and the test
// states why: it means work was executed and thrown away, which an operator must
// see without having turned debug on first. The handler here is set to info
// precisely so that a debug-level supersede would produce no record and fail.
func TestSlogObserverSupersedeLogsAtWarn(t *testing.T) {
	t.Parallel()
	h := &capturingHandler{level: slog.LevelInfo}
	NewSlog(slog.New(h)).OnSupersede(context.Background(), flywheel.SupersedeEvent{
		JobEvent:   flywheel.JobEvent{JobID: "j1", RunID: "r1", Kind: "k", Queue: "q", Attempt: 2},
		Outcome:    flywheel.OutcomeSuccess,
		State:      flywheel.StateRunning,
		Duration:   1500 * time.Millisecond,
		LeaseToken: "stale-token",
	})

	records := h.snapshot()
	require.Len(t, records, 1, "a supersede survives an info-level handler")
	assert.Equal(t, slog.LevelWarn, records[0].Level)

	attrs := attrsOf(records[0])
	assert.Equal(t, "j1", attrs["job_id"].String())
	assert.Equal(t, "success", attrs["discarded_outcome"].String(),
		"the line names the outcome that was thrown away, not a placeholder")
	assert.Equal(t, "running", attrs["job_state"].String())
}

// TestSlogObserverSweepLevelTracksReclaimed proves OnSweep logs at the configured
// (debug) level when the pass reclaimed nothing — routine maintenance — and lifts
// to info when it reclaimed a lease, because a nonzero reclaim means an executor
// died mid-attempt and an operator on info-level logs should see that.
func TestSlogObserverSweepLevelTracksReclaimed(t *testing.T) {
	t.Parallel()
	h := &capturingHandler{level: slog.LevelDebug}
	obs := NewSlog(slog.New(h))
	ctx := context.Background()

	obs.OnSweep(ctx, flywheel.SweepEvent{Reclaimed: 0, Duration: 3 * time.Millisecond})
	obs.OnSweep(ctx, flywheel.SweepEvent{Reclaimed: 2, Duration: 4 * time.Millisecond})

	records := h.snapshot()
	require.Len(t, records, 2, "both sweep passes are logged")
	assert.Equal(t, "flywheel: lease sweep completed", records[0].Message)
	assert.Equal(t, slog.LevelDebug, records[0].Level, "a no-op reclaim stays at the routine debug level")
	assert.Equal(t, slog.LevelInfo, records[1].Level, "a nonzero reclaim lifts to info so an operator sees it")
	assert.EqualValues(t, 2, attrsOf(records[1])["reclaimed"].Int64())
}

func TestNewSlogNilLoggerDoesNotPanic(t *testing.T) {
	t.Parallel()
	obs := NewSlog(nil)
	require.NotNil(t, obs)
	assert.NotPanics(t, func() {
		obs.OnStart(context.Background(), flywheel.JobEvent{JobID: "j1", Kind: "k"})
	}, "a nil logger falls back to slog.Default")
}

func TestSlogObserverImplementsObserver(t *testing.T) {
	t.Parallel()
	var obs flywheel.Observer = NewSlog(slog.Default())
	assert.NotNil(t, obs)
}
