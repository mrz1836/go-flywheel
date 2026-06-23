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

func TestSlogObserverLogsEveryEventAtDebug(t *testing.T) {
	t.Parallel()
	h := &capturingHandler{level: slog.LevelDebug}
	obs := NewSlog(slog.New(h))
	ctx := context.Background()

	obs.OnClaim(ctx, flywheel.ClaimEvent{ExecutorClass: "local", Queues: []string{"default"}, Claimed: 3})
	obs.OnStart(ctx, flywheel.JobEvent{JobID: "j1", RunID: "r1", Kind: "k", Queue: "q", Attempt: 1})
	obs.OnFinish(ctx, flywheel.FinishEvent{
		JobEvent:   flywheel.JobEvent{JobID: "j1", Kind: "k", Queue: "q", Attempt: 1},
		Outcome:    flywheel.OutcomeError,
		ErrorClass: flywheel.ErrorTransient,
		Err:        errors.New("boom"),
		Duration:   2 * time.Second,
	})
	obs.OnRetry(ctx, flywheel.RetryEvent{
		JobEvent: flywheel.JobEvent{JobID: "j1", Kind: "k"}, NextAttempt: 2, Delay: time.Second, ErrorClass: flywheel.ErrorTransient,
	})

	records := h.snapshot()
	require.Len(t, records, 4, "every lifecycle event is logged")
	for _, r := range records {
		assert.Equal(t, slog.LevelDebug, r.Level, "events log at debug so info-level serve stays quiet")
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
	assert.Equal(t, "error", finish["outcome"].String())
	assert.Equal(t, "transient", finish["error_class"].String())
	assert.Equal(t, "boom", finish["error"].String(), "the worker error message is attached on failure")

	retry := attrsOf(records[3])
	assert.Equal(t, "flywheel: job retry scheduled", records[3].Message)
	assert.EqualValues(t, 2, retry["next_attempt"].Int64())
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
