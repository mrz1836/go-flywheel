package core

import (
	"context"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/mrz1836/go-foundation/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// captureHandler is a concurrency-safe slog.Handler that records every message
// it handles, so a test can assert the scheduler heartbeat fired without parsing
// formatted log text. It is safe to read from the test goroutine while the
// scheduler's Run goroutines write.
type captureHandler struct {
	mu       sync.Mutex
	messages []string
	records  []captured
}

// captured is one handled record's message plus a snapshot of its attributes,
// which is what lets a test assert on a logged *count* rather than only on the
// fact that a message appeared. Attributes are snapshotted at handle time
// because slog.Record's attribute storage is not safe to read later.
type captured struct {
	message string
	attrs   map[string]slog.Value
}

func (h *captureHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h *captureHandler) Handle(_ context.Context, r slog.Record) error {
	attrs := make(map[string]slog.Value, r.NumAttrs())
	r.Attrs(func(a slog.Attr) bool {
		attrs[a.Key] = a.Value
		return true
	})

	h.mu.Lock()
	defer h.mu.Unlock()
	h.messages = append(h.messages, r.Message)
	h.records = append(h.records, captured{message: r.Message, attrs: attrs})
	return nil
}

func (h *captureHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *captureHandler) WithGroup(string) slog.Handler      { return h }

// has reports whether any handled record carried msg.
func (h *captureHandler) has(msg string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, m := range h.messages {
		if m == msg {
			return true
		}
	}
	return false
}

// recordsFor returns every captured record carrying msg, in handle order.
func (h *captureHandler) recordsFor(msg string) []captured {
	h.mu.Lock()
	defer h.mu.Unlock()
	var out []captured
	for _, r := range h.records {
		if r.message == msg {
			out = append(out, r)
		}
	}
	return out
}

func TestSchedulerSampleHealthReturnsSnapshot(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	now := healthAnchor
	ctx := clockCtx(context.Background(), models.NewFixedClock(now))
	seedJob(t, db, jobRow{ID: "sh1", Kind: "k", State: string(StateAvailable), ScheduledAt: now.Add(-time.Minute)})
	seedJob(t, db, jobRow{ID: "sh2", Kind: "k", State: string(StateRunning), ScheduledAt: now.Add(-time.Minute)})

	sched := newScheduler(t, db)
	qh, err := sched.SampleHealth(ctx)
	require.NoError(t, err)
	assert.EqualValues(t, 1, qh.Ready, "SampleHealth surfaces the same snapshot SampleQueueHealth reads")
	assert.EqualValues(t, 1, qh.InFlight)
	assert.Equal(t, time.Minute, qh.OldestReadyAge)
}

func TestSchedulerRunEmitsHealthHeartbeatOnCadence(t *testing.T) {
	t.Parallel()
	db := newWALFileDB(t) // file-backed WAL so the Run goroutine and the sampler don't deadlock
	now := time.Now().UTC().Truncate(time.Second)
	ctx := clockCtx(context.Background(), models.NewFixedClock(now))

	// A ready job so the heartbeat has something to report.
	seedJob(t, db, jobRow{ID: "hb1", Kind: "k", State: string(StateAvailable), ScheduledAt: now.Add(-time.Minute)})

	handler := &captureHandler{}
	sched := newSchedulerCfg(t, SchedulerConfig{
		DB: db, Client: NewClient(db),
		Logger:               slog.New(handler),
		TickInterval:         time.Hour, // keep the periodic and lease sweeps quiet
		SweepInterval:        time.Hour,
		HealthSampleInterval: 10 * time.Millisecond,
	})

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() { _ = sched.Run(runCtx) }()

	require.Eventually(t, func() bool {
		return handler.has("jobs: queue health")
	}, 3*time.Second, 10*time.Millisecond, "the heartbeat ticker logs a queue-health pulse on cadence")
}

func TestSchedulerLogHealthLogsSampleError(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	sqlDB, err := db.DB()
	require.NoError(t, err)
	require.NoError(t, sqlDB.Close())

	handler := &captureHandler{}
	sched := newSchedulerCfg(t, SchedulerConfig{
		DB: db, Client: NewClient(db), Logger: slog.New(handler),
	})
	// A failing sample is logged and swallowed — it must not panic or stop the loop.
	assert.NotPanics(t, func() { sched.logHealth(context.Background()) })
	assert.True(t, handler.has("jobs: queue health sample failed"), "a sample failure is logged, not silently dropped")
}

func TestSchedulerRunNoHeartbeatWhenIntervalZero(t *testing.T) {
	t.Parallel()
	db := newWALFileDB(t)
	now := time.Now().UTC().Truncate(time.Second)
	ctx := clockCtx(context.Background(), models.NewFixedClock(now))

	handler := &captureHandler{}
	sched := newSchedulerCfg(t, SchedulerConfig{
		DB: db, Client: NewClient(db),
		Logger:        slog.New(handler),
		TickInterval:  time.Hour,
		SweepInterval: time.Hour,
		// HealthSampleInterval left zero: the heartbeat is disabled.
	})

	runCtx, cancel := context.WithCancel(ctx)
	go func() { _ = sched.Run(runCtx) }()
	time.Sleep(50 * time.Millisecond)
	cancel()

	assert.False(t, handler.has("jobs: queue health"), "a zero interval arms no heartbeat ticker")
}
