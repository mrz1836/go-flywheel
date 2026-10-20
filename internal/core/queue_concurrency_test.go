package core

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// capRefresh is the cap tests' queue-state refresh.
const capRefresh = 20 * time.Millisecond

// capWorker holds every job until the test lets one go, and tracks how many run
// at once and the most that have since the test last reset it.
type capWorker struct {
	release  chan struct{}
	inFlight atomic.Int32
	peak     atomic.Int32
}

func newCapWorker() *capWorker { return &capWorker{release: make(chan struct{})} }

func (*capWorker) Kind() string { return "pool.peak" }

func (w *capWorker) Work(ctx context.Context, _ *Job[peakArgs]) (Result, error) {
	cur := w.inFlight.Add(1)
	for {
		peak := w.peak.Load()
		if cur <= peak || w.peak.CompareAndSwap(peak, cur) {
			break
		}
	}
	defer w.inFlight.Add(-1)
	select {
	case <-w.release:
	case <-ctx.Done():
	}
	return Result{}, nil
}

// letGo lets n held jobs finish, waiting for each to be taken.
func (w *capWorker) letGo(t *testing.T, n int) {
	t.Helper()
	for range n {
		select {
		case w.release <- struct{}{}:
		case <-time.After(5 * time.Second):
			t.Fatal("no held job took the release")
		}
	}
}

// resetPeak restarts the high-water mark from what runs now.
func (w *capWorker) resetPeak() { w.peak.Store(w.inFlight.Load()) }

// waitInFlight waits until exactly n jobs run.
func (w *capWorker) waitInFlight(t *testing.T, n int32, msg string) {
	t.Helper()
	require.Eventually(t, func() bool { return w.inFlight.Load() == n }, 5*time.Second, time.Millisecond, msg)
}

// holdsAt asserts that, across several refreshes, exactly n jobs keep running:
// the Runner claims nothing more.
func (w *capWorker) holdsAt(t *testing.T, n int32, msg string) {
	t.Helper()
	for range 5 {
		time.Sleep(capRefresh)
		require.Equal(t, n, w.inFlight.Load(), msg)
	}
}

// capRunner builds a Runner of eight slots over d, serving queues, whose jobs
// run in w. Its database holds job_queues, which the cap is read from.
func capRunner(t *testing.T, db *gorm.DB, d Driver, w *capWorker, refresh time.Duration, queues ...string) *Runner {
	t.Helper()
	reg := NewRegistry()
	Register[peakArgs](reg, w)
	r, err := NewRunner(RunnerConfig{
		DB: db, Driver: d, Registry: reg, Queues: queues, ExecutorClass: "local",
		Concurrency: 8, PollInterval: time.Millisecond, QueueStateRefresh: refresh,
	})
	require.NoError(t, err)
	return r
}

// TestAQueueConcurrencyCapLowersSlotsWithinOneRefresh proves a cap lands within
// one queue-state refresh, even with every slot busy: claims stop while the cap
// or more jobs run, never pass it once the running jobs finish, and interrupt
// none; cleared, the Runner fills every slot again. A cap above the pool's size
// changes nothing, a Runner serving capped queues takes the smallest cap, and
// with the refresh off no cap is read.
func TestAQueueConcurrencyCapLowersSlotsWithinOneRefresh(t *testing.T) {
	t.Parallel()

	t.Run("a cap lowers the slots and clearing it restores them", func(t *testing.T) {
		t.Parallel()
		db := newDB(t)
		d := newPoolDriver(100)
		w := newCapWorker()
		stop := runInBackground(t, capRunner(t, db, d, w, capRefresh, "default"))
		defer stop()
		defer close(w.release)
		ctx := context.Background()

		w.waitInFlight(t, 8, "the Runner fills its eight slots")
		two := 2
		require.NoError(t, SetQueueConcurrency(ctx, db, "default", &two))
		time.Sleep(3 * capRefresh)
		claimsBefore := len(d.claimLimits())

		w.letGo(t, 1)
		w.holdsAt(t, 7, "with the cap read, a finished job's slot is not refilled")
		w.letGo(t, 5)
		w.holdsAt(t, 2, "nor while the cap's worth of jobs runs")
		assert.Len(t, d.claimLimits(), claimsBefore, "nothing was claimed past the cap")

		w.resetPeak()
		w.letGo(t, 2)
		w.waitInFlight(t, 2, "below the cap the Runner claims again")
		w.letGo(t, 4)
		w.waitInFlight(t, 2, "and keeps the cap's worth running")
		assert.LessOrEqual(t, w.peak.Load(), int32(2), "never more than the cap")
		for _, limit := range d.claimLimits()[claimsBefore:] {
			assert.LessOrEqual(t, limit, 2, "no claim asks for more than the cap allows")
		}

		require.NoError(t, SetQueueConcurrency(ctx, db, "default", nil))
		w.waitInFlight(t, 8, "cleared, the Runner fills every slot again within a refresh")
	})

	t.Run("a cap above the pool's size changes nothing", func(t *testing.T) {
		t.Parallel()
		db := newDB(t)
		twenty := 20
		require.NoError(t, SetQueueConcurrency(context.Background(), db, "default", &twenty))
		w := newCapWorker()
		stop := runInBackground(t, capRunner(t, db, newPoolDriver(100), w, capRefresh, "default"))
		defer stop()
		defer close(w.release)

		w.waitInFlight(t, 8, "the Runner fills its eight slots")
		w.letGo(t, 3)
		w.waitInFlight(t, 8, "and refills them")
		w.holdsAt(t, 8, "and runs no more than its pool")
		assert.Equal(t, int32(8), w.peak.Load(), "a cap never raises the pool past its size")
	})

	t.Run("a Runner serving several capped queues takes the smallest cap", func(t *testing.T) {
		t.Parallel()
		db := newDB(t)
		three, five := 3, 5
		require.NoError(t, SetQueueConcurrency(context.Background(), db, "a", &three))
		require.NoError(t, SetQueueConcurrency(context.Background(), db, "b", &five))
		w := newCapWorker()
		stop := runInBackground(t, capRunner(t, db, newPoolDriver(100), w, capRefresh, "a", "b"))
		defer stop()
		defer close(w.release)

		w.waitInFlight(t, 3, "the smallest cap among its queues")
		w.holdsAt(t, 3, "in total")
		assert.LessOrEqual(t, w.peak.Load(), int32(3))
	})

	t.Run("with the refresh off no cap is read", func(t *testing.T) {
		t.Parallel()
		db := newDB(t)
		two := 2
		require.NoError(t, SetQueueConcurrency(context.Background(), db, "default", &two))
		w := newCapWorker()
		stop := runInBackground(t, capRunner(t, db, newPoolDriver(100), w, -1, "default"))
		defer stop()
		defer close(w.release)

		w.waitInFlight(t, 8, "the Runner reads no queue state, so it fills every slot")
	})
}

// queueConcurrencySuite proves SetQueueConcurrency's writes and history: a cap
// on a queue with no row creates it, ListQueues reports it, each change records
// one limits_set entry with the cap before and after, a call that changes
// nothing records nothing, and each refusal writes nothing. db carries the
// current schema.
func queueConcurrencySuite(t *testing.T, db *gorm.DB) {
	t.Helper()
	operator := WithActor(context.Background(), Actor{Type: ActorUser, ID: "operator", Reason: "the downstream is drowning"})
	two := 2

	require.NoError(t, SetQueueConcurrency(operator, db, "exports", &two))
	queues, err := ListQueues(context.Background(), db)
	require.NoError(t, err)
	require.Len(t, queues, 1, "a cap on a queue with no row creates it")
	assert.Equal(t, "exports", queues[0].Queue)
	require.NotNil(t, queues[0].MaxConcurrency)
	assert.Equal(t, 2, *queues[0].MaxConcurrency)
	assert.False(t, queues[0].IsPaused, "a cap is not a pause")

	entries := auditEntriesFor(t, db, AuditTargetQueue, "exports")
	require.Len(t, entries, 1)
	assert.Equal(t, AuditLimitsSet, entries[0].Action)
	assert.Equal(t, "operator", entries[0].Actor.ID)
	assert.JSONEq(t, `{"from":null,"to":2}`, string(entries[0].Details))

	require.NoError(t, SetQueueConcurrency(operator, db, "exports", &two))
	assert.Len(t, auditEntriesFor(t, db, AuditTargetQueue, "exports"), 1, "the same cap again records nothing")

	require.NoError(t, SetQueueConcurrency(operator, db, "exports", nil))
	entries = auditEntriesFor(t, db, AuditTargetQueue, "exports")
	require.Len(t, entries, 2)
	assert.JSONEq(t, `{"from":2,"to":null}`, string(entries[0].Details), "clearing records the cap it cleared")
	queues, err = ListQueues(context.Background(), db)
	require.NoError(t, err)
	assert.Nil(t, queues[0].MaxConcurrency)

	require.NoError(t, SetQueueConcurrency(operator, db, "never-capped", nil))
	queues, err = ListQueues(context.Background(), db)
	require.NoError(t, err)
	assert.Len(t, queues, 1, "clearing a queue with no row writes none")

	zero := 0
	for _, tc := range []struct {
		name  string
		ctx   context.Context
		queue string
		n     *int
		field string
	}{
		{"a cap of zero", operator, "refused", &zero, "max_concurrency"},
		{"an empty queue", operator, "", &two, "queue"},
		{"an actor the history can't record", WithActor(context.Background(), Actor{Type: "robot"}), "refused", &two, "actor.type"},
	} {
		err := SetQueueConcurrency(tc.ctx, db, tc.queue, tc.n)
		var verr *ValidationError
		require.ErrorAs(t, err, &verr, tc.name)
		assert.Equal(t, tc.field, verr.Field, tc.name)
	}
	queues, err = ListQueues(context.Background(), db)
	require.NoError(t, err)
	assert.Len(t, queues, 1, "no refusal wrote a row")
	assert.Empty(t, auditEntriesFor(t, db, AuditTargetQueue, "refused"), "or an entry")
}

// TestSetQueueConcurrencyRecordsOneEntryPerChange runs the suite on SQLite.
func TestSetQueueConcurrencyRecordsOneEntryPerChange(t *testing.T) {
	t.Parallel()
	queueConcurrencySuite(t, newDB(t))
}
