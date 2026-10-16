package core

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/mrz1836/go-foundation/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// queueOperator is the actor the queue-pause tests pause under.
//
//nolint:gochecknoglobals // shared fixture
var queueOperator = Actor{Type: ActorUser, ID: "u1", Reason: "provider outage", Source: "api"}

// queueRow reads queue's job_queues row, and false when it has none.
func queueRow(t testing.TB, db *gorm.DB, queue string) (jobQueueRow, bool) {
	t.Helper()
	var rows []jobQueueRow
	require.NoError(t, db.Where("queue = ?", queue).Find(&rows).Error)
	if len(rows) == 0 {
		return jobQueueRow{}, false
	}
	return rows[0], true
}

// TestPauseQueueRecordsTheActorAndReasonOnce proves a pause and a resume each
// write the queue's state and one entry, attributed to the context's actor and
// stamped in UTC; that re-pausing a paused queue, or resuming one that isn't
// paused, changes nothing and records nothing; that a queue no job has used can
// be paused; and that an empty name, an actor the history can't record, and a
// failing audit hook each leave everything as it was.
func TestPauseQueueRecordsTheActorAndReasonOnce(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	// Five hours behind UTC, so a time stamped in UTC is told apart.
	at := time.Date(2026, 1, 1, 7, 0, 0, 0, time.FixedZone("UTC-5", -5*60*60))
	ctx := clockCtx(WithActor(context.Background(), queueOperator), models.NewFixedClock(at))
	seedJob(t, db, jobRow{ID: "queued-1", Kind: "leaf", Queue: "a", State: string(StateAvailable), ScheduledAt: at})

	require.NoError(t, PauseQueue(ctx, db, "a"))
	row, ok := queueRow(t, db, "a")
	require.True(t, ok, "the pause creates the queue's row")
	require.NotNil(t, row.PausedAt)
	assert.True(t, at.Equal(*row.PausedAt))
	assert.Equal(t, time.UTC, row.PausedAt.Location(), "stored in UTC")
	assert.Equal(t, string(ActorUser), row.PausedByType)
	assert.Equal(t, "u1", row.PausedByID)
	assert.Equal(t, "provider outage", row.PausedReason)
	entry := onlyAuditRow(t, db, "a")
	assert.Equal(t, string(AuditTargetQueue), entry.TargetType)
	assert.Equal(t, string(AuditPaused), entry.Action)
	assert.Equal(t, string(ActorUser), entry.ActorType)
	assert.Equal(t, "u1", entry.ActorID)
	assert.Equal(t, "provider outage", entry.Reason)
	assert.Equal(t, "api", entry.Source)
	assert.True(t, at.Equal(entry.CreatedAt))
	assert.Equal(t, string(StateAvailable), jobState(t, db, "queued-1"), "a pause holds the queue, not its jobs")

	later := clockCtx(WithActor(context.Background(), Actor{Type: ActorAgent, ID: "bot"}), models.NewFixedClock(at.Add(time.Hour)))
	require.NoError(t, PauseQueue(later, db, "a"), "re-pausing a paused queue is not an error")
	again, _ := queueRow(t, db, "a")
	assert.Equal(t, row, again, "and changes nothing: the first pause's time and actor stay")
	assert.Len(t, auditRows(t, db, "a"), 1, "and records nothing")

	require.NoError(t, ResumeQueue(later, db, "a"))
	resumed, ok := queueRow(t, db, "a")
	require.True(t, ok, "a resume keeps the row")
	assert.Nil(t, resumed.PausedAt)
	assert.Empty(t, resumed.PausedByType)
	assert.Empty(t, resumed.PausedByID)
	assert.Empty(t, resumed.PausedReason)
	entries := auditRows(t, db, "a")
	require.Len(t, entries, 2)
	assert.Equal(t, string(AuditResumed), entries[1].Action)
	assert.Equal(t, "bot", entries[1].ActorID)

	require.NoError(t, ResumeQueue(later, db, "a"), "resuming a queue that isn't paused is not an error")
	assert.Len(t, auditRows(t, db, "a"), 2, "and records nothing")
	require.NoError(t, ResumeQueue(later, db, "never-paused"))
	_, ok = queueRow(t, db, "never-paused")
	assert.False(t, ok, "a resume creates no row")
	assert.Empty(t, auditRows(t, db, "never-paused"))

	require.NoError(t, PauseQueue(ctx, db, "unused"), "a queue no job has used can be paused")
	unused, ok := queueRow(t, db, "unused")
	require.True(t, ok)
	assert.NotNil(t, unused.PausedAt)
	assert.Len(t, auditRows(t, db, "unused"), 1)

	for _, call := range []func(context.Context, *gorm.DB, string) error{PauseQueue, ResumeQueue} {
		var ve *ValidationError
		require.ErrorAs(t, call(ctx, db, ""), &ve)
		assert.Equal(t, "queue", ve.Field)
		for _, bad := range invalidActors() {
			assertRefusedActor(t, call(WithActor(context.Background(), bad.actor), db, "refused"), bad)
		}
	}
	_, ok = queueRow(t, db, "refused")
	assert.False(t, ok, "a refused call writes no row")
	_, ok = queueRow(t, db, "")
	assert.False(t, ok)

	hookErr := errors.New("host record failed")
	failing := WithAuditHook(ctx, func(context.Context, *gorm.DB, []AuditEntry) error { return hookErr })
	require.ErrorIs(t, PauseQueue(failing, db, "hooked"), hookErr)
	hooked, ok := queueRow(t, db, "hooked")
	assert.False(t, ok && hooked.PausedAt != nil, "the hook's error rolls the pause back")
	assert.Empty(t, auditRows(t, db, "hooked"))
	var seen []AuditEntry
	recording := WithAuditHook(ctx, func(_ context.Context, _ *gorm.DB, e []AuditEntry) error {
		seen = append(seen, e...)
		return nil
	})
	require.NoError(t, PauseQueue(recording, db, "hooked"))
	require.Len(t, seen, 1, "the hook runs with the pause's entry")
	assert.Equal(t, AuditTargetQueue, seen[0].TargetType)
	assert.Equal(t, "hooked", seen[0].TargetID)

	_, err := ListQueues(ctx, nil)
	require.Error(t, err)
	require.Error(t, PauseQueue(ctx, nil, "a"))
	require.Error(t, ResumeQueue(ctx, nil, "a"))
}

// TestListQueuesMergesDepthAndPauseState proves ListQueues returns every queue
// that holds a non-terminal job or has a queue row, sorted by name, each with
// QueueDepths' figures and its pause: who paused it and why while it is paused,
// nothing once resumed, and zero depth for a queue no job holds.
func TestListQueuesMergesDepthAndPauseState(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	at := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	ctx := clockCtx(WithActor(context.Background(), queueOperator), models.NewFixedClock(at))
	past := at.Add(-time.Minute)
	seedJob(t, db, jobRow{ID: "a-1", Kind: "leaf", Queue: "a", State: string(StateAvailable), ScheduledAt: past})
	seedJob(t, db, jobRow{ID: "a-2", Kind: "leaf", Queue: "a", State: string(StateAvailable), ScheduledAt: past})
	seedJob(t, db, jobRow{ID: "b-1", Kind: "leaf", Queue: "b", State: string(StateAvailable), ScheduledAt: past})
	seedJob(t, db, jobRow{ID: "b-2", Kind: "leaf", Queue: "b", State: string(StatePaused), ScheduledAt: past})
	seedJob(t, db, jobRow{
		ID: "e-1", Kind: "leaf", Queue: "e", State: string(StateSucceeded), ScheduledAt: past, FinalizedAt: &past,
	})

	require.NoError(t, PauseQueue(ctx, db, "a"))
	require.NoError(t, PauseQueue(ctx, db, "c"))
	require.NoError(t, PauseQueue(ctx, db, "d"))
	require.NoError(t, ResumeQueue(ctx, db, "d"))

	queues, err := ListQueues(ctx, db)
	require.NoError(t, err)
	names := make([]string, len(queues))
	for i, q := range queues {
		names[i] = q.Queue
	}
	require.Equal(t, []string{"a", "b", "c", "d"}, names,
		"every queue with a live job or a row, by name; a queue of finished jobs alone is not listed")

	a := queues[0]
	assert.EqualValues(t, 2, a.Ready)
	assert.Equal(t, time.Minute, a.OldestReadyAge)
	assert.True(t, a.IsPaused)
	require.NotNil(t, a.PausedAt)
	assert.True(t, at.Equal(*a.PausedAt))
	assert.Equal(t, &Actor{Type: ActorUser, ID: "u1", Reason: "provider outage"}, a.PausedBy,
		"the pause's type, id, and reason")

	b := queues[1]
	assert.EqualValues(t, 1, b.Ready)
	assert.EqualValues(t, 1, b.Paused, "Paused counts the queue's paused jobs")
	assert.False(t, b.IsPaused, "a queue with paused jobs is not itself paused")
	assert.Nil(t, b.PausedAt)
	assert.Nil(t, b.PausedBy)

	c := queues[2]
	assert.True(t, c.IsPaused, "a paused queue no job has used")
	assert.Equal(t, QueueDepth{Queue: "c"}, c.QueueDepth, "has zero depth")

	d := queues[3]
	assert.False(t, d.IsPaused, "a resumed queue stays listed, not paused")
	assert.Nil(t, d.PausedAt)
	assert.Nil(t, d.PausedBy)
	assert.Equal(t, QueueDepth{Queue: "d"}, d.QueueDepth)
}
