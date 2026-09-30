package core

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// enqueuedAtArgs is the args for a worker that records the EnqueuedAt each
// attempt sees.
type enqueuedAtArgs struct{ V string }

func (enqueuedAtArgs) Kind() string { return "test.enqueued_at" }

// enqueuedAtWorker records Job.EnqueuedAt for every attempt and, when failFirst
// is set, fails the first attempt so the job is retried after a short delay.
type enqueuedAtWorker struct {
	failFirst bool

	mu   sync.Mutex
	seen []time.Time
}

func (*enqueuedAtWorker) Kind() string                       { return "test.enqueued_at" }
func (*enqueuedAtWorker) NextRetry(error, int) time.Duration { return 20 * time.Millisecond }

func (w *enqueuedAtWorker) Work(_ context.Context, job *Job[enqueuedAtArgs]) (Result, error) {
	w.mu.Lock()
	w.seen = append(w.seen, job.EnqueuedAt)
	w.mu.Unlock()

	if w.failFirst && job.Attempt == 1 {
		return Result{}, errTransient
	}
	return Result{}, nil
}

// enqueuedAtCase is a job whose scheduled_at ends up apart from its created_at.
type enqueuedAtCase struct {
	name      string
	failFirst bool
	opts      func() InsertOpts
	attempts  int
}

// enqueuedAtCases covers both ways scheduled_at leaves the insert time: a retry
// pushes it forward, and an InsertOpts.ScheduleAt sets it apart from the start.
func enqueuedAtCases() []enqueuedAtCase {
	return []enqueuedAtCase{
		{
			name:      "retried",
			failFirst: true,
			opts:      func() InsertOpts { return InsertOpts{} },
			attempts:  2,
		},
		{
			name: "inserted with ScheduleAt",
			opts: func() InsertOpts {
				// In the past, so the job is claimable at once.
				at := time.Now().Add(-time.Hour)
				return InsertOpts{ScheduleAt: &at}
			},
			attempts: 1,
		},
	}
}

// assertEnqueuedAtIsInsertTime runs tc's job to completion with the runner
// build returns and asserts that every attempt saw Job.EnqueuedAt equal to the
// insert time JobView.EnqueuedAt reports, not the job's scheduled_at.
func assertEnqueuedAtIsInsertTime(t *testing.T, db *gorm.DB, tc enqueuedAtCase, build func(*Registry) *Runner) {
	t.Helper()
	ctx := context.Background()

	w := &enqueuedAtWorker{failFirst: tc.failFirst}
	reg := NewRegistry()
	Register(reg, w)

	id, err := Insert(ctx, NewClient(db), enqueuedAtArgs{V: tc.name}, tc.opts())
	require.NoError(t, err)
	runToIdle(t, ctx, build(reg))

	view, err := FindJob(ctx, db, id)
	require.NoError(t, err)
	var row jobRow
	require.NoError(t, db.Where("id = ?", id).First(&row).Error)
	require.False(t, row.ScheduledAt.Equal(view.EnqueuedAt),
		"the case must move scheduled_at away from the insert time, or it proves nothing")

	require.Len(t, w.seen, tc.attempts)
	for i, got := range w.seen {
		assert.Truef(t, got.Equal(view.EnqueuedAt),
			"attempt %d: Job.EnqueuedAt is %s, want the insert time %s", i+1, got, view.EnqueuedAt)
	}
}

// TestRunnerJobEnqueuedAtIsTheInsertTime: Job.EnqueuedAt is the job's insert
// time on every attempt, matching JobView.EnqueuedAt, however far scheduled_at
// has moved.
func TestRunnerJobEnqueuedAtIsTheInsertTime(t *testing.T) {
	t.Parallel()
	for _, tc := range enqueuedAtCases() {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			db := newDB(t)
			assertEnqueuedAtIsInsertTime(t, db, tc, func(reg *Registry) *Runner { return newRunner(t, db, reg) })
		})
	}
}
