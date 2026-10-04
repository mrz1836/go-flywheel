package flywheeltest_test

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"

	flywheel "github.com/mrz1836/go-flywheel"
	"github.com/mrz1836/go-flywheel/flywheeltest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// recordingTB is a testing.TB whose failures are recorded instead of reported:
// Fatalf, Fatal, and FailNow record the message and end only the goroutine they
// run on (runtime.Goexit, as the real ones do), so a test can assert that a
// helper fails, and how, without failing itself. Every other method reaches the
// real test.
type recordingTB struct {
	testing.TB

	mu     sync.Mutex
	failed bool
	msg    string
}

// Helper is a no-op: there is no output to attribute.
func (*recordingTB) Helper() {}

// Errorf records a failure and returns.
func (r *recordingTB) Errorf(format string, args ...any) { r.record(fmt.Sprintf(format, args...)) }

// Fatalf records a failure and ends the calling goroutine.
func (r *recordingTB) Fatalf(format string, args ...any) {
	r.record(fmt.Sprintf(format, args...))
	runtime.Goexit()
}

// Fatal records a failure and ends the calling goroutine.
func (r *recordingTB) Fatal(args ...any) {
	r.record(fmt.Sprint(args...))
	runtime.Goexit()
}

// FailNow records a failure and ends the calling goroutine.
func (r *recordingTB) FailNow() {
	r.record("")
	runtime.Goexit()
}

// record notes a failure, keeping the first message.
func (r *recordingTB) record(msg string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.failed {
		r.failed, r.msg = true, msg
	}
}

// result returns whether fn failed, and its first message.
func (r *recordingTB) result() (bool, string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.failed, r.msg
}

// runRecorded runs fn against a recordingTB on its own goroutine, so a Goexit
// ends fn rather than the test, and returns the recorder once fn is done.
func runRecorded(t *testing.T, fn func(tb testing.TB)) *recordingTB {
	t.Helper()
	rec := &recordingTB{TB: t}
	done := make(chan struct{})
	go func() {
		defer close(done)
		fn(rec)
	}()
	<-done
	return rec
}

// enqueue writes one available job and returns its id.
func enqueue(t *testing.T, db *gorm.DB) string {
	t.Helper()
	id, err := flywheel.Enqueue(context.Background(), flywheel.NewClient(db), "k", []byte(`{}`), flywheel.InsertOpts{})
	require.NoError(t, err)
	return id
}

// TestWaitForJobStateReturnsAtTheState proves the helper returns, without
// failing, once the job is in the state asked for — terminal or not.
func TestWaitForJobStateReturnsAtTheState(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		shape func(t *testing.T, db *gorm.DB, id string)
		state flywheel.JobState
	}{
		{"a terminal state", func(t *testing.T, db *gorm.DB, id string) {
			t.Helper()
			require.NoError(t, flywheel.CancelJob(context.Background(), db, id))
		}, flywheel.StateCancelled},
		{"the state the job starts in", func(*testing.T, *gorm.DB, string) {}, flywheel.StateAvailable},
		{"a live state the job was moved to", func(t *testing.T, db *gorm.DB, id string) {
			t.Helper()
			require.NoError(t, db.Table("jobs").Where("id = ?", id).Update("state", "running").Error)
		}, flywheel.StateRunning},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			db := flywheeltest.NewDB(t)
			id := enqueue(t, db)
			tc.shape(t, db, id)

			rec := runRecorded(t, func(tb testing.TB) {
				flywheeltest.WaitForJobState(tb, db, id, string(tc.state), 5*time.Second)
			})
			failed, msg := rec.result()
			assert.False(t, failed, msg)
		})
	}
}

// TestWaitForJobStateFailsFast proves the helper does not sit out its timeout
// when the wait can no longer succeed: the job ended in another terminal state,
// does not exist, or cannot be read.
func TestWaitForJobStateFailsFast(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		id      func(t *testing.T, db *gorm.DB) string
		wantMsg string
	}{
		{"a different terminal state", func(t *testing.T, db *gorm.DB) string {
			t.Helper()
			id := enqueue(t, db)
			require.NoError(t, flywheel.CancelJob(context.Background(), db, id))
			return id
		}, `reached terminal state "cancelled" while waiting for "succeeded"`},
		{
			"a missing job", func(*testing.T, *gorm.DB) string { return "no-such-job" },
			`job no-such-job not found while waiting for state "succeeded"`,
		},
		{"a read error", func(t *testing.T, db *gorm.DB) string {
			t.Helper()
			id := enqueue(t, db)
			sqlDB, err := db.DB()
			require.NoError(t, err)
			require.NoError(t, sqlDB.Close())
			return id
		}, `waiting for state "succeeded": flywheel: wait for job`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			db := flywheeltest.NewDB(t)
			id := tc.id(t, db)

			start := time.Now()
			rec := runRecorded(t, func(tb testing.TB) {
				flywheeltest.WaitForJobState(tb, db, id, string(flywheel.StateSucceeded), 30*time.Second)
			})
			failed, msg := rec.result()
			require.True(t, failed, "the helper fails")
			assert.Contains(t, msg, tc.wantMsg)
			assert.Less(t, time.Since(start), 5*time.Second, "it fails at once, not at the timeout")
		})
	}
}

// TestWaitForJobStateRejectsAnUnknownState proves a state no job can reach fails
// the helper at once instead of waiting out its timeout.
func TestWaitForJobStateRejectsAnUnknownState(t *testing.T) {
	t.Parallel()
	db := flywheeltest.NewDB(t)
	id := enqueue(t, db)

	start := time.Now()
	rec := runRecorded(t, func(tb testing.TB) {
		flywheeltest.WaitForJobState(tb, db, id, "finished", 30*time.Second)
	})
	failed, msg := rec.result()
	require.True(t, failed)
	assert.Contains(t, msg, `waiting for state "finished": flywheel: states "finished" is not a job state`)
	assert.Less(t, time.Since(start), 5*time.Second, "it fails at once, not at the timeout")
}

// TestWaitForJobStateTimesOutWithTheLastState pins the timeout message, which
// names the state the job was last seen in.
func TestWaitForJobStateTimesOutWithTheLastState(t *testing.T) {
	t.Parallel()
	db := flywheeltest.NewDB(t)
	id := enqueue(t, db)

	rec := runRecorded(t, func(tb testing.TB) {
		flywheeltest.WaitForJobState(tb, db, id, string(flywheel.StateSucceeded), 200*time.Millisecond)
	})
	failed, msg := rec.result()
	require.True(t, failed)
	assert.Equal(t, fmt.Sprintf(`job %s did not reach state "succeeded" within 200ms (last: "available")`, id), msg)
}
