package core

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// countReads returns a counter of the reads issued through db.
func countReads(t *testing.T, db *gorm.DB) *atomic.Int32 {
	t.Helper()
	var n atomic.Int32
	require.NoError(t, db.Callback().Query().Before("gorm:query").Register("test:count_reads",
		func(*gorm.DB) { n.Add(1) }))
	return &n
}

// pauseAfterRead holds the n-th read through db once it has returned its rows:
// reached is closed at that point, and the reader blocks until release is called.
// A test changes the row between two reads of a wait at a known point this way,
// with no sleeps. The test's own writes run through the Update and Delete
// callbacks, so they are not counted.
func pauseAfterRead(t *testing.T, db *gorm.DB, n int32) (reached <-chan struct{}, release func()) {
	t.Helper()
	var reads atomic.Int32
	hit, gate := make(chan struct{}), make(chan struct{})
	require.NoError(t, db.Callback().Query().After("gorm:query").Register("test:pause_after_read",
		func(*gorm.DB) {
			if reads.Add(1) == n {
				close(hit)
				<-gate
			}
		}))
	var once sync.Once
	release = func() { once.Do(func() { close(gate) }) }
	t.Cleanup(release)
	return hit, release
}

// waitResult is what a wait run on its own goroutine returned.
type waitResult struct {
	view JobView
	err  error
}

// waitAsync runs WaitForJobWithOptions on its own goroutine.
func waitAsync(ctx context.Context, db *gorm.DB, id string, opts WaitOpts) <-chan waitResult {
	out := make(chan waitResult, 1)
	go func() {
		view, err := WaitForJobWithOptions(ctx, db, id, opts)
		out <- waitResult{view, err}
	}()
	return out
}

// setState moves a job to state directly.
func setState(t *testing.T, db *gorm.DB, id string, state JobState) {
	t.Helper()
	require.NoError(t, db.Model(&jobRow{}).Where("id = ?", id).Update("state", string(state)).Error)
}

// fastPoll is the cadence the wait tests poll at, so a live job is read many
// times inside a short deadline.
//
//nolint:gochecknoglobals // shared test fixture
var fastPoll = WaitOpts{PollInterval: time.Millisecond, MaxPollInterval: time.Millisecond}

func TestWaitOptsResolved(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name               string
		in                 WaitOpts
		wantPoll, wantMaxP time.Duration
	}{
		{"zero selects the defaults", WaitOpts{}, 500 * time.Millisecond, 5 * time.Second},
		{"negative selects the defaults", WaitOpts{PollInterval: -1, MaxPollInterval: -1}, 500 * time.Millisecond, 5 * time.Second},
		{"explicit values are kept", WaitOpts{PollInterval: time.Second, MaxPollInterval: time.Minute}, time.Second, time.Minute},
		{"a max below the poll is raised to it", WaitOpts{PollInterval: time.Second, MaxPollInterval: time.Millisecond}, time.Second, time.Second},
		{"a poll above the default max raises the max", WaitOpts{PollInterval: 10 * time.Second}, 10 * time.Second, 10 * time.Second},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := tc.in.resolved()
			assert.Equal(t, tc.wantPoll, got.PollInterval)
			assert.Equal(t, tc.wantMaxP, got.MaxPollInterval)
		})
	}
}

func TestWaitOptsDelayBacksOffToTheCeiling(t *testing.T) {
	t.Parallel()
	ms := time.Millisecond
	tests := []struct {
		name string
		opts WaitOpts
		want []time.Duration
	}{
		{"defaults", WaitOpts{}, []time.Duration{500 * ms, time.Second, 2 * time.Second, 4 * time.Second, 5 * time.Second, 5 * time.Second}},
		{"10ms to 40ms", WaitOpts{PollInterval: 10 * ms, MaxPollInterval: 40 * ms}, []time.Duration{10 * ms, 20 * ms, 40 * ms, 40 * ms}},
		{"fixed", WaitOpts{PollInterval: 5 * ms, MaxPollInterval: 5 * ms}, []time.Duration{5 * ms, 5 * ms, 5 * ms}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			o := tc.opts.resolved()
			got := make([]time.Duration, len(tc.want))
			for i := range got {
				got[i] = o.delay(i + 1)
			}
			assert.Equal(t, tc.want, got)
		})
	}
}

// TestWaitForJobRejectsAnUnknownState proves a States entry outside the job
// state vocabulary is a validation error before any read, rather than a wait
// for a state no job can reach.
func TestWaitForJobRejectsAnUnknownState(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	seedJob(t, db, jobRow{ID: "j", Kind: "k", State: string(StateAvailable)})
	reads := countReads(t, db)
	// The deadline only bounds a wait that wrongly starts: a valid request for a
	// state no job can reach would otherwise block this test forever.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	view, err := WaitForJobWithOptions(ctx, db, "j", WaitOpts{States: []JobState{StateRunning, "bogus"}})
	require.ErrorIs(t, err, ErrValidation)
	assert.EqualError(t, err, `flywheel: states "bogus" is not a job state`)
	assert.Equal(t, JobView{}, view)
	assert.Zero(t, reads.Load(), "rejected before any read")
}

// TestWaitForJobReturnsATerminalJobAtOnce proves the first read is immediate and
// a terminal job ends the wait on it: an hour-long poll interval and one read.
func TestWaitForJobReturnsATerminalJobAtOnce(t *testing.T) {
	t.Parallel()
	for _, state := range TerminalStates() {
		t.Run(string(state), func(t *testing.T) {
			t.Parallel()
			db := newDB(t)
			seedJob(t, db, jobRow{ID: "j", Kind: "k", State: string(state)})
			reads := countReads(t, db)

			view, err := WaitForJobWithOptions(context.Background(), db, "j", WaitOpts{PollInterval: time.Hour})
			require.NoError(t, err)
			assert.Equal(t, string(state), view.State)
			assert.Equal(t, "j", view.ID)
			assert.EqualValues(t, 1, reads.Load(), "one read, no initial sleep")

			view, err = WaitForJob(context.Background(), db, "j")
			require.NoError(t, err)
			assert.Equal(t, string(state), view.State, "WaitForJob is the zero-options wait")
		})
	}
}

// TestWaitForJobReturnsWhenTheJobBecomesTerminal changes the job between the
// first and second read and proves the second read ends the wait.
func TestWaitForJobReturnsWhenTheJobBecomesTerminal(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	seedJob(t, db, jobRow{ID: "j", Kind: "k", State: string(StateRunning)})
	reached, release := pauseAfterRead(t, db, 1)
	reads := countReads(t, db)

	done := waitAsync(context.Background(), db, "j", fastPoll)
	<-reached
	setState(t, db, "j", StateSucceeded)
	release()

	res := <-done
	require.NoError(t, res.err)
	assert.Equal(t, string(StateSucceeded), res.view.State)
	assert.EqualValues(t, 2, reads.Load())
}

// TestWaitForJobStopsOnARequestedState proves WaitOpts.States ends the wait on a
// live state, and that a terminal state still ends it first.
func TestWaitForJobStopsOnARequestedState(t *testing.T) {
	t.Parallel()
	running := WaitOpts{PollInterval: time.Millisecond, States: []JobState{StateRunning}}

	t.Run("already in the state", func(t *testing.T) {
		t.Parallel()
		db := newDB(t)
		seedJob(t, db, jobRow{ID: "j", Kind: "k", State: string(StateRunning)})
		view, err := WaitForJobWithOptions(context.Background(), db, "j", running)
		require.NoError(t, err)
		assert.Equal(t, string(StateRunning), view.State)
	})

	t.Run("reaches the state", func(t *testing.T) {
		t.Parallel()
		db := newDB(t)
		seedJob(t, db, jobRow{ID: "j", Kind: "k", State: string(StateAvailable)})
		reached, release := pauseAfterRead(t, db, 1)
		done := waitAsync(context.Background(), db, "j", running)
		<-reached
		setState(t, db, "j", StateRunning)
		release()

		res := <-done
		require.NoError(t, res.err)
		assert.Equal(t, string(StateRunning), res.view.State)
	})

	t.Run("a terminal state wins", func(t *testing.T) {
		t.Parallel()
		db := newDB(t)
		seedJob(t, db, jobRow{ID: "j", Kind: "k", State: string(StateCancelled)})
		view, err := WaitForJobWithOptions(context.Background(), db, "j", running)
		require.NoError(t, err)
		assert.Equal(t, string(StateCancelled), view.State)
	})
}

// TestWaitForJobKeepsWaitingThroughLiveStates proves no live state ends the
// wait: it runs to the deadline, many reads later, and says what state the job
// was still in.
func TestWaitForJobKeepsWaitingThroughLiveStates(t *testing.T) {
	t.Parallel()
	for _, state := range NonTerminalStates() {
		t.Run(string(state), func(t *testing.T) {
			t.Parallel()
			db := newDB(t)
			seedJob(t, db, jobRow{ID: "j", Kind: "k", State: string(state)})
			reads := countReads(t, db)
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()

			view, err := WaitForJobWithOptions(ctx, db, "j", fastPoll)
			require.ErrorIs(t, err, context.DeadlineExceeded)
			assert.EqualError(t, err, fmt.Sprintf(`flywheel: wait for job "j": still %s: context deadline exceeded`, state))
			assert.Equal(t, string(state), view.State, "the view is the last one read")
			assert.Greater(t, reads.Load(), int32(1), "the wait kept polling")
		})
	}
}

// TestWaitForJobContextAlreadyDoneReadsNothing proves a ctx that is already done
// ends the wait before its first read.
func TestWaitForJobContextAlreadyDoneReadsNothing(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	seedJob(t, db, jobRow{ID: "j", Kind: "k", State: string(StateSucceeded)})
	reads := countReads(t, db)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	view, err := WaitForJob(ctx, db, "j")
	require.ErrorIs(t, err, context.Canceled)
	assert.EqualError(t, err, `flywheel: wait for job "j": no state observed: context canceled`)
	assert.Equal(t, JobView{}, view)
	assert.Zero(t, reads.Load())
}

// TestWaitForJobContextCutsTheIntervalShort proves a long poll interval does not
// outlive ctx: the wait returns at the deadline, not after the interval.
func TestWaitForJobContextCutsTheIntervalShort(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	seedJob(t, db, jobRow{ID: "j", Kind: "k", State: string(StateAvailable)})
	reads := countReads(t, db)
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	start := time.Now()
	view, err := WaitForJobWithOptions(ctx, db, "j", WaitOpts{PollInterval: time.Hour})
	require.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Less(t, time.Since(start), 10*time.Second, "the hour-long interval was cut short")
	assert.Equal(t, string(StateAvailable), view.State)
	assert.EqualValues(t, 1, reads.Load())
}

// TestWaitForJobReadInterruptedByContextReportsTheContext proves a read that
// fails because ctx ended is reported as the wait ending, naming the state last
// seen, not as a database error.
func TestWaitForJobReadInterruptedByContextReportsTheContext(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	seedJob(t, db, jobRow{ID: "j", Kind: "k", State: string(StateRetryable)})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var reads atomic.Int32
	require.NoError(t, db.Callback().Query().Before("gorm:query").Register("test:cancel_second_read",
		func(tx *gorm.DB) {
			if reads.Add(1) == 2 {
				cancel()
				_ = tx.AddError(fmt.Errorf("driver: %w", context.Canceled))
			}
		}))

	view, err := WaitForJobWithOptions(ctx, db, "j", fastPoll)
	require.ErrorIs(t, err, context.Canceled)
	assert.EqualError(t, err, `flywheel: wait for job "j": still retryable: context canceled`)
	assert.Equal(t, string(StateRetryable), view.State)
}

// TestWaitForJobMissingJob proves a job that cannot be read returns the bare
// ErrJobNotFound at once, as FindJob does.
func TestWaitForJobMissingJob(t *testing.T) {
	t.Parallel()
	for name, seed := range map[string]func(t *testing.T, db *gorm.DB){
		"never existed": func(*testing.T, *gorm.DB) {},
		"soft-deleted": func(t *testing.T, db *gorm.DB) {
			seedJob(t, db, jobRow{ID: "j", Kind: "k", State: string(StateAvailable)})
			require.NoError(t, db.Delete(&jobRow{}, "id = ?", "j").Error)
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			db := newDB(t)
			seed(t, db)
			view, err := WaitForJob(context.Background(), db, "j")
			assert.Same(t, ErrJobNotFound, err, "the bare sentinel")
			assert.Equal(t, JobView{}, view)
		})
	}
}

// TestWaitForJobDeletedMidWait proves a job removed while it is waited on ends
// the wait with ErrJobNotFound and the view last read.
func TestWaitForJobDeletedMidWait(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	seedJob(t, db, jobRow{ID: "j", Kind: "k", State: string(StateAvailable)})
	reached, release := pauseAfterRead(t, db, 1)

	done := waitAsync(context.Background(), db, "j", fastPoll)
	<-reached
	require.NoError(t, db.Unscoped().Delete(&jobRow{}, "id = ?", "j").Error)
	release()

	res := <-done
	assert.Same(t, ErrJobNotFound, res.err)
	assert.Equal(t, string(StateAvailable), res.view.State, "the view is the last one read")
}

// TestWaitForJobSurfacesAReadError proves a read failure that is not ctx's ends
// the wait at once, wrapped in the wait's own words.
func TestWaitForJobSurfacesAReadError(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	seedJob(t, db, jobRow{ID: "j", Kind: "k", State: string(StateAvailable)})
	reads := countReads(t, db)
	errRead := errors.New("read failed")
	require.NoError(t, db.Callback().Query().Before("gorm:query").Register("test:fail_reads",
		func(tx *gorm.DB) { _ = tx.AddError(errRead) }))

	view, err := WaitForJob(context.Background(), db, "j")
	require.ErrorIs(t, err, errRead)
	assert.NotErrorIs(t, err, ErrJobNotFound)
	assert.True(t, strings.HasPrefix(err.Error(), `flywheel: wait for job "j": `), err.Error())
	assert.Equal(t, JobView{}, view)
	assert.EqualValues(t, 1, reads.Load(), "a read error is not retried")
}

// TestWaitForJobOnlyReads proves a wait never writes: every statement it issues,
// across many polls, is a SELECT.
func TestWaitForJobOnlyReads(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	seedJob(t, db, jobRow{ID: "j", Kind: "k", State: string(StateAvailable)})
	sqls, rec := countStatements(db)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	_, err := WaitForJobWithOptions(ctx, rec, "j", fastPoll)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.NotEmpty(t, *sqls)
	for _, s := range *sqls {
		assert.True(t, strings.HasPrefix(s, "SELECT"), s)
	}
}

// waitArgs is the payload of the job the runner-backed wait tests run.
type waitArgs struct {
	Subject string `json:"subject"`
}

// Kind names the worker for waitArgs.
func (waitArgs) Kind() string { return "wait.work" }

// waitWorker counts its runs, holds each one until gate is closed when gate is
// set, and returns the subject it ran for as its output.
type waitWorker struct {
	runs atomic.Int32
	gate chan struct{}
}

// Kind names the worker.
func (*waitWorker) Kind() string { return "wait.work" }

// Work records the run, waits on the gate, and succeeds.
func (w *waitWorker) Work(ctx context.Context, job *Job[waitArgs]) (Result, error) {
	w.runs.Add(1)
	if w.gate != nil {
		select {
		case <-w.gate:
		case <-ctx.Done():
			return Result{}, ctx.Err()
		}
	}
	return Result{Output: map[string]string{"subject": job.Args.Subject}}, nil
}

// startRunner runs r until the test ends, when cancelling its ctx is the only
// way it stops.
func startRunner(t *testing.T, r *Runner) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		assert.ErrorIs(t, <-done, context.Canceled)
	})
}

// waitPoll is the cadence the runner-backed tests wait at.
//
//nolint:gochecknoglobals // shared test fixture
var waitPoll = WaitOpts{PollInterval: 5 * time.Millisecond, MaxPollInterval: 20 * time.Millisecond}

// assertWaitFollowsARealRunner enqueues a job, lets a real runner work it, and
// proves the wait returns its terminal view and LatestRun its output. It is
// shared by the SQLite and (integration) PostgreSQL suites.
func assertWaitFollowsARealRunner(t *testing.T, db *gorm.DB, newRunner func(*Registry) *Runner) {
	t.Helper()
	w := &waitWorker{}
	reg := NewRegistry()
	Register(reg, w)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	id, err := Insert(ctx, NewClient(db), waitArgs{Subject: "s-1"}, InsertOpts{})
	require.NoError(t, err)
	startRunner(t, newRunner(reg))

	view, err := WaitForJobWithOptions(ctx, db, id, waitPoll)
	require.NoError(t, err)
	assert.Equal(t, string(StateSucceeded), view.State)
	assert.NotNil(t, view.FinalizedAt)

	run, ok, err := LatestRun(ctx, db, id)
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, string(OutcomeSuccess), run.Outcome)
	assert.JSONEq(t, `{"subject":"s-1"}`, string(run.Output))
	assert.EqualValues(t, 1, w.runs.Load())
}

func TestWaitForJobFollowsARealRunner(t *testing.T) {
	t.Parallel()
	db := newWALFileDB(t)
	assertWaitFollowsARealRunner(t, db, func(reg *Registry) *Runner { return rwRunner(t, db, reg) })
}

// enqueueOrJoin enqueues the work under key, or joins the live job already
// holding it, and returns the id to wait on — the COOKBOOK recipe. A collision
// that names no holder means the holder finished between the collision and the
// read, which freed the key, so one more insert lands or names the next holder.
func enqueueOrJoin(ctx context.Context, c *Client, args waitArgs, key string) (string, error) {
	for range 2 {
		id, err := Insert(ctx, c, args, InsertOpts{UniqueActiveKey: key})
		var dup *AlreadyEnqueuedError
		switch {
		case err == nil:
			return id, nil
		case !errors.As(err, &dup):
			return "", err
		case dup.ExistingID != "":
			return dup.ExistingID, nil
		}
	}
	return "", errors.New("enqueue or join: the key changed hands twice")
}

// assertEnqueueOrJoinRunsTheWorkOnce has callers enqueue-or-join one subject
// while a runner works it, then wait, and proves the work ran once: every caller
// resolves to the same job and sees it succeed. The worker holds its one run
// until every caller has joined, so no caller can arrive after the job finished
// and legitimately start a second run. Concurrent callers race the insert; the
// others join one at a time.
func assertEnqueueOrJoinRunsTheWorkOnce(
	t *testing.T, db *gorm.DB, newRunner func(*Registry) *Runner, callers int, concurrent bool,
) {
	t.Helper()
	w := &waitWorker{gate: make(chan struct{})}
	reg := NewRegistry()
	Register(reg, w)
	startRunner(t, newRunner(reg))
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	c := NewClient(db)

	ids := make([]string, callers)
	views := make([]JobView, callers)
	errs := make([]error, callers)
	if concurrent {
		var joined, waited sync.WaitGroup
		joined.Add(callers)
		waited.Add(callers)
		for i := range callers {
			go func() {
				defer waited.Done()
				ids[i], errs[i] = enqueueOrJoin(ctx, c, waitArgs{Subject: "repo-1"}, "sync:repo-1")
				joined.Done()
				if errs[i] == nil {
					views[i], errs[i] = WaitForJobWithOptions(ctx, db, ids[i], waitPoll)
				}
			}()
		}
		joined.Wait()
		close(w.gate)
		waited.Wait()
	} else {
		for i := range callers {
			ids[i], errs[i] = enqueueOrJoin(ctx, c, waitArgs{Subject: "repo-1"}, "sync:repo-1")
			require.NoError(t, errs[i])
		}
		close(w.gate)
		for i := range callers {
			views[i], errs[i] = WaitForJobWithOptions(ctx, db, ids[i], waitPoll)
		}
	}

	for i := range callers {
		require.NoError(t, errs[i], "caller %d", i)
		assert.Equal(t, ids[0], ids[i], "caller %d joined the one job", i)
		assert.Equal(t, string(StateSucceeded), views[i].State, "caller %d saw it succeed", i)
	}
	assert.EqualValues(t, 1, w.runs.Load(), "the work ran once")
	run, ok, err := LatestRun(ctx, db, ids[0])
	require.NoError(t, err)
	require.True(t, ok)
	assert.JSONEq(t, `{"subject":"repo-1"}`, string(run.Output))
}

func TestEnqueueOrJoinThenWaitRunsTheWorkOnce(t *testing.T) {
	t.Parallel()
	db := newWALFileDB(t)
	assertEnqueueOrJoinRunsTheWorkOnce(t, db, func(reg *Registry) *Runner { return rwRunner(t, db, reg) }, 5, false)
}
