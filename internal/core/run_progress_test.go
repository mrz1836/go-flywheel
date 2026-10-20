package core

import (
	"context"
	"io"
	"log/slog"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// progressHeartbeat is the progress tests' renewal cadence: fast enough to keep
// the suite quick, slow enough that a worker reports many times between two
// renewals.
const progressHeartbeat = 20 * time.Millisecond

// progressArgs is the args of the progress tests' jobs.
type progressArgs struct{ V string }

func (progressArgs) Kind() string { return "test.progress" }

// progressWorker runs the body a test gives it.
type progressWorker struct {
	work func(ctx context.Context, job *Job[progressArgs]) (Result, error)
}

func (*progressWorker) Kind() string { return "test.progress" }

func (w *progressWorker) Work(ctx context.Context, job *Job[progressArgs]) (Result, error) {
	return w.work(ctx, job)
}

// progressRunner builds a Runner with a fast heartbeat and a lease long enough
// that nothing but the test takes the claim.
func progressRunner(t *testing.T, db *gorm.DB, d Driver, w *progressWorker) *Runner {
	t.Helper()
	reg := NewRegistry()
	Register[progressArgs](reg, w)
	r, err := NewRunner(RunnerConfig{
		DB: db, Driver: d, Registry: reg,
		Queues: []string{"default"}, ClaimAnyClass: true, Concurrency: 1,
		LeaseDuration:     5 * time.Second,
		HeartbeatInterval: progressHeartbeat,
		PollInterval:      5 * time.Millisecond,
		QueueStateRefresh: -1,
		Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	require.NoError(t, err)
	return r
}

// countProgressWrites counts the updates that wrote job_runs.progress and
// changed a row.
func countProgressWrites(t testing.TB, db *gorm.DB) *atomic.Int64 {
	t.Helper()
	var n atomic.Int64
	require.NoError(t, db.Callback().Update().After("gorm:update").Register("test:count_progress_writes",
		func(tx *gorm.DB) {
			if tx.Error != nil || tx.Statement.RowsAffected == 0 || tx.Statement.Table != "job_runs" {
				return
			}
			if values, ok := tx.Statement.Dest.(map[string]any); ok {
				if _, wrote := values["progress"]; wrote {
					n.Add(1)
				}
			}
		}))
	return &n
}

// storedProgress reads a job's latest run's progress as the reads report it.
func storedProgress(t *testing.T, db *gorm.DB, jobID string) *RunProgress {
	t.Helper()
	run, ok, err := LatestRun(context.Background(), db, jobID)
	require.NoError(t, err)
	if !ok {
		return nil
	}
	return run.Progress
}

// drainInBackground runs r until idle on its own goroutine and returns a wait
// that fails the test on an error or a hang.
func drainInBackground(t *testing.T, ctx context.Context, r *Runner) func() { //nolint:revive // ctx-after-t matches the package's test helpers
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- r.RunUntilIdle(ctx) }()
	return func() {
		t.Helper()
		select {
		case err := <-done:
			require.NoError(t, err)
		case <-time.After(10 * time.Second):
			t.Fatal("the runner did not finish")
		}
	}
}

// TestSetProgressKeepsItsBounds proves a report is normalized as it is set: the
// percent clamped and NaN dropped, the message cut on a rune boundary, at most
// eight counters chosen by key, the value copied, and nothing at all on a Job
// the Runner did not build.
func TestSetProgressKeepsItsBounds(t *testing.T) {
	t.Parallel()
	pct := func(f float64) *float64 { return &f }
	set := func(p RunProgress) RunProgress {
		j := &Job[progressArgs]{progress: newProgressSink()}
		j.SetProgress(p)
		return j.progress.latest()
	}

	t.Run("the percent is clamped and NaN dropped", func(t *testing.T) {
		t.Parallel()
		for _, tc := range []struct {
			in   float64
			want *float64
		}{
			{-5, pct(0)},
			{150, pct(100)},
			{42.5, pct(42.5)},
			{math.Inf(1), pct(100)},
			{math.Inf(-1), pct(0)},
			{math.NaN(), nil},
		} {
			got := set(RunProgress{Percent: pct(tc.in)}).Percent
			if tc.want == nil {
				assert.Nil(t, got, "%v", tc.in)
				continue
			}
			require.NotNil(t, got, "%v", tc.in)
			assert.InDelta(t, *tc.want, *got, 0, "%v", tc.in)
		}
		assert.Nil(t, set(RunProgress{}).Percent, "no percent stays none")
	})

	t.Run("the message is cut at 256 bytes on a rune boundary", func(t *testing.T) {
		t.Parallel()
		long := strings.Repeat("€", 100) // three bytes each: 300 bytes
		got := set(RunProgress{Message: long}).Message
		assert.Equal(t, strings.Repeat("€", 85), got, "85 whole runes are 255 bytes; an 86th would pass 256")
		assert.LessOrEqual(t, len(got), MaxProgressMessageBytes)
		assert.True(t, utf8.ValidString(got))
		assert.Equal(t, "short", set(RunProgress{Message: "short"}).Message)
	})

	t.Run("eight counters are kept by key order", func(t *testing.T) {
		t.Parallel()
		counters := map[string]int64{"": 1, strings.Repeat("z", 33): 2, strings.Repeat("a", 32): 3}
		for _, k := range []string{"k9", "k3", "k0", "k7", "k5", "k1", "k8", "k2", "k6", "k4"} {
			counters[k] = int64(k[1] - '0')
		}
		got := set(RunProgress{Counters: counters}).Counters
		assert.Equal(t, map[string]int64{
			strings.Repeat("a", 32): 3, "k0": 0, "k1": 1, "k2": 2, "k3": 3, "k4": 4, "k5": 5, "k6": 6,
		}, got, "an empty key and one over 32 bytes are dropped; of the rest, the first eight by key")
		assert.Len(t, got, MaxProgressCounters)
	})

	t.Run("the value is a copy", func(t *testing.T) {
		t.Parallel()
		j := &Job[progressArgs]{progress: newProgressSink()}
		counters := map[string]int64{"done": 1}
		j.SetProgress(RunProgress{Counters: counters})
		counters["done"] = 99
		assert.Equal(t, map[string]int64{"done": 1}, j.progress.latest().Counters)
	})

	t.Run("a Job built by hand ignores it", func(t *testing.T) {
		t.Parallel()
		var byHand Job[progressArgs]
		assert.NotPanics(t, func() { byHand.SetProgress(RunProgress{Message: "ignored"}) })
		var none *Job[progressArgs]
		assert.NotPanics(t, func() { none.SetProgress(RunProgress{Message: "ignored"}) })
	})

	t.Run("it is safe for concurrent use", func(t *testing.T) {
		t.Parallel()
		j := &Job[progressArgs]{progress: newProgressSink()}
		var wg sync.WaitGroup
		for g := range 8 {
			wg.Go(func() {
				for i := range 100 {
					j.SetProgress(RunProgress{Counters: map[string]int64{"g": int64(g), "i": int64(i)}})
				}
			})
		}
		wg.Wait()
		assert.Len(t, j.progress.latest().Counters, 2)
	})
}

// TestProgressIsWrittenAtMostOncePerHeartbeat proves the heartbeat carries
// progress cheaply: a worker reporting far more often than the heartbeat costs
// at most one write per renewal, the last value set reaches the database, and a
// progress that stopped changing is not written again.
func TestProgressIsWrittenAtMostOncePerHeartbeat(t *testing.T) {
	t.Parallel()
	db := newWALFileDB(t)
	writes := countProgressWrites(t, db)
	driver := &countingRenewDriver{Driver: NewSQLiteDriver(db)}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	steady := make(chan struct{})
	release := make(chan struct{})
	var last atomic.Int64
	w := &progressWorker{work: func(_ context.Context, job *Job[progressArgs]) (Result, error) {
		// A burst of 500 reports, far inside one heartbeat...
		i := int64(0)
		for range 500 {
			i++
			job.SetProgress(RunProgress{Counters: map[string]int64{"done": i}})
		}
		// ...then a report every millisecond across ten heartbeats...
		for deadline := time.Now().Add(10 * progressHeartbeat); time.Now().Before(deadline); {
			i++
			job.SetProgress(RunProgress{Counters: map[string]int64{"done": i}})
			time.Sleep(time.Millisecond)
		}
		// ...then the same value again, which is no change.
		job.SetProgress(RunProgress{Counters: map[string]int64{"done": i}})
		last.Store(i)
		close(steady)
		<-release
		return Result{}, nil
	}}
	id, err := Insert(ctx, NewClient(db), progressArgs{}, InsertOpts{})
	require.NoError(t, err)
	wait := drainInBackground(t, ctx, progressRunner(t, db, driver, w))

	<-steady
	require.Eventually(t, func() bool {
		p := storedProgress(t, db, id)
		return p != nil && p.Counters["done"] == last.Load()
	}, 5*time.Second, 5*time.Millisecond, "the last value set reaches the database")
	written, renewed := writes.Load(), driver.renewals.Load()
	assert.GreaterOrEqual(t, written, int64(1))
	assert.LessOrEqual(t, written, renewed, "at most one progress write per renewal")

	require.Eventually(t, func() bool { return driver.renewals.Load() >= renewed+10 },
		5*time.Second, 5*time.Millisecond, "the heartbeat keeps renewing")
	assert.Equal(t, written, writes.Load(), "a progress that stopped changing is not written again")

	close(release)
	wait()
	p := storedProgress(t, db, id)
	require.NotNil(t, p, "the finished run keeps the last progress written")
	assert.Equal(t, last.Load(), p.Counters["done"])
}

// TestProgressShowsOnRunsAndRunningJobs proves where progress is read: on the
// running list while the attempt runs, on the run through ListRuns, LatestRun,
// and GetJob, and on the run after it ends; a run that never reported has none,
// and a stored value that does not decode reads as none rather than an error.
func TestProgressShowsOnRunsAndRunningJobs(t *testing.T) {
	t.Parallel()
	db := newWALFileDB(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	half := 40.0
	want := RunProgress{Percent: &half, Message: "halfway", Counters: map[string]int64{"rows": 4000}}
	release := make(chan struct{})
	w := &progressWorker{work: func(_ context.Context, job *Job[progressArgs]) (Result, error) {
		if job.Args.V == "quiet" {
			return Result{}, nil
		}
		job.SetProgress(want)
		<-release
		return Result{}, nil
	}}
	id, err := Insert(ctx, NewClient(db), progressArgs{V: "reports"}, InsertOpts{})
	require.NoError(t, err)
	wait := drainInBackground(t, ctx, progressRunner(t, db, NewSQLiteDriver(db), w))

	var running []RunningJob
	require.Eventually(t, func() bool {
		running, err = ListRunning(ctx, db, ListRunningParams{})
		require.NoError(t, err)
		return len(running) == 1 && running[0].Progress != nil
	}, 5*time.Second, 5*time.Millisecond, "the running list shows the progress within a heartbeat")
	assert.Equal(t, want, *running[0].Progress)

	runs, err := ListRuns(ctx, db, id, ListRunsParams{})
	require.NoError(t, err)
	require.Len(t, runs, 1)
	require.NotNil(t, runs[0].Progress)
	assert.Equal(t, want, *runs[0].Progress)
	require.NotNil(t, runs[0].ProgressAt, "with when the heartbeat wrote it")
	assert.False(t, runs[0].ProgressAt.Before(runs[0].StartedAt))
	latest, ok, err := LatestRun(ctx, db, id)
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, runs[0].Progress, latest.Progress)
	detail, err := GetJob(ctx, db, id)
	require.NoError(t, err)
	require.NotNil(t, detail.LatestRun)
	assert.Equal(t, runs[0].Progress, detail.LatestRun.Progress)
	assert.Equal(t, runs[0].ProgressAt, detail.LatestRun.ProgressAt)

	close(release)
	wait()
	runs, err = ListRuns(ctx, db, id, ListRunsParams{})
	require.NoError(t, err)
	require.NotNil(t, runs[0].Progress, "the finished run keeps its last progress")
	assert.Equal(t, want, *runs[0].Progress)

	quiet, err := Insert(ctx, NewClient(db), progressArgs{V: "quiet"}, InsertOpts{})
	require.NoError(t, err)
	require.NoError(t, progressRunner(t, db, NewSQLiteDriver(db), w).RunUntilIdle(ctx))
	runs, err = ListRuns(ctx, db, quiet, ListRunsParams{})
	require.NoError(t, err)
	require.Len(t, runs, 1)
	assert.Nil(t, runs[0].Progress, "a run that never reported has no progress")
	assert.Nil(t, runs[0].ProgressAt)

	require.NoError(t, db.Model(&jobRunRow{}).Where("id = ?", runs[0].ID).
		Update("progress", `{"percent":"most of it"}`).Error)
	runs, err = ListRuns(ctx, db, quiet, ListRunsParams{})
	require.NoError(t, err, "a stored value that does not decode is not an error")
	assert.Nil(t, runs[0].Progress, "it reads as none")
}
