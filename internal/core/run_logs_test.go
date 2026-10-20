package core

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mrz1836/go-foundation/ctxutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/datatypes"
	"gorm.io/gorm"
)

// runLogHeartbeat is the run-log tests' renewal cadence.
const runLogHeartbeat = 20 * time.Millisecond

// runnerLogKeys are the attributes the Runner binds onto Job.Logger, which a
// captured line never carries.
//
//nolint:gochecknoglobals // shared expectation fixture
var runnerLogKeys = []string{"job_id", "kind", "queue", "run_id", "attempt", "max_attempts", "request_id"}

// runLogArgs is the args of the run-log tests' jobs.
type runLogArgs struct{ V string }

func (runLogArgs) Kind() string { return "test.runlogs" }

// runLogWorker runs the body a test gives it.
type runLogWorker struct {
	work func(ctx context.Context, job *Job[runLogArgs]) (Result, error)
}

func (*runLogWorker) Kind() string { return "test.runlogs" }

func (w *runLogWorker) Work(ctx context.Context, job *Job[runLogArgs]) (Result, error) {
	return w.work(ctx, job)
}

// logging returns a worker that runs log against its job's logger and
// succeeds.
func logging(log func(l *slog.Logger)) *runLogWorker {
	return &runLogWorker{work: func(_ context.Context, job *Job[runLogArgs]) (Result, error) {
		log(job.Logger)
		return Result{}, nil
	}}
}

// runLogRunner builds a Runner over d that captures at the zero RunLogCapture,
// renews every runLogHeartbeat, and logs nowhere; tune adjusts the config.
func runLogRunner(t *testing.T, db *gorm.DB, d Driver, w *runLogWorker, tune func(*RunnerConfig)) *Runner {
	t.Helper()
	reg := NewRegistry()
	Register[runLogArgs](reg, w)
	cfg := RunnerConfig{
		DB: db, Driver: d, Registry: reg,
		Queues: []string{"default"}, ClaimAnyClass: true, Concurrency: 1,
		LeaseDuration:     5 * time.Second,
		HeartbeatInterval: runLogHeartbeat,
		PollInterval:      5 * time.Millisecond,
		QueueStateRefresh: -1,
		Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
		CaptureLogs:       &RunLogCapture{},
	}
	if tune != nil {
		tune(&cfg)
	}
	r, err := NewRunner(cfg)
	require.NoError(t, err)
	return r
}

// runLoggingJob inserts one job, with a request id, runs it to completion
// under a Runner runLogRunner builds, and returns its id and its run's id.
func runLoggingJob(t *testing.T, db *gorm.DB, d Driver, w *runLogWorker, tune func(*RunnerConfig)) (jobID, runID string) {
	t.Helper()
	ctx := ctxutil.WithRequestID(context.Background(), "req-logs")
	id, err := Insert(ctx, NewClient(db), runLogArgs{}, InsertOpts{})
	require.NoError(t, err)
	require.NoError(t, runLogRunner(t, db, d, w, tune).RunUntilIdle(context.Background()))
	run, ok, err := LatestRun(ctx, db, id)
	require.NoError(t, err)
	require.True(t, ok, "the job ran")
	return id, run.ID
}

// runLogLines reads every line of runID, failing the test on an error.
func runLogLines(t *testing.T, db *gorm.DB, runID string) []RunLogLine {
	t.Helper()
	lines, err := ListRunLogs(context.Background(), db, runID, ListRunLogsParams{})
	require.NoError(t, err)
	return lines
}

// runLogSeqs returns the sequence numbers of lines, in order.
func runLogSeqs(lines []RunLogLine) []int {
	out := make([]int, len(lines))
	for i, l := range lines {
		out[i] = l.Seq
	}
	return out
}

// runLogMessages returns the messages of lines, in order.
func runLogMessages(lines []RunLogLine) []string {
	out := make([]string, len(lines))
	for i, l := range lines {
		out[i] = l.Message
	}
	return out
}

// hostLog is a host's logger output: every record its handler handles, one
// text line each — level, message, and every attribute, those bound with With
// and WithGroup included — without the time, and with the ids that differ from
// run to run masked, so two runs compare record for record.
type hostLog struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

// Write appends one rendered record.
func (h *hostLog) Write(p []byte) (int, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.buf.Write(p)
}

// logger returns a logger writing to h at level.
func (h *hostLog) logger(level slog.Level) *slog.Logger {
	return slog.New(slog.NewTextHandler(h, &slog.HandlerOptions{
		Level: level,
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			switch {
			case len(groups) > 0:
				return a
			case a.Key == slog.TimeKey:
				return slog.Attr{}
			case a.Key == "job_id" || a.Key == "run_id":
				return slog.String(a.Key, "*")
			}
			return a
		},
	}))
}

// String returns everything h received.
func (h *hostLog) String() string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.buf.String()
}

// fiveLines is the capture tests' worker body: a debug line, an info line
// with attributes, a line through a logger with an attribute and a group, a
// warn line, and an error line.
func fiveLines(l *slog.Logger) {
	l.Debug("debug line")
	l.Info("info line", "rows", 42, "ok", true)
	l.With("tenant", "t1").WithGroup("req").Info("grouped line", "status", 200)
	l.Warn("warn line")
	l.Error("error line", "error", "boom")
}

// runLogsCaptureSuite proves what capture keeps, through the database open
// returns: the lines at or above MinLevel, with their levels, messages, times,
// and the worker's attributes, never the Runner's; the host's handler receives
// exactly what it receives without capture; and with capture off nothing is
// written.
func runLogsCaptureSuite(t *testing.T, open func(t *testing.T) (*gorm.DB, Driver)) {
	t.Helper()

	t.Run("the lines at or above MinLevel are kept with the worker's attributes", func(t *testing.T) {
		t.Parallel()
		db, d := open(t)
		before := time.Now().UTC()
		_, runID := runLoggingJob(t, db, d, logging(fiveLines), func(c *RunnerConfig) {
			c.CaptureLogs = &RunLogCapture{MinLevel: slog.LevelInfo}
		})
		after := time.Now().UTC()

		lines := runLogLines(t, db, runID)
		require.Len(t, lines, 4, "the debug line is below MinLevel")
		assert.Equal(t, []int{1, 2, 3, 4}, runLogSeqs(lines))
		assert.Equal(t, []string{"info line", "grouped line", "warn line", "error line"}, runLogMessages(lines))
		levels := make([]string, len(lines))
		for i, l := range lines {
			levels[i] = l.Level
		}
		assert.Equal(t, []string{"INFO", "INFO", "WARN", "ERROR"}, levels)
		assert.JSONEq(t, `{"rows":42,"ok":true}`, string(lines[0].Attrs))
		assert.JSONEq(t, `{"tenant":"t1","req":{"status":200}}`, string(lines[1].Attrs))
		assert.Nil(t, lines[2].Attrs, "a line with no attributes has none")
		assert.JSONEq(t, `{"error":"boom"}`, string(lines[3].Attrs))
		for _, l := range lines {
			assert.Equal(t, time.UTC, l.LoggedAt.Location(), "seq %d is in UTC", l.Seq)
			assert.False(t, l.LoggedAt.Before(before.Truncate(time.Millisecond)), "seq %d logged in the attempt", l.Seq)
			assert.False(t, l.LoggedAt.After(after), "seq %d logged in the attempt", l.Seq)
			if l.Attrs == nil {
				continue
			}
			var attrs map[string]any
			require.NoError(t, json.Unmarshal(l.Attrs, &attrs))
			for _, key := range runnerLogKeys {
				assert.NotContains(t, attrs, key, "seq %d carries no attribute the Runner adds", l.Seq)
			}
		}
	})

	t.Run("a worker's own attribute named like the Runner's is kept", func(t *testing.T) {
		t.Parallel()
		db, d := open(t)
		_, runID := runLoggingJob(t, db, d, logging(func(l *slog.Logger) {
			l.Info("mine", "job_id", "set-by-the-worker")
		}), nil)
		lines := runLogLines(t, db, runID)
		require.Len(t, lines, 1)
		assert.JSONEq(t, `{"job_id":"set-by-the-worker"}`, string(lines[0].Attrs))
	})

	t.Run("the host's handler receives what it receives without capture", func(t *testing.T) {
		t.Parallel()
		for _, level := range []slog.Level{slog.LevelDebug, slog.LevelWarn} {
			received := func(capture *RunLogCapture) string {
				db, d := open(t)
				host := &hostLog{}
				runLoggingJob(t, db, d, logging(fiveLines), func(c *RunnerConfig) {
					c.Logger = host.logger(level)
					c.CaptureLogs = capture
					// Renewal off: a heartbeat tick that lands between the finalize's
					// commit and its stop logs a lost claim to this same handler, and
					// whether one lands is timing, not capture.
					c.HeartbeatInterval = -1
				})
				return host.String()
			}
			with, without := received(&RunLogCapture{MinLevel: slog.LevelInfo}), received(nil)
			assert.Equal(t, without, with, "at %s the host's handler sees the same records either way", level)
			assert.Equal(t, level == slog.LevelDebug, strings.Contains(with, "debug line"),
				"at %s the debug line reaches the host only when its level allows it", level)
			assert.Contains(t, with, "error line")
			assert.Contains(t, with, "request_id=req-logs", "the host still receives the Runner's attributes")
		}
	})

	t.Run("without capture nothing is written", func(t *testing.T) {
		t.Parallel()
		db, d := open(t)
		runLoggingJob(t, db, d, logging(fiveLines), func(c *RunnerConfig) { c.CaptureLogs = nil })
		var n int64
		require.NoError(t, db.Model(&jobRunLogRow{}).Count(&n).Error)
		assert.Zero(t, n)
	})
}

// TestRunLogsCaptureTheWorkersLinesAndAttrs runs the capture suite on SQLite.
func TestRunLogsCaptureTheWorkersLinesAndAttrs(t *testing.T) {
	t.Parallel()
	runLogsCaptureSuite(t, func(t *testing.T) (*gorm.DB, Driver) {
		t.Helper()
		db := newDB(t)
		return db, NewSQLiteDriver(db)
	})
}

// TestListRunLogsPagesByLevelAndSequence proves the read: lines after a
// sequence number, at or above a level, a page at a time within MaxReadLimit,
// each level named as slog names it; an unknown run reads as no lines, and a
// bad request is refused before any read.
func TestListRunLogsPagesByLevelAndSequence(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	ctx := context.Background()
	_, runID := runLoggingJob(t, db, NewSQLiteDriver(db), logging(func(l *slog.Logger) {
		l.Debug("one")
		l.Info("two")
		l.Warn("three")
		l.Info("four")
		l.Error("five")
		l.Debug("six")
	}), func(c *RunnerConfig) { c.CaptureLogs = &RunLogCapture{MinLevel: slog.LevelDebug} })
	read := func(p ListRunLogsParams) []RunLogLine {
		t.Helper()
		lines, err := ListRunLogs(ctx, db, runID, p)
		require.NoError(t, err)
		return lines
	}

	assert.Equal(t, []int{1, 2, 3, 4, 5, 6}, runLogSeqs(read(ListRunLogsParams{})), "six captured lines")
	assert.Equal(t, []int{3, 4, 5, 6}, runLogSeqs(read(ListRunLogsParams{AfterSeq: 2})))
	warn := slog.LevelWarn
	atWarn := read(ListRunLogsParams{MinLevel: &warn})
	assert.Equal(t, []string{"three", "five"}, runLogMessages(atWarn))
	assert.Equal(t, "WARN", atWarn[0].Level)
	assert.Equal(t, "ERROR", atWarn[1].Level)
	assert.Equal(t, []int{1, 2}, runLogSeqs(read(ListRunLogsParams{Limit: 2})))
	assert.Equal(t, []int{5}, runLogSeqs(read(ListRunLogsParams{AfterSeq: 4, Limit: 1})))
	assert.Equal(t, "DEBUG", read(ListRunLogsParams{})[0].Level)

	// A run with more lines than a page, one of them at a level between slog's
	// named ones.
	rows := make([]jobRunLogRow, 250)
	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for i := range rows {
		rows[i] = jobRunLogRow{
			RunID: "paged-run", Seq: int64(i + 1), JobID: "paged-job", LoggedAt: at.Add(time.Duration(i) * time.Millisecond),
			Level: int64(slog.LevelInfo), Message: fmt.Sprintf("line %d", i+1), Attrs: datatypes.JSON(`{}`),
		}
	}
	rows[0].Level = int64(slog.LevelInfo + 2)
	require.NoError(t, db.CreateInBatches(rows, 100).Error)
	paged := func(p ListRunLogsParams) []RunLogLine {
		t.Helper()
		lines, err := ListRunLogs(ctx, db, "paged-run", p)
		require.NoError(t, err)
		return lines
	}
	assert.Len(t, paged(ListRunLogsParams{}), 200, "a zero Limit reads 200")
	assert.Len(t, paged(ListRunLogsParams{Limit: 500}), MaxReadLimit, "a larger Limit reads MaxReadLimit")
	assert.Len(t, paged(ListRunLogsParams{AfterSeq: 200}), 50)
	first := paged(ListRunLogsParams{Limit: 1})
	require.Len(t, first, 1)
	assert.Equal(t, "INFO+2", first[0].Level, "a level between the named ones reads as slog names it")
	assert.True(t, at.Equal(first[0].LoggedAt), "logged at %s, read back as %s", at, first[0].LoggedAt)

	unknown, err := ListRunLogs(ctx, db, "no-such-run", ListRunLogsParams{})
	require.NoError(t, err)
	assert.NotNil(t, unknown, "an unknown run reads as an empty slice")
	assert.Empty(t, unknown)

	var verr *ValidationError
	_, err = ListRunLogs(ctx, db, runID, ListRunLogsParams{AfterSeq: -1})
	require.ErrorAs(t, err, &verr)
	assert.Equal(t, "after_seq", verr.Field)
	_, err = ListRunLogs(ctx, db, "", ListRunLogsParams{})
	require.ErrorAs(t, err, &verr)
	assert.Equal(t, "run_id", verr.Field)
}

// TestRunLogsStopAtTheirBoundsWithATruncationLine proves capture is bounded:
// past MaxLines or MaxBytes every later line is dropped and counted, and the
// attempt's last write adds one warn line saying how many; zero bounds select
// the defaults and larger ones read the maxima.
func TestRunLogsStopAtTheirBoundsWithATruncationLine(t *testing.T) {
	t.Parallel()

	assertTruncation := func(t *testing.T, l RunLogLine, seq, dropped int) {
		t.Helper()
		assert.Equal(t, seq, l.Seq)
		assert.Equal(t, "WARN", l.Level)
		assert.Equal(t, "jobs: run log truncated", l.Message)
		assert.JSONEq(t, fmt.Sprintf(`{"dropped_lines":%d}`, dropped), string(l.Attrs))
	}

	t.Run("past MaxLines", func(t *testing.T) {
		t.Parallel()
		db := newDB(t)
		_, runID := runLoggingJob(t, db, NewSQLiteDriver(db), logging(func(l *slog.Logger) {
			for i := 1; i <= 25; i++ {
				l.Info(fmt.Sprintf("line %d", i))
			}
		}), func(c *RunnerConfig) { c.CaptureLogs = &RunLogCapture{MaxLines: 10} })

		lines := runLogLines(t, db, runID)
		require.Len(t, lines, 11, "ten lines kept and one truncation line")
		for i, l := range lines[:10] {
			assert.Equal(t, fmt.Sprintf("line %d", i+1), l.Message)
		}
		assertTruncation(t, lines[10], 11, 15)
	})

	t.Run("past MaxBytes, counting each line's message and attributes", func(t *testing.T) {
		t.Parallel()
		db := newDB(t)
		// Each line is 150 bytes of message and 60 of attributes: four fit in
		// 1,024 bytes (840), a fifth would pass it (1,050). Counted by message
		// alone, six would fit.
		msg, pad := strings.Repeat("m", 150), strings.Repeat("p", 50)
		_, runID := runLoggingJob(t, db, NewSQLiteDriver(db), logging(func(l *slog.Logger) {
			for range 7 {
				l.Info(msg, "pad", pad)
			}
			l.Info("t") // would fit on its own, but the log is already truncated
		}), func(c *RunnerConfig) { c.CaptureLogs = &RunLogCapture{MaxBytes: 1024} })

		lines := runLogLines(t, db, runID)
		require.Len(t, lines, 5, "four lines kept and one truncation line")
		for _, l := range lines[:4] {
			assert.Equal(t, msg, l.Message)
		}
		assertTruncation(t, lines[4], 5, 4)
	})

	t.Run("zero bounds select the defaults, and larger ones the maxima", func(t *testing.T) {
		t.Parallel()
		db := newDB(t)
		_, runID := runLoggingJob(t, db, NewSQLiteDriver(db), logging(func(l *slog.Logger) {
			for i := 1; i <= DefaultRunLogLines+1; i++ {
				l.Info("x")
			}
		}), func(c *RunnerConfig) { c.CaptureLogs = &RunLogCapture{} })
		lines, err := ListRunLogs(context.Background(), db, runID,
			ListRunLogsParams{AfterSeq: DefaultRunLogLines - 1})
		require.NoError(t, err)
		require.Len(t, lines, 2, "the last line kept, then the truncation line")
		assert.Equal(t, DefaultRunLogLines, lines[0].Seq)
		assertTruncation(t, lines[1], DefaultRunLogLines+1, 1)

		assert.Equal(t, DefaultRunLogBytes, RunLogCapture{}.bytes())
		assert.Equal(t, MaxRunLogLines, RunLogCapture{MaxLines: MaxRunLogLines + 1}.lines())
		assert.Equal(t, MaxRunLogBytes, RunLogCapture{MaxBytes: MaxRunLogBytes + 1}.bytes())
	})
}

// failRunLogWrites makes the inserts into job_run_logs that fail decides fail,
// counting each insert from 1.
func failRunLogWrites(t *testing.T, db *gorm.DB, fail func(n int64) bool) {
	t.Helper()
	var n atomic.Int64
	require.NoError(t, db.Callback().Create().Before("gorm:create").Register("test:fail_run_log_writes",
		func(tx *gorm.DB) {
			if tx.Statement.Table == "job_run_logs" && fail(n.Add(1)) {
				_ = tx.AddError(errors.New("injected run log write failure"))
			}
		}))
}

// TestRunLogsAreWrittenByTheHeartbeatAndAfterTheFinalize proves when lines reach
// the database: on the heartbeat while the attempt runs, and once after it
// ends; all at the end with renewal disabled; a failed write's lines by the
// next one, each sequence number once; and a failed last write logged.
func TestRunLogsAreWrittenByTheHeartbeatAndAfterTheFinalize(t *testing.T) {
	t.Parallel()

	// pausing is a worker that logs early lines, signals logged, waits for
	// release, then logs one late line.
	pausing := func(early int, logged, release chan struct{}) *runLogWorker {
		return &runLogWorker{work: func(_ context.Context, job *Job[runLogArgs]) (Result, error) {
			for i := 1; i <= early; i++ {
				job.Logger.Info(fmt.Sprintf("early %d", i))
			}
			close(logged)
			<-release
			job.Logger.Info("late")
			return Result{}, nil
		}}
	}
	start := func(t *testing.T, db *gorm.DB, w *runLogWorker, tune func(*RunnerConfig)) (string, func()) {
		t.Helper()
		ctx := context.Background()
		id, err := Insert(ctx, NewClient(db), runLogArgs{}, InsertOpts{})
		require.NoError(t, err)
		return id, drainInBackground(t, ctx, runLogRunner(t, db, NewSQLiteDriver(db), w, tune))
	}
	runOf := func(t *testing.T, db *gorm.DB, jobID string) string {
		t.Helper()
		run, ok, err := LatestRun(context.Background(), db, jobID)
		require.NoError(t, err)
		require.True(t, ok)
		return run.ID
	}

	t.Run("the heartbeat writes lines while the attempt runs", func(t *testing.T) {
		t.Parallel()
		db := newWALFileDB(t)
		logged, release := make(chan struct{}), make(chan struct{})
		id, wait := start(t, db, pausing(2, logged, release), nil)
		<-logged
		runID := runOf(t, db, id)
		require.Eventually(t, func() bool { return len(runLogLines(t, db, runID)) == 2 },
			5*time.Second, 5*time.Millisecond, "the early lines are readable while the attempt runs")
		assert.Equal(t, string(StateRunning), jobState(t, db, id))

		close(release)
		wait()
		assert.Equal(t, []string{"early 1", "early 2", "late"}, runLogMessages(runLogLines(t, db, runID)),
			"the rest once it ends")
	})

	t.Run("with renewal disabled every line is written once the attempt ends", func(t *testing.T) {
		t.Parallel()
		db := newWALFileDB(t)
		logged, release := make(chan struct{}), make(chan struct{})
		id, wait := start(t, db, pausing(2, logged, release), func(c *RunnerConfig) { c.HeartbeatInterval = -1 })
		<-logged
		runID := runOf(t, db, id)
		time.Sleep(5 * runLogHeartbeat)
		assert.Empty(t, runLogLines(t, db, runID), "nothing is written while the attempt runs")

		close(release)
		wait()
		assert.Equal(t, []int{1, 2, 3}, runLogSeqs(runLogLines(t, db, runID)))
	})

	t.Run("a failed write's lines are written by the next, each once", func(t *testing.T) {
		t.Parallel()
		db := newWALFileDB(t)
		// The first write takes two statements of 500 lines and less; failing the
		// second leaves the first half written, or rolled back with it, and the
		// retry must neither repeat nor lose a line.
		failRunLogWrites(t, db, func(n int64) bool { return n == 2 })
		host := &hostLog{}
		logged, release := make(chan struct{}), make(chan struct{})
		id, wait := start(t, db, pausing(700, logged, release), func(c *RunnerConfig) {
			c.Logger = host.logger(slog.LevelInfo)
		})
		<-logged
		runID := runOf(t, db, id)
		require.Eventually(t, func() bool { return len(runLogLines(t, db, runID)) > 0 },
			5*time.Second, 5*time.Millisecond, "a later heartbeat writes what the failed one held")
		close(release)
		wait()

		var seqs []int64
		require.NoError(t, db.Model(&jobRunLogRow{}).Where("run_id = ?", runID).Order("seq").Pluck("seq", &seqs).Error)
		require.Len(t, seqs, 701)
		for i, seq := range seqs {
			require.EqualValues(t, i+1, seq, "every sequence number once")
		}
		assert.Contains(t, host.String(), "jobs: run log write failed")
	})

	t.Run("a failed last write is logged", func(t *testing.T) {
		t.Parallel()
		db := newWALFileDB(t)
		failRunLogWrites(t, db, func(int64) bool { return true })
		host := &hostLog{}
		jobID, runID := runLoggingJob(t, db, NewSQLiteDriver(db), logging(func(l *slog.Logger) { l.Info("lost") }),
			func(c *RunnerConfig) {
				c.HeartbeatInterval = -1
				c.Logger = host.logger(slog.LevelInfo)
			})
		assert.Equal(t, string(StateSucceeded), jobState(t, db, jobID), "a failed write never fails the attempt")
		assert.Empty(t, runLogLines(t, db, runID))
		out := host.String()
		assert.Contains(t, out, "level=WARN")
		assert.Contains(t, out, "jobs: run log write failed")
		assert.Contains(t, out, "run_id=*")
	})
}
