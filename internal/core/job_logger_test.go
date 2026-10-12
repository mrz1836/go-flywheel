package core

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// lockedBuffer is a bytes.Buffer safe to write from a worker goroutine while the
// test reads it.
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

// lines decodes every JSON log line written so far.
func (b *lockedBuffer) lines(t *testing.T) []map[string]any {
	t.Helper()
	b.mu.Lock()
	defer b.mu.Unlock()
	var out []map[string]any
	sc := bufio.NewScanner(bytes.NewReader(b.buf.Bytes()))
	for sc.Scan() {
		var line map[string]any
		require.NoError(t, json.Unmarshal(sc.Bytes(), &line))
		out = append(out, line)
	}
	require.NoError(t, sc.Err())
	return out
}

type loggerArgs struct{}

func (loggerArgs) Kind() string { return "job_logger.worker" }

// loggerWorker logs one line through the logger the runner hands it, then fails
// its first attempt so the second attempt logs too.
type loggerWorker struct{}

func (loggerWorker) Kind() string { return "job_logger.worker" }

func (loggerWorker) NextRetry(error, int) time.Duration { return time.Millisecond }

func (loggerWorker) Work(_ context.Context, job *Job[loggerArgs]) (Result, error) {
	job.Logger.Info("worker line")
	if job.Attempt == 1 {
		return Result{}, errors.New("try again")
	}
	return Result{}, nil
}

// TestJobLoggerCarriesQueueAndAttempt reads the lines a worker writes through
// Job.Logger and expects each to carry the job, its kind, its queue, the run, the
// attempt, and the attempt budget, under the keys the slog observer's own lines
// use, so a log search can filter a worker's lines by queue or by attempt, and a
// line says whether its attempt was the last.
func TestJobLoggerCarriesQueueAndAttempt(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	ctx := context.Background()
	logs := &lockedBuffer{}

	reg := NewRegistry()
	Register[loggerArgs](reg, loggerWorker{})
	r := rwRunner(t, db, reg, func(cfg *RunnerConfig) {
		cfg.Logger = slog.New(slog.NewJSONHandler(logs, nil))
	})

	id, err := Insert(ctx, NewClient(db), loggerArgs{}, InsertOpts{Queue: "periodic", MaxAttempts: 2})
	require.NoError(t, err)
	runToIdle(t, ctx, r)
	require.Equal(t, string(StateSucceeded), jobState(t, db, id))

	runs, err := ListRuns(ctx, db, id, ListRunsParams{})
	require.NoError(t, err)
	require.Len(t, runs, 2)
	runIDs := map[float64]string{}
	for _, run := range runs {
		runIDs[float64(run.Attempt)] = run.ID
	}

	var worker []map[string]any
	for _, line := range logs.lines(t) {
		if line["msg"] == "worker line" {
			worker = append(worker, line)
		}
	}
	require.Len(t, worker, 2, "one worker line per attempt")
	for i, line := range worker {
		attempt := float64(i + 1)
		assert.Equal(t, id, line["job_id"])
		assert.Equal(t, "job_logger.worker", line["kind"])
		assert.Equal(t, "periodic", line["queue"], "the line carries the job's queue")
		assert.Equal(t, attempt, line["attempt"], "the line carries the attempt that wrote it")
		assert.Equal(t, float64(2), line["max_attempts"], "the line carries the job's attempt budget")
		assert.Equal(t, runIDs[attempt], line["run_id"])
	}
}
