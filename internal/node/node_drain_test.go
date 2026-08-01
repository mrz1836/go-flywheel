package node

import (
	"context"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	core "github.com/mrz1836/go-flywheel/internal/core"
	ft "github.com/mrz1836/go-flywheel/internal/flywheeltest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type nodeDrainArgs struct{}

func (nodeDrainArgs) Kind() string { return "test.nodedrain" }

// nodeDrainWorker blocks until released *or* until its context ends, recording which
// happened.
//
// It is what distinguishes a drain from an abort, and it is why blockingWorker
// could not be reused: blockingWorker ignores its context entirely, so it cannot
// tell whether the host's cancel reached it. Under a run context derived from the
// host's, a cancel propagates into the worker and this reports cancelled.
type nodeDrainWorker struct {
	started   chan struct{}
	release   chan struct{}
	cancelled atomic.Bool
	completed atomic.Bool
}

func (*nodeDrainWorker) Kind() string { return "test.nodedrain" }

func (w *nodeDrainWorker) Work(ctx context.Context, _ *core.Job[nodeDrainArgs]) (core.Result, error) {
	close(w.started)
	select {
	case <-w.release:
		w.completed.Store(true)
		return core.Result{}, nil
	case <-ctx.Done():
		w.cancelled.Store(true)
		return core.Result{}, ctx.Err()
	}
}

// TestNodeAwaitDrainDrainsEachRunnerBeforeReturning proves a host cancel is a
// drain request rather than an abort.
//
// It is the test the detached run context exists for. Previously runCtx was a
// child of the caller's, so a cancel reached every worker's context: the job
// above would observe the cancellation, fail its attempt, and be retried later —
// while Run reported a clean drain. Now the runner is told to stop claiming, the
// in-flight job runs to its own completion, and only then does the node tear down.
func TestNodeAwaitDrainDrainsEachRunnerBeforeReturning(t *testing.T) {
	t.Parallel()
	db := ft.NewWALFileDB(t)
	reg := core.NewRegistry()
	w := &nodeDrainWorker{started: make(chan struct{}), release: make(chan struct{})}
	core.Register(reg, w)

	// No DrainTimeout: the drain waits for in-flight work however long it takes.
	node, err := NewNode(NodeConfig{Runners: []core.RunnerConfig{sqliteRunner(db, reg)}})
	require.NoError(t, err)

	id, err := core.Insert(context.Background(), core.NewClient(db), nodeDrainArgs{}, core.InsertOpts{})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	runErr := make(chan error, 1)
	go func() { runErr <- node.Run(ctx) }()

	<-w.started
	cancel()

	select {
	case err := <-runErr:
		t.Fatalf("Run returned while a job was still in flight: %v", err)
	case <-time.After(200 * time.Millisecond):
	}
	assert.False(t, w.cancelled.Load(), "the host cancel must not reach the worker's context")

	close(w.release)
	select {
	case err := <-runErr:
		require.NoError(t, err, "a drained node returns nil")
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after the job finished")
	}

	assert.True(t, w.completed.Load(), "the worker ran to its own completion")
	assert.Equal(t, string(core.StateSucceeded), ft.JobState(t, db, id),
		"the drained job recorded its success rather than being retried")
}

// TestNodeDrainTimeoutWarningNamesTheInFlightCount proves the timeout warning
// carries the number. "Some in-flight jobs may not have finished" is what the
// warning used to say, and it is the difference between a warning and a
// diagnostic — a host with a 30-second drain timeout and two-minute jobs needs to
// know how many it abandoned.
func TestNodeDrainTimeoutWarningNamesTheInFlightCount(t *testing.T) {
	t.Parallel()
	db := ft.NewWALFileDB(t)
	reg := core.NewRegistry()
	w := &blockingWorker{started: make(chan struct{}), release: make(chan struct{}), done: make(chan struct{})}
	core.Register(reg, w)

	rec := &ft.RecordingHandler{}
	node, err := NewNode(NodeConfig{
		Runners:      []core.RunnerConfig{sqliteRunner(db, reg)},
		DrainTimeout: 100 * time.Millisecond,
		Logger:       slog.New(rec),
	})
	require.NoError(t, err)

	_, err = core.Insert(context.Background(), core.NewClient(db), blockingArgs{}, core.InsertOpts{})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	runErr := make(chan error, 1)
	go func() { runErr <- node.Run(ctx) }()

	<-w.started
	cancel()

	select {
	case err := <-runErr:
		require.NoError(t, err, "Run returns on the drain timeout, not on the stuck worker")
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not return within the drain timeout")
	}

	var warned bool
	for _, entry := range rec.Records() {
		if entry["level"] != slog.LevelWarn.String() {
			continue
		}
		warned = true
		assert.EqualValues(t, 1, entry["in_flight"], "the warning names how many jobs were abandoned")
		assert.EqualValues(t, 0, entry["runner"], "and which runner they were on")
		assert.Equal(t, "100ms", entry["drain_timeout"])
	}
	assert.True(t, warned, "a drain that timed out logs a warning")

	// Release the stuck worker so its goroutine unwinds before the DB goes away.
	close(w.release)
	<-w.done
}
