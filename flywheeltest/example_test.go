package flywheeltest_test

import (
	"context"
	"testing"
	"time"

	flywheel "github.com/mrz1836/go-flywheel"
	"github.com/mrz1836/go-flywheel/flywheeltest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestExampleEnqueueAndWait shows the common flow a consumer embedding flywheel
// follows in its own tests: open a database carrying the runtime schema, run a
// node against it, enqueue a job through the public flywheel API, and wait for it
// to reach a terminal state — every step a one-liner from this package, with no
// bespoke harness.
func TestExampleEnqueueAndWait(t *testing.T) {
	t.Parallel()

	// A file-backed WAL database lets the runner write while the test polls.
	db := flywheeltest.NewWALFileDB(t)

	// Register a worker. SuccessWorker is the trivial always-succeeding worker the
	// package ships for exactly this kind of end-to-end check.
	reg := flywheel.NewRegistry()
	worker := &flywheeltest.SuccessWorker{}
	flywheel.Register(reg, worker)

	// A single SQLite runner drains the queue (SQLite serializes writers, so one
	// runner is the supported shape).
	node, err := flywheel.NewNode(flywheel.NodeConfig{
		Runners: []flywheel.RunnerConfig{{
			DB:            db,
			Driver:        flywheel.NewSQLiteDriver(db),
			Registry:      reg,
			Queues:        []string{"default"},
			ClaimAnyClass: true,
			PollInterval:  5 * time.Millisecond,
		}},
	})
	require.NoError(t, err)

	// Enqueue one job through the public API.
	id, err := flywheel.Insert(
		context.Background(),
		flywheel.NewClient(db),
		flywheeltest.SuccessArgs{V: "hello"},
		flywheel.InsertOpts{},
	)
	require.NoError(t, err)

	// Run the node in the background, then wait for the job to succeed.
	ctx, cancel := context.WithCancel(context.Background())
	runErr := make(chan error, 1)
	go func() { runErr <- node.Run(ctx) }()

	flywheeltest.WaitForJobState(t, db, id, string(flywheel.StateSucceeded), 5*time.Second)

	cancel()
	require.NoError(t, <-runErr)
	assert.EqualValues(t, 1, worker.Calls.Load(), "the enqueued job ran exactly once")
}
