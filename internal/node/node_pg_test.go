//go:build integration

package node

import (
	"context"
	"fmt"
	"testing"
	"time"

	ft "github.com/mrz1836/go-flywheel/flywheeltest"
	core "github.com/mrz1836/go-flywheel/internal/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestNodePGMultipleRunnersExactlyOnce builds one Node hosting two Postgres
// runners (concurrency 4 each) and asserts every enqueued job runs exactly once
// — SKIP LOCKED across both runners prevents any double-dispatch, and the Node
// drains cleanly on cancel.
func TestNodePGMultipleRunnersExactlyOnce(t *testing.T) {
	t.Parallel()
	db := ft.NewPostgresIsolatedDB(t)

	worker := &ft.SuccessWorker{}
	reg := core.NewRegistry()
	core.Register(reg, worker)

	const totalJobs = 80
	for i := range totalJobs {
		_, err := core.Insert(context.Background(), core.NewClient(db),
			ft.SuccessArgs{V: fmt.Sprintf("v%d", i)}, core.InsertOpts{})
		require.NoError(t, err)
	}

	mkRunner := func() core.RunnerConfig {
		return core.RunnerConfig{
			DB: db, Driver: core.NewPostgresDriver(db), Registry: reg,
			Queues: []string{"default", "periodic"}, ClaimAnyClass: true,
			Concurrency: 4, PollInterval: 5 * time.Millisecond,
		}
	}
	node, err := NewNode(NodeConfig{Runners: []core.RunnerConfig{mkRunner(), mkRunner()}})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	runErr := make(chan error, 1)
	go func() { runErr <- node.Run(ctx) }()

	succeeded := func() int {
		ov, e := core.Overview(context.Background(), db, core.OverviewParams{})
		require.NoError(t, e)
		return ov.CountsByState[string(core.StateSucceeded)]
	}
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) && succeeded() < totalJobs {
		time.Sleep(20 * time.Millisecond)
	}
	cancel()
	require.NoError(t, <-runErr)

	assert.EqualValues(t, totalJobs, worker.Calls.Load(), "every job ran exactly once across both Node runners")
	assert.EqualValues(t, totalJobs, succeeded())
}
