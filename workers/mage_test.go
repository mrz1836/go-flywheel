package workers_test

import (
	"context"
	"testing"
	"time"

	flywheel "github.com/mrz1836/go-flywheel"
	"github.com/mrz1836/go-flywheel/workers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mageJob wraps args in the Job shape MageWorker.Work expects.
func mageJob(a workers.MageArgs) *flywheel.Job[workers.MageArgs] {
	return &flywheel.Job[workers.MageArgs]{Kind: workers.MageKind, Args: a}
}

func TestMageWorkerKind(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "mage", workers.MageArgs{}.Kind())
	assert.Equal(t, "mage", workers.MageWorker{}.Kind())
}

// Pointing Binary at printf keeps the execution path deterministic without invoking
// the (side-effecting) real magex/mage. The exact magex command line is asserted in
// the white-box TestMageInvocation.
func TestMageWorkerRunsResolvedBinary(t *testing.T) {
	t.Parallel()
	res, err := workers.MageWorker{}.Work(context.Background(),
		mageJob(workers.MageArgs{Binary: "printf", Targets: []string{"mage-ok"}}))
	require.NoError(t, err)
	assert.Equal(t, "mage-ok", res.Output.(workers.ExecOutput).Stdout)
}

func TestMageWorkerBinaryOverrideWins(t *testing.T) {
	t.Parallel()
	// The worker's Binary overrides MageArgs.Binary: printf runs, not sh.
	res, err := workers.MageWorker{Binary: "printf"}.Work(context.Background(),
		mageJob(workers.MageArgs{Binary: "sh", Targets: []string{"override-ok"}}))
	require.NoError(t, err)
	assert.Equal(t, "override-ok", res.Output.(workers.ExecOutput).Stdout)
}

func TestMageWorkerRequiresTarget(t *testing.T) {
	t.Parallel()
	_, err := workers.MageWorker{}.Work(context.Background(), mageJob(workers.MageArgs{}))
	require.Error(t, err)
}

func TestMageWorkerNonZeroExitIsTransient(t *testing.T) {
	t.Parallel()
	w := workers.MageWorker{Binary: "sh"}
	_, err := w.Work(context.Background(), mageJob(workers.MageArgs{Targets: []string{"-c", "exit 4"}}))
	require.Error(t, err)
	assert.Equal(t, flywheel.ErrorTransient, w.Classify(err))
}

func TestMageWorkerPermanentExitCode(t *testing.T) {
	t.Parallel()
	w := workers.MageWorker{Binary: "sh", PermanentExitCodes: []int{2}}
	_, err := w.Work(context.Background(), mageJob(workers.MageArgs{Targets: []string{"-c", "exit 2"}}))
	require.Error(t, err)
	assert.Equal(t, flywheel.ErrorPermanent, w.Classify(err))
}

func TestMageWorkerMissingBinaryIsPermanent(t *testing.T) {
	t.Parallel()
	w := workers.MageWorker{}
	_, err := w.Work(context.Background(),
		mageJob(workers.MageArgs{Binary: "flywheel-no-such-magex-xyz", Targets: []string{"test"}}))
	require.Error(t, err)
	assert.Equal(t, flywheel.ErrorPermanent, w.Classify(err))
}

func TestMageWorkerTimeoutSurfacesDeadline(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	_, err := workers.MageWorker{Binary: "sh"}.Work(ctx,
		mageJob(workers.MageArgs{Targets: []string{"-c", "sleep 5"}}))
	require.ErrorIs(t, err, context.DeadlineExceeded)
}
