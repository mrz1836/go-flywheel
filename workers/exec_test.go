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

// execJob wraps args in the Job shape ExecWorker.Work expects.
func execJob(a workers.ExecArgs) *flywheel.Job[workers.ExecArgs] {
	return &flywheel.Job[workers.ExecArgs]{Kind: workers.ExecKind, Args: a}
}

func TestExecWorkerSuccessCapturesStreamsAndExitCode(t *testing.T) {
	t.Parallel()
	res, err := workers.ExecWorker{}.Work(context.Background(),
		execJob(workers.ExecArgs{Command: "sh", Args: []string{"-c", "printf hello; printf oops 1>&2"}}))
	require.NoError(t, err)

	out, ok := res.Output.(workers.ExecOutput)
	require.True(t, ok)
	assert.Equal(t, 0, out.ExitCode)
	assert.Equal(t, "hello", out.Stdout)
	assert.Equal(t, "oops", out.Stderr)
}

func TestExecWorkerNonZeroExitIsTransientErrorWithOutput(t *testing.T) {
	t.Parallel()
	w := workers.ExecWorker{}
	res, err := w.Work(context.Background(), execJob(workers.ExecArgs{Command: "sh", Args: []string{"-c", "exit 3"}}))
	require.Error(t, err)

	out, ok := res.Output.(workers.ExecOutput)
	require.True(t, ok)
	assert.Equal(t, 3, out.ExitCode, "the exit code is captured even on failure")
	assert.Equal(t, flywheel.ErrorTransient, w.Classify(err), "a non-zero exit is transient by default")
}

func TestExecWorkerPermanentExitCodeClassifiesPermanent(t *testing.T) {
	t.Parallel()
	w := workers.ExecWorker{PermanentExitCodes: []int{2}}

	_, err := w.Work(context.Background(), execJob(workers.ExecArgs{Command: "sh", Args: []string{"-c", "exit 2"}}))
	require.Error(t, err)
	assert.Equal(t, flywheel.ErrorPermanent, w.Classify(err))

	_, err5 := w.Work(context.Background(), execJob(workers.ExecArgs{Command: "sh", Args: []string{"-c", "exit 5"}}))
	require.Error(t, err5)
	assert.Equal(t, flywheel.ErrorTransient, w.Classify(err5), "an unlisted exit code stays transient")
}

func TestExecWorkerMissingCommandIsPermanent(t *testing.T) {
	t.Parallel()
	w := workers.ExecWorker{}
	_, err := w.Work(context.Background(), execJob(workers.ExecArgs{Command: "flywheel-no-such-command-xyz"}))
	require.Error(t, err)
	assert.Equal(t, flywheel.ErrorPermanent, w.Classify(err), "an unresolvable command will never succeed")
}

func TestExecWorkerRequiresCommand(t *testing.T) {
	t.Parallel()
	_, err := workers.ExecWorker{}.Work(context.Background(), execJob(workers.ExecArgs{}))
	require.Error(t, err)
}

func TestExecWorkerTimeoutSurfacesDeadline(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	_, err := workers.ExecWorker{}.Work(ctx, execJob(workers.ExecArgs{Command: "sh", Args: []string{"-c", "sleep 5"}}))
	require.Error(t, err)
	require.ErrorIs(t, err, context.DeadlineExceeded, "the Runner classifies this as a timeout")
}

func TestExecWorkerOutputIsCapped(t *testing.T) {
	t.Parallel()
	res, err := workers.ExecWorker{MaxOutputBytes: 1024}.Work(context.Background(),
		execJob(workers.ExecArgs{Command: "sh", Args: []string{"-c", "yes | head -c 100000"}}))
	require.NoError(t, err)
	assert.Len(t, res.Output.(workers.ExecOutput).Stdout, 1024, "captured stdout is capped at MaxOutputBytes")
}

func TestExecWorkerEnvAllowlist(t *testing.T) {
	// Not parallel: t.Setenv forbids it.
	t.Setenv("FW_EXEC_TEST_VAR", "secret-value")

	// Allowlisted: the variable reaches the child.
	res, err := workers.ExecWorker{EnvAllowlist: []string{"FW_EXEC_TEST_VAR"}}.Work(context.Background(),
		execJob(workers.ExecArgs{Command: "sh", Args: []string{"-c", `printf %s "$FW_EXEC_TEST_VAR"`}}))
	require.NoError(t, err)
	assert.Equal(t, "secret-value", res.Output.(workers.ExecOutput).Stdout)

	// Explicit empty allowlist: no host environment passes through.
	res2, err := workers.ExecWorker{EnvAllowlist: []string{}}.Work(context.Background(),
		execJob(workers.ExecArgs{Command: "sh", Args: []string{"-c", `printf %s "$FW_EXEC_TEST_VAR"`}}))
	require.NoError(t, err)
	assert.Empty(t, res2.Output.(workers.ExecOutput).Stdout, "an explicit empty allowlist drops host env")

	// Per-job Env always passes, even with an empty allowlist.
	res3, err := workers.ExecWorker{EnvAllowlist: []string{}}.Work(context.Background(),
		execJob(workers.ExecArgs{Command: "sh", Args: []string{"-c", `printf %s "$EXTRA"`}, Env: map[string]string{"EXTRA": "x"}}))
	require.NoError(t, err)
	assert.Equal(t, "x", res3.Output.(workers.ExecOutput).Stdout)
}
