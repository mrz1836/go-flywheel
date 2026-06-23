package workers_test

import (
	"context"
	"os/exec"
	"testing"
	"time"

	flywheel "github.com/mrz1836/go-flywheel"
	"github.com/mrz1836/go-flywheel/workers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// pythonJob wraps args in the Job shape PythonWorker.Work expects.
func pythonJob(a workers.PythonArgs) *flywheel.Job[workers.PythonArgs] {
	return &flywheel.Job[workers.PythonArgs]{Kind: workers.PythonKind, Args: a}
}

func TestPythonWorkerKind(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "python", workers.PythonArgs{}.Kind())
	assert.Equal(t, "python", workers.PythonWorker{}.Kind())
}

// Using sh as a stand-in interpreter keeps the execution path deterministic on any
// host (sh -c <snippet> mirrors python -c). The exact python command line is
// asserted in the white-box TestPythonInvocation.
func TestPythonWorkerInlineRuns(t *testing.T) {
	t.Parallel()
	res, err := workers.PythonWorker{Interpreter: "sh"}.Work(context.Background(),
		pythonJob(workers.PythonArgs{Inline: "printf py-ok"}))
	require.NoError(t, err)
	assert.Equal(t, "py-ok", res.Output.(workers.ExecOutput).Stdout)
}

func TestPythonWorkerRequiresExactlyOneSource(t *testing.T) {
	t.Parallel()
	_, err := workers.PythonWorker{}.Work(context.Background(), pythonJob(workers.PythonArgs{}))
	require.Error(t, err, "no source")

	_, err = workers.PythonWorker{}.Work(context.Background(),
		pythonJob(workers.PythonArgs{Script: "a.py", Inline: "print(1)"}))
	require.Error(t, err, "two sources")
}

func TestPythonWorkerNonZeroExitIsTransient(t *testing.T) {
	t.Parallel()
	w := workers.PythonWorker{Interpreter: "sh"}
	_, err := w.Work(context.Background(), pythonJob(workers.PythonArgs{Inline: "exit 4"}))
	require.Error(t, err)
	assert.Equal(t, flywheel.ErrorTransient, w.Classify(err))
}

func TestPythonWorkerMissingInterpreterIsPermanent(t *testing.T) {
	t.Parallel()
	w := workers.PythonWorker{Interpreter: "flywheel-no-such-python-xyz"}
	_, err := w.Work(context.Background(), pythonJob(workers.PythonArgs{Inline: "print(1)"}))
	require.Error(t, err)
	assert.Equal(t, flywheel.ErrorPermanent, w.Classify(err))
}

func TestPythonWorkerTimeoutSurfacesDeadline(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	_, err := workers.PythonWorker{Interpreter: "sh"}.Work(ctx,
		pythonJob(workers.PythonArgs{Inline: "sleep 5"}))
	require.ErrorIs(t, err, context.DeadlineExceeded)
}

// TestPythonWorkerRealPython3 exercises the default interpreter resolution against
// a real python3. It is skipped (not failed) when python3 is absent, so CI on a
// minimal image stays green while developers get a true end-to-end check.
func TestPythonWorkerRealPython3(t *testing.T) {
	t.Parallel()
	if _, err := exec.LookPath("python3"); err != nil {
		t.Skip("python3 not installed")
	}
	res, err := workers.PythonWorker{}.Work(context.Background(),
		pythonJob(workers.PythonArgs{Inline: "import sys; sys.stdout.write('hi-' + sys.argv[1])", Args: []string{"py"}}))
	require.NoError(t, err)
	assert.Equal(t, "hi-py", res.Output.(workers.ExecOutput).Stdout)
}
