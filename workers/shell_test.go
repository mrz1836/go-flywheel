package workers_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	flywheel "github.com/mrz1836/go-flywheel"
	"github.com/mrz1836/go-flywheel/workers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// shellJob wraps args in the Job shape ShellWorker.Work expects.
func shellJob(a workers.ShellArgs) *flywheel.Job[workers.ShellArgs] {
	return &flywheel.Job[workers.ShellArgs]{Kind: workers.ShellKind, Args: a}
}

func TestShellWorkerKind(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "shell", workers.ShellArgs{}.Kind())
	assert.Equal(t, "shell", workers.ShellWorker{}.Kind())
}

func TestShellWorkerInlineCapturesStreams(t *testing.T) {
	t.Parallel()
	res, err := workers.ShellWorker{}.Work(context.Background(),
		shellJob(workers.ShellArgs{Inline: "printf hello; printf oops 1>&2"}))
	require.NoError(t, err)

	out := res.Output.(workers.ExecOutput)
	assert.Equal(t, 0, out.ExitCode)
	assert.Equal(t, "hello", out.Stdout)
	assert.Equal(t, "oops", out.Stderr)
}

func TestShellWorkerInlineArgsArePositional(t *testing.T) {
	t.Parallel()
	// Args become $1, $2 (with the shell name as $0).
	res, err := workers.ShellWorker{}.Work(context.Background(),
		shellJob(workers.ShellArgs{Inline: `printf '%s-%s' "$1" "$2"`, Args: []string{"a", "b"}}))
	require.NoError(t, err)
	assert.Equal(t, "a-b", res.Output.(workers.ExecOutput).Stdout)
}

func TestShellWorkerScriptFileRuns(t *testing.T) {
	t.Parallel()
	script := filepath.Join(t.TempDir(), "hello.sh")
	require.NoError(t, os.WriteFile(script, []byte("#!/bin/sh\nprintf 'from-%s' \"$1\"\n"), 0o600))

	res, err := workers.ShellWorker{}.Work(context.Background(),
		shellJob(workers.ShellArgs{Script: script, Args: []string{"file"}}))
	require.NoError(t, err)
	assert.Equal(t, "from-file", res.Output.(workers.ExecOutput).Stdout)
}

func TestShellWorkerRequiresScriptOrInline(t *testing.T) {
	t.Parallel()
	_, err := workers.ShellWorker{}.Work(context.Background(), shellJob(workers.ShellArgs{}))
	require.Error(t, err)
}

func TestShellWorkerScriptAndInlineConflict(t *testing.T) {
	t.Parallel()
	_, err := workers.ShellWorker{}.Work(context.Background(),
		shellJob(workers.ShellArgs{Script: "x.sh", Inline: "echo hi"}))
	require.Error(t, err)
}

func TestShellWorkerNonZeroExitIsTransient(t *testing.T) {
	t.Parallel()
	w := workers.ShellWorker{}
	_, err := w.Work(context.Background(), shellJob(workers.ShellArgs{Inline: "exit 4"}))
	require.Error(t, err)
	assert.ErrorContains(t, err, "exited with code 4")
	assert.Equal(t, flywheel.ErrorTransient, w.Classify(err))
}

func TestShellWorkerPermanentExitCode(t *testing.T) {
	t.Parallel()
	w := workers.ShellWorker{PermanentExitCodes: []int{2}}
	_, err := w.Work(context.Background(), shellJob(workers.ShellArgs{Inline: "exit 2"}))
	require.Error(t, err)
	assert.Equal(t, flywheel.ErrorPermanent, w.Classify(err))
}

func TestShellWorkerMissingShellIsPermanent(t *testing.T) {
	t.Parallel()
	w := workers.ShellWorker{}
	_, err := w.Work(context.Background(),
		shellJob(workers.ShellArgs{Shell: "flywheel-no-such-shell-xyz", Inline: "echo hi"}))
	require.Error(t, err)
	assert.Equal(t, flywheel.ErrorPermanent, w.Classify(err))
}

func TestShellWorkerTimeoutSurfacesDeadline(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	_, err := workers.ShellWorker{}.Work(ctx, shellJob(workers.ShellArgs{Inline: "sleep 5"}))
	require.ErrorIs(t, err, context.DeadlineExceeded)
}
