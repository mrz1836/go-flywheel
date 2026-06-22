package workers_test

import (
	"context"
	"errors"
	"net/http"
	"testing"

	flywheel "github.com/mrz1836/go-flywheel"
	"github.com/mrz1836/go-flywheel/workers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWorkerKindsAreStable(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "exec", workers.ExecArgs{}.Kind())
	assert.Equal(t, "exec", workers.ExecWorker{}.Kind())
	assert.Equal(t, "http", workers.HTTPArgs{}.Kind())
	assert.Equal(t, "http", workers.HTTPWorker{}.Kind())
}

func TestExecWorkerExitErrorExposesCode(t *testing.T) {
	t.Parallel()
	_, err := workers.ExecWorker{}.Work(context.Background(),
		execJob(workers.ExecArgs{Command: "sh", Args: []string{"-c", "exit 7"}}))
	require.Error(t, err)

	var ec interface{ ExitCode() int }
	require.ErrorAs(t, err, &ec)
	assert.Equal(t, 7, ec.ExitCode())
}

func TestExecWorkerTimeoutSecondsAndStdin(t *testing.T) {
	t.Parallel()
	// TimeoutSeconds > 0 wraps the context; Stdin is piped to the command. cat
	// echoes its stdin, so both code paths are exercised by one fast command.
	res, err := workers.ExecWorker{}.Work(context.Background(),
		execJob(workers.ExecArgs{Command: "cat", Stdin: "piped-input", TimeoutSeconds: 30}))
	require.NoError(t, err)
	assert.Equal(t, "piped-input", res.Output.(workers.ExecOutput).Stdout)
}

func TestHTTPWorkerInvalidMethodIsError(t *testing.T) {
	t.Parallel()
	doer := flywheel.NewFakeHTTPDoer().StubDefault(http.StatusOK, "")
	_, err := workers.HTTPWorker{Doer: doer}.Work(context.Background(),
		httpJob(workers.HTTPArgs{Method: "BAD METHOD", URL: "https://x.test/y"}))
	require.Error(t, err, "an invalid HTTP method fails request construction")
}

func TestHTTPWorkerBodyReadErrorIsError(t *testing.T) {
	t.Parallel()
	doer := flywheel.NewFakeHTTPDoer().StubBodyReadError("https://x.test/trunc", http.StatusOK, errors.New("truncated"))
	_, err := workers.HTTPWorker{Doer: doer}.Work(context.Background(),
		httpJob(workers.HTTPArgs{URL: "https://x.test/trunc"}))
	require.Error(t, err, "a body read failure surfaces as an error")
}

func TestHTTPWorkerExplicitSuccessListNoMatchIsError(t *testing.T) {
	t.Parallel()
	doer := flywheel.NewFakeHTTPDoer().StubURL("https://x.test/z", http.StatusInternalServerError, "")
	_, err := workers.HTTPWorker{Doer: doer}.Work(context.Background(),
		httpJob(workers.HTTPArgs{URL: "https://x.test/z", SuccessStatus: []int{http.StatusOK}}))
	require.Error(t, err, "a status not in the explicit success list is an error")
}
