package workers_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	flywheel "github.com/mrz1836/go-flywheel"
	"github.com/mrz1836/go-flywheel/workers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// httpJob wraps args in the Job shape HTTPWorker.Work expects.
func httpJob(a workers.HTTPArgs) *flywheel.Job[workers.HTTPArgs] {
	return &flywheel.Job[workers.HTTPArgs]{Kind: workers.HTTPKind, Args: a}
}

func TestHTTPWorkerSuccessCapturesStatusAndBody(t *testing.T) {
	t.Parallel()
	doer := flywheel.NewFakeHTTPDoer().StubURL("https://x.test/ping", http.StatusOK, "pong")
	res, err := workers.HTTPWorker{Doer: doer}.Work(context.Background(), httpJob(workers.HTTPArgs{URL: "https://x.test/ping"}))
	require.NoError(t, err)

	out := res.Output.(workers.HTTPOutput)
	assert.Equal(t, http.StatusOK, out.Status)
	assert.Equal(t, "pong", out.Body)
}

func TestHTTPWorkerNonSuccessStatusIsError(t *testing.T) {
	t.Parallel()
	doer := flywheel.NewFakeHTTPDoer().StubURL("https://x.test/bad", http.StatusInternalServerError, "boom")
	res, err := workers.HTTPWorker{Doer: doer}.Work(context.Background(), httpJob(workers.HTTPArgs{URL: "https://x.test/bad"}))
	require.Error(t, err)
	assert.Equal(t, http.StatusInternalServerError, res.Output.(workers.HTTPOutput).Status, "the status is captured even on failure")
}

func TestHTTPWorkerCustomSuccessStatus(t *testing.T) {
	t.Parallel()
	doer := flywheel.NewFakeHTTPDoer().StubURL("https://x.test/created", http.StatusCreated, "")
	_, err := workers.HTTPWorker{Doer: doer}.Work(context.Background(),
		httpJob(workers.HTTPArgs{URL: "https://x.test/created", SuccessStatus: []int{http.StatusCreated}}))
	require.NoError(t, err, "201 is success when listed in SuccessStatus")
}

func TestHTTPWorkerTransportErrorIsError(t *testing.T) {
	t.Parallel()
	doer := flywheel.NewFakeHTTPDoer().StubError("https://x.test/down", errors.New("connection refused"))
	_, err := workers.HTTPWorker{Doer: doer}.Work(context.Background(), httpJob(workers.HTTPArgs{URL: "https://x.test/down"}))
	require.Error(t, err)
}

func TestHTTPWorkerRequiresURL(t *testing.T) {
	t.Parallel()
	_, err := workers.HTTPWorker{}.Work(context.Background(), httpJob(workers.HTTPArgs{}))
	require.Error(t, err)
}

func TestHTTPWorkerSendsRequestAndUsesDefaultDoer(t *testing.T) {
	t.Parallel()
	var gotMethod, gotHeader, gotBody string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotHeader = r.Header.Get("X-Test")
		b, _ := io.ReadAll(r.Body)
		gotBody = string(b)
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "ok")
	}))
	defer srv.Close()

	// A nil Doer falls back to the default real HTTP client.
	res, err := workers.HTTPWorker{}.Work(context.Background(), httpJob(workers.HTTPArgs{
		Method:  http.MethodPost,
		URL:     srv.URL,
		Headers: map[string]string{"X-Test": "v"},
		Body:    "payload",
	}))
	require.NoError(t, err)
	assert.Equal(t, http.MethodPost, gotMethod)
	assert.Equal(t, "v", gotHeader)
	assert.Equal(t, "payload", gotBody)
	assert.Equal(t, "ok", res.Output.(workers.HTTPOutput).Body)
}
