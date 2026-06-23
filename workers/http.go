package workers

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strings"
	"time"

	flywheel "github.com/mrz1836/go-flywheel"
)

// HTTPKind is the job kind HTTPWorker handles.
const HTTPKind = "http"

// HTTPArgs is the JSON payload for an http job: the request to make and what
// counts as success.
type HTTPArgs struct {
	// Method is the HTTP method. Empty defaults to GET.
	Method string `json:"method,omitempty"`
	// URL is the request URL (required).
	URL string `json:"url"`
	// Headers are request headers to set.
	Headers map[string]string `json:"headers,omitempty"`
	// Body, when non-empty, is the request body.
	Body string `json:"body,omitempty"`
	// TimeoutSeconds, when > 0, bounds the request.
	TimeoutSeconds int `json:"timeout_seconds,omitempty"`
	// SuccessStatus lists the status codes treated as success. Empty means any
	// 2xx.
	SuccessStatus []int `json:"success_status,omitempty"`
}

// Kind names the http job kind so flywheel.Insert/Enqueue can target it.
func (HTTPArgs) Kind() string { return HTTPKind }

// HTTPOutput is the structured Result.Output HTTPWorker persists to
// job_runs.output: the response status, captured (capped) body, and timing.
type HTTPOutput struct {
	Status     int    `json:"status"`
	Body       string `json:"body"`
	DurationMs int64  `json:"duration_ms"`
}

// HTTPWorker calls a URL durably: it records the response status, body, and timing
// into the audit trail and turns a non-success status or transport error into a
// retryable error so flywheel's backoff applies. It reuses flywheel's HTTPDoer
// seam, so it is testable with flywheel.FakeHTTPDoer.
type HTTPWorker struct {
	// Doer performs the request. Nil uses flywheel.DefaultHTTPDoer().
	Doer flywheel.HTTPDoer
	// MaxBodyBytes caps the captured response body stored in HTTPOutput. Zero uses
	// a 64 KiB default.
	MaxBodyBytes int
}

// Kind names the http job kind.
func (HTTPWorker) Kind() string { return HTTPKind }

// Work performs the request and captures its outcome. A non-success status or a
// transport error is returned as an error (recorded on the run row); the status
// and captured body are always present in Result.Output.
func (w HTTPWorker) Work(ctx context.Context, job *flywheel.Job[HTTPArgs]) (flywheel.Result, error) {
	a := job.Args
	if a.URL == "" {
		return flywheel.Result{}, errors.New("http: url is required")
	}
	method := a.Method
	if method == "" {
		method = http.MethodGet
	}
	if a.TimeoutSeconds > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Duration(a.TimeoutSeconds)*time.Second)
		defer cancel()
	}

	var body io.Reader
	if a.Body != "" {
		body = strings.NewReader(a.Body)
	}
	req, err := http.NewRequestWithContext(ctx, method, a.URL, body) //nolint:gosec // HTTPWorker intentionally calls operator-configured URLs
	if err != nil {
		return flywheel.Result{}, fmt.Errorf("http: build request: %w", err)
	}
	for k, v := range a.Headers {
		req.Header.Set(k, v)
	}

	doer := w.Doer
	if doer == nil {
		doer = flywheel.DefaultHTTPDoer()
	}

	start := time.Now()
	resp, err := doer.Do(req)
	if err != nil {
		return flywheel.Result{}, fmt.Errorf("http: %s %s: %w", method, a.URL, err)
	}
	defer func() { _ = resp.Body.Close() }()

	maxBody := w.MaxBodyBytes
	if maxBody <= 0 {
		maxBody = defaultMaxOutputBytes
	}
	bodyBytes, readErr := io.ReadAll(io.LimitReader(resp.Body, int64(maxBody)))
	if readErr != nil {
		return flywheel.Result{}, fmt.Errorf("http: read body from %s: %w", a.URL, readErr)
	}
	out := HTTPOutput{
		Status:     resp.StatusCode,
		Body:       string(bodyBytes),
		DurationMs: time.Since(start).Milliseconds(),
	}
	res := flywheel.Result{Output: out}

	if !isSuccessStatus(resp.StatusCode, a.SuccessStatus) {
		return res, fmt.Errorf("http: %s %s returned status %d", method, a.URL, resp.StatusCode)
	}
	return res, nil
}

// isSuccessStatus reports whether status counts as success: a member of the
// explicit success list, or any 2xx when the list is empty.
func isSuccessStatus(status int, success []int) bool {
	if len(success) == 0 {
		return status >= http.StatusOK && status < http.StatusMultipleChoices
	}
	return slices.Contains(success, status)
}
