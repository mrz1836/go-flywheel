package flywheel

import (
	"io"
	"net/http"
	"strings"
	"sync"
)

// HTTPDoer is the seam through which workers make external HTTP calls. Tests
// substitute a FakeHTTPDoer so no real network call is made.
type HTTPDoer interface {
	Do(req *http.Request) (*http.Response, error)
}

// DefaultHTTPDoer returns an HTTPDoer backed by http.DefaultClient.
func DefaultHTTPDoer() HTTPDoer {
	return defaultDoer{}
}

// defaultDoer wraps http.DefaultClient.
type defaultDoer struct{}

// Do delegates to http.DefaultClient. The HTTPDoer seam intentionally performs
// caller-controlled requests; SSRF guarding is the worker's responsibility, and
// tests substitute FakeHTTPDoer.
func (defaultDoer) Do(req *http.Request) (*http.Response, error) {
	return http.DefaultClient.Do(req) //nolint:wrapcheck,gosec // intentional pass-through seam
}

// fakeResponse is a single programmed reply.
type fakeResponse struct {
	status      int
	body        string
	err         error
	bodyReadErr error // when non-nil, the response Body returns this error on Read
}

// errReader is an io.ReadCloser that fails every Read with err. It models a
// truncated-stream / mid-body network failure for FakeHTTPDoer.
type errReader struct{ err error }

// Read always returns the configured error and zero bytes.
func (r errReader) Read(_ []byte) (int, error) { return 0, r.err }

// Close is a no-op; nothing to release.
func (errReader) Close() error { return nil }

// FakeHTTPDoer is a programmable HTTPDoer test double. Replies are stubbed per
// request URL with an optional fallback. It is safe for concurrent use.
type FakeHTTPDoer struct {
	mu       sync.Mutex
	byURL    map[string]fakeResponse
	fallback *fakeResponse
	requests []*http.Request
}

// NewFakeHTTPDoer returns an empty FakeHTTPDoer. Unstubbed requests get a
// 200 OK with an empty body unless StubDefault overrides the fallback.
func NewFakeHTTPDoer() *FakeHTTPDoer {
	return &FakeHTTPDoer{byURL: map[string]fakeResponse{}}
}

// StubURL programs the reply for requests to url.
func (f *FakeHTTPDoer) StubURL(url string, status int, body string) *FakeHTTPDoer {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.byURL[url] = fakeResponse{status: status, body: body}
	return f
}

// StubError programs requests to url to fail with err.
func (f *FakeHTTPDoer) StubError(url string, err error) *FakeHTTPDoer {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.byURL[url] = fakeResponse{err: err}
	return f
}

// StubBodyReadError programs requests to url to return a response whose Body
// fails every Read with readErr — modeling a truncated stream (FR-017).
func (f *FakeHTTPDoer) StubBodyReadError(url string, status int, readErr error) *FakeHTTPDoer {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.byURL[url] = fakeResponse{status: status, bodyReadErr: readErr}
	return f
}

// StubDefaultBodyReadError programs the fallback so unstubbed requests return a
// response whose Body fails every Read with readErr.
func (f *FakeHTTPDoer) StubDefaultBodyReadError(status int, readErr error) *FakeHTTPDoer {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.fallback = &fakeResponse{status: status, bodyReadErr: readErr}
	return f
}

// StubDefault programs the fallback reply for any unstubbed request URL.
func (f *FakeHTTPDoer) StubDefault(status int, body string) *FakeHTTPDoer {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.fallback = &fakeResponse{status: status, body: body}
	return f
}

// Do records the request and returns its programmed reply.
func (f *FakeHTTPDoer) Do(req *http.Request) (*http.Response, error) {
	f.mu.Lock()
	f.requests = append(f.requests, req)
	resp, ok := f.byURL[req.URL.String()]
	if !ok {
		if f.fallback != nil {
			resp = *f.fallback
		} else {
			resp = fakeResponse{status: http.StatusOK}
		}
	}
	f.mu.Unlock()

	if resp.err != nil {
		return nil, resp.err
	}
	var body io.ReadCloser
	if resp.bodyReadErr != nil {
		body = errReader{err: resp.bodyReadErr}
	} else {
		body = io.NopCloser(strings.NewReader(resp.body))
	}
	return &http.Response{
		StatusCode: resp.status,
		Status:     http.StatusText(resp.status),
		Body:       body,
		Header:     make(http.Header),
		Request:    req,
	}, nil
}

// Calls reports how many requests have been made.
func (f *FakeHTTPDoer) Calls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.requests)
}
