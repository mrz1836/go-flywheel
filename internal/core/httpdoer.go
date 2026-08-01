package core

import (
	"net/http"
)

// HTTPDoer is the seam through which workers make external HTTP calls. Tests
// substitute a fake so no real network call is made — see
// go-foundation/testutil.FakeHTTPDoer, re-exported as flywheel.FakeHTTPDoer,
// which satisfies this interface structurally.
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
// tests substitute a fake doer.
func (defaultDoer) Do(req *http.Request) (*http.Response, error) {
	return http.DefaultClient.Do(req) //nolint:wrapcheck,gosec // intentional pass-through seam
}
