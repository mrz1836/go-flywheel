package flywheel

import (
	"context"
	"errors"
	"io"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// errFakeNetwork stands in for any caller-error the stubbed doer surfaces.
// Keeping it as a package-level sentinel avoids err113's "dynamic error"
// warnings and lets ErrorIs match exactly.
var (
	errFakeNetwork   = errors.New("fake: connection refused")
	errFakeBodyTrunc = errors.New("fake: unexpected EOF")
)

func newReq(t *testing.T, url string) *http.Request {
	t.Helper()
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, url, nil)
	require.NoError(t, err)
	return req
}

// closeBody is a no-fail close helper for the stub responses. The FakeHTTPDoer
// returns bodies backed by strings.Reader or errReader, both of which Close()
// to nil.
func closeBody(t *testing.T, body io.Closer) {
	t.Helper()
	require.NoError(t, body.Close())
}

func TestFakeHTTPDoerStubURLReturnsProgrammedBody(t *testing.T) {
	t.Parallel()
	f := NewFakeHTTPDoer().StubURL("https://api/x", http.StatusCreated, `{"ok":true}`)

	resp, err := f.Do(newReq(t, "https://api/x"))
	require.NoError(t, err)
	defer closeBody(t, resp.Body)

	require.Equal(t, http.StatusCreated, resp.StatusCode)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.JSONEq(t, `{"ok":true}`, string(body))
	assert.Equal(t, 1, f.Calls())
}

func TestFakeHTTPDoerStubErrorPropagates(t *testing.T) {
	t.Parallel()
	f := NewFakeHTTPDoer().StubError("https://api/down", errFakeNetwork)

	resp, err := f.Do(newReq(t, "https://api/down")) //nolint:bodyclose // err path: resp is nil
	require.Nil(t, resp)
	require.ErrorIs(t, err, errFakeNetwork)
}

func TestFakeHTTPDoerStubDefaultAppliesToUnknownURLs(t *testing.T) {
	t.Parallel()
	f := NewFakeHTTPDoer().StubDefault(http.StatusTeapot, "I am a teapot")

	resp, err := f.Do(newReq(t, "https://api/anything"))
	require.NoError(t, err)
	defer closeBody(t, resp.Body)

	require.Equal(t, http.StatusTeapot, resp.StatusCode)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Equal(t, "I am a teapot", string(body))
}

func TestFakeHTTPDoerStubBodyReadErrorFailsMidStream(t *testing.T) {
	t.Parallel()
	f := NewFakeHTTPDoer().StubBodyReadError("https://api/truncated", http.StatusOK, errFakeBodyTrunc)

	resp, err := f.Do(newReq(t, "https://api/truncated"))
	require.NoError(t, err, "the response itself returns successfully — the failure is on Body.Read")
	defer closeBody(t, resp.Body)

	require.Equal(t, http.StatusOK, resp.StatusCode)
	_, readErr := io.ReadAll(resp.Body)
	require.ErrorIs(t, readErr, errFakeBodyTrunc)
}

func TestFakeHTTPDoerUnstubbedURLFallsBackTo200OK(t *testing.T) {
	t.Parallel()
	f := NewFakeHTTPDoer()

	resp, err := f.Do(newReq(t, "https://api/unknown"))
	require.NoError(t, err)
	defer closeBody(t, resp.Body)

	assert.Equal(t, http.StatusOK, resp.StatusCode, "no Stub, no StubDefault → 200 OK with empty body")
}

func TestFakeHTTPDoerCallsCounterIncrementsPerRequest(t *testing.T) {
	t.Parallel()
	f := NewFakeHTTPDoer().StubDefault(http.StatusOK, "")

	for range 3 {
		resp, err := f.Do(newReq(t, "https://api/x"))
		require.NoError(t, err)
		require.NoError(t, resp.Body.Close())
	}
	assert.Equal(t, 3, f.Calls())
}
