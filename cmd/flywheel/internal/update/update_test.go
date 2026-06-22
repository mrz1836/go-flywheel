package update

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeFetcher is an injectable ReleaseFetcher for network-free tests.
type fakeFetcher struct {
	rel    *Release
	err    error
	called int
}

func (f *fakeFetcher) Latest(context.Context) (*Release, error) {
	f.called++
	return f.rel, f.err
}

func TestCheckReportsUpdateAvailableThenCaches(t *testing.T) {
	useTempCache(t)
	f := &fakeFetcher{rel: &Release{TagName: "v2.0.0"}}

	r := Check(context.Background(), "v1.0.0", f)
	require.NoError(t, r.Err)
	assert.True(t, r.UpdateAvailable)
	assert.Equal(t, "v2.0.0", r.LatestVersion)
	assert.False(t, r.FromCache)
	assert.Equal(t, 1, f.called)

	// A second check within the TTL is served from cache without re-fetching.
	r2 := Check(context.Background(), "v1.0.0", f)
	require.NoError(t, r2.Err)
	assert.True(t, r2.FromCache)
	assert.Equal(t, 1, f.called, "a valid cache entry avoids a second fetch")
}

func TestCheckReportsUpToDate(t *testing.T) {
	useTempCache(t)
	r := Check(context.Background(), "v2.0.0", &fakeFetcher{rel: &Release{TagName: "v2.0.0"}})
	require.NoError(t, r.Err)
	assert.False(t, r.UpdateAvailable)
}

func TestCheckSurfacesFetchError(t *testing.T) {
	useTempCache(t)
	sentinel := errors.New("boom")
	r := Check(context.Background(), "v1.0.0", &fakeFetcher{err: sentinel})
	assert.ErrorIs(t, r.Err, sentinel)
}

func TestCheckFreshBypassesCache(t *testing.T) {
	useTempCache(t)
	require.NoError(t, WriteCache(&CacheEntry{CurrentVersion: "v1.0.0", LatestVersion: "v1.5.0"}))

	f := &fakeFetcher{rel: &Release{TagName: "v3.0.0"}}
	r := CheckFresh(context.Background(), "v1.0.0", f)
	require.NoError(t, r.Err)
	assert.Equal(t, 1, f.called, "CheckFresh ignores the cache and fetches")
	assert.Equal(t, "v3.0.0", r.LatestVersion)
	assert.False(t, r.FromCache)
}

func TestStartBackgroundCheckReturnsResult(t *testing.T) {
	useTempCache(t)
	t.Setenv("CI", "")
	t.Setenv("FLYWHEEL_DISABLE_UPDATE_CHECK", "")

	ch := StartBackgroundCheck(context.Background(), "v1.0.0", &fakeFetcher{rel: &Release{TagName: "v2.0.0"}})
	res := <-ch
	require.NotNil(t, res)
	assert.True(t, res.UpdateAvailable)
}

func TestStartBackgroundCheckSkipsDevAndDisabled(t *testing.T) {
	useTempCache(t)
	t.Setenv("CI", "")
	t.Setenv("FLYWHEEL_DISABLE_UPDATE_CHECK", "")

	// A dev build never nags.
	ch := StartBackgroundCheck(context.Background(), "dev", &fakeFetcher{rel: &Release{TagName: "v9.9.9"}})
	_, ok := <-ch
	assert.False(t, ok, "a dev build yields no result")

	// Disabled via env never nags.
	t.Setenv("FLYWHEEL_DISABLE_UPDATE_CHECK", "1")
	ch = StartBackgroundCheck(context.Background(), "v1.0.0", &fakeFetcher{rel: &Release{TagName: "v9.9.9"}})
	_, ok = <-ch
	assert.False(t, ok, "a disabled check yields no result")
}

func TestGitHubFetcherLatest(t *testing.T) {
	t.Setenv("FLYWHEEL_GITHUB_TOKEN", "tok")
	var gotAuth, gotUA, gotAccept string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		gotUA = r.Header.Get("User-Agent")
		gotAccept = r.Header.Get("Accept")
		assert.Equal(t, "/repos/o/r/releases/latest", r.URL.Path)
		_, _ = w.Write([]byte(`{"tag_name":"v3.1.4","assets":[{"name":"a.tar.gz","browser_download_url":"http://x/a"}]}`))
	}))
	defer srv.Close()

	g := &GitHubFetcher{Owner: "o", Repo: "r", CurrentVersion: "v1.0.0", Client: srv.Client(), baseURL: srv.URL}
	rel, err := g.Latest(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "v3.1.4", rel.TagName)
	require.Len(t, rel.Assets, 1)
	assert.Equal(t, "a.tar.gz", rel.Assets[0].Name)
	assert.Equal(t, "Bearer tok", gotAuth)
	assert.Contains(t, gotUA, "flywheel/v1.0.0")
	assert.Contains(t, gotAccept, "github")
}

func TestGitHubFetcherLatestNon200(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte("rate limited"))
	}))
	defer srv.Close()

	g := &GitHubFetcher{Owner: "o", Repo: "r", Client: srv.Client(), baseURL: srv.URL}
	_, err := g.Latest(context.Background())
	assert.ErrorIs(t, err, ErrGitHubAPI)
}

func TestGitHubTokenPriority(t *testing.T) {
	t.Setenv("FLYWHEEL_GITHUB_TOKEN", "")
	t.Setenv("GITHUB_TOKEN", "")
	t.Setenv("GH_TOKEN", "")
	assert.Empty(t, GitHubToken())

	t.Setenv("GH_TOKEN", "gh")
	assert.Equal(t, "gh", GitHubToken())

	t.Setenv("GITHUB_TOKEN", "github")
	assert.Equal(t, "github", GitHubToken())

	t.Setenv("FLYWHEEL_GITHUB_TOKEN", "flywheel")
	assert.Equal(t, "flywheel", GitHubToken(), "the flywheel-specific token wins")
}
