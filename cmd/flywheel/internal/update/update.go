package update

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"runtime"
	"time"

	iver "github.com/mrz1836/go-flywheel/cmd/flywheel/internal/version"
)

// GitHub coordinates for flywheel releases.
const (
	defaultOwner = "mrz1836"
	defaultRepo  = "go-flywheel"
	// fetchTimeout bounds a single GitHub Releases API call.
	fetchTimeout = 10 * time.Second
)

// ErrGitHubAPI is returned when the GitHub Releases API responds non-200.
var ErrGitHubAPI = errors.New("github releases api request failed")

// Release is the subset of a GitHub release the updater needs.
type Release struct {
	TagName string  `json:"tag_name"`
	Assets  []Asset `json:"assets"`
}

// Asset is one downloadable release artifact.
type Asset struct {
	Name string `json:"name"`
	URL  string `json:"browser_download_url"`
}

// ReleaseFetcher returns the latest release. The production GitHubFetcher hits
// the GitHub API; tests inject a fake to avoid the network.
type ReleaseFetcher interface {
	Latest(ctx context.Context) (*Release, error)
}

// GitHubFetcher fetches the latest release from the GitHub Releases API.
type GitHubFetcher struct {
	Owner          string
	Repo           string
	CurrentVersion string
	Client         *http.Client
	// baseURL overrides the GitHub API host; empty means the real API. It is a
	// test seam so the fetcher can be pointed at an httptest server.
	baseURL string
}

// NewGitHubFetcher returns a GitHubFetcher for flywheel with a bounded client.
func NewGitHubFetcher(currentVersion string) *GitHubFetcher {
	return &GitHubFetcher{
		Owner:          defaultOwner,
		Repo:           defaultRepo,
		CurrentVersion: currentVersion,
		Client:         &http.Client{Timeout: fetchTimeout},
	}
}

// Latest fetches /repos/{owner}/{repo}/releases/latest and decodes it.
func (g *GitHubFetcher) Latest(ctx context.Context) (*Release, error) {
	base := g.baseURL
	if base == "" {
		base = "https://api.github.com"
	}
	url := fmt.Sprintf("%s/repos/%s/%s/releases/latest", base, g.Owner, g.Repo)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, http.NoBody)
	if err != nil {
		return nil, fmt.Errorf("create release request: %w", err)
	}
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("User-Agent", fmt.Sprintf("flywheel/%s (%s/%s)", g.CurrentVersion, runtime.GOOS, runtime.GOARCH))
	if token := GitHubToken(); token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}

	client := g.Client
	if client == nil {
		client = &http.Client{Timeout: fetchTimeout}
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetch latest release: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return nil, fmt.Errorf("%w: status %d: %s", ErrGitHubAPI, resp.StatusCode, string(body))
	}
	var rel Release
	if err := json.NewDecoder(resp.Body).Decode(&rel); err != nil {
		return nil, fmt.Errorf("decode release: %w", err)
	}
	return &rel, nil
}

// GitHubToken resolves the API token, preferring the flywheel-specific var:
// FLYWHEEL_GITHUB_TOKEN > GITHUB_TOKEN > GH_TOKEN.
func GitHubToken() string {
	for _, key := range []string{"FLYWHEEL_GITHUB_TOKEN", "GITHUB_TOKEN", "GH_TOKEN"} {
		if v := os.Getenv(key); v != "" {
			return v
		}
	}
	return ""
}

// Result is the outcome of an update check.
type Result struct {
	CurrentVersion  string
	LatestVersion   string
	UpdateAvailable bool
	FromCache       bool
	Err             error
}

// Check performs a cache-aware update check: it trusts a fresh cache entry,
// otherwise fetches the latest release (bounded by ctx), records it, and reports
// whether an update is available. It is the engine behind the startup nudge.
func Check(ctx context.Context, currentVersion string, fetcher ReleaseFetcher) *Result {
	if cached, err := ReadCache(); err == nil && IsCacheValid(cached) {
		return &Result{
			CurrentVersion:  currentVersion,
			LatestVersion:   cached.LatestVersion,
			UpdateAvailable: iver.IsNewer(currentVersion, cached.LatestVersion),
			FromCache:       true,
		}
	}
	return fetchAndCompare(ctx, currentVersion, fetcher)
}

// CheckFresh forces a network check, bypassing the cache (but still recording
// its result). It is the engine behind the explicit `flywheel update --check`.
func CheckFresh(ctx context.Context, currentVersion string, fetcher ReleaseFetcher) *Result {
	return fetchAndCompare(ctx, currentVersion, fetcher)
}

// fetchAndCompare fetches the latest release, records it in the cache, and
// reports whether it is newer than currentVersion.
func fetchAndCompare(ctx context.Context, currentVersion string, fetcher ReleaseFetcher) *Result {
	fetchCtx, cancel := context.WithTimeout(ctx, fetchTimeout)
	defer cancel()

	rel, err := fetcher.Latest(fetchCtx)
	if err != nil {
		return &Result{CurrentVersion: currentVersion, Err: err}
	}

	_ = WriteCache(&CacheEntry{CurrentVersion: currentVersion, LatestVersion: rel.TagName})
	return &Result{
		CurrentVersion:  currentVersion,
		LatestVersion:   rel.TagName,
		UpdateAvailable: iver.IsNewer(currentVersion, rel.TagName),
	}
}

// StartBackgroundCheck runs Check in a goroutine, returning a buffered channel
// that yields the result (or nothing on disable/dev/panic). It is the startup
// nudge: callers drain it later with a short timeout and never block on it.
func StartBackgroundCheck(ctx context.Context, currentVersion string, fetcher ReleaseFetcher) <-chan *Result {
	ch := make(chan *Result, 1)
	go func() {
		defer close(ch)
		defer func() { _ = recover() }() // never crash the CLI over an update check

		if IsDisabled() || currentVersion == "" || currentVersion == "dev" {
			return
		}
		if r := Check(ctx, currentVersion, fetcher); r != nil && r.Err == nil {
			ch <- r
		}
	}()
	return ch
}
