package main

import (
	"context"
	"testing"

	"github.com/mrz1836/go-flywheel/cmd/flywheel/internal/update"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// stubFetcher is a ReleaseFetcher returning a fixed tag for command-level tests.
type stubFetcher struct{ tag string }

func (s stubFetcher) Latest(context.Context) (*update.Release, error) {
	return &update.Release{TagName: s.tag}, nil
}

// withStub pins the resolved version and injects a stub fetcher, redirecting the
// update cache under a temp HOME so `update --check` stays hermetic and offline.
// It mutates package globals, so callers must not run in parallel.
func withStub(t *testing.T, current, latest string) {
	t.Helper()
	t.Setenv("HOME", t.TempDir())
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	origVer, origFetch := version, newFetcher
	version = current
	newFetcher = func(string) update.ReleaseFetcher { return stubFetcher{tag: latest} }
	t.Cleanup(func() { version, newFetcher = origVer, origFetch })
}

func TestUpdateCheckReportsAvailable(t *testing.T) {
	withStub(t, "v1.0.0", "v999.0.0")
	out, err := runRoot(context.Background(), "update", "--check")
	require.NoError(t, err)
	assert.Contains(t, out, "update available")
	assert.Contains(t, out, "v999.0.0")
}

func TestUpdateCheckReportsUpToDate(t *testing.T) {
	withStub(t, "v2.0.0", "v0.0.1")
	out, err := runRoot(context.Background(), "update", "--check")
	require.NoError(t, err)
	assert.Contains(t, out, "up to date")
}
