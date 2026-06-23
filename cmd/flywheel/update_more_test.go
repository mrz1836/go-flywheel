package main

import (
	"context"
	"errors"
	"testing"

	"github.com/mrz1836/go-flywheel/cmd/flywheel/internal/update"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// errFetcher is a ReleaseFetcher whose Latest call always fails, exercising the
// command's fetch-error branches without the network.
type errFetcher struct{ err error }

func (e errFetcher) Latest(context.Context) (*update.Release, error) { return nil, e.err }

// withFetcher pins the resolved version and swaps in a fetcher factory, keeping
// the update cache under a temp HOME so the command stays offline. It mutates
// package globals, so callers must not run in parallel.
func withFetcher(t *testing.T, current string, factory func(string) update.ReleaseFetcher) {
	t.Helper()
	t.Setenv("HOME", t.TempDir())
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	origVer, origFetch := version, newFetcher
	version = current
	newFetcher = factory
	t.Cleanup(func() { version, newFetcher = origVer, origFetch })
}

func TestUpdateInstallAlreadyUpToDate(t *testing.T) {
	// The latest tag is not newer than current and --force is unset, so SelfUpdate
	// reports no update without downloading anything.
	withFetcher(t, "v9.9.9", func(string) update.ReleaseFetcher { return stubFetcher{tag: "v1.0.0"} })
	out, err := runRoot(context.Background(), "update")
	require.NoError(t, err)
	assert.Contains(t, out, "already up to date")
}

func TestUpdateInstallForceFailsWithoutAsset(t *testing.T) {
	// --force makes SelfUpdate proceed to download, but the stub release carries no
	// asset for this os/arch, so the install fails — the command's install-error
	// branch, still fully offline.
	withFetcher(t, "v1.0.0", func(string) update.ReleaseFetcher { return stubFetcher{tag: "v2.0.0"} })
	_, err := runRoot(context.Background(), "update", "--force")
	require.Error(t, err, "an install with no matching asset surfaces an error")
}

func TestUpdateCheckSurfacesFetchError(t *testing.T) {
	// A fetch failure during --check is returned to the caller, not swallowed.
	withFetcher(t, "v1.0.0", func(string) update.ReleaseFetcher {
		return errFetcher{err: errors.New("boom")}
	})
	_, err := runRoot(context.Background(), "update", "--check")
	require.Error(t, err, "a fetch failure during --check is surfaced")
}
