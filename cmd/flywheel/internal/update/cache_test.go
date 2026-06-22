package update

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// useTempCache points the cache file at a temp directory for the duration of a
// test and restores the real path (and clock) afterward. Tests using it must not
// run in parallel — they mutate package-level seams.
func useTempCache(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "update-check.json")
	origPath, origNow := cachePathFn, nowFn
	cachePathFn = func() (string, error) { return path, nil }
	t.Cleanup(func() { cachePathFn, nowFn = origPath, origNow })
	return path
}

func TestCacheRoundTripAndTTL(t *testing.T) {
	useTempCache(t)
	base := time.Date(2026, 6, 22, 12, 0, 0, 0, time.UTC)
	nowFn = func() time.Time { return base }

	entry, err := ReadCache()
	require.NoError(t, err)
	assert.Nil(t, entry, "a missing cache reads as (nil, nil)")

	require.NoError(t, WriteCache(&CacheEntry{CurrentVersion: "v1.0.0", LatestVersion: "v1.2.0"}))

	got, err := ReadCache()
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, "v1.2.0", got.LatestVersion)
	assert.Equal(t, base, got.CheckedAt)
	assert.True(t, IsCacheValid(got), "a just-written entry is within the TTL")

	nowFn = func() time.Time { return base.Add(25 * time.Hour) }
	assert.False(t, IsCacheValid(got), "an entry older than the TTL is invalid")

	nowFn = func() time.Time { return base }
	require.NoError(t, ClearCache())
	gone, err := ReadCache()
	require.NoError(t, err)
	assert.Nil(t, gone, "ClearCache removes the file")
}

func TestIsDisabled(t *testing.T) {
	t.Setenv("FLYWHEEL_DISABLE_UPDATE_CHECK", "")
	t.Setenv("CI", "")
	assert.False(t, IsDisabled())

	t.Setenv("FLYWHEEL_DISABLE_UPDATE_CHECK", "1")
	assert.True(t, IsDisabled(), "the explicit disable env turns checking off")

	t.Setenv("FLYWHEEL_DISABLE_UPDATE_CHECK", "")
	t.Setenv("CI", "true")
	assert.True(t, IsDisabled(), "CI turns checking off")
}
