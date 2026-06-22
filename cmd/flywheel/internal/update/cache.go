// Package update implements flywheel's self-update: a cached "new version
// available" check against the GitHub Releases API, a TTY-aware banner, and a
// checksum-verified self-replace of the running binary. It is CLI-private
// (under cmd/flywheel/internal) so the update machinery never widens the
// library's API surface.
package update

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

const (
	// cacheTTL is how long a recorded check is trusted before re-fetching.
	cacheTTL = 24 * time.Hour
	// cacheFileName is the cache file under the flywheel config dir.
	cacheFileName = "update-check.json"
	// configSubdir is the per-app config subdirectory.
	configSubdir = "flywheel"
)

// CacheEntry is the persisted result of the last update check.
type CacheEntry struct {
	CheckedAt      time.Time `json:"checked_at"`
	CurrentVersion string    `json:"current_version"`
	LatestVersion  string    `json:"latest_version"`
}

// cachePathFn resolves the cache file path. It is a variable so tests can point
// it at a temp directory instead of the real user config dir.
//
//nolint:gochecknoglobals // injectable seam for tests; defaults to the real path
var cachePathFn = defaultCachePath

// defaultCachePath returns os.UserConfigDir()/flywheel/update-check.json.
func defaultCachePath() (string, error) {
	dir, err := os.UserConfigDir()
	if err != nil {
		return "", fmt.Errorf("resolve user config dir: %w", err)
	}
	return filepath.Join(dir, configSubdir, cacheFileName), nil
}

// IsDisabled reports whether update checking is turned off — under CI, or when
// FLYWHEEL_DISABLE_UPDATE_CHECK is set to a truthy value.
func IsDisabled() bool {
	if truthy(os.Getenv("FLYWHEEL_DISABLE_UPDATE_CHECK")) {
		return true
	}
	return truthy(os.Getenv("CI"))
}

// truthy reports whether an env var value enables a flag.
func truthy(v string) bool {
	return v == "1" || v == "true" || v == "TRUE" || v == "yes"
}

// ReadCache returns the cached entry, or (nil, nil) when no cache file exists.
func ReadCache() (*CacheEntry, error) {
	path, err := cachePathFn()
	if err != nil {
		return nil, err
	}
	data, err := os.ReadFile(path) //nolint:gosec // path is the app's own config file
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil //nolint:nilnil // missing cache is the expected "no entry" signal
		}
		return nil, fmt.Errorf("read update cache %s: %w", path, err)
	}
	var entry CacheEntry
	if err := json.Unmarshal(data, &entry); err != nil {
		return nil, fmt.Errorf("parse update cache %s: %w", path, err)
	}
	return &entry, nil
}

// WriteCache persists entry atomically (temp file + rename), stamping CheckedAt.
func WriteCache(entry *CacheEntry) error {
	path, err := cachePathFn()
	if err != nil {
		return err
	}
	if mkErr := os.MkdirAll(filepath.Dir(path), 0o700); mkErr != nil {
		return fmt.Errorf("create update cache dir: %w", mkErr)
	}
	entry.CheckedAt = nowFn()
	data, err := json.MarshalIndent(entry, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal update cache: %w", err)
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return fmt.Errorf("write update cache temp: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("rename update cache: %w", err)
	}
	return nil
}

// ClearCache removes the cache file (a no-op when it is already absent).
func ClearCache() error {
	path, err := cachePathFn()
	if err != nil {
		return err
	}
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove update cache: %w", err)
	}
	return nil
}

// IsCacheValid reports whether entry is non-nil and younger than the TTL.
func IsCacheValid(entry *CacheEntry) bool {
	if entry == nil {
		return false
	}
	return nowFn().Sub(entry.CheckedAt) <= cacheTTL
}

// nowFn returns the current time. It is a variable so tests can freeze time for
// deterministic TTL assertions.
//
//nolint:gochecknoglobals // injectable clock for tests; defaults to time.Now
var nowFn = time.Now
