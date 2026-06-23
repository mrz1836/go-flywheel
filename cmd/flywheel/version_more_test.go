package main

import (
	"runtime/debug"
	"testing"

	"github.com/stretchr/testify/assert"
)

// buildInfoWith assembles a synthetic *debug.BuildInfo with a main module version
// and the given vcs settings, for driving the resolve*From fallback branches.
func buildInfoWith(mainVersion string, settings map[string]string) *debug.BuildInfo {
	info := &debug.BuildInfo{}
	info.Main.Version = mainVersion
	for k, v := range settings {
		info.Settings = append(info.Settings, debug.BuildSetting{Key: k, Value: v})
	}
	return info
}

// withBuildVars sets the ldflags-injected build vars for the duration of a test
// and restores them after. It mutates package globals, so callers must not run in
// parallel.
func withBuildVars(t *testing.T, v, c, d string) {
	t.Helper()
	ov, oc, od := version, commit, buildDate
	version, commit, buildDate = v, c, d
	t.Cleanup(func() { version, commit, buildDate = ov, oc, od })
}

func TestResolveCommitPrefersLdflags(t *testing.T) {
	withBuildVars(t, "dev", "deadbeef", "unknown")
	assert.Equal(t, "deadbeef", resolveCommit(), "an explicit commit wins over the VCS stamp")

	// A leftover template falls back to the embedded VCS stamp (or "none").
	withBuildVars(t, "dev", "{{.Commit}}", "unknown")
	assert.NotEqual(t, "{{.Commit}}", resolveCommit(), "a template commit is never printed literally")
}

func TestResolveBuildDatePrefersLdflags(t *testing.T) {
	withBuildVars(t, "dev", "none", "2026-06-22_00:00:00_UTC")
	assert.Equal(t, "2026-06-22_00:00:00_UTC", resolveBuildDate(), "an explicit build date wins")

	withBuildVars(t, "dev", "none", "{{.Date}}")
	assert.NotEqual(t, "{{.Date}}", resolveBuildDate(), "a template build date is never printed literally")
}

func TestResolveBuildInfoPopulatesAllFields(t *testing.T) {
	withBuildVars(t, "v1.2.3", "abcdef0", "2026-01-01_00:00:00_UTC")
	info := resolveBuildInfo()
	assert.Equal(t, "v1.2.3", info.Version)
	assert.Equal(t, "abcdef0", info.Commit)
	assert.Equal(t, "2026-01-01_00:00:00_UTC", info.BuildDate)
	assert.NotEmpty(t, info.GoVersion)
	assert.NotEmpty(t, info.OS)
	assert.NotEmpty(t, info.Arch)
}

func TestResolveVersionFromFallbacks(t *testing.T) {
	// The default vars route every call through the build-info fallback.
	withBuildVars(t, "dev", "none", "unknown")

	// A module version (go install @vX.Y.Z) wins when present.
	assert.Equal(t, "v1.4.2", resolveVersionFrom(buildInfoWith("v1.4.2", nil), true))

	// A (devel) module with a VCS revision falls through to the short commit.
	got := resolveVersionFrom(buildInfoWith("(devel)", map[string]string{"vcs.revision": "abcdef0123456789"}), true)
	assert.Equal(t, "abcdef0", got)

	// No build info at all yields the "dev" sentinel.
	assert.Equal(t, "dev", resolveVersionFrom(nil, false))
}

func TestResolveCommitFromFallbacks(t *testing.T) {
	withBuildVars(t, "dev", "none", "unknown")

	got := resolveCommitFrom(buildInfoWith("(devel)", map[string]string{"vcs.revision": "0123456789abcdef"}), true)
	assert.Equal(t, "0123456", got, "the embedded revision is shortened")

	assert.Equal(t, "none", resolveCommitFrom(buildInfoWith("(devel)", nil), true), "no revision yields the none sentinel")
	assert.Equal(t, "none", resolveCommitFrom(nil, false))
}

func TestResolveBuildDateFromFallbacks(t *testing.T) {
	withBuildVars(t, "dev", "none", "unknown")

	// A valid RFC3339 vcs.time is reformatted into the compact UTC stamp.
	got := resolveBuildDateFrom(buildInfoWith("(devel)", map[string]string{"vcs.time": "2026-06-22T15:04:05Z"}), true)
	assert.Equal(t, "2026-06-22_15:04:05_UTC", got)

	// A non-RFC3339 vcs.time is passed through verbatim.
	got = resolveBuildDateFrom(buildInfoWith("(devel)", map[string]string{"vcs.time": "garbage"}), true)
	assert.Equal(t, "garbage", got)

	// A go-install build (module version, no vcs stamp) reports the generic marker.
	assert.Equal(t, "go-install", resolveBuildDateFrom(buildInfoWith("v1.0.0", nil), true))

	// Nothing at all yields the unknown sentinel.
	assert.Equal(t, "unknown", resolveBuildDateFrom(buildInfoWith("(devel)", nil), true))
	assert.Equal(t, "unknown", resolveBuildDateFrom(nil, false))
}

func TestVcsSettingMissingKeyReturnsEmpty(t *testing.T) {
	t.Parallel()
	info := &debug.BuildInfo{Settings: []debug.BuildSetting{{Key: "vcs.revision", Value: "abc"}}}
	assert.Equal(t, "abc", vcsSetting(info, "vcs.revision"), "a present setting is returned")
	assert.Empty(t, vcsSetting(info, "vcs.absent"), "an absent setting yields the empty string")
}
