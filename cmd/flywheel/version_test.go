package main

import (
	"context"
	"encoding/json"
	"runtime"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestVersionCommandText(t *testing.T) {
	t.Parallel()
	out, err := runRoot(context.Background(), "version")
	require.NoError(t, err)
	assert.Contains(t, out, "flywheel ")
	assert.Contains(t, out, "go:")
	assert.Contains(t, out, "platform:")
	assert.Contains(t, out, runtime.GOOS+"/"+runtime.GOARCH)
}

func TestVersionCommandJSON(t *testing.T) {
	t.Parallel()
	out, err := runRoot(context.Background(), "version", "--json")
	require.NoError(t, err)

	var info buildInfo
	require.NoError(t, json.Unmarshal([]byte(out), &info))
	assert.NotEmpty(t, info.Version)
	assert.Equal(t, runtime.Version(), info.GoVersion)
	assert.Equal(t, runtime.GOOS, info.OS)
	assert.Equal(t, runtime.GOARCH, info.Arch)
}

func TestResolveVersionPrefersLdflags(t *testing.T) {
	// Not parallel: it mutates the package-level build vars and restores them.
	orig := version
	t.Cleanup(func() { version = orig })

	version = "v9.9.9"
	assert.Equal(t, "v9.9.9", resolveVersion())

	// A leftover goreleaser template falls back rather than printing literally.
	version = "{{.Version}}"
	assert.NotEqual(t, "{{.Version}}", resolveVersion())
}

func TestIsTemplateString(t *testing.T) {
	t.Parallel()
	assert.True(t, isTemplateString("{{.Version}}"))
	assert.False(t, isTemplateString("v1.2.3"))
}

func TestShortCommit(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "abcdef0", shortCommit("abcdef0123456789"))
	assert.Equal(t, "short", shortCommit("short"))
	assert.Equal(t, strings.Repeat("a", 7), shortCommit(strings.Repeat("a", 7)))
}
