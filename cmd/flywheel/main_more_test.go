package main

import (
	"bytes"
	"context"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRunReturnsZeroOnSuccess(t *testing.T) {
	// Not parallel: run starts a background update check that reads HOME for its
	// cache; pin it to a temp dir so the check is hermetic.
	t.Setenv("HOME", t.TempDir())
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	t.Setenv("FLYWHEEL_NO_UPDATE_CHECK", "1")

	var stderr bytes.Buffer
	code := run(context.Background(), []string{"version"}, &stderr)
	assert.Equal(t, 0, code, "a successful command exits 0")
	assert.Empty(t, stderr.String(), "no error is written on success")
}

func TestRunReturnsOneAndWritesErrorOnFailure(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	t.Setenv("FLYWHEEL_NO_UPDATE_CHECK", "1")

	var stderr bytes.Buffer
	// An unknown command makes Execute return an error, so run reports exit code 1.
	code := run(context.Background(), []string{"no-such-command"}, &stderr)
	assert.Equal(t, 1, code, "a failing command exits 1")
	assert.Contains(t, stderr.String(), "flywheel:", "the error is prefixed and written to stderr")
}

// TestUpdateCommandWiring locks the seam between newRootCmd and the go-selfupdate
// cobracmd package: the update command is registered with the upgrade alias and
// the check/force/verbose boolean flags. The command's behavior itself is covered
// by the library's own suites, so this asserts only the wiring.
func TestUpdateCommandWiring(t *testing.T) {
	t.Parallel()

	var updateCmd *cobra.Command
	for _, c := range newRootCmd().Commands() {
		if c.Name() == "update" {
			updateCmd = c
			break
		}
	}
	require.NotNil(t, updateCmd, "newRootCmd registers an update command")
	assert.Contains(t, updateCmd.Aliases, "upgrade", "the update command carries the upgrade alias")

	for _, name := range []string{"check", "force", "verbose"} {
		flag := updateCmd.Flags().Lookup(name)
		require.NotNilf(t, flag, "the update command registers --%s", name)
		assert.Equalf(t, "bool", flag.Value.Type(), "--%s is a boolean flag", name)
	}
}
