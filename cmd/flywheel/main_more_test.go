package main

import (
	"bytes"
	"context"
	"testing"

	"github.com/mrz1836/go-flywheel/cmd/flywheel/internal/update"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRunReturnsZeroOnSuccess(t *testing.T) {
	// Not parallel: run starts a background update check that reads HOME for its
	// cache; pin it to a temp dir so the check is hermetic.
	t.Setenv("HOME", t.TempDir())
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	t.Setenv("FLYWHEEL_DISABLE_UPDATE_CHECK", "1")

	var stderr bytes.Buffer
	code := run(context.Background(), []string{"version"}, &stderr)
	assert.Equal(t, 0, code, "a successful command exits 0")
	assert.Empty(t, stderr.String(), "no error is written on success")
}

func TestRunReturnsOneAndWritesErrorOnFailure(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	t.Setenv("FLYWHEEL_DISABLE_UPDATE_CHECK", "1")

	var stderr bytes.Buffer
	// An unknown command makes Execute return an error, so run reports exit code 1.
	code := run(context.Background(), []string{"no-such-command"}, &stderr)
	assert.Equal(t, 1, code, "a failing command exits 1")
	assert.Contains(t, stderr.String(), "flywheel:", "the error is prefixed and written to stderr")
}

func TestShowUpdateBannerDrainsAvailableResult(t *testing.T) {
	t.Parallel()
	// A non-nil channel carrying an available update is drained without blocking; the
	// banner itself writes to os.Stderr, so here we just prove the drain path runs.
	ch := make(chan *update.Result, 1)
	ch <- &update.Result{CurrentVersion: "v1.0.0", LatestVersion: "v2.0.0", UpdateAvailable: true}

	cmd := &cobra.Command{Use: "status"}
	assert.NotPanics(t, func() { showUpdateBanner(cmd, ch) }, "draining an available result runs the banner path")
}

func TestShowUpdateBannerTimesOutWhenChannelSilent(t *testing.T) {
	t.Parallel()
	// An open, empty channel forces the drain-timeout branch (bounded, never hangs).
	ch := make(chan *update.Result)
	cmd := &cobra.Command{Use: "status"}
	assert.NotPanics(t, func() { showUpdateBanner(cmd, ch) }, "a silent channel falls through the bounded timeout")
}

func TestShowUpdateBannerSkippedForNilAndUpdateCommand(t *testing.T) {
	t.Parallel()
	// A nil channel is a no-op (banner disabled, e.g. in tests).
	assert.NotPanics(t, func() { showUpdateBanner(&cobra.Command{Use: "status"}, nil) })

	// The update command speaks for itself, so the banner is suppressed even with a
	// ready result on the channel.
	ch := make(chan *update.Result, 1)
	ch <- &update.Result{UpdateAvailable: true}
	require.NotNil(t, ch)
	assert.NotPanics(t, func() { showUpdateBanner(&cobra.Command{Use: "update"}, ch) })
}
