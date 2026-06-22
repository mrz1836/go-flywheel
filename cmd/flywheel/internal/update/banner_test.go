package update

import (
	"bytes"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFormatBanner(t *testing.T) {
	t.Parallel()
	b := FormatBanner("v1.0.0", "v1.2.0")
	assert.Contains(t, b, "v1.0.0")
	assert.Contains(t, b, "v1.2.0")
	assert.Contains(t, b, "flywheel update")
	assert.Contains(t, b, "available")
}

func TestWriteBannerColorToggle(t *testing.T) {
	t.Parallel()
	res := &Result{CurrentVersion: "v1.0.0", LatestVersion: "v2.0.0", UpdateAvailable: true}

	var plain bytes.Buffer
	writeBanner(&plain, res, false)
	assert.NotContains(t, plain.String(), "\033[", "non-TTY output carries no ANSI color")
	assert.Contains(t, plain.String(), "v2.0.0")

	var colored bytes.Buffer
	writeBanner(&colored, res, true)
	assert.Contains(t, colored.String(), "\033[33m", "TTY output is colorized")
	assert.Contains(t, colored.String(), "\033[0m")
}

func TestShowBannerSilentCases(t *testing.T) {
	t.Parallel()
	// None of these should write to stderr or panic.
	ShowBanner(nil)
	ShowBanner(&Result{UpdateAvailable: false})
	ShowBanner(&Result{UpdateAvailable: true, Err: errors.New("x")})
}
