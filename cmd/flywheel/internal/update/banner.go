package update

import (
	"fmt"
	"io"
	"os"
	"strings"
)

// FormatBanner renders the "update available" notice for current→latest. It is
// pure (no I/O) so tests can assert its content directly.
func FormatBanner(current, latest string) string {
	lines := []string{
		"",
		"  A new version of flywheel is available!",
		fmt.Sprintf("    current: %s", current),
		fmt.Sprintf("    latest:  %s", latest),
		"    upgrade: flywheel update",
		"",
	}
	return strings.Join(lines, "\n")
}

// ShowBanner writes the update banner to stderr when result reports an available
// update. It is silent otherwise (nil result, error, or no update). The banner
// is colorized only when stderr is a terminal and color is not suppressed.
func ShowBanner(result *Result) {
	if result == nil || result.Err != nil || !result.UpdateAvailable {
		return
	}
	writeBanner(os.Stderr, result, isTerminal(os.Stderr))
}

// writeBanner renders the banner to w, applying ANSI color only when color is
// true. It is the testable core of ShowBanner.
func writeBanner(w io.Writer, result *Result, color bool) {
	banner := FormatBanner(result.CurrentVersion, result.LatestVersion)
	if color {
		_, _ = fmt.Fprintf(w, "\033[33m%s\033[0m\n", banner)
		return
	}
	_, _ = fmt.Fprintf(w, "%s\n", banner)
}

// isTerminal reports whether f is attached to a character device (a TTY),
// without depending on golang.org/x/term. Color is also suppressed under CI or
// when NO_COLOR is set.
func isTerminal(f *os.File) bool {
	if os.Getenv("NO_COLOR") != "" || truthy(os.Getenv("CI")) {
		return false
	}
	info, err := f.Stat()
	if err != nil {
		return false
	}
	return info.Mode()&os.ModeCharDevice != 0
}
