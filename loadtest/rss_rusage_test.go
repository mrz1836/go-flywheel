//go:build loadtest && (linux || darwin)

package loadtest

import "testing"

// This test lives apart from rss_test.go because it names maxRSSUnitBytes,
// which only the getrusage platforms define: in the shared file it kept the
// package's tests from compiling anywhere else.

// TestGetrusageMaxRSSIsPlausible checks the platform's unit scaling, which is
// the one thing about RSS that is silently wrong rather than loudly wrong:
// getrusage reports ru_maxrss in kilobytes on linux and bytes on darwin, so a
// misconfigured maxRSSUnitBytes is a 1024× error in a published number.
//
// The bounds are wide on purpose. This asserts the order of magnitude — a Go
// test binary is somewhere between a megabyte and a few gigabytes of resident
// memory — which is exactly the resolution at which a factor of 1024 shows up.
func TestGetrusageMaxRSSIsPlausible(t *testing.T) {
	t.Parallel()

	got, ok := getrusageMaxRSS()
	if !ok {
		t.Skip("getrusage is unavailable on this platform")
	}
	const oneMB, fourGB = 1 << 20, uint64(4) << 30
	if got < oneMB || got > fourGB {
		t.Fatalf("peak RSS = %d bytes, which is outside [1 MiB, 4 GiB]: maxRSSUnitBytes (%d) "+
			"is probably wrong for this platform", got, maxRSSUnitBytes)
	}
}
