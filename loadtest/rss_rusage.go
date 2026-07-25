//go:build loadtest && (linux || darwin)

package loadtest

import "syscall"

// getrusageMaxRSS returns the process's peak resident set size in bytes, or
// false when the call fails.
//
// The unit is where this bites: getrusage reports ru_maxrss in kilobytes on
// linux and in bytes on darwin. maxRSSUnitBytes carries the difference, and
// getting it wrong is a silent 1024× error in a published number — which is why
// a test asserts the result's order of magnitude rather than trusting the
// constant.
func getrusageMaxRSS() (uint64, bool) {
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		return 0, false
	}
	if ru.Maxrss <= 0 {
		return 0, false
	}
	return uint64(ru.Maxrss) * maxRSSUnitBytes, true //nolint:gosec // positive by the guard above
}
