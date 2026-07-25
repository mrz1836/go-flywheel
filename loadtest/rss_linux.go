//go:build loadtest && linux

package loadtest

import (
	"os"
)

// maxRSSUnitBytes converts getrusage's ru_maxrss to bytes. Linux reports
// kilobytes.
const maxRSSUnitBytes = 1024

// currentRSS reads the process's current resident set from /proc/self/statm.
//
// This is a genuine instantaneous reading, so the sampled series tracks a
// process that frees memory — unlike the darwin path, which can only ever
// climb. The cost is that a spike between two ticks is invisible, which is what
// Report.PeakRSS's getrusage term repairs.
func currentRSS() (uint64, bool) {
	data, err := os.ReadFile("/proc/self/statm")
	if err != nil {
		return 0, false
	}
	return parseStatm(data, os.Getpagesize())
}

// rssMechanism names the source, for the report's Notes.
func rssMechanism() string {
	return "/proc/self/statm (current resident set), with getrusage(RUSAGE_SELF).ru_maxrss as the peak"
}
