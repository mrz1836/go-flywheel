//go:build loadtest && !linux && !darwin

package loadtest

// getrusageMaxRSS reports no reading.
//
// Windows has no syscall.Getrusage, so there is no peak-RSS source here either.
// The harness reports no memory number at all on such a platform rather than a
// partial one.
func getrusageMaxRSS() (uint64, bool) {
	return 0, false
}

// currentRSS reports no reading.
//
// It returns false rather than zero, and the difference is the whole point: a
// zero would travel into the report as a measurement, and a reader would
// conclude the harness used no memory. False makes the sample carry nothing and
// the note say why.
func currentRSS() (uint64, bool) {
	return 0, false
}

// rssMechanism names the source, for the report's Notes.
func rssMechanism() string {
	return "none available on this platform; RSS is not reported"
}
