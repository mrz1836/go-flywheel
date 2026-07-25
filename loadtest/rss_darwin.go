//go:build loadtest && darwin

package loadtest

// maxRSSUnitBytes converts getrusage's ru_maxrss to bytes. Darwin reports bytes
// already, where linux reports kilobytes — the same field, a 1024× difference.
const maxRSSUnitBytes = 1

// currentRSS returns the process's peak resident set so far.
//
// Darwin has no /proc, and the only resident-set figure the standard library
// exposes is getrusage's high-water mark. So this is not a current reading and
// the report must not be read as one: the sampled series is a monotone envelope
// that plateaus rather than falling when the process frees memory.
//
// Reading the true current RSS here would mean mach_task_basic_info through
// cgo, or a third-party package. Both are refused for the same reason: a
// //go:build loadtest dependency is still a published module requirement for
// every consumer of this library, and a benchmark harness does not get to widen
// the dependency surface of the thing it benchmarks. Disclosing the limitation
// costs a sentence in Notes; the alternative costs every downstream host.
func currentRSS() (uint64, bool) {
	return getrusageMaxRSS()
}

// rssMechanism names the source, for the report's Notes.
func rssMechanism() string {
	return "getrusage(RUSAGE_SELF).ru_maxrss (peak resident set; darwin has no current-RSS source " +
		"in the standard library, so the sampled series is a monotone high-water envelope)"
}
