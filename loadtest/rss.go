//go:build loadtest

package loadtest

import (
	"strconv"
)

// RSS reporting is split three ways for one reason: the platforms differ in what
// they can actually tell you, and pretending otherwise would put a number in the
// report that means something different depending on where it ran.
//
//   - linux reads /proc/self/statm, which is the current resident set. Sampling
//     it can miss a spike between two ticks.
//   - darwin has no /proc, so the only stdlib source is getrusage's high-water
//     mark. Every "current" reading there is really a monotone envelope, and the
//     samples plateau rather than tracking a process that frees memory.
//   - everywhere else reports nothing, and the report says so. Windows has no
//     syscall.Getrusage at all, which is why even the peak-RSS call lives in the
//     platform files rather than here.
//
// The parser lives here rather than in the linux file so it is unit-tested on
// every platform, including the ones that never call it. A parser exercised only
// where it runs is a parser whose edge cases are tested only by production.
//
// Only the standard library's syscall package is used. A //go:build loadtest
// import is still a published module requirement for every consumer of this
// library, so the harness does not get to add a dependency for convenience.

// statmResidentField is the index of the resident-set field in
// /proc/self/statm, whose fields are: size resident shared text lib data dt.
const statmResidentField = 1

// parseStatm extracts the resident set size, in bytes, from the contents of
// /proc/self/statm. The file reports page counts, so the caller supplies the
// page size.
//
// It returns false rather than a zero on anything it does not recognize. A zero
// RSS would be reported as a measurement; a false is reported as an absence.
func parseStatm(data []byte, pageSize int) (uint64, bool) {
	if pageSize <= 0 {
		return 0, false
	}

	field := 0
	start := -1
	for i := 0; i <= len(data); i++ {
		atEnd := i == len(data)
		isSep := atEnd || data[i] == ' ' || data[i] == '\n' || data[i] == '\t'

		if !isSep {
			if start < 0 {
				start = i
			}
			continue
		}
		if start < 0 {
			continue
		}
		if field == statmResidentField {
			pages, err := strconv.ParseUint(string(data[start:i]), 10, 64)
			if err != nil {
				return 0, false
			}
			return pages * uint64(pageSize), true //nolint:gosec // guarded positive above
		}
		field++
		start = -1
	}
	return 0, false
}
