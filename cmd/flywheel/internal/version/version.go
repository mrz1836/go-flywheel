// Package version provides semantic version comparison for the self-updater. A
// development build (the literal "dev", an empty string, or a bare commit hash)
// always sorts older than any tagged release, so the updater never reports a
// release as older than an un-tagged local build.
package version

import (
	"strconv"
	"strings"
)

// Compare returns 1 if v1 > v2, 0 if they are equal, and -1 if v1 < v2. A
// leading "v" is ignored. Pre-release/build suffixes (after "-" or "+") are
// dropped before the numeric major.minor.patch comparison.
func Compare(v1, v2 string) int {
	v1 = strings.TrimPrefix(strings.TrimSpace(v1), "v")
	v2 = strings.TrimPrefix(strings.TrimSpace(v2), "v")

	dev1 := isDev(v1)
	dev2 := isDev(v2)
	switch {
	case dev1 && dev2:
		return 0
	case dev1:
		return -1
	case dev2:
		return 1
	}

	p1 := parse(v1)
	p2 := parse(v2)
	for i := range 3 {
		a, b := 0, 0
		if i < len(p1) {
			a = p1[i]
		}
		if i < len(p2) {
			b = p2[i]
		}
		if a != b {
			if a > b {
				return 1
			}
			return -1
		}
	}
	return 0
}

// IsNewer reports whether latest is a strictly newer release than current.
func IsNewer(current, latest string) bool {
	return Compare(latest, current) > 0
}

// isDev reports whether v is a development marker — empty, the literal "dev", or
// a bare commit hash — which always sorts older than a real release.
func isDev(v string) bool {
	return v == "" || v == "dev" || isCommitHash(v)
}

// parse splits a version into its major/minor/patch integers, ignoring any
// pre-release or build suffix.
func parse(v string) []int {
	if i := strings.IndexAny(v, "-+"); i != -1 {
		v = v[:i]
	}
	parts := strings.Split(v, ".")
	out := make([]int, 0, len(parts))
	for _, p := range parts {
		n, err := strconv.Atoi(p)
		if err != nil {
			break
		}
		out = append(out, n)
	}
	return out
}

// isCommitHash reports whether s looks like a git commit hash (7–40 hex chars).
func isCommitHash(s string) bool {
	if len(s) < 7 || len(s) > 40 {
		return false
	}
	for _, c := range s {
		isHex := (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')
		if !isHex {
			return false
		}
	}
	return true
}
