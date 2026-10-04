//go:build loadtest

package loadtest

import (
	"strings"
	"testing"
)

// TestParseStatm exercises the /proc/self/statm parser on every platform,
// including the ones that never call it.
//
// That is the reason the parser lives in the GOOS-independent file: a parser
// tested only where it runs is a parser whose edge cases are tested only by
// production, and this one runs on the CI platform that produces the published
// numbers.
func TestParseStatm(t *testing.T) {
	t.Parallel()

	const page = 4096

	cases := []struct {
		name string
		in   string
		page int
		want uint64
		ok   bool
	}{
		{
			name: "a real statm line",
			in:   "5432 1234 567 8 0 900 0\n",
			page: page,
			want: 1234 * page,
			ok:   true,
		},
		{
			name: "no trailing newline",
			in:   "5432 1234 567 8 0 900 0",
			page: page,
			want: 1234 * page,
			ok:   true,
		},
		{
			name: "extra whitespace between fields",
			in:   "5432   1234  567 8 0 900 0\n",
			page: page,
			want: 1234 * page,
			ok:   true,
		},
		{
			name: "a large page size",
			in:   "1 2 3 4 5 6 7",
			page: 16384,
			want: 2 * 16384,
			ok:   true,
		},
		{
			name: "zero resident is a real reading, not an absence",
			in:   "10 0 0 0 0 0 0",
			page: page,
			want: 0,
			ok:   true,
		},
		{name: "empty", in: "", page: page, ok: false},
		{name: "only one field", in: "5432", page: page, ok: false},
		{name: "only whitespace", in: "   \n\t ", page: page, ok: false},
		{name: "non-numeric resident", in: "5432 abc 1", page: page, ok: false},
		{name: "negative resident", in: "5432 -1 1", page: page, ok: false},
		{name: "zero page size", in: "5432 1234 1", page: 0, ok: false},
		{name: "negative page size", in: "5432 1234 1", page: -1, ok: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, ok := parseStatm([]byte(tc.in), tc.page)
			if ok != tc.ok {
				t.Fatalf("parseStatm(%q, %d) ok = %v, want %v", tc.in, tc.page, ok, tc.ok)
			}
			if ok && got != tc.want {
				t.Errorf("parseStatm(%q, %d) = %d, want %d", tc.in, tc.page, got, tc.want)
			}
		})
	}
}

// TestParseStatmFailsClosed states the contract the report depends on: an
// unreadable statm yields "no reading", never a zero.
//
// A zero would travel into the report as a measurement, and a reader would
// conclude the harness used no memory. There is no way to tell those apart once
// the number is in a JSON file, so they must not be the same value.
func TestParseStatmFailsClosed(t *testing.T) {
	t.Parallel()

	for _, in := range []string{"", "garbage", "1"} {
		if got, ok := parseStatm([]byte(in), 4096); ok || got != 0 {
			t.Errorf("parseStatm(%q) = (%d, %v), want (0, false)", in, got, ok)
		}
	}
}

// TestCurrentRSSAgreesWithItsMechanism proves the platform's reading and its
// description are consistent — a platform that reports a value must not also
// describe itself as having none.
func TestCurrentRSSAgreesWithItsMechanism(t *testing.T) {
	t.Parallel()

	mechanism := rssMechanism()
	if mechanism == "" {
		t.Fatal("every platform must describe its RSS mechanism, including the ones that have none")
	}

	_, ok := currentRSS()
	unavailable := strings.Contains(mechanism, "none available")
	if ok == unavailable {
		t.Fatalf("currentRSS ok = %v but the mechanism says %q", ok, mechanism)
	}
}
