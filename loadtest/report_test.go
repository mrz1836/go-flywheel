//go:build loadtest

package loadtest

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

// TestRedactDSNNeverLeaksCredentials is the test this file exists for. These
// reports are committed under docs/, so a DSN that reached one would publish a
// password to a public repository and trip the secret scanner on the way.
//
// The cases cover both DSN forms gorm accepts and, deliberately, several
// malformed ones: the function must fail closed, because a best-effort redaction
// of a string it could not parse is exactly how a credential escapes.
func TestRedactDSNNeverLeaksCredentials(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name, dsn, want string
	}{
		{
			name: "url with credentials",
			dsn:  "postgres://flywheel:flywheel@localhost:5432/flywheel_test?sslmode=disable",
			want: "postgres://localhost:5432/flywheel_test",
		},
		{
			name: "url without credentials",
			dsn:  "postgres://localhost:5432/flywheel_test",
			want: "postgres://localhost:5432/flywheel_test",
		},
		{
			name: "postgresql scheme",
			dsn:  "postgresql://u:p@db.internal:6432/app",
			want: "postgres://db.internal:6432/app",
		},
		{
			name: "keyword form",
			dsn:  "host=localhost port=5432 dbname=flywheel_test user=flywheel password=hunter2 sslmode=disable",
			want: "host=localhost port=5432 dbname=flywheel_test",
		},
		{
			name: "empty",
			dsn:  "",
			want: "",
		},
		{
			name: "unrecognized shape fails closed",
			dsn:  "this is not a dsn",
			want: "(unparsed dsn, redacted)",
		},
		{
			name: "unparsable url fails closed",
			dsn:  "postgres://user:pass@%%%/db",
			want: "(unparsed dsn, redacted)",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := redactDSN(tc.dsn)
			if got != tc.want {
				t.Fatalf("redactDSN(%q) = %q, want %q", tc.dsn, got, tc.want)
			}
			for _, secret := range []string{"hunter2", "password", "flywheel:flywheel", "u:p", "user:pass"} {
				if strings.Contains(got, secret) {
					t.Fatalf("redactDSN leaked %q in %q", secret, got)
				}
			}
		})
	}
}

// TestReportMarshalRedactsTheDSN proves the redaction is wired into the wire
// form, not merely available next to it. A correct redactDSN that Report did not
// call would pass the test above and still leak.
func TestReportMarshalRedactsTheDSN(t *testing.T) {
	t.Parallel()

	data, err := json.Marshal(Report{Config: Config{DSN: testDSN, Jobs: 100}})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if strings.Contains(string(data), "hunter2") {
		t.Fatalf("the marshaled report contains the DSN password: %s", data)
	}
	if !strings.Contains(string(data), "localhost:5432") {
		t.Fatalf("the marshaled report must keep the host so a reader knows the target: %s", data)
	}
}

// TestReportRoundTripsThroughJSON proves the wire form is lossless for
// everything except the credentials it deliberately drops. The declared struct
// does not round-trip — []error marshals to {} and cannot be read back — so this
// asserts the mirror actually replaces it rather than merely existing.
func TestReportRoundTripsThroughJSON(t *testing.T) {
	t.Parallel()

	src := Report{
		Config: Config{
			DSN: testDSN, Jobs: 100_000, Seed: 7, Runners: 4, Workers: 8,
			Mix: WorkloadDrainOnly, Indexes: IndexesFull,
			WorkDuration: 2 * time.Millisecond, WorkJitter: time.Millisecond,
			SampleInterval: time.Second, Timeout: 30 * time.Minute,
			Queue: "default", ExecutorClass: "loadtest",
		},
		StartedAt:         time.Date(2026, 7, 25, 12, 0, 0, 0, time.UTC),
		Duration:          92 * time.Second,
		EnqueueThroughput: 5123.5,
		DrainThroughput:   1088.25,
		Claim: Latency{
			Count: 12_500, Min: 80 * time.Microsecond, P50: 900 * time.Microsecond,
			P95: 4 * time.Millisecond, P99: 11 * time.Millisecond, Max: 210 * time.Millisecond,
			Mean: 1200 * time.Microsecond,
		},
		Finalize: Latency{Count: 100_000, Min: time.Microsecond, P50: 2 * time.Millisecond,
			P95: 5 * time.Millisecond, P99: 9 * time.Millisecond, Max: time.Second, Mean: 3 * time.Millisecond},
		Sweep: Latency{Count: 90, Min: time.Millisecond, P50: time.Millisecond, P95: 2 * time.Millisecond,
			P99: 3 * time.Millisecond, Max: 4 * time.Millisecond, Mean: time.Millisecond},
		PeakRSS:    412 * 1024 * 1024,
		Enqueued:   100_000,
		Drained:    99_998,
		Retried:    41,
		Discarded:  2,
		Superseded: 1,
		Errors: []error{
			errors.New("loadtest: seed job 4: context deadline exceeded"),
			errors.New("loadtest: completion check: connection refused (×2201)"),
		},
		Notes:          []string{"RSS is the harness client process, not the server."},
		WorkloadDigest: "9f2b1c0d",
		Histogram:      HistogramSpec{SubBucketsPerOctave: 3, MinExponent: 10, MaxExponent: 37, Buckets: 83},
		Schema:         "lt_abc_1",
	}

	data, err := json.Marshal(src)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var got Report
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	// The DSN is the one deliberate loss: the credentials were never written, so
	// there is nothing to restore and a placeholder would name a target that does
	// not exist.
	if got.Config.DSN != "" {
		t.Errorf("Config.DSN = %q, want empty: a report must not carry a target back", got.Config.DSN)
	}
	want := src
	want.Config.DSN = ""
	want.Errors = nil
	gotErrs := got.Errors
	got.Errors = nil

	if got.Config != want.Config {
		t.Errorf("Config round trip:\n got %+v\nwant %+v", got.Config, want.Config)
	}
	if got.Claim != want.Claim || got.Finalize != want.Finalize || got.Sweep != want.Sweep {
		t.Errorf("latency round trip:\n got %+v/%+v/%+v\nwant %+v/%+v/%+v",
			got.Claim, got.Finalize, got.Sweep, want.Claim, want.Finalize, want.Sweep)
	}
	if got.Duration != want.Duration || got.PeakRSS != want.PeakRSS ||
		got.Drained != want.Drained || got.Superseded != want.Superseded ||
		got.WorkloadDigest != want.WorkloadDigest || got.Histogram != want.Histogram ||
		got.Schema != want.Schema {
		t.Errorf("scalar round trip:\n got %+v\nwant %+v", got, want)
	}
	if len(gotErrs) != len(src.Errors) {
		t.Fatalf("errors round trip: got %d, want %d", len(gotErrs), len(src.Errors))
	}
	for i := range gotErrs {
		if gotErrs[i].Error() != src.Errors[i].Error() {
			t.Errorf("error %d = %q, want %q", i, gotErrs[i], src.Errors[i])
		}
	}
}

// TestNeverMeasuredLatencyIsOmitted guards a specific way a report could lie. A
// zero-valued Latency printed as "p50: 0, p99: 0" reads as an instantaneous
// operation; absent, it reads as what it is — never observed. The distinction
// matters most for Sweep, which only has observations because the harness runs
// its own sweeper.
func TestNeverMeasuredLatencyIsOmitted(t *testing.T) {
	t.Parallel()

	data, err := json.Marshal(Report{Config: Config{DSN: testDSN}})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	for _, key := range []string{`"claim"`, `"finalize"`, `"sweep"`} {
		if strings.Contains(string(data), key) {
			t.Errorf("a never-measured distribution must be absent, found %s in %s", key, data)
		}
	}

	measured, err := json.Marshal(Report{Claim: Latency{Count: 1, P99: time.Millisecond}})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if !strings.Contains(string(measured), `"claim"`) {
		t.Errorf("a measured distribution must be present: %s", measured)
	}
	if !strings.Contains(string(measured), `"1ms"`) {
		t.Errorf("durations are written readably: %s", measured)
	}
}

// TestSplitErrorCount proves the count survives the trip out of an error's
// message and back, including for messages that merely look like they carry one.
func TestSplitErrorCount(t *testing.T) {
	t.Parallel()

	cases := []struct {
		in      string
		wantMsg string
		wantN   int64
	}{
		{"connection refused (×2201)", "connection refused", 2201},
		{"connection refused", "connection refused", 1},
		{"weird (×)", "weird (×)", 1},
		{"weird (×zero)", "weird (×zero)", 1},
		{"parenthesized (but not a count)", "parenthesized (but not a count)", 1},
	}
	for _, tc := range cases {
		msg, n := splitErrorCount(tc.in)
		if msg != tc.wantMsg || n != tc.wantN {
			t.Errorf("splitErrorCount(%q) = (%q, %d), want (%q, %d)", tc.in, msg, n, tc.wantMsg, tc.wantN)
		}
	}
}
