package flywheel

import (
	"encoding/json"
	"testing"
	"unicode/utf8"
)

// FuzzRequestIDMetadataRoundTrip exercises the two JSON metadata parsers on
// arbitrary input: metadataWithRequestID must always emit a valid, non-empty
// JSON object, requestIDFromMetadata must never panic on arbitrary bytes, and a
// non-empty request_id stamped in must read back unchanged.
func FuzzRequestIDMetadataRoundTrip(f *testing.F) {
	f.Add([]byte(`{}`), "req-123")
	f.Add([]byte(`{"existing":"tag"}`), "")
	f.Add([]byte(``), "abc")
	f.Add([]byte(`not json`), "x")
	f.Add([]byte(`{"request_id":"old"}`), "new")
	f.Add([]byte(`[1,2,3]`), "arr")

	f.Fuzz(func(t *testing.T, base []byte, requestID string) {
		// The parser must tolerate arbitrary persisted blobs without panicking.
		_ = requestIDFromMetadata(base)

		out := metadataWithRequestID(base, requestID)
		if len(out) == 0 {
			t.Fatalf("metadataWithRequestID returned empty output (base=%q id=%q)", base, requestID)
		}
		var obj map[string]any
		if err := json.Unmarshal(out, &obj); err != nil {
			t.Fatalf("output is not a valid JSON object: %v (out=%q)", err, out)
		}

		// Round-trip only holds for valid-UTF-8 ids: encoding/json replaces
		// invalid bytes with U+FFFD, which is fine because a real request_id is
		// an ASCII X-Request-ID header value.
		if requestID != "" && utf8.ValidString(requestID) {
			if got := requestIDFromMetadata(out); got != requestID {
				t.Fatalf("round-trip mismatch: stamped %q, read back %q", requestID, got)
			}
		}
	})
}
