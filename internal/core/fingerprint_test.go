package core

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fingerprintShape is what every fingerprint looks like: 16 lowercase hex
// characters.
//
//nolint:gochecknoglobals // a fixed pattern shared by the fingerprint tests
var fingerprintShape = regexp.MustCompile(`^[0-9a-f]{16}$`)

// customFingerprintError is a worker error that names its own failure, the way
// an upstream's error code does.
type customFingerprintError struct{ code, message string }

func (e customFingerprintError) Error() string       { return e.message }
func (e customFingerprintError) Fingerprint() string { return e.code }

// TestFingerprintIgnoresIdsNumbersAndEmails proves two failures that differ
// only in the values a message carries — an address, a port, an id, a quoted
// name — share a fingerprint, while a different failure of the same kind does
// not; and that each value is masked as its own token.
func TestFingerprintIgnoresIdsNumbersAndEmails(t *testing.T) {
	t.Parallel()

	refused := ErrorFingerprint("send_mail", ErrorTransient, "dial tcp 10.0.0.12:5432: connect: connection refused")
	assert.Regexp(t, fingerprintShape, refused)
	assert.Equal(t, refused,
		ErrorFingerprint("send_mail", ErrorTransient, "dial tcp 10.0.0.7:5432: connect: connection refused"),
		"the same failure against another address is one group")
	assert.NotEqual(t, refused,
		ErrorFingerprint("send_mail", ErrorTransient, "dial tcp 10.0.0.7:5432: i/o timeout"),
		"a different failure is another group")
	assert.NotEqual(t, refused,
		ErrorFingerprint("send_sms", ErrorTransient, "dial tcp 10.0.0.12:5432: connect: connection refused"),
		"and so is the same failure of another kind")
	assert.NotEqual(t, refused,
		ErrorFingerprint("send_mail", ErrorPermanent, "dial tcp 10.0.0.12:5432: connect: connection refused"),
		"or of another class")

	for _, tc := range []struct {
		name       string
		a, b       string
		normalized string
	}{
		{
			name:       "a uuid",
			a:          "order 0190e9a2-7c1d-7b3e-8f00-1234567890ab not found",
			b:          "order 7D444840-9DC0-11D1-B245-5FFDCE74FAD2 not found",
			normalized: "order <uuid> not found",
		},
		{
			name:       "an email address",
			a:          "bounce from ana@example.com",
			b:          "bounce from Bo.Lee+news@mail.example.org",
			normalized: "bounce from <email>",
		},
		{
			name:       "a url",
			a:          "GET https://api.example.com/v2/items?id=42 failed",
			b:          "GET http://localhost:8080/health failed",
			normalized: "get <url> failed",
		},
		{
			name:       "a url whose user part is an email address",
			a:          "fetch https://ana@example.com/feed failed",
			b:          "fetch https://example.net/feed failed",
			normalized: "fetch <url> failed",
		},
		{
			name:       "a hex run",
			a:          "checksum deadbeefcafe0001 mismatch at 0x7ffd5e8a",
			b:          "checksum 0123456789abcdef mismatch at 0x00001000",
			normalized: "checksum <hex> mismatch at <hex>",
		},
		{
			name:       "a quoted value",
			a:          `unknown field "amount" in 'invoices'`,
			b:          `unknown field "total_due" in 'credit notes'`,
			normalized: "unknown field <q> in <q>",
		},
		{
			name:       "a number",
			a:          "retry 3 of 5 after 1.5s at 10:30:00, -2 left",
			b:          "retry 4 of 25 after 12.25s at 23:59:59, +7 left",
			normalized: "retry <n> of <n> after <n>s at <n>, <n> left",
		},
		{
			name:       "whitespace",
			a:          "  connection\treset\n\nby peer  ",
			b:          "connection reset by peer",
			normalized: "connection reset by peer",
		},
		{
			name:       "an apostrophe inside a word is not a quote",
			a:          "can't open 'ledger-7': don't retry",
			b:          "can't open 'ledger-9': don't retry",
			normalized: "can't open <q>: don't retry",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.normalized, NormalizeErrorMessage(tc.a))
			assert.Equal(t, tc.normalized, NormalizeErrorMessage(tc.b))
			assert.Equal(t, ErrorFingerprint("sync", ErrorPermanent, tc.a), ErrorFingerprint("sync", ErrorPermanent, tc.b))
		})
	}
}

// TestFingerprintVectorsAreStable pins version 1 to vectors computed outside
// the code: each normalized text was written by hand from the documented
// masks, and each fingerprint is the first 16 hex characters of
//
//	printf 'v1\037%s\037%s\037%s' '<kind>' '<class>' '<normalized text>' | shasum -a 256
//
// (the custom row hashes "custom", the separator, and the worker's value in
// place of the text). A change to any mask changes a vector, which is a new
// ErrorFingerprintVersion.
func TestFingerprintVectorsAreStable(t *testing.T) {
	t.Parallel()
	assert.Equal(t, 1, ErrorFingerprintVersion)

	for _, tc := range []struct {
		name        string
		kind        string
		class       ErrorClass
		message     string
		normalized  string
		fingerprint string
	}{
		{
			name: "an address and a port", kind: "send_mail", class: ErrorTransient,
			message:     "dial tcp 10.0.0.12:5432: connect: connection refused",
			normalized:  "dial tcp <n>: connect: connection refused",
			fingerprint: "8810a35fd746d590",
		},
		{
			name: "a uuid, an email address, a number, and a url", kind: "sync", class: ErrorPermanent,
			message:     "Job 0190E9A2-7C1D-7B3E-8F00-1234567890AB failed for Ana@Example.com: HTTP 503 from https://api.example.com/v2/items?id=42",
			normalized:  "job <uuid> failed for <email>: http <n> from <url>",
			fingerprint: "081a3002a68d611e",
		},
		{
			name: "a duration and a quoted name", kind: "report", class: ErrorTimeout,
			message:     "context deadline exceeded after 30s waiting on 'warehouse-7'",
			normalized:  "context deadline exceeded after <n>s waiting on <q>",
			fingerprint: "141de50f60b5203e",
		},
		{
			name: "a contraction, quoted values, and a hex run", kind: "import", class: ErrorValidation,
			message:     `can't parse row 1,204: field "amount" = "12.50 USD" (sha deadbeefcafe0001)`,
			normalized:  "can't parse row <n>: field <q> = <q> (sha <hex>)",
			fingerprint: "8e28d4542043e2fb",
		},
		{
			name: "a message cut at 512 bytes", kind: "export", class: ErrorPermanent,
			message:     strings.Repeat("x", 600),
			normalized:  strings.Repeat("x", 512),
			fingerprint: "2975bdaeb0d8ed4b",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.normalized, NormalizeErrorMessage(tc.message))
			assert.Equal(t, tc.fingerprint, ErrorFingerprint(tc.kind, tc.class, tc.message))
		})
	}

	t.Run("a worker's own fingerprint", func(t *testing.T) {
		t.Parallel()
		cause := fmt.Errorf("deliver: %w", customFingerprintError{code: "smtp-421", message: "421 4.7.0 try again later"})
		got := fingerprintOf("send_mail", ErrorTransient, cause.Error(), cause)
		assert.Equal(t, "b988885cd8179df6", got)
		assert.NotEqual(t, ErrorFingerprint("send_mail", ErrorTransient, cause.Error()), got,
			"the worker's value replaces the message")

		empty := customFingerprintError{code: "", message: "421 4.7.0 try again later"}
		assert.Equal(t, ErrorFingerprint("send_mail", ErrorTransient, empty.Error()),
			fingerprintOf("send_mail", ErrorTransient, empty.Error(), empty), "an empty value is ignored")
	})
}

// TestFingerprintHashesTheStoredMessage proves the fingerprint is over the
// message as a run stores it — cut at 4,096 bytes — so a finalize, a seed, a
// read of an older run, and a host calling ErrorFingerprint agree even when
// the worker's message was longer, and the text past the cut changes nothing.
func TestFingerprintHashesTheStoredMessage(t *testing.T) {
	t.Parallel()
	// The first 4,096 bytes normalize to "payload <hex>"; the text past them does
	// not survive the store, so it must not reach the fingerprint either.
	long := "payload " + strings.Repeat("7", 5000) + " and a tail the store cuts off"
	require.Greater(t, len(long), 5000)
	stored := truncate(long, maxErrorMessage)
	require.Len(t, stored, maxErrorMessage)

	want := fingerprintText("ingest", ErrorTransient, "payload <hex>")
	assert.Equal(t, want, ErrorFingerprint("ingest", ErrorTransient, long), "ErrorFingerprint cuts as the store does")
	assert.Equal(t, want, ErrorFingerprint("ingest", ErrorTransient, stored))
	assert.Equal(t, want, fingerprintOf("ingest", ErrorTransient, stored, errors.New(long)),
		"the write path hashes the stored message, not the worker's")
	assert.NotEqual(t, want, fingerprintText("ingest", ErrorTransient, NormalizeErrorMessage(long)),
		"the uncut message hashes differently, so the cut is what makes them agree")

	assert.True(t, validFingerprint(want))
	for _, bad := range []string{"", "8810A35FD746D590", "8810a35fd746d59", "8810a35fd746d5900", "8810a35fd746d59g"} {
		assert.False(t, validFingerprint(bad), "%q is not a fingerprint", bad)
	}
}
