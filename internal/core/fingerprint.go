package core

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"regexp"
	"strings"
)

// ErrorFingerprintVersion is the version of the rules NormalizeErrorMessage
// applies and ErrorFingerprint hashes under. Every fingerprint carries it in
// its hashed text, so a change to any rule is a new version, and the error
// groups counted under the old one restart rather than merge with the new.
const ErrorFingerprintVersion = 1

// fingerprintPrefix is ErrorFingerprintVersion as the hashed text spells it.
const fingerprintPrefix = "v1"

// fingerprintSep separates the hashed fields: the unit separator, which no
// kind, class, or normalized message is expected to hold.
const fingerprintSep = "\x1f"

// maxNormalizedMessage is the most bytes NormalizeErrorMessage returns.
const maxNormalizedMessage = 512

// fingerprintLen is how many hex characters a fingerprint has: the first eight
// bytes of the hash.
const fingerprintLen = 16

// ErrorFingerprinter is implemented by a worker error that knows which failure
// it is better than its message does: a worker whose messages vary while the
// failure is one thing, such as an upstream's error code. When an error in a
// failed attempt's chain implements it and Fingerprint returns a non-empty
// value, the attempt's fingerprint hashes that value in place of the message,
// so every attempt that returns it joins one error group. An empty value is
// ignored, and the message is hashed as usual.
type ErrorFingerprinter interface {
	// Fingerprint returns the failure's stable name, or "" to fall back to the
	// message.
	Fingerprint() string
}

// NormalizeErrorMessage returns the text ErrorFingerprint hashes: message with
// the values that differ between two occurrences of one failure masked, so the
// two read alike. Under ErrorFingerprintVersion 1 it lowercases the message,
// then masks, in this order, UUIDs as <uuid>, email addresses as <email>, URLs
// as <url> (a URL whose user part was masked as an email address is still
// masked whole), runs of eight or more hex digits, with or without a 0x prefix,
// as <hex>, values in single or double quotes as <q> (an apostrophe inside a
// word, as in "can't", is not a quote), and numbers — digits with an optional
// sign and ".", ":", or "," between groups — as <n>; then it collapses each run
// of whitespace to one space, trims the ends, and cuts the result at 512 bytes
// on a rune boundary.
//
// A change to any of these rules is a new ErrorFingerprintVersion, after which
// the error groups restart.
func NormalizeErrorMessage(message string) string {
	s := strings.ToLower(message)
	s = uuidPattern.ReplaceAllString(s, "<uuid>")
	s = emailPattern.ReplaceAllString(s, "<email>")
	s = urlPattern.ReplaceAllString(s, "<url>")
	s = hexPattern.ReplaceAllString(s, "<hex>")
	s = quotedPattern.ReplaceAllString(s, "${1}<q>")
	s = numberPattern.ReplaceAllString(s, "<n>")
	s = spacePattern.ReplaceAllString(s, " ")
	return truncate(strings.TrimSpace(s), maxNormalizedMessage)
}

// ErrorFingerprint returns the fingerprint a failed attempt of kind with error
// class and message stores: 16 lowercase hex characters, the first eight bytes
// of SHA-256 over the version, the kind, the class, and the message as
// NormalizeErrorMessage leaves it, each separated by the 0x1f byte. The message
// is first cut at 4,096 bytes, as a run stores it, so a host computing a
// fingerprint from a worker's full message gets the one the run stored.
//
// A worker that supplies its own value through ErrorFingerprinter stores a
// fingerprint over that value instead, which this function does not compute.
func ErrorFingerprint(kind string, class ErrorClass, message string) string {
	return fingerprintText(kind, class, NormalizeErrorMessage(truncate(message, maxErrorMessage)))
}

// fingerprintText hashes kind, class, and an already-normalized text.
func fingerprintText(kind string, class ErrorClass, text string) string {
	sum := sha256.Sum256([]byte(fingerprintPrefix + fingerprintSep + kind + fingerprintSep + string(class) +
		fingerprintSep + text))
	return hex.EncodeToString(sum[:fingerprintLen/2])
}

// fingerprintOf is the fingerprint every write path stores: over the worker's
// own value when an error in cause's chain implements ErrorFingerprinter with a
// non-empty result, and otherwise over storedMessage, the message as the run
// stores it — never the worker's full message, which a read of the run no
// longer has.
func fingerprintOf(kind string, class ErrorClass, storedMessage string, cause error) string {
	var fp ErrorFingerprinter
	if cause != nil && errors.As(cause, &fp) {
		if custom := fp.Fingerprint(); custom != "" {
			return fingerprintText(kind, class, "custom"+fingerprintSep+custom)
		}
	}
	return fingerprintText(kind, class, NormalizeErrorMessage(storedMessage))
}

// failedOutcome reports whether an attempt that ended with outcome failed —
// error or timeout — which are the outcomes a run stores a fingerprint for.
func failedOutcome(outcome RunOutcome) bool {
	return outcome == OutcomeError || outcome == OutcomeTimeout
}

// validFingerprint reports whether s has a fingerprint's shape: 16 lowercase
// hex characters.
func validFingerprint(s string) bool {
	if len(s) != fingerprintLen {
		return false
	}
	for i := range len(s) {
		c := s[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

// The masks of NormalizeErrorMessage, version 1, compiled once. They apply to
// lowercased text, in the order NormalizeErrorMessage lists them.
//
//nolint:gochecknoglobals // package-level compiled regexes for NormalizeErrorMessage
var (
	uuidPattern  = regexp.MustCompile(`[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}`)
	emailPattern = regexp.MustCompile(`[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}`)
	// urlPattern runs after emailPattern, so it accepts the < and > of an
	// <email> mask in a URL's user part and masks the whole URL.
	urlPattern = regexp.MustCompile(`[a-z][a-z0-9+.-]*://[^\s"']+`)
	hexPattern = regexp.MustCompile(`\b(?:0x)?[0-9a-f]{8,}\b`)
	// quotedPattern keeps the character before the opening quote, which may not
	// be a letter, a digit, or an underscore, so a contraction's apostrophe
	// never opens a quote.
	quotedPattern = regexp.MustCompile(`(^|[^\pL\pN_])(?:'[^']*'|"[^"]*")`)
	numberPattern = regexp.MustCompile(`[-+]?[0-9]+(?:[.:,][0-9]+)*`)
	spacePattern  = regexp.MustCompile(`\s+`)
)
