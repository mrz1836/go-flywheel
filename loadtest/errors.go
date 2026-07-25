//go:build loadtest

package loadtest

import (
	"errors"
	"fmt"
	"sync"
)

// Sentinel errors the harness returns. They are matchable with errors.Is so a
// caller — a benchmark, a scenario main, a test — can tell a bad configuration
// from a failed run without matching on message text.
var (
	// ErrNoDSN reports a Config with no target.
	ErrNoDSN = errors.New("no DSN")
	// ErrInvalidConfig reports a field that cannot be defaulted into something
	// runnable.
	ErrInvalidConfig = errors.New("invalid config")
	// ErrTooManyConnections reports a Runners/Workers pair whose pool would
	// exceed the server's connection budget. It is separate from
	// ErrInvalidConfig because the fix is different: reduce concurrency, or raise
	// max_connections on the target.
	ErrTooManyConnections = errors.New("connection budget exceeded")
	// ErrRunTimedOut reports a run that hit Config.Timeout with work outstanding.
	ErrRunTimedOut = errors.New("run timed out")
	// ErrExactlyOnceViolated reports that the same job was observed executing in
	// two workers at once. It is the runtime's central guarantee, so it is a
	// distinct sentinel: a caller must be able to tell it from any other failure.
	ErrExactlyOnceViolated = errors.New("exactly-once violated: concurrent execution observed")
	// ErrUnsupportedDialect reports a target that is not PostgreSQL. The harness
	// is deliberately Postgres-only: the claim path it exists to measure is
	// FOR UPDATE SKIP LOCKED, which has no SQLite equivalent.
	ErrUnsupportedDialect = errors.New("unsupported dialect")
)

// maxDistinctErrors bounds how many distinct error messages an errset retains.
// Past it, messages are counted in aggregate rather than kept.
const maxDistinctErrors = 32

// errset is a bounded, deduplicating error collector.
//
// It exists because of the shape of the failures this harness produces: a
// paused-database fault at 100k depth generates tens of thousands of identical
// driver errors, one per in-flight operation. Retaining them all would turn a
// report into a log file and could outweigh the run's own memory. Retaining the
// distinct messages with their counts loses nothing a reader needs — "this
// happened 41,802 times" is the information, not 41,802 copies of it.
type errset struct {
	mu      sync.Mutex
	counts  map[string]int64
	order   []string
	dropped int64
}

// add records one error. A nil error is ignored, so callers may add
// unconditionally.
func (e *errset) add(err error) {
	if err == nil {
		return
	}
	msg := err.Error()

	e.mu.Lock()
	defer e.mu.Unlock()
	if e.counts == nil {
		e.counts = make(map[string]int64, maxDistinctErrors)
	}
	if _, seen := e.counts[msg]; !seen {
		if len(e.order) >= maxDistinctErrors {
			e.dropped++
			return
		}
		e.order = append(e.order, msg)
	}
	e.counts[msg]++
}

// errEntry is one distinct error message and how many times it occurred. It is
// the wire form: an error interface cannot round-trip through JSON, and a count
// is what a reader of a report actually needs.
type errEntry struct {
	Message string `json:"message"`
	Count   int64  `json:"count"`
}

// entries returns the distinct messages in first-seen order, each with its
// count, followed by a synthetic entry for anything dropped past the cap. The
// synthetic entry matters: a silently truncated error list reads as a run with
// fewer failure modes than it had.
func (e *errset) entries() []errEntry {
	e.mu.Lock()
	defer e.mu.Unlock()

	if len(e.order) == 0 && e.dropped == 0 {
		return nil
	}
	out := make([]errEntry, 0, len(e.order)+1)
	for _, msg := range e.order {
		out = append(out, errEntry{Message: msg, Count: e.counts[msg]})
	}
	if e.dropped > 0 {
		out = append(out, errEntry{
			Message: fmt.Sprintf("(%d further distinct error messages were not retained)", e.dropped),
			Count:   e.dropped,
		})
	}
	return out
}

// errs returns the collected messages as errors, for Report.Errors. Each carries
// its count in the message, because the in-process form has nowhere else to put
// it and a reader who sees only the message would read a mass failure as a
// single incident.
func (e *errset) errs() []error {
	entries := e.entries()
	if len(entries) == 0 {
		return nil
	}
	out := make([]error, len(entries))
	for i, entry := range entries {
		if entry.Count > 1 {
			out[i] = fmt.Errorf("%s (×%d)", entry.Message, entry.Count)
			continue
		}
		out[i] = errors.New(entry.Message)
	}
	return out
}

// total reports how many errors were added, including those past the cap.
func (e *errset) total() int64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	var n int64
	for _, c := range e.counts {
		n += c
	}
	return n + e.dropped
}
