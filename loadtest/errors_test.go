//go:build loadtest

package loadtest

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
)

// TestErrsetDeduplicatesAndCounts covers the case the type exists for: a fault
// at depth produces tens of thousands of identical driver errors, and a report
// that carried every copy would be a log file. The count is the information.
func TestErrsetDeduplicatesAndCounts(t *testing.T) {
	t.Parallel()

	var set errset
	for range 5000 {
		set.add(errors.New("connection refused"))
	}
	set.add(errors.New("deadlock detected"))
	set.add(nil) // ignored, so callers may add unconditionally

	entries := set.entries()
	if len(entries) != 2 {
		t.Fatalf("got %d distinct entries, want 2: %+v", len(entries), entries)
	}
	if entries[0].Message != "connection refused" || entries[0].Count != 5000 {
		t.Errorf("entry 0 = %+v, want connection refused ×5000", entries[0])
	}
	if entries[1].Message != "deadlock detected" || entries[1].Count != 1 {
		t.Errorf("entry 1 = %+v, want deadlock detected ×1", entries[1])
	}
	if got := set.total(); got != 5001 {
		t.Errorf("total = %d, want 5001", got)
	}

	// The in-process form carries the count in the message, because []error has
	// nowhere else to put it and a reader seeing only "connection refused" would
	// read a mass failure as one incident.
	errs := set.errs()
	if len(errs) != 2 || !strings.Contains(errs[0].Error(), "×5000") {
		t.Errorf("errs = %v, want the count in the message", errs)
	}
	if strings.Contains(errs[1].Error(), "×") {
		t.Errorf("a single occurrence must not be annotated: %q", errs[1])
	}
}

// TestErrsetCapIsDisclosed proves a truncated error list says it was truncated.
// A silently capped list reads as a run with fewer failure modes than it had,
// which is the same class of lie as a zeroed percentile.
func TestErrsetCapIsDisclosed(t *testing.T) {
	t.Parallel()

	var set errset
	for i := range maxDistinctErrors + 17 {
		set.add(fmt.Errorf("distinct failure %d", i))
	}

	entries := set.entries()
	if len(entries) != maxDistinctErrors+1 {
		t.Fatalf("got %d entries, want %d retained plus one disclosure", len(entries), maxDistinctErrors)
	}
	last := entries[len(entries)-1]
	if !strings.Contains(last.Message, "not retained") || last.Count != 17 {
		t.Errorf("the disclosure entry must name the count of what was dropped, got %+v", last)
	}
	if got := set.total(); got != int64(maxDistinctErrors+17) {
		t.Errorf("total = %d, want %d — dropped errors still count", got, maxDistinctErrors+17)
	}
}

// TestErrsetIsConcurrencySafe runs the collector the way the harness does: from
// every runner goroutine at once. Under -race, an unsynchronized map write here
// would be a failure rather than a rare corruption in a long run.
func TestErrsetIsConcurrencySafe(t *testing.T) {
	t.Parallel()

	var set errset
	var wg sync.WaitGroup
	for g := range 8 {
		wg.Go(func() {
			for i := range 500 {
				set.add(fmt.Errorf("failure %d", i%4))
				_ = set.entries()
			}
			_ = g
		})
	}
	wg.Wait()

	if got := set.total(); got != 4000 {
		t.Errorf("total = %d, want 4000", got)
	}
	if got := len(set.entries()); got != 4 {
		t.Errorf("distinct entries = %d, want 4", got)
	}
}

// TestErrsetOrderIsFirstSeen keeps the report's error list stable across runs
// with the same failures, which is what makes two reports diffable.
func TestErrsetOrderIsFirstSeen(t *testing.T) {
	t.Parallel()

	var set errset
	set.add(errors.New("second-in-volume"))
	set.add(errors.New("first-in-volume"))
	set.add(errors.New("first-in-volume"))

	entries := set.entries()
	if entries[0].Message != "second-in-volume" {
		t.Errorf("entries must be in first-seen order, got %+v", entries)
	}
}
