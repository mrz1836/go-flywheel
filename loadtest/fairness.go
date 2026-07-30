//go:build loadtest

package loadtest

import "sync"

// fairnessRecorder captures the parent ordinal of each claim in the order the
// workers ran them.
//
// It is a mutex-guarded slice rather than per-parent atomics because the metric
// is about *order*, not just totals: whether the parents interleave or one drains
// before the other is the whole question, and a per-parent counter would answer
// "did both run" while hiding "did the second wait for the first". The lock is
// held for one append against millisecond-scale database work, so it does not
// become the thing being measured.
type fairnessRecorder struct {
	mu    sync.Mutex
	order []int
}

// record appends one claim's parent ordinal.
func (r *fairnessRecorder) record(group int) {
	r.mu.Lock()
	r.order = append(r.order, group)
	r.mu.Unlock()
}

// snapshot returns a copy of the recorded order.
func (r *fairnessRecorder) snapshot() []int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]int(nil), r.order...)
}

// fairnessWindow is how many consecutive claims one interleaving window spans. It
// is sized to a couple of full round-robin cycles across the parents, so a window
// that a fair claim would split evenly is large enough not to be dominated by the
// concurrency jitter of a single claimed batch.
const fairnessWindow = 64

// computeFairness turns a claim-order trace into the interleaving and
// non-starvation numbers.
//
// The two findings are MaxConsecutiveSameParent — under strict FIFO the first
// parent's whole batch runs before the second, so this is near the batch size,
// and under banding it is a small handful — and MinActiveWindowShare, the
// smallest slice any still-working parent got in any window, which is ~1/Parents
// under banding and ~0 under FIFO for the parent that is waiting its turn.
func computeFairness(cfg Config, order []int) *FairnessReport {
	rep := &FairnessReport{
		Strategy:             string(cfg.Fairness),
		Parents:              cfg.Parents,
		ChildrenPerParent:    childCount(cfg),
		WindowSize:           fairnessWindow,
		ClaimsPerParent:      make([]int64, cfg.Parents),
		MinActiveWindowShare: 1,
	}
	if len(order) == 0 {
		rep.MinActiveWindowShare = 0
		return rep
	}

	// Totals and the longest single-parent run.
	longest, run, prev := 1, 1, order[0]
	for i, g := range order {
		if g >= 0 && g < len(rep.ClaimsPerParent) {
			rep.ClaimsPerParent[g]++
		}
		if i == 0 {
			continue
		}
		if g == prev {
			run++
			if run > longest {
				longest = run
			}
		} else {
			run, prev = 1, g
		}
	}
	rep.MaxConsecutiveSameParent = longest

	// Remaining work per parent, walked down as the window advances, so a parent
	// that has finished is not counted as "starved" in the windows after it is done.
	remaining := make([]int64, cfg.Parents)
	copy(remaining, rep.ClaimsPerParent)

	for start := 0; start < len(order); start += fairnessWindow {
		end := min(start+fairnessWindow, len(order))
		counts := make([]int, cfg.Parents)
		for _, g := range order[start:end] {
			if g >= 0 && g < len(counts) {
				counts[g]++
			}
		}
		width := float64(end - start)
		// The smallest share among parents that still had work to claim entering this
		// window: a parent already drained is not owed a slice of it.
		for p := range counts {
			if remaining[p] <= 0 {
				continue
			}
			share := float64(counts[p]) / width
			if share < rep.MinActiveWindowShare {
				rep.MinActiveWindowShare = share
			}
		}
		for p, c := range counts {
			remaining[p] -= int64(c)
		}
		rep.Windows++
	}
	return rep
}
