//go:build loadtest

package loadtest

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestComputeFairnessSeparatesFIFOFromInterleaved pins the two findings the
// metric exists to produce, against hand-built claim traces whose answer is known
// by construction: a strict-FIFO trace (one parent fully, then the other) versus a
// perfectly interleaved one.
func TestComputeFairnessSeparatesFIFOFromInterleaved(t *testing.T) {
	t.Parallel()

	const children = 200
	cfg := Config{Mix: WorkloadFairness, Parents: 2, Children: children}

	// FIFO: parent 0's whole batch, then parent 1's.
	fifo := make([]int, 0, 2*children)
	for range children {
		fifo = append(fifo, 0)
	}
	for range children {
		fifo = append(fifo, 1)
	}
	f := computeFairness(cfg, fifo)
	assert.Equal(t, []int64{children, children}, f.ClaimsPerParent, "every child runs once under either strategy")
	assert.Equal(t, children, f.MaxConsecutiveSameParent, "FIFO runs one parent's whole batch before the other")
	assert.Zero(t, f.MinActiveWindowShare, "the waiting parent gets no share of the early windows")

	// Interleaved: one claim from each parent, in turn.
	inter := make([]int, 0, 2*children)
	for range children {
		inter = append(inter, 0, 1)
	}
	b := computeFairness(cfg, inter)
	assert.Equal(t, []int64{children, children}, b.ClaimsPerParent, "totals are unchanged — the difference is order")
	assert.Equal(t, 1, b.MaxConsecutiveSameParent, "a perfect interleave never runs the same parent twice in a row")
	assert.InDelta(t, 0.5, b.MinActiveWindowShare, 0.01, "each parent holds ~half of every window")
}

// TestComputeFairnessIgnoresDrainedParents guards the non-starvation number
// against a false positive: once a parent has no work left, the windows after it
// finishes must not count it as starved, or every fair run would look unfair in
// its tail.
func TestComputeFairnessIgnoresDrainedParents(t *testing.T) {
	t.Parallel()

	cfg := Config{Mix: WorkloadFairness, Parents: 2, Children: 100}
	// Parent 0 and 1 interleave for the first 100 claims; then parent 0 is done and
	// parent 1 finishes alone. The tail is not starvation — parent 0 has nothing left.
	order := make([]int, 0, 300)
	for range 100 {
		order = append(order, 0, 1)
	}
	for range 100 {
		order = append(order, 1)
	}
	f := computeFairness(cfg, order)
	assert.Positive(t, f.MinActiveWindowShare,
		"the tail where only parent 1 has work is not starvation of parent 0")
}

// TestFairnessConfigValidation covers the mix's own rules: it defaults Parents and
// Children, rejects a single parent, and rejects a fairness strategy on any other
// mix.
func TestFairnessConfigValidation(t *testing.T) {
	t.Parallel()

	got, err := Config{DSN: testDSN, Jobs: 1, Mix: WorkloadFairness}.validate()
	assert.NoError(t, err)
	assert.Equal(t, defaultFairnessParents, got.Parents, "Parents defaults to two")
	assert.Equal(t, defaultFairnessChildren, got.Children, "Children defaults to a deep batch")

	_, err = Config{DSN: testDSN, Jobs: 1, Mix: WorkloadFairness, Parents: 1}.validate()
	assert.Error(t, err, "one parent cannot starve itself")

	_, err = Config{DSN: testDSN, Jobs: 1, Mix: WorkloadDrainOnly, Fairness: FairnessRoundRobinParent}.validate()
	assert.Error(t, err, "a fairness strategy is meaningless without the fairness mix")

	_, err = Config{DSN: testDSN, Jobs: 1, Mix: WorkloadFairness, Fairness: "nonsense"}.validate()
	assert.Error(t, err, "an unknown strategy is rejected")
}
