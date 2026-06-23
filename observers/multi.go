package observers

import (
	"context"

	flywheel "github.com/mrz1836/go-flywheel"
)

// Multi is a flywheel.Observer that fans every event out to several observers in
// turn. It is how a Node runs more than one observer at once — typically a
// SlogObserver beside a MetricsObserver — behind the single RunnerConfig.Observer
// slot.
//
// Fan-out is synchronous and ordered: each event is delivered to every child in
// the order they were given before the call returns, preserving the Observer
// contract that the dispatch path must not be left with work outstanding. A child
// that blocks therefore blocks its siblings, so each must honor the
// non-blocking contract — the same as any single Observer.
type Multi struct {
	observers []flywheel.Observer
}

// NewMulti returns a Multi that fans out to the given observers, in order. With
// no observers it is a valid no-op.
func NewMulti(obs ...flywheel.Observer) *Multi {
	return &Multi{observers: obs}
}

// Compile-time proof Multi satisfies flywheel.Observer.
var _ flywheel.Observer = (*Multi)(nil)

// OnClaim delivers the claim event to every child in order.
func (m *Multi) OnClaim(ctx context.Context, ev flywheel.ClaimEvent) {
	for _, o := range m.observers {
		o.OnClaim(ctx, ev)
	}
}

// OnStart delivers the start event to every child in order.
func (m *Multi) OnStart(ctx context.Context, ev flywheel.JobEvent) {
	for _, o := range m.observers {
		o.OnStart(ctx, ev)
	}
}

// OnFinish delivers the finish event to every child in order.
func (m *Multi) OnFinish(ctx context.Context, ev flywheel.FinishEvent) {
	for _, o := range m.observers {
		o.OnFinish(ctx, ev)
	}
}

// OnRetry delivers the retry event to every child in order.
func (m *Multi) OnRetry(ctx context.Context, ev flywheel.RetryEvent) {
	for _, o := range m.observers {
		o.OnRetry(ctx, ev)
	}
}
