package observers

import (
	"context"
	"sync"
	"testing"

	flywheel "github.com/mrz1836/go-flywheel"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// orderObs is an Observer that appends "<name>:<event>" to a shared sink, so a
// test can assert both that every child received an event and the order across
// children.
type orderObs struct {
	name string
	mu   *sync.Mutex
	sink *[]string
}

func (o orderObs) record(event string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	*o.sink = append(*o.sink, o.name+":"+event)
}

func (o orderObs) OnClaim(context.Context, flywheel.ClaimEvent)   { o.record("claim") }
func (o orderObs) OnStart(context.Context, flywheel.JobEvent)     { o.record("start") }
func (o orderObs) OnFinish(context.Context, flywheel.FinishEvent) { o.record("finish") }
func (o orderObs) OnRetry(context.Context, flywheel.RetryEvent)   { o.record("retry") }

func TestMultiFansEveryEventToAllInOrder(t *testing.T) {
	t.Parallel()
	var mu sync.Mutex
	var sink []string
	a := orderObs{name: "a", mu: &mu, sink: &sink}
	b := orderObs{name: "b", mu: &mu, sink: &sink}

	m := NewMulti(a, b)
	ctx := context.Background()
	m.OnClaim(ctx, flywheel.ClaimEvent{})
	m.OnStart(ctx, flywheel.JobEvent{})
	m.OnFinish(ctx, flywheel.FinishEvent{})
	m.OnRetry(ctx, flywheel.RetryEvent{})

	assert.Equal(t, []string{
		"a:claim", "b:claim",
		"a:start", "b:start",
		"a:finish", "b:finish",
		"a:retry", "b:retry",
	}, sink, "each event reaches both children, parent order preserved")
}

func TestMultiWithNoObserversIsANoOp(t *testing.T) {
	t.Parallel()
	m := NewMulti()
	assert.NotPanics(t, func() {
		ctx := context.Background()
		m.OnClaim(ctx, flywheel.ClaimEvent{})
		m.OnStart(ctx, flywheel.JobEvent{})
		m.OnFinish(ctx, flywheel.FinishEvent{})
		m.OnRetry(ctx, flywheel.RetryEvent{})
	}, "an empty Multi swallows every event without panicking")
}

func TestMultiDeliversToRealAdapters(t *testing.T) {
	t.Parallel()
	rec := &fakeRecorder{}
	var mu sync.Mutex
	var sink []string
	logged := orderObs{name: "log", mu: &mu, sink: &sink}

	m := NewMulti(logged, NewMetrics(rec))
	m.OnStart(context.Background(), flywheel.JobEvent{Kind: "k", Queue: "q"})

	assert.Equal(t, []string{"log:start"}, sink, "the logging child saw the event")
	require.Len(t, rec.calls, 1, "the metrics child saw the same event")
	assert.Equal(t, MetricJobsStarted, rec.calls[0].name)
}

func TestMultiImplementsObserver(t *testing.T) {
	t.Parallel()
	var obs flywheel.Observer = NewMulti()
	assert.NotNil(t, obs)
}

// countObs is a trivial no-op Observer that counts finishes, so a benchmark can
// measure Multi's fan-out cost without per-child work distorting the result.
type countObs struct{ finishes int }

func (c *countObs) OnClaim(context.Context, flywheel.ClaimEvent)   {}
func (c *countObs) OnStart(context.Context, flywheel.JobEvent)     {}
func (c *countObs) OnFinish(context.Context, flywheel.FinishEvent) { c.finishes++ }
func (c *countObs) OnRetry(context.Context, flywheel.RetryEvent)   {}

// BenchmarkMultiObserverOnFinish measures the per-event fan-out cost of
// dispatching one finish event across N child observers.
func BenchmarkMultiObserverOnFinish(b *testing.B) {
	const children = 8
	obs := make([]flywheel.Observer, children)
	for i := range obs {
		obs[i] = &countObs{}
	}
	m := NewMulti(obs...)
	ctx := context.Background()
	ev := flywheel.FinishEvent{
		JobEvent: flywheel.JobEvent{Kind: "k", Queue: "q"},
		Outcome:  flywheel.OutcomeSuccess,
	}
	for b.Loop() {
		m.OnFinish(ctx, ev)
	}
}
