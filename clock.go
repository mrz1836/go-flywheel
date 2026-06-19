package flywheel

import (
	"context"
	"time"
)

// Clock abstracts "now" so callers can be given deterministic time. The runtime
// reads time exclusively through a Clock pulled from the context, which lets
// tests drive the scheduler and lease logic with a fixed clock.
type Clock interface {
	Now(ctx context.Context) time.Time
}

// FixedClock is a Clock that always returns a constant time, regardless of the
// context or the wall clock.
type FixedClock struct {
	anchor time.Time
}

// NewFixedClock returns a FixedClock anchored at t.
func NewFixedClock(t time.Time) FixedClock {
	return FixedClock{anchor: t}
}

// Now returns the anchored time.
func (c FixedClock) Now(context.Context) time.Time {
	return c.anchor
}

// realClock is the default Clock; it returns the wall-clock time. It is
// unexported so callers can only obtain it via ClockFrom on a clock-less
// context.
type realClock struct{}

func (realClock) Now(context.Context) time.Time {
	return time.Now()
}

// clockKey is the unexported context-key type. Keeping it unexported means no
// other package can collide with or overwrite the key.
type clockKey struct{}

// WithClock returns a child context carrying clk.
func WithClock(ctx context.Context, clk Clock) context.Context {
	return context.WithValue(ctx, clockKey{}, clk)
}

// ClockFrom returns the Clock attached to ctx, or a real-time default clock
// when none is attached.
func ClockFrom(ctx context.Context) Clock {
	if clk, ok := ctx.Value(clockKey{}).(Clock); ok && clk != nil {
		return clk
	}

	return realClock{}
}
