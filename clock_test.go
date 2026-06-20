package flywheel

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestClockFrom_DefaultsToRealClock(t *testing.T) {
	t.Parallel()
	before := time.Now()
	got := ClockFrom(context.Background()).Now(context.Background())
	after := time.Now()

	// A clock-less context yields the real clock, so Now must fall within the
	// wall-clock window bracketing the call.
	assert.False(t, got.Before(before), "real clock must not predate the call")
	assert.False(t, got.After(after), "real clock must not postdate the call")
}

func TestClockFrom_ReturnsAttachedClock(t *testing.T) {
	t.Parallel()
	anchor := time.Date(2026, time.June, 19, 12, 0, 0, 0, time.UTC)
	ctx := WithClock(context.Background(), NewFixedClock(anchor))

	got := ClockFrom(ctx).Now(ctx)
	assert.Equal(t, anchor, got, "ClockFrom must return the clock attached via WithClock")
}

func TestClockFrom_IgnoresNilClock(t *testing.T) {
	t.Parallel()
	// A nil Clock stored under the key must fall through to the real clock,
	// never panic.
	ctx := context.WithValue(context.Background(), clockKey{}, Clock(nil))

	assert.NotPanics(t, func() {
		_ = ClockFrom(ctx).Now(ctx)
	})
}

func TestNewFixedClock_IsConstant(t *testing.T) {
	t.Parallel()
	anchor := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
	clk := NewFixedClock(anchor)

	assert.Equal(t, anchor, clk.Now(context.Background()))
	assert.Equal(t, anchor, clk.Now(context.Background()), "FixedClock must return the same instant on every call")
}
