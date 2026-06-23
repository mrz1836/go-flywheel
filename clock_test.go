package flywheel

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestClockFrom(t *testing.T) {
	t.Parallel()
	anchor := time.Date(2026, time.June, 19, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name  string
		ctx   func() context.Context
		check func(t *testing.T, ctx context.Context)
	}{
		{
			name: "defaults to the real clock without a clock in context",
			ctx:  context.Background,
			check: func(t *testing.T, ctx context.Context) {
				t.Helper()
				// A clock-less context yields the real clock, so Now must fall
				// within the wall-clock window bracketing the call.
				before := time.Now()
				got := ClockFrom(ctx).Now(ctx)
				after := time.Now()
				assert.False(t, got.Before(before), "real clock must not predate the call")
				assert.False(t, got.After(after), "real clock must not postdate the call")
			},
		},
		{
			name: "returns the clock attached via WithClock",
			ctx:  func() context.Context { return WithClock(context.Background(), NewFixedClock(anchor)) },
			check: func(t *testing.T, ctx context.Context) {
				t.Helper()
				assert.Equal(t, anchor, ClockFrom(ctx).Now(ctx), "ClockFrom must return the attached clock")
			},
		},
		{
			name: "falls through to the real clock when a nil Clock is stored",
			ctx:  func() context.Context { return context.WithValue(context.Background(), clockKey{}, Clock(nil)) },
			check: func(t *testing.T, ctx context.Context) {
				t.Helper()
				// A nil Clock stored under the key must fall through to the real
				// clock, never panic.
				assert.NotPanics(t, func() { _ = ClockFrom(ctx).Now(ctx) })
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.check(t, tt.ctx())
		})
	}
}

func TestNewFixedClock_IsConstant(t *testing.T) {
	t.Parallel()
	anchor := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
	clk := NewFixedClock(anchor)

	assert.Equal(t, anchor, clk.Now(context.Background()))
	assert.Equal(t, anchor, clk.Now(context.Background()), "FixedClock must return the same instant on every call")
}
