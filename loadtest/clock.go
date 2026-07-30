//go:build loadtest

package loadtest

import (
	"context"
	"sync/atomic"
	"time"
)

// simClockBase anchors the harness's advanceable clock. It sits a day past the
// bulk seed epoch so every seeded row's scheduled_at is already in the past at
// sim-time zero and therefore immediately claimable: the outage measurement is
// about how the runner retries, not about waiting for the seed to become due.
//
// It is a constant, not time.Now(), for the same reason seedEpoch is — a run
// anchored to the wall clock is not reproducible, and reproducibility is the
// property this whole harness is built to keep.
var simClockBase = seedEpoch.Add(24 * time.Hour) //nolint:gochecknoglobals // fixed reproducibility anchor

// harnessClock is a models.Clock whose "now" moves only when the harness advances
// it. It is the time-compression seam the outage measurement runs on: the runner
// schedules a retry at now+backoff and re-claims it once now reaches that instant,
// so advancing the clock between drain rounds runs a multi-hour backoff ladder in
// seconds of wall time without shortening a single rung.
//
// The wall clock stays the measurement clock — a Report's StartedAt and Duration
// are real time. This clock governs only the runtime's own sense of when a retry
// is due, which is exactly what the configurable cap changes.
type harnessClock struct {
	// offset is nanoseconds past simClockBase. It only ever grows.
	offset atomic.Int64
}

// newHarnessClock returns a clock anchored at simClockBase.
func newHarnessClock() *harnessClock { return &harnessClock{} }

// Now returns the current simulated instant.
func (c *harnessClock) Now(context.Context) time.Time {
	return simClockBase.Add(time.Duration(c.offset.Load()))
}

// advanceTo moves the clock forward to t, never backward. Advancing is the only
// thing that makes a scheduled retry become due, so the caller advances only once
// the current retry generation has quiesced — every job either claimed or waiting
// on a strictly future scheduled_at.
func (c *harnessClock) advanceTo(t time.Time) {
	target := t.Sub(simClockBase).Nanoseconds()
	for {
		cur := c.offset.Load()
		if target <= cur {
			return
		}
		if c.offset.CompareAndSwap(cur, target) {
			return
		}
	}
}

// span reports how far simulated time has advanced from the base — the window of
// outage the retry ladder rode out, in simulated time.
func (c *harnessClock) span() time.Duration {
	return time.Duration(c.offset.Load())
}
