//go:build loadtest

package loadtest

import (
	"context"
	"errors"
	"testing"
	"time"

	flywheel "github.com/mrz1836/go-flywheel"
)

// TestObserverAccumulatesAttemptTimeForUtilization proves the numerator of
// SlotUtilization comes from what the runtime reported, not from a guess.
//
// Superseded attempts are included on purpose: the work ran and held its slot for
// as long as it took, whatever the runtime then did with the outcome. They are
// still kept out of the drain fraction, which is what schedules every fault, so
// the two counters cannot be collapsed into one.
func TestObserverAccumulatesAttemptTimeForUtilization(t *testing.T) {
	t.Parallel()

	p := &progress{target: 3}
	obs := harnessObserver{prog: p}
	ctx := context.Background()

	obs.OnFinish(ctx, flywheel.FinishEvent{Duration: 30 * time.Millisecond})
	obs.OnFinish(ctx, flywheel.FinishEvent{Duration: 20 * time.Millisecond})
	obs.OnSupersede(ctx, flywheel.SupersedeEvent{Duration: 50 * time.Millisecond})

	if got, want := p.busyNanos.Load(), (100 * time.Millisecond).Nanoseconds(); got != want {
		t.Errorf("busyNanos = %d, want %d", got, want)
	}
	if got := p.terminal(); got != 2 {
		t.Errorf("terminal = %d, want 2: a superseded attempt advanced nothing", got)
	}
	if got := p.superseded.Load(); got != 1 {
		t.Errorf("superseded = %d, want 1", got)
	}

	// The utilization arithmetic the report performs, at 2 runners × 1 worker over
	// 100 ms of wall clock: 100 ms of attempt time against 200 ms of capacity.
	capacity := float64(2) * float64(1) * float64((100 * time.Millisecond).Nanoseconds())
	if util := float64(p.busyNanos.Load()) / capacity; util != 0.5 {
		t.Errorf("slot utilization = %v, want 0.5", util)
	}
}

// TestGateCountsBlockedClaims proves a paused database leaves a number behind.
//
// Nothing else measures it: a gated call records no latency observation by
// design — gating sits outside the timing driver so a fault cannot improve the
// reported percentiles — so without this counter the report of a pause-database
// run has no evidence of how hard the runners hammered the gate, and "the backoff
// engaged" would be a narration rather than a measurement.
func TestGateCountsBlockedClaims(t *testing.T) {
	t.Parallel()

	g := &gate{}
	d := &gateDriver{inner: panicDriver{}, gate: g}
	ctx := context.Background()

	g.shutGate()
	for range 4 {
		if _, err := d.Dequeue(ctx, nil, "", false, 1, time.Second); !errors.Is(err, ErrGated) {
			t.Fatalf("Dequeue error = %v, want ErrGated", err)
		}
	}
	if got := g.blockedClaims.Load(); got != 4 {
		t.Errorf("blockedClaims = %d, want 4", got)
	}

	// A blocked finalize is not a blocked claim: the counter answers "how often did
	// the runner try to poll", and folding other calls in would make it answer
	// nothing in particular.
	if _, err := d.Finalize(ctx, flywheel.RawJob{}, "r", flywheel.Result{}, nil, time.Now()); !errors.Is(err, ErrGated) {
		t.Fatalf("Finalize error = %v, want ErrGated", err)
	}
	if got := g.blockedClaims.Load(); got != 4 {
		t.Errorf("blockedClaims = %d after a blocked finalize, want 4", got)
	}

	// Reopened, the claim reaches the inner driver again and stops counting.
	g.openGate()
	func() {
		defer func() {
			if recover() == nil {
				t.Error("Dequeue did not reach the inner driver after reopen")
			}
		}()
		_, _ = d.Dequeue(ctx, nil, "", false, 1, time.Second)
	}()
	if got := g.blockedClaims.Load(); got != 4 {
		t.Errorf("blockedClaims = %d after reopen, want 4", got)
	}
}

// TestHarnessBlockedClaimsSumsEveryGate proves the report's number covers the
// whole fleet, not runner zero. A fault that stops one runner of four and a
// report that only counted that one would understate the outage by 4×.
func TestHarnessBlockedClaimsSumsEveryGate(t *testing.T) {
	t.Parallel()

	h := &Harness{gates: []*gate{{}, {}, {}}}
	h.gates[0].blockedClaims.Add(2)
	h.gates[2].blockedClaims.Add(5)

	if got := h.blockedClaims(); got != 7 {
		t.Errorf("blockedClaims = %d, want 7", got)
	}
}
