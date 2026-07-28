//go:build loadtest

package loadtest

import (
	"context"
	"testing"
	"time"

	"gorm.io/gorm"
)

// TestSampleXactAgeObservesALongTransaction proves the instrument works.
//
// This test exists because the instrument's correct reading on a healthy run is
// zero: bounded maintenance batches finish in single-digit milliseconds, far
// below any practical sample interval, and the sampler's guarantee is
// explicitly one-sided about that. An instrument that reads zero because there
// is nothing to see and one that reads zero because it is broken are
// indistinguishable from the report — so the working case has to be
// demonstrated somewhere, and this is that somewhere.
//
// It holds a transaction open on a second connection and asserts the sampler
// sees it, aged at least as long as it has been open.
func TestSampleXactAgeObservesALongTransaction(t *testing.T) {
	t.Parallel()
	dsn := requireDSN(t)
	ctx := context.Background()

	h, err := newHarness(ctx, Config{DSN: dsn, Jobs: 1, Runners: 1, Workers: 1})
	if err != nil {
		t.Fatalf("newHarness: %v", err)
	}
	t.Cleanup(func() { _ = h.Close(ctx) })

	// The baseline is "nothing long-running", not "nothing at all": the probe is
	// scoped to the database, and the harness isolates runs by schema within one
	// database, so a sibling test's short transactions are visible here. That is
	// the same scoping the report's caveat describes.
	if age, ageErr := h.sampleXactAge(ctx); ageErr != nil {
		t.Fatalf("baseline sample: %v", ageErr)
	} else if age > 0.1 {
		t.Fatalf("baseline must carry no long transaction, got %.3fs", age)
	}

	const held = 300 * time.Millisecond
	opened := make(chan struct{})
	release := make(chan struct{})
	done := make(chan struct{})

	go func() {
		defer close(done)
		_ = h.work.Transaction(func(tx *gorm.DB) error {
			// Touch a row so the transaction genuinely starts on the server:
			// xact_start is set by the first statement, not by BEGIN alone.
			var n int64
			_ = tx.Table("jobs").Count(&n).Error
			close(opened)
			<-release
			return nil
		})
	}()

	<-opened
	time.Sleep(held)

	age, err := h.sampleXactAge(ctx)
	close(release)
	<-done

	if err != nil {
		t.Fatalf("sampleXactAge: %v", err)
	}
	// Compare against a fraction of the hold so the assertion is about the
	// instrument rather than about scheduler jitter on a loaded machine.
	if minAge := held.Seconds() * 0.5; age < minAge {
		t.Fatalf("the sampler must observe the open transaction: got %.3fs, want >= %.3fs", age, minAge)
	}
	t.Logf("observed transaction age %.3fs after holding one open for %s", age, held)
}

// TestSampleLockWaitsObservesABlockedBackend proves the other half of the pair.
//
// The bare ungranted count this replaced read zero in every published report,
// and the join has to be shown to produce a non-zero waiter count and a
// non-zero wait duration when a backend is genuinely blocked -- otherwise the
// replacement is a different query with the same silence.
func TestSampleLockWaitsObservesABlockedBackend(t *testing.T) {
	t.Parallel()
	dsn := requireDSN(t)
	ctx := context.Background()

	h, err := newHarness(ctx, Config{DSN: dsn, Jobs: 1, Runners: 1, Workers: 2})
	if err != nil {
		t.Fatalf("newHarness: %v", err)
	}
	t.Cleanup(func() { _ = h.Close(ctx) })

	// One row, locked by one transaction, wanted by another.
	if err := h.work.Exec(`
		INSERT INTO jobs (id, created_at, updated_at, metadata, kind, queue, args, priority,
		                  state, attempt, max_attempts, scheduled_at, executor_class, tags)
		VALUES ('lockrow', now(), now(), '{}'::jsonb, 'k', 'default', '{}'::jsonb, 100,
		        'available', 0, 25, now(), '', '[]'::jsonb)`).Error; err != nil {
		t.Fatalf("seed row: %v", err)
	}

	holding := make(chan struct{})
	release := make(chan struct{})
	holder := make(chan struct{})
	blocked := make(chan struct{})

	go func() {
		defer close(holder)
		_ = h.work.Transaction(func(tx *gorm.DB) error {
			if err := tx.Exec(`SELECT 1 FROM jobs WHERE id = 'lockrow' FOR UPDATE`).Error; err != nil {
				return err
			}
			close(holding)
			<-release
			return nil
		})
	}()
	<-holding

	go func() {
		defer close(blocked)
		// This blocks on the row lock until the holder commits.
		_ = h.work.Exec(`SELECT 1 FROM jobs WHERE id = 'lockrow' FOR UPDATE`).Error
	}()

	// Give the second backend time to reach the lock and register as ungranted.
	var (
		waiters  int64
		longest  float64
		observed bool
	)
	for range 50 {
		time.Sleep(20 * time.Millisecond)
		w, l, sErr := h.sampleLockWaits(ctx)
		if sErr != nil {
			t.Fatalf("sampleLockWaits: %v", sErr)
		}
		if w > 0 {
			waiters, longest, observed = w, l, true
			break
		}
	}

	close(release)
	<-holder
	<-blocked

	if !observed {
		t.Fatal("the sampler must observe a genuinely blocked backend")
	}
	if longest <= 0 {
		t.Fatalf("a blocked backend must report a positive wait, got %.3fs", longest)
	}
	t.Logf("observed %d waiter(s), longest wait %.3fs", waiters, longest)
}

// TestSampleXactAgeIgnoresTheSamplersOwnBackend proves the sampler does not
// measure itself.
//
// It matters because the probe would otherwise always report a non-zero age:
// the sampler's own query is a transaction from pg_stat_activity's point of
// view, and a self-observing instrument would put a floor under every report
// that no workload earned.
func TestSampleXactAgeIgnoresTheSamplersOwnBackend(t *testing.T) {
	t.Parallel()
	dsn := requireDSN(t)
	ctx := context.Background()

	h, err := newHarness(ctx, Config{DSN: dsn, Jobs: 1, Runners: 1, Workers: 1})
	if err != nil {
		t.Fatalf("newHarness: %v", err)
	}
	t.Cleanup(func() { _ = h.Close(ctx) })

	// Repeated immediately: were the sampler counting its own backend, the age
	// would grow with each call rather than staying at the floor.
	for range 5 {
		age, sErr := h.sampleXactAge(ctx)
		if sErr != nil {
			t.Fatalf("sampleXactAge: %v", sErr)
		}
		if age > 0.1 {
			t.Fatalf("an idle harness must report no long transaction, got %.3fs", age)
		}
	}
}
