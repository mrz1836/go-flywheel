package flywheel

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/mrz1836/go-foundation/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// --- limiter test doubles ----------------------------------------------------

// countingLimiter is a programmable Limiter. next decides each grant; the tallies
// let a test assert that the in-flight permit count returns to zero and that every
// acquired permit is released — the accounting the runner's gate depends on.
type countingLimiter struct {
	mu       sync.Mutex
	next     func(resource string, n int) (Grant, error)
	acquires int
	granted  int
	released int
}

// Acquire records the call, runs next, and tallies the permits it handed out. It
// honors the interface contract that an error yields a zero grant, so a test
// limiter can never leak a permit through the error path.
func (l *countingLimiter) Acquire(_ context.Context, resource string, n int) (Grant, error) {
	l.mu.Lock()
	l.acquires++
	next := l.next
	l.mu.Unlock()

	g, err := next(resource, n)
	if err != nil {
		return Grant{}, err
	}
	l.mu.Lock()
	l.granted += g.N
	l.mu.Unlock()
	return g, nil
}

// Release tallies the permits handed back.
func (l *countingLimiter) Release(_ context.Context, g Grant) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.released += g.N
}

// inFlight is granted minus released — the permits currently held.
func (l *countingLimiter) inFlight() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.granted - l.released
}

func (l *countingLimiter) acquireCount() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.acquires
}

func (l *countingLimiter) grantedCount() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.granted
}

// manualClock is a models.Clock a test advances by hand, so a token bucket's
// refill and RetryAfter can be asserted exactly rather than raced against the
// wall clock.
type manualClock struct {
	mu  sync.Mutex
	now time.Time
}

func newManualClock(anchor time.Time) *manualClock { return &manualClock{now: anchor} }

func (c *manualClock) Now(context.Context) time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *manualClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// grantAll grants everything asked, minting a token per grant.
func grantAll() func(string, int) (Grant, error) {
	return func(resource string, n int) (Grant, error) {
		return Grant{Resource: resource, N: n, Token: models.NewID()}, nil
	}
}

// grantUpTo grants min(n, cap) each call, so a runner claims at most cap at a time.
func grantUpTo(capacity int) func(string, int) (Grant, error) {
	return func(resource string, n int) (Grant, error) {
		return Grant{Resource: resource, N: min(n, capacity), Token: models.NewID()}, nil
	}
}

// denyAll grants nothing, hinting retryAfter so the runner parks rather than
// busy-polls.
func denyAll(retryAfter time.Duration) func(string, int) (Grant, error) {
	return func(resource string, _ int) (Grant, error) {
		return Grant{Resource: resource, RetryAfter: retryAfter}, nil
	}
}

// failWith always errors, returning a zero grant.
func failWith(err error) func(string, int) (Grant, error) {
	return func(string, int) (Grant, error) { return Grant{}, err }
}

// permitDriver serves one fixed batch, then reports empty. Its Finalize outcome
// and stub error are configurable, so a test can drive every dispatch exit path —
// a stub failure, an unknown kind, a normal finish, a superseded finish — through
// the runner and assert the limiter permit was returned on each.
type permitDriver struct {
	mu        sync.Mutex
	batch     []RawJob
	served    bool
	stubErr   error
	outcome   FinalizeOutcome
	finalized int
}

func (d *permitDriver) Dequeue(
	_ context.Context, _ []string, _ ExecutorClass, _ bool, limit int, _ time.Duration,
) ([]RawJob, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.served || limit <= 0 {
		return nil, nil
	}
	d.served = true
	return d.batch[:min(limit, len(d.batch))], nil
}

func (d *permitDriver) InsertRunStub(
	context.Context, string, RawJob, time.Time, ExecutorClass, string,
) error {
	return d.stubErr
}

func (d *permitDriver) Finalize(
	context.Context, RawJob, string, Result, error, time.Time,
) (FinalizeOutcome, error) {
	d.mu.Lock()
	d.finalized++
	d.mu.Unlock()
	out := d.outcome
	if out.State == "" && !out.Superseded {
		out = FinalizeOutcome{State: StateSucceeded, RunOutcome: OutcomeSuccess}
	}
	return out, nil
}

func (*permitDriver) RenewLease(context.Context, string, string, time.Time) (bool, error) {
	return true, nil
}
func (*permitDriver) InsertChild(context.Context, *gorm.DB, FollowUp, string) error { return nil }
func (*permitDriver) Sweep(context.Context, time.Time) (int, error)                 { return 0, nil }

// newGatedRunner builds a Runner over a fake Driver with a Limiter, defaulting the
// fields a limiter test does not care about. It is the shared constructor for the
// runner-integration limiter tests.
func newGatedRunner(t testing.TB, cfg RunnerConfig) *Runner {
	t.Helper()
	if cfg.DB == nil {
		cfg.DB = newDB(t)
	}
	if len(cfg.Queues) == 0 {
		cfg.Queues = []string{"default"}
	}
	if cfg.ExecutorClass == "" {
		cfg.ExecutorClass = "local"
	}
	if cfg.Concurrency == 0 {
		cfg.Concurrency = 1
	}
	if cfg.PollInterval == 0 {
		cfg.PollInterval = time.Millisecond
	}
	if cfg.Resource == "" {
		cfg.Resource = "provider:test"
	}
	r, err := NewRunner(cfg)
	require.NoError(t, err)
	return r
}

// --- construction ------------------------------------------------------------

// TestNewRunnerRequiresResourceWithLimiter pins the one validation the gate adds:
// a Limiter is meaningless without a Resource, because the gate runs before the
// claim and the resource is the only thing it can key on.
func TestNewRunnerRequiresResourceWithLimiter(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	reg := NewRegistry()
	Register(reg, &successWorker{})

	_, err := NewRunner(RunnerConfig{
		DB: db, Driver: NewSQLiteDriver(db), Registry: reg,
		Queues: []string{"default"}, Concurrency: 1,
		Limiter: &countingLimiter{next: grantAll()},
	})
	require.ErrorIs(t, err, errRunnerNeedsResource, "a Limiter without a Resource is rejected")

	_, err = NewRunner(RunnerConfig{
		DB: db, Driver: NewSQLiteDriver(db), Registry: reg,
		Queues: []string{"default"}, Concurrency: 1,
		Limiter: &countingLimiter{next: grantAll()}, Resource: "provider:x",
	})
	require.NoError(t, err, "a Limiter with a Resource constructs")
}

// --- the grant reduces the claim ---------------------------------------------

// TestGatedRunnerClaimsOnlyWhatIsGranted proves the gate reduces a claim below the
// pool size: an eight-slot pool under a grant of two never claims more than two,
// and every permit it took is returned by the end.
func TestGatedRunnerClaimsOnlyWhatIsGranted(t *testing.T) {
	t.Parallel()
	const grant = 2
	lim := &countingLimiter{next: grantUpTo(grant)}
	w := &peakWorker{hold: func(context.Context, int) { time.Sleep(time.Millisecond) }}
	d := newPoolDriver(40)

	reg := NewRegistry()
	Register(reg, w)
	r := newGatedRunner(t, RunnerConfig{Driver: d, Registry: reg, Limiter: lim, Concurrency: 8})

	stop := runInBackground(t, r)
	require.Eventually(t, func() bool { return w.done.Load() == 40 },
		10*time.Second, 5*time.Millisecond, "the backlog drains under the gate")
	stop()

	limits := d.claimLimits()
	require.NotEmpty(t, limits)
	for i, limit := range limits {
		assert.LessOrEqualf(t, limit, grant, "claim %d asked for %d, above the grant", i, limit)
	}
	// The grant caps arrival, not concurrency: a per-poll rate limit lets claimed
	// jobs accumulate into the pool. Bounding simultaneous holders is MaxConcurrent's
	// job, asserted against the real token bucket in the burst/holder test.
	assert.Zero(t, lim.inFlight(), "every permit is released once the backlog drains")
}

// --- a denied runner parks, and never claims ---------------------------------

// TestGatedRunnerDeniedNeverClaims proves a fully-denied limiter produces a
// bounded wait derived from its own RetryAfter and no claim at all — the
// distinction the whole plan exists to make, versus claim-then-snooze.
func TestGatedRunnerDeniedNeverClaims(t *testing.T) {
	t.Parallel()
	lim := &countingLimiter{next: denyAll(25 * time.Millisecond)}
	fd := &fakeDriver{}
	reg := NewRegistry()
	Register(reg, &successWorker{})
	r := newGatedRunner(t, RunnerConfig{Driver: fd, Registry: reg, Limiter: lim})

	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()
	_ = r.Run(ctx)

	assert.Zero(t, fd.dequeueCalls(), "a fully-denied limiter never issues a Dequeue")
	got := lim.acquireCount()
	assert.Greater(t, got, 1, "the gate is consulted repeatedly")
	assert.Less(t, got, 100, "denial parks on RetryAfter rather than busy-polling")
}

// --- a limiter error never stops the runner ----------------------------------

// TestGatedRunnerLimiterErrorDoesNotStopTheRunner covers both failure policies:
// fail-open claims anyway and warns, fail-closed defers and never claims, and
// neither returns from Run.
func TestGatedRunnerLimiterErrorDoesNotStopTheRunner(t *testing.T) {
	t.Parallel()

	t.Run("fail-open claims and warns", func(t *testing.T) {
		t.Parallel()
		lim := &countingLimiter{next: failWith(errors.New("limiter down"))}
		fd := &fakeDriver{batch: []RawJob{{ID: "a", Kind: "test.success", Args: []byte(`{}`)}}}
		rec := &captureHandler{}
		reg := NewRegistry()
		Register(reg, &successWorker{})
		r := newGatedRunner(t, RunnerConfig{
			Driver: fd, Registry: reg, Limiter: lim,
			LimiterFailClosed: false, Logger: slog.New(rec),
		})

		done := make(chan error, 1)
		ctx, cancel := context.WithCancel(context.Background())
		go func() { done <- r.Run(ctx) }()

		require.Eventually(t, func() bool { return fd.dequeueCalls() > 0 },
			2*time.Second, 5*time.Millisecond, "fail-open claims despite the limiter error")
		select {
		case <-done:
			t.Fatal("a limiter error stopped the runner")
		case <-time.After(50 * time.Millisecond):
		}
		cancel()
		<-done
		assert.True(t, rec.has("jobs: limiter failed; admitting anyway"), "fail-open logs a warning")
	})

	t.Run("fail-closed defers and never claims", func(t *testing.T) {
		t.Parallel()
		lim := &countingLimiter{next: failWith(errors.New("limiter down"))}
		fd := &fakeDriver{batch: []RawJob{{ID: "a", Kind: "test.success", Args: []byte(`{}`)}}}
		reg := NewRegistry()
		Register(reg, &successWorker{})
		r := newGatedRunner(t, RunnerConfig{
			Driver: fd, Registry: reg, Limiter: lim, LimiterFailClosed: true,
			Logger: slog.New(&captureHandler{}),
		})

		done := make(chan error, 1)
		ctx, cancel := context.WithCancel(context.Background())
		go func() { done <- r.Run(ctx) }()

		require.Eventually(t, func() bool { return lim.acquireCount() > 2 },
			2*time.Second, 5*time.Millisecond, "the gate keeps consulting the failed limiter")
		select {
		case <-done:
			t.Fatal("a limiter error stopped the runner")
		case <-time.After(50 * time.Millisecond):
		}
		assert.Zero(t, fd.dequeueCalls(), "fail-closed never issues a Dequeue on a limiter error")
		cancel()
		<-done
	})
}

// --- the permit is released on every dispatch exit ---------------------------

// TestGatedRunnerReleasesPermitOnEveryFinalizePath drives each way a dispatch can
// end — a stub failure and an unknown kind (both early returns before the
// heartbeat), a recovered panic, an execution timeout, and a superseded finalize —
// and asserts the limiter's in-flight count returns to zero for all of them. The
// early-return cases are what the permit-release defer being dispatch's first
// statement exists to cover.
func TestGatedRunnerReleasesPermitOnEveryFinalizePath(t *testing.T) {
	t.Parallel()

	job := func(kind string) []RawJob {
		return []RawJob{{ID: "j", Kind: kind, Queue: "default", Args: []byte(`{}`), Attempt: 1, MaxAttempts: 1}}
	}

	for _, tc := range []struct {
		name       string
		driver     *permitDriver
		register   func(*Registry)
		timeout    time.Duration
		expectErr  bool
		finalizeGT int
	}{
		{
			name:     "recovered panic",
			driver:   &permitDriver{batch: job("test.panic")},
			register: func(reg *Registry) { Register(reg, &panicWorker{}) },
		},
		{
			name:     "execution timeout",
			driver:   &permitDriver{batch: job("test.timeout")},
			register: func(reg *Registry) { Register(reg, &timeoutWorker{}) },
			timeout:  40 * time.Millisecond,
		},
		{
			name:     "superseded finalize",
			driver:   &permitDriver{batch: job("test.success"), outcome: FinalizeOutcome{Superseded: true, State: StateAvailable}},
			register: func(reg *Registry) { Register(reg, &successWorker{}) },
		},
		{
			name:      "stub failure before the heartbeat",
			driver:    &permitDriver{batch: job("test.success"), stubErr: errors.New("stub down")},
			register:  func(reg *Registry) { Register(reg, &successWorker{}) },
			expectErr: true,
		},
		{
			name:     "unknown kind fast-finalize",
			driver:   &permitDriver{batch: job("test.nope")},
			register: func(*Registry) {},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			lim := &countingLimiter{next: grantAll()}
			reg := NewRegistry()
			tc.register(reg)
			r := newGatedRunner(t, RunnerConfig{
				Driver: tc.driver, Registry: reg, Limiter: lim, DefaultTimeout: tc.timeout,
				Logger: slog.New(&captureHandler{}),
			})

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			err := r.RunUntilIdle(ctx)
			if tc.expectErr {
				require.Error(t, err, "a stub failure surfaces to the caller")
			}

			assert.Positive(t, lim.grantedCount(), "a permit was taken for the job")
			assert.Zero(t, lim.inFlight(), "the permit is released on this dispatch path")
		})
	}
}

// --- the coordinator-starvation heuristic ------------------------------------

// starvedMsg is the exact warning the starvation heuristic logs.
const starvedMsg = "jobs: gated runner may be starved; the limiter has denied " +
	"every claim while jobs are in flight on the same resource"

// TestGatedRunnerWarnsWhenCoordinatorsStarveChildren covers both halves of the
// coordinator rule: the misconfigured shape — work that holds every permit sharing
// one gated runner with the work it blocks — trips the heuristic, and the
// documented topology — that work ungated — does not, and completes.
func TestGatedRunnerWarnsWhenCoordinatorsStarveChildren(t *testing.T) {
	t.Parallel()

	t.Run("shared gated runner trips the heuristic", func(t *testing.T) {
		t.Parallel()
		release := make(chan struct{})
		var once sync.Once
		closeRelease := func() { once.Do(func() { close(release) }) }
		t.Cleanup(closeRelease)

		// Two permits, and workers that hold theirs: they fill the ceiling and the
		// runner is then denied every further claim while they are in flight.
		w := &peakWorker{hold: func(context.Context, int) { <-release }}
		reg := NewRegistry()
		Register(reg, w)
		rec := &captureHandler{}
		r := newGatedRunner(t, RunnerConfig{
			Driver: newPoolDriver(10), Registry: reg,
			Limiter:     NewTokenBucket(TokenBucketConfig{MaxConcurrent: 2}),
			Concurrency: 4, LimiterStarvationInterval: 30 * time.Millisecond, Logger: slog.New(rec),
		})

		stop := runInBackground(t, r)
		require.Eventually(t, func() bool { return rec.has(starvedMsg) },
			3*time.Second, 10*time.Millisecond, "the starvation warning fires while permits are held")
		closeRelease() // let the blocked jobs finish so the runner can drain
		stop()
	})

	t.Run("ungated runner completes without warning", func(t *testing.T) {
		t.Parallel()
		w := &peakWorker{hold: func(context.Context, int) { time.Sleep(time.Millisecond) }}
		reg := NewRegistry()
		Register(reg, w)
		rec := &captureHandler{}
		r := newGatedRunner(t, RunnerConfig{
			Driver: newPoolDriver(10), Registry: reg, // no Limiter: work that spawns work runs ungated
			Concurrency: 4, LimiterStarvationInterval: 30 * time.Millisecond, Logger: slog.New(rec),
		})

		stop := runInBackground(t, r)
		require.Eventually(t, func() bool { return w.done.Load() == 10 },
			5*time.Second, 5*time.Millisecond, "the batch completes ungated")
		stop()
		assert.False(t, rec.has(starvedMsg), "an ungated runner never warns about starvation")
	})
}

// TestStarvationTrackerThresholdAndReset pins the heuristic's timing precisely
// with an injected clock: it fires only after the denial streak outlasts the
// threshold with work in flight, at most once per threshold, and it resets on a
// grant or when nothing is in flight.
func TestStarvationTrackerThresholdAndReset(t *testing.T) {
	t.Parallel()
	rec := &captureHandler{}
	r := &Runner{
		cfg:  RunnerConfig{Resource: "provider:z", LimiterStarvationInterval: 100 * time.Millisecond, Logger: slog.New(rec)},
		pool: newPool(4),
	}
	setInFlight := func(n int) { r.pool.mu.Lock(); r.pool.running = n; r.pool.mu.Unlock() }
	warnings := func() int { return len(rec.recordsFor(starvedMsg)) }

	clk := newManualClock(time.Unix(1_000_000, 0))
	ctx := models.WithClock(context.Background(), clk)
	var s starvationTracker
	setInFlight(2)

	s.denied(ctx, r) // start the streak
	clk.advance(50 * time.Millisecond)
	s.denied(ctx, r)
	assert.Zero(t, warnings(), "no warning before the threshold")

	clk.advance(60 * time.Millisecond) // streak 110ms > 100ms
	s.denied(ctx, r)
	assert.Equal(t, 1, warnings(), "one warning once the streak outlasts the threshold")

	clk.advance(10 * time.Millisecond)
	s.denied(ctx, r)
	assert.Equal(t, 1, warnings(), "rate-limited to one warning per threshold")

	// A denial with nothing in flight is not starvation: it resets the streak.
	setInFlight(0)
	clk.advance(time.Second)
	s.denied(ctx, r)
	assert.Equal(t, 1, warnings(), "no warning when nothing is in flight")

	// A grant clears the streak; the next spell re-accumulates from scratch.
	setInFlight(2)
	s.granted()
	s.denied(ctx, r)
	clk.advance(50 * time.Millisecond)
	s.denied(ctx, r)
	assert.Equal(t, 1, warnings(), "a fresh streak has not yet reached the threshold")
}
