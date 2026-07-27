package flywheel

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/mrz1836/go-foundation/ctxutil"
	"github.com/mrz1836/go-foundation/models"
	"gorm.io/gorm"
)

// Runner defaults applied when a RunnerConfig field is left zero.
const (
	defaultLeaseDuration    = 30 * time.Second
	defaultPollInterval     = 100 * time.Millisecond
	defaultRetryBackoffBase = time.Second
	maxRetryBackoff         = time.Minute
	backoffJitterSpread     = 0.5 // ±25% — the jitter multiplier spans [0.75, 1.25).
	// defaultMaxPollBackoff caps the delay between polls during a sustained
	// failure. A failing database must not be polled at the empty-queue rate:
	// every runner in a fleet hitting it ten times a second is a retry storm
	// aimed at a recovering database, plus an unbounded volume of error logs.
	defaultMaxPollBackoff = 30 * time.Second
	// heartbeatDivisor derives the renewal interval from the lease: renewing at
	// one third means two consecutive renewals may fail before the lease can
	// expire.
	heartbeatDivisor = 3
	// minHeartbeatInterval floors the derived interval. A lease short enough to
	// want sub-second renewal is a lease being used as a timeout, and renewing
	// faster than this would cost more write amplification than the lease is
	// worth.
	minHeartbeatInterval = time.Second
)

// nonTerminalStates are the job states that keep RunUntilIdle polling.
var nonTerminalStates = []string{ //nolint:gochecknoglobals // intentional shared constant slice
	string(StateAvailable), string(StateRunning), string(StateRetryable), string(StateScheduled),
}

// RunnerConfig configures a Runner.
type RunnerConfig struct {
	// DB is the database the Runner reads queue state from (RunUntilIdle).
	DB *gorm.DB
	// Driver claims and finalizes jobs.
	Driver Driver
	// Registry maps job kinds to workers.
	Registry *Registry
	// Queues are the logical queues this Runner claims from.
	Queues []string
	// ExecutorClass is the routing label this Runner serves: it claims jobs whose
	// executor_class equals it (or is the empty wildcard) unless ClaimAnyClass is
	// set, and stamps it on every job_runs row this Runner writes.
	ExecutorClass ExecutorClass
	// ClaimAnyClass, when true, makes this Runner claim jobs of every executor
	// class, not only its own class and the wildcard. A single-node local
	// deployment typically sets it so one Runner drains the whole queue.
	ClaimAnyClass bool
	// LeaseDuration is the visibility timeout on a claimed job. It bounds
	// *dispatch liveness*, not run duration: a running job's lease is renewed on
	// the heartbeat below for as long as its worker is alive, so this is how long
	// a crashed executor's job stays stranded before the sweep reclaims it — not
	// a ceiling on how long a worker may take. Size it to how quickly you want a
	// crash noticed. DefaultTimeout is what bounds a hung run.
	LeaseDuration time.Duration
	// HeartbeatInterval is how often a running job's lease is renewed. Zero (the
	// default) derives it from LeaseDuration, renewing at one third of it, so two
	// renewals may fail before the lease can expire.
	//
	// Set it negative to disable renewal entirely, restoring the fixed-lease
	// behavior in which a job slower than its lease is reclaimed and
	// re-dispatched while it is still running. Disabling it is a choice about a
	// specific workload — one whose jobs are reliably shorter than the lease and
	// whose write budget is tight — never a default.
	HeartbeatInterval time.Duration
	// OnLeaseRenewed, when set, is called after each successful renewal with the
	// job and its new expiry. It is the seam for a host that holds its own
	// time-bounded resource for the duration of an attempt — an external
	// reservation, a distributed lock, an advisory claim — and needs to extend it
	// on the same cadence, and for exactly as long as the job actually runs.
	//
	// It is called from the heartbeat goroutine, not the worker's, and must not
	// block for long. An error is logged and does not stop renewal: the lease was
	// already extended by the time it is called, so refusing to renew afterwards
	// would strand a job whose worker is still running.
	OnLeaseRenewed func(ctx context.Context, renewal LeaseRenewal) error
	// PollInterval is the pause between empty polls.
	PollInterval time.Duration
	// MaxPollBackoff caps the delay between polls after consecutive failures.
	// Zero selects thirty seconds. The delay starts at PollInterval, doubles per
	// consecutive failure with jitter, and resets on the first success.
	//
	// It is floored at twice PollInterval. A ceiling the first rung already
	// reaches is a ladder with nothing to climb, and RunUntilIdle — which gives
	// up once the ladder saturates — would abandon its drain on a single blip.
	MaxPollBackoff time.Duration
	// Concurrency is the maximum number of jobs this Runner runs at once — its
	// pool size. The Runner claims to fill its free slots and dispatches each
	// job independently, so a slow job occupies one slot rather than stalling
	// the others. A SQLite driver requires 1.
	//
	// At 1 the Runner dispatches inline on its own loop goroutine: one job in
	// flight, sequential, no goroutine per job.
	Concurrency int
	// ClaimBatchSize caps how many jobs one Dequeue asks for. Zero (the default)
	// claims exactly the number of free slots, which is right for almost every
	// deployment. Set it below Concurrency to smooth claim bursts across a fleet;
	// it is never raised above the free-slot count, since a claimed job the
	// Runner cannot start is a job leased and not running.
	ClaimBatchSize int
	// RetryBackoffBase is the base delay for the exponential retry backoff.
	// Optional; defaults to one second.
	RetryBackoffBase time.Duration
	// DefaultTimeout, when > 0, is the execution ceiling applied to every attempt
	// that specifies no timeout of its own (per-job InsertOpts.Timeout or per-kind
	// Timeouter). Optional; zero means no default timeout.
	DefaultTimeout time.Duration
	// Observer, when set, receives lifecycle events (claim/start/finish/retry) for
	// metrics or tracing. Optional; a nil Observer installs an internal no-op.
	Observer Observer
	// Logger is the base logger bound onto each Job. Optional.
	Logger *slog.Logger
}

// LeaseRenewal describes one successful lease extension. It is what
// RunnerConfig.OnLeaseRenewed receives.
type LeaseRenewal struct {
	JobID   string
	RunID   string
	Kind    string
	Attempt int
	// LeaseToken is the claim this renewal extended.
	LeaseToken string
	// RenewedAt is when the renewal was applied; ExpiresAt is the lease's new
	// expiry. A host extending its own resource wants ExpiresAt, not an
	// interval — the runtime renews to an absolute time so a stalled heartbeat
	// cannot bank an ever-growing lease.
	RenewedAt time.Time
	ExpiresAt time.Time
}

// Runner claims jobs from a Driver and dispatches them to registered workers.
type Runner struct {
	cfg        RunnerConfig
	executorID string
	// pool bounds how many dispatches run at once. It is built in NewRunner, not
	// in the dispatch loop, so every method that consults it is total: callable
	// before Run, concurrently with it, and after it returns — which is exactly
	// when a host wants InFlight for a drain warning. A pool created inside the
	// loop would leave Drain reporting "drained" in the window before the loop
	// assigned it.
	pool *pool
	// stopCh is closed by Stop, once. The loop checks it before claiming and every
	// wait inside the loop is derived from it.
	stopCh   chan struct{}
	stopOnce sync.Once
}

// NewRunner validates cfg and returns a Runner. It returns ErrSQLiteConcurrency
// when a SQLite driver is wired with Concurrency greater than 1: the SQLite
// claim is a serialized SELECT-then-UPDATE with no SKIP LOCKED, so it is only
// correct with a single claimant.
//
//nolint:gocognit,gocyclo // straight-line config validation and zero-value defaulting
func NewRunner(cfg RunnerConfig) (*Runner, error) {
	if cfg.DB == nil {
		return nil, errRunnerNeedsDB
	}
	if cfg.Driver == nil {
		return nil, errRunnerNeedsDriver
	}
	if cfg.Registry == nil {
		return nil, errRunnerNeedsRegistry
	}
	if len(cfg.Queues) == 0 {
		return nil, errRunnerNeedsQueue
	}
	if _, isSQLite := cfg.Driver.(*sqliteDriver); isSQLite && cfg.Concurrency > 1 {
		return nil, ErrSQLiteConcurrency
	}

	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 1
	}
	if cfg.LeaseDuration <= 0 {
		cfg.LeaseDuration = defaultLeaseDuration
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = defaultPollInterval
	}
	if cfg.MaxPollBackoff <= 0 {
		cfg.MaxPollBackoff = defaultMaxPollBackoff
	}
	// expBackoff does not clamp on the first attempt, so a ceiling at or below
	// PollInterval would saturate the ladder on the very first failure — and
	// RunUntilIdle, whose give-up rule is saturation, would abandon a drain that
	// had budget left. Two intervals is the smallest ceiling with a rung to climb.
	if floor := 2 * cfg.PollInterval; cfg.MaxPollBackoff < floor {
		cfg.MaxPollBackoff = floor
	}
	if cfg.RetryBackoffBase <= 0 {
		cfg.RetryBackoffBase = defaultRetryBackoffBase
	}
	if cfg.Observer == nil {
		cfg.Observer = noopObserver{}
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}

	return &Runner{
		cfg:        cfg,
		executorID: executorIdentity(),
		pool:       newPool(cfg.Concurrency),
		stopCh:     make(chan struct{}),
	}, nil
}

// --- stop and drain ----------------------------------------------------------

// Stop signals the dispatch loop to claim nothing further. It does not block, it
// is idempotent, and it is final — there is no restart.
//
// It bounds *when the next claim is issued*, not what happens to a claim already
// in flight. A batch that came back from Dequeue after Stop landed is already
// leased, so the Runner dispatches it rather than stranding it until the lease
// sweep reclaims it; Drain waits for that batch too.
//
// Run returns nil once the loop notices, because a requested stop is how Run is
// meant to end. RunUntilIdle returns ErrRunnerStopped, because it promised a
// drained queue and did not deliver one.
//
// Stop is safe before Run, concurrently with it, and after it returns.
func (r *Runner) Stop() {
	r.stopOnce.Do(func() { close(r.stopCh) })
}

// Drain stops claiming and waits for in-flight jobs to finish, bounded by ctx.
//
// It returns nil when the pool is empty, or a *DrainTimeoutError naming the
// still-running count when ctx ends first — in which case those jobs keep their
// leases and are recovered by the lease sweep, exactly as they would be after a
// process kill.
//
// Drain does not cancel in-flight work. A worker that must be interrupted should
// respect the context it was given, and per-job timeouts already exist; Drain's
// contract is "no new claims, then wait", which is what makes a rolling deploy
// lose no work.
//
// Like Stop, it is safe before Run, concurrently with it, and after it returns.
func (r *Runner) Drain(ctx context.Context) error {
	r.Stop()
	if err := r.pool.waitIdle(ctx); err != nil {
		return &DrainTimeoutError{InFlight: r.pool.inFlight(), Err: err}
	}
	return nil
}

// InFlight reports how many jobs this Runner is executing right now.
func (r *Runner) InFlight() int { return r.pool.inFlight() }

// stopping reports whether Stop has been called. A nil stopCh — a Runner built as
// a bare struct literal rather than through NewRunner, which some unit tests do
// for the pure helpers — reads as "not stopping" rather than blocking, because a
// receive on a nil channel falls through to the default arm.
func (r *Runner) stopping() bool {
	select {
	case <-r.stopCh:
		return true
	default:
		return false
	}
}

// loopContext derives the context backing every *wait* inside the dispatch loop:
// the slot reservation, the poll sleep, the backoff sleep, the drain wait. It
// ends when ctx does or when Stop is called.
//
// Dequeue, dispatch, and pendingCount deliberately take the caller's ctx instead.
// Stop must not cancel work already running — that is Drain's whole contract —
// and it must not cancel a Dequeue mid-transaction: on a one-connection SQLite
// pool that makes database/sql discard the connection, which for a :memory:
// database destroys the database.
func (r *Runner) loopContext(ctx context.Context) (context.Context, context.CancelFunc) {
	loopCtx, cancel := context.WithCancel(ctx)
	watching := make(chan struct{})
	go func() {
		defer close(watching)
		select {
		case <-r.stopCh:
			cancel()
		case <-loopCtx.Done():
		}
	}()
	return loopCtx, func() {
		cancel()
		<-watching
	}
}

// stopResult is what the loop returns when Stop ended it.
func (r *Runner) stopResult(untilIdle bool) error {
	if untilIdle {
		return ErrRunnerStopped
	}
	return nil
}

// ended names why a wait inside the loop was cut short — the caller's context, or
// Stop — and returns what the loop should report for it.
func (r *Runner) ended(ctx context.Context, untilIdle bool) error {
	if err := ctx.Err(); err != nil {
		return r.stopped(untilIdle, err)
	}
	return r.stopResult(untilIdle)
}

// --- worker pool -------------------------------------------------------------

// pool bounds how many dispatches run at once and broadcasts when it is empty.
//
// It is a counting semaphore, not a goroutine pool: each dispatch runs on its
// own goroutine and releases its slot on completion, so a slot's lifetime is
// exactly one job's lifetime and a slow job holds one slot rather than the loop.
//
// # Reserve before claiming
//
// The load-bearing choice is that the loop takes its slots *before* issuing
// Dequeue, so a claimed job always has somewhere to run. A claimed job is
// leased, and a leased job the runner has not started is a lease burning in a
// queue. held therefore counts the claim window as well as the running jobs.
//
// # Why there is no sync.WaitGroup
//
// Drain calls the wait from a different goroutine than the loop that starts
// jobs, so a wg.Add with the counter at zero can race a wg.Wait — documented
// misuse that lets Wait return early, which for Drain means reporting a clean
// drain while a job is still running. The close-and-replace idle channel
// releases any number of concurrent waiters from one close, under the same
// mutex that decrements the counter.
type pool struct {
	// limit is the pool size. inline is set when limit is 1, which makes start
	// run its function on the caller's goroutine.
	limit  int
	inline bool

	mu sync.Mutex
	// held counts outstanding reservations: the claim window plus the running
	// jobs. running counts the dispatches actually executing.
	held    int
	running int
	// idle is closed while held is zero and replaced when held rises from zero.
	// It keys on held rather than running so a waiter cannot slip through the
	// window between Dequeue returning a batch and the first start.
	idle chan struct{}
	// freed is a capacity-1 nudge, sent without blocking whenever held drops, so
	// a waiter in reserve rechecks.
	freed chan struct{}
	errs  errCollector
}

// newPool builds a pool of the given size, initially idle. A size below 1 is
// treated as 1, matching the config's own defaulting.
func newPool(limit int) *pool {
	if limit < 1 {
		limit = 1
	}
	p := &pool{
		limit:  limit,
		inline: limit == 1,
		idle:   make(chan struct{}),
		freed:  make(chan struct{}, 1),
	}
	close(p.idle)
	return p
}

// reserve takes between 1 and want slots, blocking until at least one is free or
// ctx ends, and reports how many it took. The caller owns every slot it returns
// and must pass each one to start or hand it back with release.
func (p *pool) reserve(ctx context.Context, want int) (int, error) {
	if want < 1 {
		want = 1
	}
	for {
		p.mu.Lock()
		if free := p.limit - p.held; free > 0 {
			n := min(want, free)
			p.admitLocked(n)
			p.mu.Unlock()
			return n, nil
		}
		p.mu.Unlock()

		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-p.freed:
		}
	}
}

// admit takes n slots unconditionally, past the limit if need be. It is the
// escape hatch for a Driver that serves more jobs than the limit it was given:
// those jobs are leased and must run, and starting a dispatch the pool has not
// accounted for would let a Drain report empty while it is still executing.
func (p *pool) admit(n int) {
	if n <= 0 {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.admitLocked(n)
}

// admitLocked records n reservations. The caller holds mu.
func (p *pool) admitLocked(n int) {
	if p.held == 0 {
		p.idle = make(chan struct{})
	}
	p.held += n
}

// release hands n unused reservations back and wakes a waiter.
func (p *pool) release(n int) {
	if n <= 0 {
		return
	}
	p.mu.Lock()
	n = min(n, p.held)
	p.held -= n
	if n > 0 && p.held == 0 {
		close(p.idle)
	}
	p.mu.Unlock()

	if n > 0 {
		select {
		case p.freed <- struct{}{}:
		default:
		}
	}
}

// start runs fn against one reservation the caller already holds, collecting its
// error and releasing the slot when it returns.
//
// At limit 1 it runs fn on the calling goroutine: no goroutine, no channel
// handoff, and a dispatch error deposited before the caller reads the collector.
// It still takes the slot and counts the job, because Drain has to be correct at
// the concurrency most deployments actually run.
func (p *pool) start(fn func() error) {
	p.mu.Lock()
	p.running++
	p.mu.Unlock()

	run := func() {
		defer func() {
			p.mu.Lock()
			p.running--
			p.mu.Unlock()
			p.release(1)
		}()
		p.errs.add(fn())
	}

	if p.inline {
		run()
		return
	}
	go run()
}

// waitIdle blocks until the pool holds nothing — no reservation and no running
// job — or ctx ends, in which case it returns ctx's error.
func (p *pool) waitIdle(ctx context.Context) error {
	p.mu.Lock()
	idle := p.idle
	p.mu.Unlock()

	select {
	case <-idle:
		return nil
	default:
	}
	select {
	case <-idle:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// inFlight reports how many dispatches are executing. It reports running rather
// than held so a drain warning names actual jobs, not a claim window.
func (p *pool) inFlight() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.running
}

// takeErr returns the dispatch errors collected since the last call, joined, and
// clears them.
func (p *pool) takeErr() error { return p.errs.take() }

// errCollector accumulates dispatch errors from the pool's goroutines. It
// replaces the errors.Join over one batch: with independent slots there is no
// batch to scope the aggregation to, so errors are collected as they land and
// drained by whoever asks.
type errCollector struct {
	mu   sync.Mutex
	errs []error
}

// add records one non-nil error.
func (c *errCollector) add(err error) {
	if err == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.errs = append(c.errs, err)
}

// take joins and clears what has been collected, returning nil when nothing has.
func (c *errCollector) take() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.errs) == 0 {
		return nil
	}
	joined := errors.Join(c.errs...)
	c.errs = nil
	return joined
}

// Run drives the dispatch loop until ctx is cancelled.
func (r *Runner) Run(ctx context.Context) error { return r.run(ctx, false) }

// RunUntilIdle drives the dispatch loop until every job has reached a terminal
// state, then returns. It is the deterministic test driver.
func (r *Runner) RunUntilIdle(ctx context.Context) error { return r.run(ctx, true) }

// run is the dispatch loop both entry points share. The untilIdle flag selects
// between the two contracts that differ only in how the loop ends and how a poll
// failure is treated:
//
//   - Run polls forever. A poll failure is logged and the loop carries on.
//   - RunUntilIdle stops the moment nothing is claimable and no job is in a
//     non-terminal state. A poll failure is returned to the caller.
//
// Each iteration reserves at most the free-slot count, claims into those
// reservations, and dispatches every claimed job into the pool without waiting
// for it. A slow job therefore holds one slot, not the loop: the remaining slots
// keep claiming.
func (r *Runner) run(ctx context.Context, untilIdle bool) error {
	loopCtx, endLoop := r.loopContext(ctx)
	defer endLoop()

	// Every dispatch this invocation started is finished before it returns, so a
	// Runner reused for a second invocation starts from an empty pool. The wait is
	// unbounded and on a context of its own: Stop and a host cancel both mean
	// "stop claiming", never "abandon a running job".
	defer func() { _ = r.pool.waitIdle(context.WithoutCancel(ctx)) }()

	var backoff pollBackoff
	for {
		if err := ctx.Err(); err != nil {
			return r.stopped(untilIdle, err)
		}
		if r.stopping() {
			return r.stopResult(untilIdle)
		}

		reserved, err := r.pool.reserve(loopCtx, r.claimLimit())
		if err != nil {
			return r.ended(ctx, untilIdle)
		}
		// Stop may have landed while the reservation waited for a slot. Checking
		// again here is what keeps "Stop bounds when the next claim is issued" true
		// for a loop that was parked in a full pool.
		if r.stopping() {
			r.pool.release(reserved)
			return r.stopResult(untilIdle)
		}

		claimed, claimErr := r.claimAndDispatch(ctx, reserved)
		if dispatchErr := r.pool.takeErr(); dispatchErr != nil {
			if untilIdle {
				return dispatchErr
			}
			r.cfg.Logger.ErrorContext(ctx, "jobs: dispatch failed", "error", dispatchErr)
		}
		if claimErr != nil {
			if stop := r.onPollError(ctx, loopCtx, &backoff, untilIdle, claimErr); stop != nil {
				return stop
			}
			continue
		}
		if claimed > 0 {
			backoff.reset()
			continue
		}

		// An empty claim reached the database, but for RunUntilIdle the iteration
		// is not over: the pending count is part of the same poll. Resetting the
		// ladder here instead of after the count would mean a queue that is always
		// empty and a count that always fails never climb a rung — the ladder would
		// reset on every claim and the loop would spin forever.
		if !untilIdle {
			backoff.reset()
		} else {
			// "No job is in a non-terminal state" includes this runner's
			// own in-flight jobs, and with independent slots the claim going empty
			// no longer implies the pool is empty. Waiting for it here makes the
			// guarantee hold by construction rather than by the accident that
			// 'running' happens to be one of nonTerminalStates — and it also pins
			// down the narrower window where a worker body has returned but its
			// Finalize has not committed.
			//
			// It costs nothing in the common case: the pool is already empty.
			if waitErr := r.pool.waitIdle(loopCtx); waitErr != nil {
				return r.ended(ctx, untilIdle)
			}
			if dispatchErr := r.pool.takeErr(); dispatchErr != nil {
				return dispatchErr
			}

			// The count hits the same database the claim does, so a blip on it is as
			// transient as a blip on the claim and takes the same ladder.
			pending, countErr := r.pendingCount(ctx)
			if countErr != nil {
				if stop := r.onPollError(ctx, loopCtx, &backoff, untilIdle, countErr); stop != nil {
					return stop
				}
				continue
			}
			backoff.reset()
			if pending == 0 {
				return nil
			}
			// Jobs remain but none are claimable yet (retry/snooze backoff);
			// wait one interval and poll again.
		}

		if err := r.sleep(loopCtx, r.cfg.PollInterval); err != nil {
			return r.ended(ctx, untilIdle)
		}
	}
}

// sleep waits d out, reporting ctx's error when the wait was cut short.
func (r *Runner) sleep(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// pollBackoff is one dispatch loop's poll-failure ladder.
//
// It is loop-local rather than a Runner field on purpose: two invocations of the
// loop have independent ladders, and a field would be shared state for the drain
// path to reach into for no reason.
type pollBackoff struct{ failures int }

// reset clears the ladder, which a successful poll does immediately.
func (b *pollBackoff) reset() { b.failures = 0 }

// next records one failure and reports how long to wait, plus whether the ladder
// has reached its ceiling.
//
// Saturation is computed from the un-jittered rung, so it is a property of the
// failure count alone: the ladder saturates on failure ceil(log2(maxDelay/base))
// + 1, deterministically. That is what lets RunUntilIdle give up after a fixed
// number of attempts rather than depending on a deadline it may not have.
func (b *pollBackoff) next(base, maxDelay time.Duration) (time.Duration, bool) {
	b.failures++
	rung := expBackoff(base, maxDelay, b.failures)
	return jittered(rung), rung >= maxDelay
}

// onPollError applies the ladder for one failed poll and reports the error the
// loop must stop with, or nil to carry on.
//
// One log line per failed attempt, and attempts are ladder-spaced, so the log
// rate follows the backoff rather than the poll interval — a decaying
// trickle during a sustained outage instead of ten lines a second per runner,
// with no rate limiter to get wrong.
//
// Run never gives up: it backs off forever at the ceiling, because being there
// when the database returns is the whole job. RunUntilIdle gives up on the first
// failure whose ladder rung reaches MaxPollBackoff — bounded by the
// ladder, not by the context, because its callers include harnesses that pass a
// context with no deadline at all, and a context-only bound would hang them.
//
// A wait cut short returns nil, leaving the loop's own top-of-iteration checks to
// own the exit.
func (r *Runner) onPollError(
	ctx, loopCtx context.Context, b *pollBackoff, untilIdle bool, err error,
) error {
	delay, saturated := b.next(r.cfg.PollInterval, r.cfg.MaxPollBackoff)
	r.cfg.Logger.ErrorContext(
		ctx, "jobs: poll failed",
		"error", err,
		"consecutive_failures", b.failures,
		"backoff", delay.String(),
	)
	if untilIdle && saturated {
		return fmt.Errorf("jobs: run-until-idle: poll failed %d consecutive times: %w", b.failures, err)
	}
	_ = r.sleep(loopCtx, delay)
	return nil
}

// stopped wraps the reason the loop ended, naming which entry point ended.
func (r *Runner) stopped(untilIdle bool, err error) error {
	if untilIdle {
		return fmt.Errorf("jobs: run-until-idle stopped: %w", err)
	}
	return fmt.Errorf("jobs: runner stopped: %w", err)
}

// pendingCount reports how many jobs are still in a non-terminal state.
func (r *Runner) pendingCount(ctx context.Context) (int64, error) {
	var count int64
	if err := r.cfg.DB.WithContext(ctx).Model(&jobRow{}).
		Where("state IN ?", nonTerminalStates).Count(&count).Error; err != nil {
		return 0, fmt.Errorf("jobs: count pending: %w", err)
	}
	return count, nil
}

// claimLimit is the largest batch one Dequeue may ask for, before the free-slot
// count reserve applies on top. ClaimBatchSize can only lower it: raising it
// above the pool size would claim a job the Runner has no slot to start, and a
// claimed job the Runner has not started is a lease burning in a queue.
func (r *Runner) claimLimit() int {
	if r.cfg.ClaimBatchSize > 0 && r.cfg.ClaimBatchSize < r.cfg.Concurrency {
		return r.cfg.ClaimBatchSize
	}
	return r.cfg.Concurrency
}

// claimAndDispatch claims into reserved slots and starts every claimed job in
// the pool without waiting for any of it, returning how many were claimed.
//
// Its error is the Dequeue error and nothing else: per-job errors land in the
// pool's collector, where the loop drains them separately. That keeps a poll
// failure — which feeds the backoff ladder and, for RunUntilIdle, the give-up
// rule — distinct from a job that failed to finalize.
//
// Every reservation the claim does not use is released here, so a short claim
// frees its slots immediately rather than at the end of the iteration.
func (r *Runner) claimAndDispatch(ctx context.Context, reserved int) (int, error) {
	batch, err := r.cfg.Driver.Dequeue(
		ctx, r.cfg.Queues, r.cfg.ExecutorClass, r.cfg.ClaimAnyClass, reserved, r.cfg.LeaseDuration,
	)
	if err != nil {
		r.pool.release(reserved)
		return 0, err
	}
	if len(batch) == 0 {
		r.pool.release(reserved)
		return 0, nil
	}

	switch {
	case len(batch) < reserved:
		r.pool.release(reserved - len(batch))
	case len(batch) > reserved:
		// A Driver that served past its limit still handed back leased jobs. They
		// run, and the pool accounts for them, because a dispatch the pool has not
		// counted is one Drain would report as finished.
		r.pool.admit(len(batch) - reserved)
	}

	r.cfg.Observer.OnClaim(ctx, ClaimEvent{
		ExecutorClass: r.cfg.ExecutorClass,
		Queues:        r.cfg.Queues,
		Claimed:       len(batch),
	})
	for i := range batch {
		raw := batch[i]
		r.pool.start(func() error { return r.dispatch(ctx, raw) })
	}
	return len(batch), nil
}

// pollOnce claims one batch and dispatches it *to completion*, returning the
// batch size and the first error the attempt produced.
//
// It is a synchronous seam for tests, and the dispatch loop deliberately does
// not call it: waiting for the whole batch is precisely the barrier the pool
// exists to remove. It survives because half a dozen tests use it as "run one
// attempt to completion" to pin heartbeat and supersede invariants, and
// rewriting those to poll a running loop would trade a sharp assertion for a
// timing-dependent one.
func (r *Runner) pollOnce(ctx context.Context) (int, error) {
	reserved, err := r.pool.reserve(ctx, r.claimLimit())
	if err != nil {
		return 0, err
	}

	claimed, claimErr := r.claimAndDispatch(ctx, reserved)
	if waitErr := r.pool.waitIdle(ctx); waitErr != nil {
		return claimed, waitErr
	}
	if claimErr != nil {
		return claimed, claimErr
	}
	return claimed, r.pool.takeErr()
}

// dispatch runs one claimed job: it pre-allocates the audit stub, runs the
// worker outside any transaction with panic recovery, then finalizes.
func (r *Runner) dispatch(ctx context.Context, raw RawJob) error {
	runID := models.NewID()
	startedAt := models.ClockFrom(ctx).Now(ctx)

	if err := r.cfg.Driver.InsertRunStub(
		ctx, runID, raw, startedAt, r.cfg.ExecutorClass, r.executorID,
	); err != nil {
		return err
	}

	jobEv := JobEvent{JobID: raw.ID, RunID: runID, Kind: raw.Kind, Queue: raw.Queue, Attempt: raw.Attempt}

	entry, known := r.cfg.Registry.lookup(raw.Kind)
	if !known {
		finishedAt := models.ClockFrom(ctx).Now(ctx)
		unknown := &classifiedError{cause: ErrUnknownKind, class: ErrorPermanent}
		out, err := r.cfg.Driver.Finalize(ctx, raw, runID, Result{}, unknown, finishedAt)
		if err != nil {
			return err
		}
		r.observe(ctx, raw, jobEv, out, unknown, startedAt, finishedAt)
		return nil
	}

	logger := r.cfg.Logger.With("job_id", raw.ID, "kind", raw.Kind, "run_id", runID)
	if reqID := ctxutil.RequestIDFromMetadata(raw.Metadata); reqID != "" {
		ctx = ctxutil.WithRequestID(ctx, reqID)
		logger = logger.With("request_id", reqID)
	}

	in := dispatchInput{
		ID:          raw.ID,
		Kind:        raw.Kind,
		Queue:       raw.Queue,
		RawArgs:     raw.Args,
		Attempt:     raw.Attempt,
		MaxAttempts: raw.MaxAttempts,
		ParentJobID: raw.ParentJobID,
		EnqueuedAt:  raw.ScheduledAt,
		Tags:        raw.Tags,
		Logger:      logger,
		RunID:       runID,
	}

	r.cfg.Observer.OnStart(ctx, jobEv)

	// Renewal runs for the whole attempt, finalize included. The deferred stop is
	// what stops renewal on every exit path — normal return, recovered panic,
	// and execution timeout alike.
	defer r.startHeartbeat(ctx, raw, runID)()

	workCtx := ctx
	if d := r.resolveTimeout(entry, raw); d > 0 {
		var cancel context.CancelFunc
		workCtx, cancel = context.WithTimeout(ctx, d)
		defer cancel()
	}

	result, workErr := r.runWork(workCtx, entry, in)
	finishedAt := models.ClockFrom(ctx).Now(ctx)

	var finalErr error
	if workErr != nil {
		finalErr = r.classify(entry, workErr, raw)
	}
	// Finalize first, then report what was actually persisted. Emitting the
	// outcome before the driver has applied it means a superseded attempt is
	// reported as a success it never had.
	//
	// Finalize runs on the parent ctx, not the (possibly expired) workCtx, so a
	// timed-out attempt still records its outcome.
	out, err := r.cfg.Driver.Finalize(ctx, raw, runID, result, finalErr, finishedAt)
	if err != nil {
		// Nothing was persisted, so there is nothing to report. Emitting an event
		// here would describe an outcome the database does not hold.
		return err
	}
	r.observe(ctx, raw, jobEv, out, finalErr, startedAt, finishedAt)
	return nil
}

// observe reports one finalized attempt: OnSupersede when the driver persisted
// nothing, otherwise OnFinish and — when the job will run again — OnRetry.
//
// It is a projection of what the driver persisted and derives nothing of its
// own. It used to call planFinalize a second time, which was a latent bug as
// well as a blind spot: the runner and the driver computed the same state-machine
// decision independently and could diverge, and the observer was told an outcome
// before the driver had agreed to it.
func (r *Runner) observe(
	ctx context.Context, raw RawJob, ev JobEvent, out FinalizeOutcome, finalErr error, startedAt, finishedAt time.Time,
) {
	duration := finishedAt.Sub(startedAt)

	if out.Superseded {
		r.cfg.Observer.OnSupersede(ctx, SupersedeEvent{
			JobEvent:   ev,
			Outcome:    out.RunOutcome,
			State:      out.State,
			Duration:   duration,
			LeaseToken: raw.LeaseToken,
		})
		return
	}

	r.cfg.Observer.OnFinish(ctx, FinishEvent{
		JobEvent:   ev,
		Outcome:    out.RunOutcome,
		ErrorClass: out.ErrorClass,
		Err:        finalErr,
		Duration:   duration,
	})

	if out.State == StateRetryable {
		var delay time.Duration
		if out.ScheduledAt != nil {
			delay = out.ScheduledAt.Sub(finishedAt)
		}
		r.cfg.Observer.OnRetry(ctx, RetryEvent{
			JobEvent:    ev,
			NextAttempt: ev.Attempt + 1,
			Delay:       delay,
			ErrorClass:  out.ErrorClass,
		})
	}
}

// resolveTimeout selects the execution timeout for an attempt, preferring the
// per-job timeout, then the worker's per-kind Timeouter, then the runner's
// DefaultTimeout. A zero result means no timeout is applied.
func (r *Runner) resolveTimeout(entry registryEntry, raw RawJob) time.Duration {
	if raw.TimeoutMs != nil && *raw.TimeoutMs > 0 {
		return time.Duration(*raw.TimeoutMs) * time.Millisecond
	}
	if entry.timeouter != nil {
		if d := entry.timeouter.Timeout(); d > 0 {
			return d
		}
	}
	return r.cfg.DefaultTimeout
}

// runWork invokes the worker, recovering a panic into an error so the executor
// survives it. A panicking worker must cost one attempt, not the whole process:
// the recovered value becomes an ordinary job error that retries under the
// normal backoff, and the other in-flight jobs on this runner are unaffected.
func (r *Runner) runWork(
	ctx context.Context, entry registryEntry, in dispatchInput,
) (result Result, err error) {
	defer func() {
		if rec := recover(); rec != nil {
			result = Result{}
			err = fmt.Errorf("%w: %v", errWorkerPanicked, rec)
		}
	}()
	return entry.dispatch(ctx, in)
}

// classify wraps a worker error with the Runner's verdict — the error class
// (worker Classifier or transient) and the retry delay (worker Retryable or
// the exponential backoff) — for the Driver to apply.
func (r *Runner) classify(entry registryEntry, workErr error, raw RawJob) error {
	class := ErrorTransient
	if entry.classifier != nil {
		if c := entry.classifier.Classify(workErr); c != "" {
			class = c
		}
	}
	// An execution-timeout deadline always classifies as timeout, overriding any
	// worker classifier, so a hung attempt is distinguishable in the audit trail.
	if errors.Is(workErr, context.DeadlineExceeded) {
		class = ErrorTimeout
	}
	var delay time.Duration
	if entry.retryable != nil {
		delay = entry.retryable.NextRetry(workErr, raw.Attempt)
	}
	if delay <= 0 {
		delay = r.backoff(raw.Attempt)
	}
	return &classifiedError{cause: workErr, class: class, retryDelay: delay}
}

// backoff is the exponential retry delay with ±25% jitter.
func (r *Runner) backoff(attempt int) time.Duration {
	return jittered(expBackoff(r.cfg.RetryBackoffBase, maxRetryBackoff, attempt))
}

// jittered spreads d by ±25%. It is the one jitter this package applies — the
// retry ladder and the poll ladder share it, so there is a single spread to
// reason about.
func jittered(d time.Duration) time.Duration {
	spread := (1.0 - backoffJitterSpread/2) + rand.Float64()*backoffJitterSpread //nolint:gosec // jitter, not security
	return time.Duration(float64(d) * spread)
}

// executorIdentity returns this process's executor identity (hostname:pid).
func executorIdentity() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "unknown"
	}
	return host + ":" + strconv.Itoa(os.Getpid())
}
