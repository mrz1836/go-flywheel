package core

import (
	"context"
	"sync"
	"time"

	"github.com/mrz1836/go-foundation/models"
)

// Limiter admits work before it is claimed. It is consulted by the Runner on
// every poll, before Dequeue, so a job that cannot run yet is never claimed,
// never leased, and never writes an audit row — as distinct from claiming the
// job and having its worker snooze, which spends a claim cycle and a run record
// per deferral.
//
// A resource is an arbitrary string naming what is being protected: a downstream
// host, an API tenant, an external account. It is chosen by the host, not derived
// from the runtime's routing labels, because the thing that needs protecting is a
// dependency, not a pool. A Runner is therefore the unit of resource scoping — a
// host protecting several downstreams runs one Runner per downstream, each with
// its own queue and its own resource, rather than one Runner sorting jobs by
// destination after claiming them.
//
// Implementations must be safe for concurrent use and must not block
// indefinitely; a Runner calls Acquire on its poll loop.
type Limiter interface {
	// Acquire requests permission to claim up to n jobs against resource. It
	// returns how many are granted (0 to n) and, when the grant is less than n,
	// how long to wait before asking again.
	//
	// A zero grant with a zero RetryAfter means "no capacity and no estimate"; the
	// Runner falls back to its poll interval.
	//
	// On error the grant is empty: an implementation never returns a partial grant
	// alongside a non-nil error, so a caller that treats an error as "nothing was
	// acquired" never leaks a permit. When the error is nil the returned N is the
	// number of permits the caller now holds and must release.
	Acquire(ctx context.Context, resource string, n int) (Grant, error)

	// Release returns capacity held by a Grant. It is called once per acquired
	// permit when the attempt finishes, by any path — and, before that, once for
	// every permit a claim did not use. Releasing an already-released or expired
	// Grant is a no-op, so the Runner leans on idempotence rather than a per-job
	// guard.
	//
	// It is deliberately errorless: it runs on the finalize path, where a failure
	// must not propagate into a job's outcome. An implementation logs its own
	// failures, and any capacity a failed Release stranded is reclaimed by the
	// implementation's own TTL.
	Release(ctx context.Context, g Grant)
}

// Grant is capacity a Limiter admitted. A Release names the same Grant (or a
// partial copy carrying the same Resource and Token with a smaller N) to return
// what Acquire handed out.
type Grant struct {
	// Resource and N are what was granted.
	Resource string
	N        int
	// RetryAfter is how long to wait before asking for the ungranted remainder.
	// Zero means the Limiter has no estimate.
	RetryAfter time.Duration
	// Token identifies this grant to Release. Implementations that hold no
	// per-grant state leave it empty.
	Token string
	// ExpiresAt, when non-zero, is when the grant's concurrency capacity is
	// reclaimed automatically even if Release is never called — the crashed-holder
	// case. A holder still running at expiry does not lose its lease; it loses its
	// reservation, and the Limiter may over-admit until it releases.
	ExpiresAt time.Time
}

// TokenBucketConfig configures a NewTokenBucket.
type TokenBucketConfig struct {
	// Rate and Burst define refill: Rate tokens per Interval, with Burst as the
	// bucket's capacity. A zero Rate disables rate limiting; a positive Rate
	// requires a positive Interval, and Burst defaults to Rate when left zero.
	Rate     int
	Interval time.Duration
	Burst    int
	// MaxConcurrent, when > 0, additionally caps simultaneous holders. It is
	// returned by Release, so it bounds in-flight work rather than arrival rate. A
	// zero MaxConcurrent disables the concurrency cap, and grants carry no token.
	MaxConcurrent int
}

// TokenBucket is an in-process Limiter enforcing a rate and, optionally, a
// concurrency ceiling per resource. It is correct for a single process: a
// deployment running N processes against one downstream gets N times the
// configured budget and should use a shared Limiter (NewDBLimiter) instead.
//
// It holds one lazily-refilled bucket per resource under a single mutex and runs
// no background goroutine, so a TokenBucket tracking a thousand idle resources
// costs a map entry each and nothing else. It carries no TTL: a permit is held
// only in memory, so a process death drops every reservation with the map, and
// the Runner's release-on-every-path defer is what returns a permit in a live
// process.
//
// The per-resource bucket map is intentionally unbounded, unlike the evict-on-cap
// metrics recorder: a bucket cell holds live budget — the rate reservoir plus the
// held/live concurrency permits — so evicting one would either over-admit (drop a
// cell that still holds permits) or starve (discard a partly-drained reservoir).
// The design mandates one resource per Runner, which keeps the resource set small
// and fixed; a host that needs a bounded or shared gate across many resources uses
// NewDBLimiter instead.
type TokenBucket struct {
	rate          int
	interval      time.Duration
	burst         int
	maxConcurrent int

	mu      sync.Mutex
	buckets map[string]*bucketState
}

// bucketState is one resource's live capacity. tokens is the rate reservoir;
// held and live track the concurrency permits — live maps a grant's token to the
// count it still holds, which is what makes Release idempotent and partial.
type bucketState struct {
	tokens float64
	last   time.Time
	held   int
	live   map[string]int
}

// NewTokenBucket returns a TokenBucket for cfg. It panics on a configuration that
// could never admit work — a negative field, or a positive Rate with a
// non-positive Interval — because those are wiring bugs a host fixes in source,
// caught loudly at construction rather than as a silent zero budget at runtime.
func NewTokenBucket(cfg TokenBucketConfig) *TokenBucket {
	switch {
	case cfg.Rate < 0:
		panic("flywheel: NewTokenBucket: Rate must not be negative")
	case cfg.Burst < 0:
		panic("flywheel: NewTokenBucket: Burst must not be negative")
	case cfg.MaxConcurrent < 0:
		panic("flywheel: NewTokenBucket: MaxConcurrent must not be negative")
	case cfg.Interval < 0:
		panic("flywheel: NewTokenBucket: Interval must not be negative")
	case cfg.Rate > 0 && cfg.Interval <= 0:
		panic("flywheel: NewTokenBucket: a positive Rate requires a positive Interval")
	}
	burst := cfg.Burst
	if cfg.Rate > 0 && burst <= 0 {
		burst = cfg.Rate
	}
	return &TokenBucket{
		rate:          cfg.Rate,
		interval:      cfg.Interval,
		burst:         burst,
		maxConcurrent: cfg.MaxConcurrent,
		buckets:       map[string]*bucketState{},
	}
}

// Acquire grants up to n permits against resource, bounded by the refilled rate
// reservoir and the concurrency ceiling. A resource's bucket starts full, so the
// first Acquire admits an initial burst up to Burst. The clock is read from ctx
// (models.ClockFrom), the runtime-wide time seam, so a test drives refill
// deterministically by injecting a clock.
func (b *TokenBucket) Acquire(ctx context.Context, resource string, n int) (Grant, error) {
	if n <= 0 {
		return Grant{Resource: resource}, nil
	}
	now := models.ClockFrom(ctx).Now(ctx)

	b.mu.Lock()
	defer b.mu.Unlock()

	s := b.buckets[resource]
	if s == nil {
		s = &bucketState{tokens: float64(b.burst), last: now, live: map[string]int{}}
		b.buckets[resource] = s
	}
	b.refill(s, now)

	granted := n
	if b.rate > 0 && granted > int(s.tokens) {
		granted = int(s.tokens)
	}
	if b.maxConcurrent > 0 {
		if room := b.maxConcurrent - s.held; granted > room {
			granted = room
		}
	}
	if granted <= 0 {
		// A rate-bound denial hints the exact wait to the next token; a
		// concurrency-bound one (tokens available, holders full) returns zero and the
		// Runner falls back to its poll interval, because a holder freeing a permit is
		// not something the bucket can time.
		return Grant{Resource: resource, RetryAfter: b.retryAfter(s.tokens)}, nil
	}

	if b.rate > 0 {
		s.tokens -= float64(granted)
	}
	g := Grant{Resource: resource, N: granted}
	if b.maxConcurrent > 0 {
		token := models.NewID()
		s.held += granted
		s.live[token] = granted
		g.Token = token
	}
	if granted < n {
		g.RetryAfter = b.retryAfter(s.tokens)
	}
	return g, nil
}

// Release returns a grant's concurrency permits. It is a no-op for a grant that
// carries no token (rate-only, or no concurrency cap), and it clamps to what the
// token still holds, so releasing an already-released or over-large grant cannot
// drive held negative. Rate tokens are never returned: they meter arrival, and a
// dispatched job is an arrival that happened.
func (b *TokenBucket) Release(_ context.Context, g Grant) {
	if g.N <= 0 || g.Token == "" {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()

	s := b.buckets[g.Resource]
	if s == nil {
		return
	}
	held := s.live[g.Token]
	if held <= 0 {
		return
	}
	rel := min(g.N, held)
	s.held -= rel
	s.live[g.Token] -= rel
	if s.live[g.Token] <= 0 {
		delete(s.live, g.Token)
	}
}

// refill adds the tokens accrued since the last Acquire, capped at Burst. It runs
// under b.mu and advances the bucket's clock only when time has moved, so a fixed
// clock (a test that means to freeze refill) adds nothing.
func (b *TokenBucket) refill(s *bucketState, now time.Time) {
	if b.rate <= 0 {
		return
	}
	elapsed := now.Sub(s.last)
	if elapsed <= 0 {
		return
	}
	s.last = now
	added := float64(b.rate) * float64(elapsed) / float64(b.interval)
	s.tokens = min(float64(b.burst), s.tokens+added)
}

// retryAfter is the exact wait until at least one more rate token is available,
// given the current token count. It returns zero when rate limiting is disabled
// or a whole token is already available (a concurrency-bound denial), leaving the
// Runner to fall back to its poll interval.
func (b *TokenBucket) retryAfter(tokens float64) time.Duration {
	if b.rate <= 0 {
		return 0
	}
	deficit := 1 - tokens
	if deficit <= 0 {
		return 0
	}
	return time.Duration(deficit * float64(b.interval) / float64(b.rate))
}

// ensure TokenBucket satisfies Limiter.
var _ Limiter = (*TokenBucket)(nil)
