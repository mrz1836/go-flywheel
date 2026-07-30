package flywheel

import (
	"context"
	"time"
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
