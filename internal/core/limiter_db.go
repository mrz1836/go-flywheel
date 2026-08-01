package core

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/mrz1836/go-foundation/models"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// defaultLimiterSweepBatch bounds how many expired holds one Sweep transaction
// reclaims. Like the retention and lease-sweep bounds it is a ceiling, not a knob
// to switch off: it caps transaction duration and, on PostgreSQL, the delete's
// bind-parameter count.
const defaultLimiterSweepBatch = 1000

// defaultLimiterSweepInterval is the RunSweeper cadence when neither SweepInterval
// nor HoldTTL yields one — a rate-only limiter has no holds to reap, so the loop
// only needs a sane non-zero tick.
const defaultLimiterSweepInterval = time.Minute

// DBLimiterConfig configures a NewDBLimiter.
type DBLimiterConfig struct {
	// Rate and Burst define refill, exactly as for the token bucket: Rate tokens
	// per Interval, capped at Burst (which defaults to Rate). A zero Rate disables
	// rate limiting.
	Rate     int
	Interval time.Duration
	Burst    int
	// MaxConcurrent, when > 0, caps simultaneous holders across every process
	// sharing the database. It requires a positive HoldTTL.
	MaxConcurrent int
	// HoldTTL bounds how long one grant's concurrency reservation survives without a
	// Release — the crashed-holder case. It is required when MaxConcurrent is set,
	// and must exceed the longest expected job: a TTL shorter than the work reclaims
	// capacity from a healthy holder and over-admits. A holder that outlives its TTL
	// keeps running (its lease, not the limiter, governs a running job); it loses
	// only its reservation, and the limiter may over-admit until it releases.
	HoldTTL time.Duration
	// SweepInterval is how often RunSweeper reclaims expired holds. Zero derives it
	// from HoldTTL. The sweeper is an optimization only — Acquire reclaims a
	// resource's expired holds inline before counting, so correctness never depends
	// on it running.
	SweepInterval time.Duration
}

// DBLimiter is a Limiter backed by two tables, correct across every process
// sharing one database. It is the reference implementation of a shared gate, not a
// high-throughput one: every Acquire is a database round trip, so it suits budgets
// in the tens-to-hundreds per second rather than the thousands.
//
// Capacity is reclaimed by TTL as well as by Release, so a process that dies
// holding permits does not permanently reduce the budget. The bucket row is the
// per-resource mutex — locked for the whole of Acquire's reclaim, count, and
// grant — which is what makes the count correct under READ COMMITTED.
//
// NewDBLimiter starts no goroutine. The sweeper is host-driven: call RunSweeper to
// run it, or leave it off and rely on Acquire's inline reclaim.
type DBLimiter struct {
	db            *gorm.DB
	rate          int
	interval      time.Duration
	burst         int
	maxConcurrent int
	holdTTL       time.Duration
	sweepInterval time.Duration
	log           *slog.Logger
}

// NewDBLimiter returns a DBLimiter over db. It returns an error for a
// configuration that could never admit work — a negative field, a positive Rate
// with a non-positive Interval, or MaxConcurrent without the HoldTTL that bounds
// its reservations.
func NewDBLimiter(db *gorm.DB, cfg DBLimiterConfig) (*DBLimiter, error) {
	if db == nil {
		return nil, errors.New("flywheel: NewDBLimiter: db is nil")
	}
	switch {
	case cfg.Rate < 0 || cfg.Burst < 0 || cfg.MaxConcurrent < 0:
		return nil, fmt.Errorf("flywheel: NewDBLimiter: rate, burst, and max concurrent must not be negative")
	case cfg.Rate > 0 && cfg.Interval <= 0:
		return nil, fmt.Errorf("flywheel: NewDBLimiter: a positive Rate requires a positive Interval")
	case cfg.MaxConcurrent > 0 && cfg.HoldTTL <= 0:
		return nil, fmt.Errorf("flywheel: NewDBLimiter: MaxConcurrent requires a positive HoldTTL")
	}
	burst := cfg.Burst
	if cfg.Rate > 0 && burst <= 0 {
		burst = cfg.Rate
	}
	sweepInterval := orDuration(cfg.SweepInterval, cfg.HoldTTL)
	sweepInterval = orDuration(sweepInterval, defaultLimiterSweepInterval)
	return &DBLimiter{
		db:            db,
		rate:          cfg.Rate,
		interval:      cfg.Interval,
		burst:         burst,
		maxConcurrent: cfg.MaxConcurrent,
		holdTTL:       cfg.HoldTTL,
		sweepInterval: sweepInterval,
		log:           slog.Default(),
	}, nil
}

// ensure DBLimiter satisfies Limiter.
var _ Limiter = (*DBLimiter)(nil)

// Acquire grants up to n permits against resource in one transaction. When both
// caps are off it short-circuits with no round trip. Otherwise it locks the
// resource's bucket row (the per-resource mutex), refills the integer token count
// carrying the sub-token remainder forward in refilled_at, reclaims the resource's
// expired holds inline, counts the live holds, grants the minimum of the ask, the
// tokens, and the concurrency headroom, and — when a concurrency cap is set and
// the grant is positive — records one hold row carrying the whole grant under a
// minted token.
//
//nolint:gocognit,gocyclo // one transaction with the cohesive refill/reclaim/grant steps
func (l *DBLimiter) Acquire(ctx context.Context, resource string, n int) (Grant, error) {
	if n <= 0 {
		return Grant{Resource: resource}, nil
	}
	if l.rate <= 0 && l.maxConcurrent <= 0 {
		return Grant{Resource: resource, N: n}, nil
	}
	now := models.ClockFrom(ctx).Now(ctx)

	var g Grant
	err := l.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// (1) Insert-first, then lock. The bucket row is the per-resource mutex, held
		// even when Rate is zero, so reclaim + count + grant serialize per resource.
		seed := limiterBucketRow{Resource: resource, Tokens: l.burst, RefilledAt: now, UpdatedAt: now}
		if err := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&seed).Error; err != nil {
			return fmt.Errorf("seed bucket: %w", err)
		}
		locked, err := l.lockBucket(tx, resource)
		if err != nil {
			return err
		}

		// (2) Refill, carrying the fraction forward in refilled_at.
		l.refill(&locked, now)

		// (3) Reclaim this resource's expired holds inline, before counting, so
		// self-healing does not depend on the sweeper running.
		held := 0
		if l.maxConcurrent > 0 {
			if err := tx.Where("resource = ? AND expires_at < ?", resource, now).
				Delete(&limiterHoldRow{}).Error; err != nil {
				return fmt.Errorf("reclaim expired holds: %w", err)
			}
			// (4) held = SUM(n) over the survivors.
			var agg struct{ Total int64 }
			if err := tx.Model(&limiterHoldRow{}).Where("resource = ?", resource).
				Select("COALESCE(SUM(n), 0) AS total").Scan(&agg).Error; err != nil {
				return fmt.Errorf("count holds: %w", err)
			}
			held = int(agg.Total)
		}

		// (5) granted = min(ask, tokens, headroom), floored at zero.
		granted := n
		if l.rate > 0 && granted > locked.Tokens {
			granted = locked.Tokens
		}
		if l.maxConcurrent > 0 {
			if room := l.maxConcurrent - held; granted > room {
				granted = room
			}
		}
		if granted < 0 {
			granted = 0
		}

		// Persist the refill even on a zero grant: the tokens accrued and the
		// carried refilled_at must not be lost to a denial.
		if l.rate > 0 {
			locked.Tokens -= granted
		}
		if err := tx.Model(&limiterBucketRow{}).Where("resource = ?", resource).
			Updates(map[string]any{"tokens": locked.Tokens, "refilled_at": locked.RefilledAt, "updated_at": now}).
			Error; err != nil {
			return fmt.Errorf("persist bucket: %w", err)
		}

		g = Grant{Resource: resource, N: granted}
		if granted == 0 {
			g.RetryAfter = l.retryAfter(locked.Tokens, now, locked.RefilledAt)
			return nil
		}
		if l.maxConcurrent > 0 {
			token := models.NewID()
			expiresAt := now.Add(l.holdTTL)
			hold := limiterHoldRow{Token: token, Resource: resource, N: granted, ExpiresAt: expiresAt}
			if err := tx.Create(&hold).Error; err != nil {
				return fmt.Errorf("record hold: %w", err)
			}
			g.Token = token
			g.ExpiresAt = expiresAt
		}
		if granted < n {
			g.RetryAfter = l.retryAfter(locked.Tokens, now, locked.RefilledAt)
		}
		return nil
	})
	if err != nil {
		// N==0 on error: never a partial grant alongside a failure, so the runner's
		// fail-closed path cannot leak a permit.
		return Grant{}, fmt.Errorf("flywheel: limiter acquire: %w", err)
	}
	return g, nil
}

// lockBucket loads the resource's bucket row for update. The FOR UPDATE clause is
// PostgreSQL-only: SQLite has no row lock, but the library opens it with
// _txlock=immediate so a plain managed transaction is already a BEGIN IMMEDIATE
// that serializes writers — the same idiom the barrier's parent lock uses.
func (l *DBLimiter) lockBucket(tx *gorm.DB, resource string) (limiterBucketRow, error) {
	q := tx.Model(&limiterBucketRow{}).Where("resource = ?", resource)
	if tx.Name() == "postgres" {
		q = q.Clauses(clause.Locking{Strength: "UPDATE"})
	}
	var row limiterBucketRow
	if err := q.First(&row).Error; err != nil {
		return limiterBucketRow{}, fmt.Errorf("lock bucket: %w", err)
	}
	return row, nil
}

// Release returns a grant's concurrency permits in one transaction: it decrements
// the hold's n atomically, then deletes the row once it reaches zero. It is a
// no-op for a grant with no token, and — because the decrement is a single
// expression, not a read-modify-write — it neither loses a concurrent decrement
// nor errors when the row is already gone. It is errorless by contract: a failure
// is logged, and the stranded capacity self-heals via the TTL.
func (l *DBLimiter) Release(ctx context.Context, g Grant) {
	if g.N <= 0 || g.Token == "" {
		return
	}
	err := l.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Model(&limiterHoldRow{}).Where("token = ?", g.Token).
			UpdateColumn("n", gorm.Expr("n - ?", g.N)).Error; err != nil {
			return err
		}
		return tx.Where("token = ? AND n <= 0", g.Token).Delete(&limiterHoldRow{}).Error
	})
	if err != nil {
		l.log.WarnContext(ctx, "flywheel: limiter release failed; capacity self-heals via TTL",
			slog.String("resource", g.Resource), slog.String("token", g.Token), slog.String("error", err.Error()))
	}
}

// refill adds the whole tokens accrued since refilled_at, capped at Burst, and
// advances refilled_at by exactly the time those tokens cost — carrying the
// sub-token remainder forward rather than discarding it, which is what keeps the
// integer bucket's long-run rate exact. When the bucket fills, refilled_at snaps
// to now (a full bucket has no fraction to keep).
func (l *DBLimiter) refill(b *limiterBucketRow, now time.Time) {
	if l.rate <= 0 {
		return
	}
	elapsed := now.Sub(b.RefilledAt)
	if elapsed <= 0 {
		return
	}
	// Time to fill the whole bucket from empty. It is also the overflow guard: past
	// it the bucket is full regardless, so the elapsed*rate multiply below stays
	// bounded by burst*interval.
	burstTime := time.Duration(int64(l.burst) * int64(l.interval) / int64(l.rate))
	if elapsed >= burstTime {
		b.Tokens = l.burst
		b.RefilledAt = now
		return
	}
	add := int(int64(elapsed) * int64(l.rate) / int64(l.interval))
	if add <= 0 {
		return // less than one token's worth elapsed; keep refilled_at so the fraction carries
	}
	b.Tokens = min(l.burst, b.Tokens+add)
	if b.Tokens >= l.burst {
		b.RefilledAt = now
		return
	}
	b.RefilledAt = b.RefilledAt.Add(time.Duration(int64(add) * int64(l.interval) / int64(l.rate)))
}

// retryAfter is the exact wait to the next whole token, given the post-refill
// token count and refilled_at. It returns zero when rate limiting is disabled or a
// whole token is already available — a concurrency-bound denial, which the caller
// cannot time and so falls back to its poll interval.
func (l *DBLimiter) retryAfter(tokens int, now, refilledAt time.Time) time.Duration {
	if l.rate <= 0 || tokens >= 1 {
		return 0
	}
	perToken := time.Duration(int64(l.interval) / int64(l.rate))
	if wait := refilledAt.Add(perToken).Sub(now); wait > 0 {
		return wait
	}
	return 0
}

// Sweep reclaims expired and drained holds in bounded batches, returning how many
// rows it deleted across every batch. It is a pure optimization — Acquire's inline
// reclaim (step 3) is what makes over-admission self-heal — so a host may run it,
// on RunSweeper or its own scheduler, or never.
//
// It reaps a hold whose lease has expired or whose n has fallen to zero (a fully
// released grant whose delete lost a race). Each batch is its own transaction, so
// a deep backlog never becomes one lock-holding statement; a short batch ends the
// pass.
func (l *DBLimiter) Sweep(ctx context.Context) (int, error) {
	now := models.ClockFrom(ctx).Now(ctx)
	total := 0
	for {
		if err := ctx.Err(); err != nil {
			return total, fmt.Errorf("flywheel: limiter sweep cancelled after %d reclaimed: %w", total, err)
		}
		n, err := l.sweepBatch(ctx, now, defaultLimiterSweepBatch)
		total += n
		if err != nil {
			return total, err
		}
		if n < defaultLimiterSweepBatch {
			return total, nil
		}
	}
}

// sweepBatch reclaims one bounded batch inside a single transaction, selecting the
// tokens to reap then deleting by id — the retention shape, not DELETE ... LIMIT,
// which SQLite does not accept without a compile-time option.
func (l *DBLimiter) sweepBatch(ctx context.Context, now time.Time, limit int) (int, error) {
	var deleted int
	err := l.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var tokens []string
		if err := tx.Model(&limiterHoldRow{}).
			Where("expires_at < ? OR n <= 0", now).
			Limit(limit).Pluck("token", &tokens).Error; err != nil {
			return fmt.Errorf("find expired holds: %w", err)
		}
		if len(tokens) == 0 {
			return nil
		}
		res := tx.Where("token IN ?", tokens).Delete(&limiterHoldRow{})
		if res.Error != nil {
			return fmt.Errorf("delete expired holds: %w", res.Error)
		}
		deleted = int(res.RowsAffected)
		return nil
	})
	if err != nil {
		return 0, fmt.Errorf("flywheel: limiter sweep: %w", err)
	}
	return deleted, nil
}

// RunSweeper runs Sweep on the configured interval until ctx is cancelled. It
// blocks, like Scheduler.Run, so a host starts it on its own goroutine. The
// interval is synchronous — one Sweep completes before the next tick — so a slow
// pass cannot overlap itself. A sweep failure is logged and the loop carries on,
// because the inline reclaim keeps correctness independent of it.
func (l *DBLimiter) RunSweeper(ctx context.Context) {
	ticker := time.NewTicker(l.sweepInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if _, err := l.Sweep(ctx); err != nil && ctx.Err() == nil {
				l.log.WarnContext(ctx, "flywheel: limiter sweep failed", slog.String("error", err.Error()))
			}
		}
	}
}
