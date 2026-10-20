package core

import (
	"context"
	"fmt"
	"time"

	"github.com/mrz1836/go-foundation/models"
	"gorm.io/gorm"
)

// LimiterInfo is one shared limiter resource's state, read from the tables a
// DBLimiter writes: its rate bucket's tokens as last stored, the permits its
// live concurrency holds take, when the soonest of those holds lapses, and the
// overrides SetLimiterBudget set on it. A resource's configured budget is the
// host's DBLimiterConfig, so it carries none.
type LimiterInfo struct {
	Resource       string     `json:"resource"`
	Tokens         int        `json:"tokens"`                     // a rate limiter's tokens as last stored; Acquire refills before it grants
	Held           int64      `json:"held"`                       // permits held by live concurrency holds
	NextHoldExpiry *time.Time `json:"next_hold_expiry,omitempty"` // the soonest live hold's expiry; nil when nothing is held
	UpdatedAt      time.Time  `json:"updated_at"`
	// Overrides are the overrides set on the resource's bucket row, whether or
	// not a given process's limiter can apply them (a Rate override needs a
	// positive Interval, a MaxConcurrent override a positive HoldTTL).
	Overrides LimiterBudget `json:"overrides"`
}

// ListLimiters returns the state of each resource a DBLimiter has admitted
// against: one entry per limiter_buckets row, sorted by resource, at most
// MaxReadLimit of them. Held sums n over the resource's limiter_holds rows whose
// expires_at has not passed by the context clock, as Acquire counts them, and
// NextHoldExpiry is the earliest of those. Tokens and UpdatedAt are as stored,
// so a rate bucket's tokens are those of its last grant, before any refill since.
// Overrides are as SetLimiterBudget last set them. The resources listed are
// exactly those SetLimiterBudget can override.
//
// It only reads: it takes no lock, opens no transaction, and leaves expired
// holds in place for Acquire or the sweeper to reclaim. A limiter with no cap
// and neither an Interval nor a HoldTTL admits without a round trip and writes
// no bucket, so it is not listed.
// It costs three reads: the buckets, one grouped sum of their live holds, and
// one ordered single-row read per held resource for its soonest expiry.
func ListLimiters(ctx context.Context, db *gorm.DB) ([]LimiterInfo, error) {
	if db == nil {
		return nil, fmt.Errorf("flywheel: ListLimiters: db is nil")
	}
	now := models.ClockFrom(ctx).Now(ctx)

	var buckets []limiterBucketRow
	if err := db.WithContext(ctx).Model(&limiterBucketRow{}).
		Select("resource, tokens, updated_at, max_concurrent_override, rate_override, burst_override").
		Order("resource").Limit(MaxReadLimit).Find(&buckets).Error; err != nil {
		return nil, fmt.Errorf("flywheel: ListLimiters: read buckets: %w", models.WrapDBError(err))
	}
	out := make([]LimiterInfo, len(buckets))
	if len(buckets) == 0 {
		return out, nil
	}
	resources := make([]string, len(buckets))
	for i, b := range buckets {
		resources[i] = b.Resource
		out[i] = LimiterInfo{Resource: b.Resource, Tokens: b.Tokens, UpdatedAt: b.UpdatedAt, Overrides: b.overrides()}
	}

	// Live holds are the complement of Acquire's reclaim (expires_at < now).
	var sums []struct {
		Resource string
		Held     int64
	}
	if err := db.WithContext(ctx).Model(&limiterHoldRow{}).Select("resource, COALESCE(SUM(n), 0) AS held").
		Where("resource IN ? AND expires_at >= ?", resources, now).Group("resource").
		Scan(&sums).Error; err != nil {
		return nil, fmt.Errorf("flywheel: ListLimiters: sum holds: %w", models.WrapDBError(err))
	}
	held := make(map[string]int64, len(sums))
	for _, s := range sums {
		held[s.Resource] = s.Held
	}

	for i := range out {
		if held[out[i].Resource] == 0 {
			continue
		}
		out[i].Held = held[out[i].Resource]
		// An ordered single-row read, not MIN: SQLite returns a bare aggregate over
		// a time column as text.
		var soonest []limiterHoldRow
		if err := db.WithContext(ctx).Model(&limiterHoldRow{}).Select("expires_at").
			Where("resource = ? AND expires_at >= ?", out[i].Resource, now).
			Order("expires_at").Limit(1).Find(&soonest).Error; err != nil {
			return nil, fmt.Errorf("flywheel: ListLimiters: read next expiry: %w", models.WrapDBError(err))
		}
		if len(soonest) == 1 {
			at := soonest[0].ExpiresAt
			out[i].NextHoldExpiry = &at
		}
	}
	return out, nil
}
