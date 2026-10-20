//go:build integration

package core

import (
	"context"
	"testing"
	"time"
)

// BenchmarkDBLimiterAcquirePostgres measures one Acquire and its Release on a
// DBLimiter with a concurrency cap of 16 on PostgreSQL: on a resource with no
// override, and on one whose cap an override lowers to 8. Both read the same
// locked bucket row; the second applies what it carries.
func BenchmarkDBLimiterAcquirePostgres(b *testing.B) {
	db := NewPostgresIsolatedDB(b)
	ctx := context.Background()
	limiter, err := NewDBLimiter(db, DBLimiterConfig{MaxConcurrent: 16, HoldTTL: time.Minute})
	if err != nil {
		b.Fatal(err)
	}
	eight := 8
	for _, resource := range []string{"without-overrides", "with-overrides"} {
		g, err := limiter.Acquire(ctx, resource, 1)
		if err != nil {
			b.Fatal(err)
		}
		limiter.Release(ctx, g)
	}
	if err := SetLimiterBudget(ctx, db, "with-overrides", LimiterBudget{MaxConcurrent: &eight}); err != nil {
		b.Fatal(err)
	}

	for _, tc := range []struct{ name, resource string }{
		{"without overrides", "without-overrides"},
		{"with overrides", "with-overrides"},
	} {
		b.Run(tc.name, func(b *testing.B) {
			for range b.N {
				g, err := limiter.Acquire(ctx, tc.resource, 1)
				if err != nil {
					b.Fatal(err)
				}
				if g.N != 1 {
					b.Fatalf("granted %d, want 1", g.N)
				}
				limiter.Release(ctx, g)
			}
		})
	}
}
