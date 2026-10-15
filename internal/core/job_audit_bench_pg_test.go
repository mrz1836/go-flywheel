//go:build integration

package core

import (
	"context"
	"testing"
	"time"

	"github.com/mrz1836/go-foundation/models"
	"gorm.io/gorm"
)

// scopedBenchChildren is how many children each iteration's parent has.
const scopedBenchChildren = 10_000

// BenchmarkScopedControlsPostgres measures the two batched controls that touch
// the most jobs at once, CancelByParent and ReplayByParent, over a parent with
// 10,000 children on PostgreSQL. Each iteration seeds a fresh parent outside the
// timer, so every run changes all 10,000 children: a cancel of available
// children, and a replay of discarded ones. The controls run on a plain context,
// so what they record is attributed to the system.
func BenchmarkScopedControlsPostgres(b *testing.B) {
	db := NewPostgresIsolatedDB(b)
	ctx := context.Background()

	b.Run("CancelByParent10k", func(b *testing.B) {
		for range b.N {
			b.StopTimer()
			parent := seedScopedBenchChildren(b, db, StateAvailable)
			b.StartTimer()
			res, err := CancelByParent(ctx, db, parent, ScopeOpts{})
			if err != nil {
				b.Fatal(err)
			}
			if res.Changed != scopedBenchChildren {
				b.Fatalf("cancelled %d children, want %d", res.Changed, scopedBenchChildren)
			}
		}
	})
	b.Run("ReplayByParent10k", func(b *testing.B) {
		for range b.N {
			b.StopTimer()
			parent := seedScopedBenchChildren(b, db, StateDiscarded)
			b.StartTimer()
			res, err := ReplayByParent(ctx, db, parent, ReplayOpts{})
			if err != nil {
				b.Fatal(err)
			}
			if res.Changed != scopedBenchChildren {
				b.Fatalf("replayed %d children, want %d", res.Changed, scopedBenchChildren)
			}
		}
	})
}

// seedScopedBenchChildren writes a fresh parent id's scopedBenchChildren
// children in state, server-side, analyzes jobs, and returns the parent id. A
// discarded child carries finalized_at, as a finalize leaves it.
func seedScopedBenchChildren(b *testing.B, db *gorm.DB, state JobState) string {
	b.Helper()
	parent := models.NewID()
	at := time.Now().UTC()
	var finalized *time.Time
	if isTerminalStateString(string(state)) {
		finalized = &at
	}
	execSeriesInSlicesPG(b, db, `
		INSERT INTO jobs (id, created_at, updated_at, metadata, kind, queue, args, priority,
		                  state, attempt, max_attempts, scheduled_at, finalized_at,
		                  executor_class, tags, parent_job_id)
		SELECT ? || '-' || g, ?, ?, '{}'::jsonb, 'bench.leaf', 'default', '{}'::jsonb, 100,
		       ?, 1, 25, ?, ?, '', '[]'::jsonb, ?
		FROM generate_series(?::int, ?::int) AS g`,
		scopedBenchChildren, parent, at, at, string(state), at, finalized, parent)
	// Fresh statistics, so every iteration is planned as a settled table is,
	// rather than as one autovacuum hasn't analyzed yet.
	if err := db.Exec(`ANALYZE jobs`).Error; err != nil {
		b.Fatal(err)
	}
	return parent
}
