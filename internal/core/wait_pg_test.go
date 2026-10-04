//go:build integration

package core

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// fastPostgresRunner builds a PostgreSQL Runner that claims every class with a
// tight poll, so a waiting test is not paced by the runner's idle backoff.
func fastPostgresRunner(t *testing.T, db *gorm.DB) func(*Registry) *Runner {
	t.Helper()
	return func(reg *Registry) *Runner {
		r, err := NewRunner(RunnerConfig{
			DB: db, Driver: NewPostgresDriver(db), Registry: reg,
			Queues: []string{"default"}, ClaimAnyClass: true, Concurrency: 4,
			PollInterval: 5 * time.Millisecond,
		})
		require.NoError(t, err)
		return r
	}
}

// TestWaitForJobFollowsARealRunnerPostgres waits on a job a PostgreSQL runner
// works, and reads its output with LatestRun.
func TestWaitForJobFollowsARealRunnerPostgres(t *testing.T) {
	t.Parallel()
	db := NewPostgresIsolatedDB(t)
	assertWaitFollowsARealRunner(t, db, fastPostgresRunner(t, db))
}

// TestEnqueueOrJoinThenWaitRunsTheWorkOncePostgres races 20 callers to
// enqueue-or-join one subject under a UniqueActiveKey, then wait: one insert
// lands, the other 19 name it and join, and the work runs once.
func TestEnqueueOrJoinThenWaitRunsTheWorkOncePostgres(t *testing.T) {
	t.Parallel()
	db := NewPostgresIsolatedDB(t)
	assertEnqueueOrJoinRunsTheWorkOnce(t, db, fastPostgresRunner(t, db), 20, true)
}
