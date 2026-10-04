//go:build integration

package core

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/mrz1836/go-foundation/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestReconcilePeriodicsConcurrentSchedulersInsertOncePostgres runs the reconcile
// the way a fleet does — several Schedulers on one database, all declaring the
// same periodics, passing at the same moment — and proves each missing definition
// is re-created exactly once and reported exactly once, by the Scheduler whose
// insert landed. Every round deletes them all again.
func TestReconcilePeriodicsConcurrentSchedulersInsertOncePostgres(t *testing.T) {
	t.Parallel()
	db := NewPostgresIsolatedDB(t)
	ctx := context.Background()

	const schedulers, rounds = 4, 10
	specs := make([]PeriodicSpec, 5)
	for i := range specs {
		specs[i] = declaredSpec(fmt.Sprintf("fleet-%d", i))
	}
	handlers := make([]*captureHandler, schedulers)
	fleet := make([]*Scheduler, schedulers)
	for i := range fleet {
		handlers[i] = &captureHandler{}
		fleet[i] = declaringScheduler(t, db, handlers[i], 0, specs...)
	}

	for round := range rounds {
		require.NoError(t, db.Exec(`DELETE FROM job_periodics`).Error)

		start := make(chan struct{})
		counts := make([]int, schedulers)
		errs := make([]error, schedulers)
		var wg sync.WaitGroup
		for i, s := range fleet {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				counts[i], errs[i] = s.ReconcilePeriodics(ctx)
			}()
		}
		close(start)
		wg.Wait()

		total := 0
		for i := range fleet {
			require.NoError(t, errs[i], "round %d scheduler %d", round, i)
			total += counts[i]
		}
		assert.Equal(t, len(specs), total, "round %d: each missing definition is counted once across the fleet", round)
		var rows int64
		require.NoError(t, db.Model(&jobPeriodicRow{}).Count(&rows).Error)
		assert.EqualValues(t, len(specs), rows, "round %d: one row per declared slug", round)
	}

	warns := 0
	for _, h := range handlers {
		warns += len(h.recordsFor(recreatedMessage))
	}
	assert.Equal(t, rounds*len(specs), warns, "each re-creation is logged once, by the Scheduler that made it")
}

// TestStartupApplyAcrossSchedulersPostgres starts a fleet at once against an empty
// database: every Scheduler applies the same declarations concurrently, which is
// the race UpsertPeriodic's adopt path closes. None of them logs a failed apply,
// and there is one row per declaration.
func TestStartupApplyAcrossSchedulersPostgres(t *testing.T) {
	t.Parallel()
	db := NewPostgresIsolatedDB(t)
	ctx := context.Background()

	const schedulers = 4
	specs := make([]PeriodicSpec, 5)
	for i := range specs {
		specs[i] = declaredSpec(fmt.Sprintf("boot-%d", i))
	}
	handlers := make([]*captureHandler, schedulers)
	fleet := make([]*Scheduler, schedulers)
	for i := range fleet {
		handlers[i] = &captureHandler{}
		fleet[i] = declaringScheduler(t, db, handlers[i], 0, specs...)
	}

	start := make(chan struct{})
	var wg sync.WaitGroup
	for _, s := range fleet {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			s.applyDeclaredPeriodics(ctx)
		}()
	}
	close(start)
	wg.Wait()

	for i, h := range handlers {
		assert.False(t, h.has(applyFailedMessage), "scheduler %d applied every declaration", i)
		assert.False(t, fleet[i].hasPendingPeriodics(), "scheduler %d has nothing left to retry", i)
	}
	views, err := ListPeriodics(ctx, db)
	require.NoError(t, err)
	assert.Len(t, views, len(specs))
}

// TestReconcilePeriodicsRecreatesADeletedDefinitionPostgres is
// TestReconcilePeriodicsRecreatesADeletedDefinition on PostgreSQL, where the
// conflict insert takes the RETURNING path the warn-once attribution rests on.
func TestReconcilePeriodicsRecreatesADeletedDefinitionPostgres(t *testing.T) {
	t.Parallel()
	db := NewPostgresIsolatedDB(t)
	h := &captureHandler{}
	off := declaredSpec("off")
	off.Active = false
	s := declaringScheduler(t, db, h, 0, declaredSpec("a"), off)
	t0 := time.Now().UTC().Truncate(time.Second)
	t1 := t0.Add(time.Hour)

	s.applyDeclaredPeriodics(clockCtx(context.Background(), models.NewFixedClock(t0)))
	require.NoError(t, DeletePeriodic(context.Background(), db, "a"))
	require.NoError(t, DeletePeriodic(context.Background(), db, "off"))

	ctx := clockCtx(context.Background(), models.NewFixedClock(t1))
	n, err := s.ReconcilePeriodics(ctx)
	require.NoError(t, err)
	assert.Equal(t, 2, n)

	row, found := periodicRowBySlug(t, db, "a")
	require.True(t, found)
	assert.True(t, *row.IsActive)
	assert.True(t, row.NextRunAt.Equal(t1.Add(time.Minute)), "it fires next at its first fire time after now")
	assert.False(t, periodicIsActive(t, db, "off"), "a definition declared off comes back off")

	fired, err := s.Tick(ctx)
	require.NoError(t, err)
	assert.Zero(t, fired, "the hour it was gone is not backfilled")
	assert.Len(t, h.recordsFor(recreatedMessage), 2)

	n, err = s.ReconcilePeriodics(ctx)
	require.NoError(t, err)
	assert.Zero(t, n, "a second pass finds nothing missing")
	assert.Len(t, h.recordsFor(recreatedMessage), 2)
}

// TestReconcileRecreatesOthersWhileOneDeclarationKeepsFailingPostgres is the
// same isolation with a failure PostgreSQL produces on its own: an args template
// that is valid JSON but that jsonb refuses (a \u0000 escape) fails every apply
// of "bad", and a deleted "good" is still re-created.
func TestReconcileRecreatesOthersWhileOneDeclarationKeepsFailingPostgres(t *testing.T) {
	t.Parallel()
	db := NewPostgresIsolatedDB(t)
	h := &captureHandler{}
	bad := declaredSpec("bad")
	bad.ArgsTemplate = []byte(`{"v":"\u0000"}`)
	s := declaringScheduler(t, db, h, 0, bad, declaredSpec("good"))
	ctx := context.Background()

	s.applyDeclaredPeriodics(ctx)
	require.Len(t, h.recordsFor(applyFailedMessage), 1, "jsonb refuses bad's template")
	require.NoError(t, DeletePeriodic(ctx, db, "good"))

	s.reconcileOnce(ctx)
	_, found := periodicRowBySlug(t, db, "good")
	assert.True(t, found, "the pass re-creates good although bad is still pending")
	warns := h.recordsFor(recreatedMessage)
	require.Len(t, warns, 1)
	assert.Equal(t, "good", warns[0].attrs["slug"].String())
	assert.True(t, s.hasPendingPeriodics(), "bad is still pending")

	n, err := s.ReconcilePeriodics(ctx)
	require.NoError(t, err)
	assert.Zero(t, n, "nothing else is missing, and bad is skipped")
}
