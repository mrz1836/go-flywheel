package core

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mrz1836/go-foundation/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// TestEntriesOutliveTheirJob proves retention deletes a job and keeps its
// history: the entry still reads back for the job's id.
func TestEntriesOutliveTheirJob(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	ctx := context.Background()
	seedJob(t, db, jobRow{ID: "gone-1", Kind: "leaf", State: string(StateAvailable), ScheduledAt: time.Now().UTC()})
	require.NoError(t, CancelJob(ctx, db, "gone-1"))

	n, err := DeleteFinishedJobs(ctx, db, time.Now().Add(time.Hour))
	require.NoError(t, err)
	require.EqualValues(t, 1, n)
	assert.Zero(t, countRows(t, db.Unscoped(), &jobRow{}), "the job is gone")

	page, err := ListAudit(ctx, db, ListAuditParams{TargetType: AuditTargetJob, TargetID: "gone-1"})
	require.NoError(t, err)
	require.Len(t, page.Entries, 1, "its history is not")
	assert.Equal(t, AuditCancelled, page.Entries[0].Action)
}

// seedAgedAuditEntries writes five entries older than cutoff, oldest first
// (old-1 … old-5), and two newer ones.
func seedAgedAuditEntries(t testing.TB, db *gorm.DB, cutoff time.Time) {
	t.Helper()
	for i, id := range []string{"old-1", "old-2", "old-3", "old-4", "old-5"} {
		seedAuditEntry(t, db, jobAuditRow{
			ID: id, CreatedAt: cutoff.Add(-time.Duration(5-i) * time.Hour),
			TargetType: string(AuditTargetJob), TargetID: "aged", Action: string(AuditRetried),
		})
	}
	for i, id := range []string{"new-1", "new-2"} {
		seedAuditEntry(t, db, jobAuditRow{
			ID: id, CreatedAt: cutoff.Add(time.Duration(i+1) * time.Hour),
			TargetType: string(AuditTargetJob), TargetID: "aged", Action: string(AuditRetried),
		})
	}
}

// remainingAuditIDs returns the ids job_audit still holds, oldest first.
func remainingAuditIDs(t testing.TB, db *gorm.DB) []string {
	t.Helper()
	var ids []string
	require.NoError(t, db.Model(&jobAuditRow{}).Order("created_at, id").Pluck("id", &ids).Error)
	return ids
}

// cancelAfterContext is a context whose Err reports cancellation once cancel
// is called, while Done never closes: a statement already running is not cut
// short, and the cancellation shows at the next check between batches.
type cancelAfterContext struct {
	context.Context

	cancelled atomic.Bool
}

func (c *cancelAfterContext) cancel() { c.cancelled.Store(true) }

func (c *cancelAfterContext) Err() error {
	if c.cancelled.Load() {
		return context.Canceled
	}
	return c.Context.Err()
}

// TestDeleteAuditEntriesPrunesOnlyOlderEntriesInBatches proves the prune deletes
// the entries created before its cutoff, oldest first, in batches, honoring
// MaxBatches and a cancel between batches.
func TestDeleteAuditEntriesPrunesOnlyOlderEntriesInBatches(t *testing.T) {
	t.Parallel()
	cutoff := auditBase.Add(24 * time.Hour)

	t.Run("every older entry, in batches", func(t *testing.T) {
		t.Parallel()
		db := newDB(t)
		seedAgedAuditEntries(t, db, cutoff)
		var deletes atomic.Int32
		require.NoError(t, db.Callback().Delete().After("gorm:delete").Register("test:count_deletes",
			func(*gorm.DB) { deletes.Add(1) }))

		n, err := DeleteAuditEntriesWithOptions(context.Background(), db, cutoff, RetentionOpts{BatchSize: 2})
		require.NoError(t, err)
		assert.EqualValues(t, 5, n)
		assert.EqualValues(t, 3, deletes.Load(), "batches of two: 2, 2, and 1")
		assert.Equal(t, []string{"new-1", "new-2"}, remainingAuditIDs(t, db), "the newer entries stay")
	})

	t.Run("oldest first, up to MaxBatches", func(t *testing.T) {
		t.Parallel()
		db := newDB(t)
		seedAgedAuditEntries(t, db, cutoff)

		n, err := DeleteAuditEntriesWithOptions(context.Background(), db, cutoff,
			RetentionOpts{BatchSize: 2, MaxBatches: 1})
		require.NoError(t, err)
		assert.EqualValues(t, 2, n)
		assert.Equal(t, []string{"old-3", "old-4", "old-5", "new-1", "new-2"}, remainingAuditIDs(t, db))
	})

	t.Run("a cancel between batches", func(t *testing.T) {
		t.Parallel()
		db := newDB(t)
		seedAgedAuditEntries(t, db, cutoff)
		ctx := &cancelAfterContext{Context: context.Background()}
		require.NoError(t, db.Callback().Delete().After("gorm:delete").Register("test:cancel_after_first",
			func(*gorm.DB) { ctx.cancel() }))

		n, err := DeleteAuditEntriesWithOptions(ctx, db, cutoff, RetentionOpts{BatchSize: 2})
		require.ErrorIs(t, err, context.Canceled)
		assert.Contains(t, err.Error(), "cancelled after 2 deleted")
		assert.EqualValues(t, 2, n, "the first batch committed")
		assert.Equal(t, []string{"old-3", "old-4", "old-5", "new-1", "new-2"}, remainingAuditIDs(t, db))
	})

	t.Run("DeleteAuditEntries is the zero-options call", func(t *testing.T) {
		t.Parallel()
		db := newDB(t)
		seedAgedAuditEntries(t, db, cutoff)

		n, err := DeleteAuditEntries(context.Background(), db, cutoff)
		require.NoError(t, err)
		assert.EqualValues(t, 5, n)
		assert.Equal(t, []string{"new-1", "new-2"}, remainingAuditIDs(t, db))
	})
}

// syncBuffer is a bytes.Buffer safe for a logger and a test to share.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

// lines decodes every JSON log line written so far.
func (b *syncBuffer) lines(t testing.TB) []map[string]any {
	t.Helper()
	b.mu.Lock()
	defer b.mu.Unlock()
	var out []map[string]any
	scanner := bufio.NewScanner(bytes.NewReader(b.buf.Bytes()))
	for scanner.Scan() {
		var line map[string]any
		require.NoError(t, json.Unmarshal(scanner.Bytes(), &line))
		out = append(out, line)
	}
	return out
}

// activityNames lists the names of s's maintenance activities.
func activityNames(s *Scheduler) []string {
	var names []string
	for _, a := range s.activities() {
		names = append(names, a.name)
	}
	return names
}

// TestSchedulerPrunesAuditEntriesOnlyWithAuditRetention proves AuditRetention
// alone turns the retention activity on, and its pass prunes the entries older
// than the window and logs how many; without it no pass deletes an entry.
func TestSchedulerPrunesAuditEntriesOnlyWithAuditRetention(t *testing.T) {
	t.Parallel()
	now := auditBase.Add(30 * 24 * time.Hour)

	t.Run("AuditRetention alone", func(t *testing.T) {
		t.Parallel()
		db := newDB(t)
		seedAgedAuditEntries(t, db, now.Add(-24*time.Hour))
		logs := &syncBuffer{}
		s := newSchedulerCfg(t, SchedulerConfig{
			DB: db, Client: NewClient(db), Logger: slog.New(slog.NewJSONHandler(logs, nil)),
			AuditRetention: 24 * time.Hour,
		})
		assert.Contains(t, activityNames(s), "retention", "the activity runs without RetentionMaxAge")

		s.pruneOnce(clockCtx(context.Background(), models.NewFixedClock(now)))
		assert.Equal(t, []string{"new-1", "new-2"}, remainingAuditIDs(t, db), "entries older than 24 hours are gone")
		var pruned []map[string]any
		for _, line := range logs.lines(t) {
			if line["msg"] == "jobs: retention sweep pruned audit entries" {
				pruned = append(pruned, line)
			}
		}
		require.Len(t, pruned, 1)
		assert.Equal(t, float64(5), pruned[0]["deleted"])
	})

	t.Run("zero keeps every entry", func(t *testing.T) {
		t.Parallel()
		db := newDB(t)
		seedAgedAuditEntries(t, db, now.Add(-24*time.Hour))
		ctx := clockCtx(context.Background(), models.NewFixedClock(now))

		plain := newSchedulerCfg(t, SchedulerConfig{DB: db, Client: NewClient(db)})
		assert.NotContains(t, activityNames(plain), "retention")
		plain.pruneOnce(ctx)

		jobsOnly := newSchedulerCfg(t, SchedulerConfig{DB: db, Client: NewClient(db), RetentionMaxAge: 48 * time.Hour})
		assert.Contains(t, activityNames(jobsOnly), "retention")
		jobsOnly.pruneOnce(ctx)

		assert.Len(t, remainingAuditIDs(t, db), 7, "no pass deletes an entry")
	})
}

// TestSchedulerWithAuditRetentionRefusesASchemaWithoutTheAuditTable is the
// deploy-order guard for the prune: a Scheduler that would delete entries stops
// before starting any activity on a schema without the table, naming it.
func TestSchedulerWithAuditRetentionRefusesASchemaWithoutTheAuditTable(t *testing.T) {
	t.Parallel()
	db := newBareSQLite(t)
	installPreviousReleaseSchema(t, db)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	s := newSchedulerCfg(t, SchedulerConfig{
		DB: db, Client: NewClient(db), Driver: driverForTest(t, db), AuditRetention: 24 * time.Hour,
	})
	err := s.Run(ctx)
	require.ErrorIs(t, err, ErrSchemaOutdated)
	assert.Contains(t, err.Error(), "job_audit (table)", "the error names the missing table")
	assert.Contains(t, err.Error(), "flywheel migrate", "and how to fix it")
	assert.NoError(t, ctx.Err(), "it stopped at once, before any activity")
}

// TestTheRuntimeStartsWithoutTheAuditTable guards the other side: a Runner, and
// a Scheduler without AuditRetention, start and run a job on the previous
// release's schema, since neither writes the history.
func TestTheRuntimeStartsWithoutTheAuditTable(t *testing.T) {
	t.Parallel()
	db := newBareSQLite(t)
	installPreviousReleaseSchema(t, db)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	s := newSchedulerCfg(t, SchedulerConfig{DB: db, Client: NewClient(db), Driver: driverForTest(t, db)})
	require.NoError(t, s.checkSchema(ctx), "a Scheduler without AuditRetention starts")
	// In the Runner's zone: on SQLite, jobs compares scheduled_at as text in the
	// zone it was written in.
	due := time.Now().Truncate(time.Second).Add(-30 * time.Second)
	require.NoError(t, UpsertPeriodic(ctx, db, PeriodicSpec{Slug: "audit-free", Kind: statsWriteArgs{}.Kind(), Every: time.Minute, Active: true}))
	require.NoError(t, db.Model(&jobPeriodicRow{}).Where("slug = ?", "audit-free").Update("next_run_at", due).Error)
	fired, err := s.Tick(clockCtx(ctx, models.NewFixedClock(due.Add(time.Second))))
	require.NoError(t, err)
	require.Equal(t, 1, fired, "the Scheduler fires its schedule")

	reg := NewRegistry()
	Register[statsWriteArgs](reg, statsWriteWorker{})
	r, err := NewRunner(RunnerConfig{
		DB: db, Driver: driverForTest(t, db), Registry: reg, Queues: []string{"periodic"}, ClaimAnyClass: true,
	})
	require.NoError(t, err)
	require.NoError(t, r.RunUntilIdle(ctx), "a Runner starts")
	var states []string
	require.NoError(t, db.Model(&jobRow{}).Where("kind = ?", statsWriteArgs{}.Kind()).Pluck("state", &states).Error)
	assert.Equal(t, []string{string(StateSucceeded)}, states, "and runs the job")
}
