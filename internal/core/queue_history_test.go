package core

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// queueSampleRows reads every stored sample, oldest first, then by queue.
func queueSampleRows(t testing.TB, db *gorm.DB) []jobQueueSampleRow {
	t.Helper()
	var rows []jobQueueSampleRow
	require.NoError(t, db.Order("sampled_at, queue").Find(&rows).Error)
	for i := range rows {
		rows[i].SampledAt = rows[i].SampledAt.UTC()
	}
	return rows
}

// queueSamplesSuite proves the sampling activity: one row per queue with live
// jobs at the pass's instant, in UTC, holding what QueueDepths reports; a
// repeat at the same instant kept once; samples past the retention deleted by
// the next pass, 30 days by default. Off, the activity does not exist and the
// Scheduler needs no table for it; on, it refuses a schema without one. open
// gives a current schema, and bare an empty database.
func queueSamplesSuite(t *testing.T, open dbOpener, bare func(*testing.T) *gorm.DB) {
	t.Helper()
	db := open(t)
	at := rollupBase.Add(10 * time.Hour)
	for range 3 {
		_, err := Enqueue(fixedClockCtx(at.Add(-90*time.Second)), NewClient(db), "sample.kind", []byte(`{}`), InsertOpts{})
		require.NoError(t, err)
	}
	seedJob(t, db, jobRow{
		ID: mintIDAt(at), Kind: "sample.kind", Queue: "bulk", State: string(StatePaused), MaxAttempts: 1,
		CreatedAt: at, UpdatedAt: at, ScheduledAt: at,
	})
	old := jobQueueSampleRow{Queue: defaultQueue, SampledAt: at.Add(-31 * 24 * time.Hour), Ready: 9}
	kept := jobQueueSampleRow{Queue: defaultQueue, SampledAt: at.Add(-29 * 24 * time.Hour), Ready: 8}
	require.NoError(t, db.Create(&[]jobQueueSampleRow{old, kept}).Error)

	s := newSchedulerCfg(t, SchedulerConfig{
		DB: db, Client: NewClient(db), Driver: driverForTest(t, db), QueueSampleInterval: time.Minute,
	})
	assert.Contains(t, activityNames(s), "queue samples")
	s.sampleQueuesOnce(fixedClockCtx(at))
	s.sampleQueuesOnce(fixedClockCtx(at))
	assert.Equal(t, []jobQueueSampleRow{
		{Queue: defaultQueue, SampledAt: kept.SampledAt, Ready: 8},
		{Queue: "bulk", SampledAt: at, Paused: 1},
		{Queue: defaultQueue, SampledAt: at, Ready: 3, OldestReadyMs: 90_000},
	}, queueSampleRows(t, db), "one row per queue at the pass's instant, once; the 31-day-old sample pruned")

	short := newSchedulerCfg(t, SchedulerConfig{
		DB: db, Client: NewClient(db), Driver: driverForTest(t, db), QueueSampleInterval: time.Minute,
		QueueSampleRetention: time.Hour,
	})
	next := at.Add(time.Hour + time.Minute)
	short.sampleQueuesOnce(fixedClockCtx(next))
	rows := queueSampleRows(t, db)
	require.Len(t, rows, 2, "a shorter retention prunes everything but this pass's samples")
	for _, r := range rows {
		assert.True(t, next.Equal(r.SampledAt), "%s sampled at %s", r.Queue, r.SampledAt)
	}

	off := newSchedulerCfg(t, SchedulerConfig{DB: db, Client: NewClient(db), Driver: driverForTest(t, db)})
	assert.NotContains(t, activityNames(off), "queue samples", "off, the activity does not exist")
	legacy := bare(t)
	installLegacySchema(t, legacy)
	err := newSchedulerCfg(t, SchedulerConfig{DB: legacy, Client: NewClient(legacy), Driver: driverForTest(t, legacy)}).
		checkSchema(context.Background())
	require.ErrorIs(t, err, ErrSchemaOutdated)
	assert.NotContains(t, err.Error(), "job_queue_samples", "off, the Scheduler does not need the table")
	err = newSchedulerCfg(t, SchedulerConfig{
		DB: legacy, Client: NewClient(legacy), Driver: driverForTest(t, legacy), QueueSampleInterval: time.Minute,
	}).checkSchema(context.Background())
	require.ErrorIs(t, err, ErrSchemaOutdated)
	assert.Contains(t, err.Error(), "job_queue_samples (table)", "on, it refuses a schema without it")
}

// TestQueueSamplesAreWrittenAndPruned runs the suite on SQLite.
func TestQueueSamplesAreWrittenAndPruned(t *testing.T) {
	t.Parallel()
	queueSamplesSuite(t, sqliteOpener, func(t *testing.T) *gorm.DB {
		t.Helper()
		return newBareSQLite(t)
	})
}

// TestQueueDepthHistoryKeepsEachBucketsMaximum proves the buckets: each count
// is the bucket's maximum over its samples, Samples how many there were, an
// empty bucket present with zeros, every Start on a Step boundary of the Unix
// epoch at or before From, samples before From left out, and a zero Step the
// smallest whole minute that keeps the points at or under MaxHistoryPoints.
func TestQueueDepthHistoryKeepsEachBucketsMaximum(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	ctx := context.Background()
	base := rollupBase.Add(2 * time.Hour)
	var samples []jobQueueSampleRow
	for m := range 180 {
		if m >= 60 && m < 75 {
			continue // an hour in, the sampler was down for a bucket
		}
		samples = append(samples, jobQueueSampleRow{
			Queue: defaultQueue, SampledAt: base.Add(time.Duration(m) * time.Minute),
			Ready: int64(m % 15), ScheduledAhead: int64(14 - m%15), Running: int64(m), Paused: int64(m % 2),
			OldestReadyMs: int64(1000 * (m % 15)),
		})
	}
	samples = append(samples, jobQueueSampleRow{Queue: "other", SampledAt: base, Ready: 500})
	require.NoError(t, db.CreateInBatches(samples, 100).Error)

	points, err := QueueDepthHistory(ctx, db, QueueHistoryParams{
		Queue: defaultQueue, From: base, To: base.Add(3 * time.Hour), Step: 15 * time.Minute,
	})
	require.NoError(t, err)
	require.Len(t, points, 12)
	for i, p := range points {
		start := base.Add(time.Duration(i) * 15 * time.Minute)
		assert.True(t, start.Equal(p.Start), "point %d starts at %s, want %s", i, p.Start, start)
		assert.Equal(t, time.UTC, p.Start.Location())
		if i == 4 {
			assert.Equal(t, QueueDepthPoint{Start: p.Start}, p, "the empty bucket is present, with zeros")
			continue
		}
		assert.Equal(t, 15, p.Samples)
		assert.EqualValues(t, 14, p.Ready, "the bucket's maximum, not its mean")
		assert.EqualValues(t, 14, p.ScheduledAhead)
		assert.EqualValues(t, 15*i+14, p.Running)
		assert.EqualValues(t, 1, p.Paused)
		assert.Equal(t, 14*time.Second, p.OldestReadyAge)
	}

	points, err = QueueDepthHistory(ctx, db, QueueHistoryParams{
		Queue: defaultQueue, From: base.Add(7 * time.Minute), To: base.Add(30 * time.Minute), Step: 15 * time.Minute,
	})
	require.NoError(t, err)
	require.Len(t, points, 2)
	assert.True(t, base.Equal(points[0].Start), "the first bucket starts on the boundary at or before From")
	assert.Equal(t, 8, points[0].Samples, "and holds only the samples from From on")
	assert.EqualValues(t, 14, points[0].Ready)

	points, err = QueueDepthHistory(ctx, db, QueueHistoryParams{
		Queue: defaultQueue, From: base.Add(5 * time.Minute), To: base.Add(40 * time.Minute), Step: 7 * time.Minute,
	})
	require.NoError(t, err)
	require.NotEmpty(t, points)
	assert.Zero(t, points[0].Start.Unix()%(7*60), "a step that does not divide a day still sits on the epoch's boundaries")

	points, err = QueueDepthHistory(ctx, db, QueueHistoryParams{Queue: defaultQueue, From: base, To: base.Add(24 * time.Hour)})
	require.NoError(t, err)
	require.Len(t, points, MaxHistoryPoints, "a day reads one-minute buckets")
	assert.Equal(t, time.Minute, points[1].Start.Sub(points[0].Start))

	points, err = QueueDepthHistory(ctx, db, QueueHistoryParams{
		Queue: defaultQueue, From: base, To: base.Add(30 * 24 * time.Hour),
	})
	require.NoError(t, err)
	require.Len(t, points, MaxHistoryPoints, "thirty days read thirty-minute buckets")
	assert.Equal(t, 30*time.Minute, points[1].Start.Sub(points[0].Start))

	points, err = QueueDepthHistory(ctx, db, QueueHistoryParams{
		Queue: defaultQueue, From: base.Add(time.Minute + time.Second), To: base.Add(24*time.Hour + time.Minute + time.Second),
	})
	require.NoError(t, err)
	assert.LessOrEqual(t, len(points), MaxHistoryPoints, "an unaligned day takes a wider step rather than 1,441 points")
	assert.Equal(t, 2*time.Minute, points[1].Start.Sub(points[0].Start))

	ago, err := QueueDepthHistory(fixedClockCtx(base.Add(time.Hour)), db, QueueHistoryParams{
		Queue: defaultQueue, From: base, Step: 15 * time.Minute,
	})
	require.NoError(t, err)
	assert.Len(t, ago, 4, "a zero To is the context clock's now")
}

// TestQueueDepthHistoryRefusesAnUnboundedRequest proves each refusal names its
// field before anything is read: the database here has no tables.
func TestQueueDepthHistoryRefusesAnUnboundedRequest(t *testing.T) {
	t.Parallel()
	bare := newBareSQLite(t)
	day := 24 * time.Hour
	for _, tc := range []struct {
		name  string
		p     QueueHistoryParams
		field string
	}{
		{name: "no queue", p: QueueHistoryParams{From: rollupBase, To: rollupBase.Add(time.Hour)}, field: "queue"},
		{name: "no From", p: QueueHistoryParams{Queue: "q", To: rollupBase}, field: "from"},
		{
			name: "To before From", p: QueueHistoryParams{Queue: "q", From: rollupBase, To: rollupBase.Add(-time.Hour)},
			field: "window",
		},
		{name: "To equal to From", p: QueueHistoryParams{Queue: "q", From: rollupBase, To: rollupBase}, field: "window"},
		{name: "a step of seconds", p: QueueHistoryParams{
			Queue: "q", From: rollupBase, To: rollupBase.Add(time.Hour),
			Step: 90 * time.Second,
		}, field: "step"},
		{name: "a negative step", p: QueueHistoryParams{
			Queue: "q", From: rollupBase, To: rollupBase.Add(time.Hour),
			Step: -time.Minute,
		}, field: "step"},
		{name: "too many points", p: QueueHistoryParams{
			Queue: "q", From: rollupBase, To: rollupBase.Add(2 * day),
			Step: time.Minute,
		}, field: "step"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := QueueDepthHistory(context.Background(), bare, tc.p)
			require.ErrorIs(t, err, ErrValidation)
			var ve *ValidationError
			require.True(t, errors.As(err, &ve))
			assert.Equal(t, tc.field, ve.Field)
		})
	}
}
