package core

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// storedRunText reads a run's three timestamps as SQLite holds them.
func storedRunText(t *testing.T, db *gorm.DB, id string) (string, string, string) {
	t.Helper()
	var s struct {
		StartedAt  string
		FinishedAt *string
		CreatedAt  string
	}
	require.NoError(t, db.Raw(`SELECT CAST(started_at AS TEXT) AS started_at, CAST(finished_at AS TEXT) AS finished_at,
		CAST(created_at AS TEXT) AS created_at FROM job_runs WHERE id = ?`, id).Scan(&s).Error)
	fin := ""
	if s.FinishedAt != nil {
		fin = *s.FinishedAt
	}
	return s.StartedAt, fin, s.CreatedAt
}

// TestNormalizeRunTimestampsRestampsLegacyRowsSQLite proves the one-shot
// re-stamp: rows an older release wrote in a DST zone — on both sides of a
// change, so two different offsets — come out in UTC, as the same instants; a
// re-run changes nothing; and a window across the change selects correctly
// afterwards, where the raw text would not have.
func TestNormalizeRunTimestampsRestampsLegacyRowsSQLite(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	ctx := context.Background()
	ny, err := time.LoadLocation("America/New_York")
	require.NoError(t, err)
	// 2026-11-01 01:30 EDT and 01:10 EST — the second is 40 minutes *later*, but
	// its local text sorts first.
	edt := time.Date(2026, 11, 1, 1, 30, 0, 0, ny)
	est := edt.Add(40 * time.Minute)
	require.Equal(t, 1, est.Hour())
	require.Equal(t, 10, est.Minute())
	writeLegacyHistory(t, db, "edt", "k", StateSucceeded, edt)
	writeLegacyHistory(t, db, "est", "k", StateSucceeded, est)
	unfinished := edt.Add(-time.Hour)
	require.NoError(t, db.Create(&legacyJobRunRow{
		ID: "open-run", JobID: "edt", Attempt: 2, ExecutorID: "old", StartedAt: unfinished,
		Outcome: string(OutcomeStarted), CreatedAt: unfinished,
	}).Error)

	start, _, _ := storedRunText(t, db, "edt-run")
	assert.Regexp(t, `-04:00$`, start, "the fixture stores the old local stamp")

	n, err := NormalizeRunTimestamps(ctx, db)
	require.NoError(t, err)
	assert.EqualValues(t, 3, n)

	for _, id := range []string{"edt-run", "est-run", "open-run"} {
		s, f, c := storedRunText(t, db, id)
		assert.Regexp(t, `\+00:00$`, s, id)
		assert.Regexp(t, `\+00:00$`, c, id)
		if f != "" {
			assert.Regexp(t, `\+00:00$`, f, id)
		}
	}
	_, f, _ := storedRunText(t, db, "open-run")
	assert.Empty(t, f, "a NULL finished_at stays NULL")
	var row jobRunRow
	require.NoError(t, db.Where("id = ?", "est-run").First(&row).Error)
	require.NotNil(t, row.FinishedAt)
	assert.True(t, row.FinishedAt.Equal(est), "the instant is unchanged; only its spelling is")

	again, err := NormalizeRunTimestamps(ctx, db)
	require.NoError(t, err)
	assert.Zero(t, again, "idempotent")

	_, err = BackfillRunFinishes(ctx, db)
	require.NoError(t, err)

	res, err := Stats(ctx, db, StatsParams{From: edt.Add(10 * time.Minute), To: est.Add(time.Minute)})
	require.NoError(t, err)
	assert.EqualValues(t, 1, res.Total.Attempts,
		"only the EST run is in a window that starts after the EDT one — as instants, not as text")
}

// TestNormalizeKeepsAFreshUTCValueSQLite pins the concurrent-finalize guard: a
// column already in UTC when the write lands is left as it is, so a finalize
// that stamped it between the batch's read and its write is never overwritten
// with the stale value the batch read.
func TestNormalizeKeepsAFreshUTCValueSQLite(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	ny, err := time.LoadLocation("America/New_York")
	require.NoError(t, err)
	local := time.Date(2026, 9, 1, 9, 0, 0, 0, ny)
	require.NoError(t, db.Create(&legacyJobRunRow{
		ID: "r", JobID: "j", Attempt: 1, ExecutorID: "old", StartedAt: local, Outcome: string(OutcomeStarted),
		CreatedAt: local,
	}).Error)
	fresh := time.Date(2026, 9, 1, 13, 30, 0, 0, time.UTC)
	require.NoError(t, db.Exec(`UPDATE job_runs SET finished_at = ? WHERE id = 'r'`, fresh).Error)

	_, err = NormalizeRunTimestamps(context.Background(), db)
	require.NoError(t, err)
	_, f, _ := storedRunText(t, db, "r")
	assert.Equal(t, "2026-09-01 13:30:00+00:00", f)
}

// TestRollupNormalizesOnceSQLite proves the upgrade needs no manual step: the
// first rollup pass of a Scheduler re-stamps legacy rows before it reads them.
func TestRollupNormalizesOnceSQLite(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	ny, err := time.LoadLocation("America/New_York")
	require.NoError(t, err)
	at := time.Date(2026, 9, 1, 9, 0, 0, 0, ny)
	writeLegacyHistory(t, db, "old", "k", StateSucceeded, at)
	s := newSchedulerCfg(t, SchedulerConfig{DB: db, Client: NewClient(db), StatsRollupInterval: time.Minute})

	_, err = s.RollupStats(fixedClockCtx(at.Add(3 * time.Hour)))
	require.NoError(t, err)
	start, _, _ := storedRunText(t, db, "old-run")
	assert.Regexp(t, `\+00:00$`, start)
	assert.EqualValues(t, 1, s.stats.backfills.Load(), "the first pass backfilled, which normalizes first")
}
