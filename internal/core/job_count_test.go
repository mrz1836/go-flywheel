package core

import (
	"context"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// seedKindRows writes n available jobs of kind in one statement, their ids
// prefix-0 through prefix-(n-1).
func seedKindRows(t *testing.T, db *gorm.DB, kind, prefix string, n int) {
	t.Helper()
	at := time.Now().UTC().Add(-time.Hour)
	if db.Name() == "postgres" {
		require.NoError(t, db.Exec(`
			INSERT INTO jobs (id, created_at, updated_at, metadata, kind, queue, args, priority,
			                  state, attempt, max_attempts, scheduled_at, executor_class, tags)
			SELECT ? || '-' || g, ?, ?, '{}'::jsonb, ?, 'default', '{}'::jsonb, 100,
			       'available', 0, 25, ?, '', '[]'::jsonb
			FROM generate_series(0, ?) AS g`,
			prefix, at, at, kind, at, n-1).Error)
		return
	}
	require.NoError(t, db.Exec(`
		WITH RECURSIVE g(n) AS (SELECT 0 UNION ALL SELECT n + 1 FROM g WHERE n < ?)
		INSERT INTO jobs (id, created_at, updated_at, metadata, kind, queue, args, priority,
		                  state, attempt, max_attempts, scheduled_at, executor_class, tags)
		SELECT ? || '-' || n, ?, ?, '{}', ?, 'default', '{}', 100, 'available', 0, 25, ?, '', '[]'
		FROM g`,
		n-1, prefix, at, at, kind, at).Error)
}

// countStatementLimit matches the LIMIT a count's capped sub-select carries.
//
//nolint:gochecknoglobals // compiled once
var countStatementLimit = regexp.MustCompile(`(?i)\bLIMIT (\d+)\b`)

// countOnce runs CountJobsWithOptions and returns its result and the LIMIT its
// count statement read under, taken from the statement itself: a count without
// the bound returns the same numbers, so only the statement tells them apart.
func countOnce(t *testing.T, db *gorm.DB, q JobQuery, opts CountOpts) (JobCount, string) {
	t.Helper()
	var got JobCount
	stmts := captureSQL(t, db, func(db *gorm.DB) {
		var err error
		got, err = CountJobsWithOptions(context.Background(), db, q, opts)
		require.NoError(t, err)
	})
	for _, s := range stmts {
		if strings.Contains(strings.ToLower(s), "count(") {
			m := countStatementLimit.FindStringSubmatch(s)
			require.NotNil(t, m, "the count reads a bounded sub-select: %s", s)
			return got, m[1]
		}
	}
	t.Fatalf("no count statement among %v", stmts)
	return got, ""
}

// countSuite proves CountJobs is exact up to its cap, then reports the cap and,
// on PostgreSQL, the planner's estimate, never reading past cap + 1 rows.
func countSuite(t *testing.T, open dbOpener) {
	t.Helper()
	db := open(t)
	seedKindRows(t, db, "count.big", "big", 10_000)
	seedKindRows(t, db, "count.small", "small", 6)
	big := JobQuery{Kinds: []string{"count.big"}}
	small := JobQuery{Kinds: []string{"count.small"}}
	postgres := db.Name() == "postgres"

	got, limit := countOnce(t, db, big, CountOpts{})
	assert.Equal(t, JobCount{N: 10_000}, got, "a count of exactly the cap is exact")
	assert.Equal(t, "10001", limit)

	plain, err := CountJobs(context.Background(), db, big)
	require.NoError(t, err)
	assert.Equal(t, got, plain, "CountJobs is the zero-options call")

	seedKindRows(t, db, "count.big", "big-extra", 1)
	got, limit = countOnce(t, db, big, CountOpts{})
	assert.Equal(t, int64(10_000), got.N)
	assert.True(t, got.Capped)
	assert.Equal(t, "10001", limit)
	if postgres {
		assert.GreaterOrEqual(t, got.Estimate, int64(10_001), "the estimate is at least the cap plus one")
	} else {
		assert.Zero(t, got.Estimate, "SQLite has no estimate")
	}

	got, limit = countOnce(t, db, small, CountOpts{Cap: 5})
	assert.Equal(t, int64(5), got.N)
	assert.True(t, got.Capped)
	assert.Equal(t, "6", limit)
	if postgres {
		assert.GreaterOrEqual(t, got.Estimate, int64(6))

		// Past a small cap the estimate is the planner's, not the cap plus one; the
		// planner estimates from the table's statistics, which ANALYZE gathers.
		require.NoError(t, db.Exec(`ANALYZE jobs`).Error)
		got, _ = countOnce(t, db, big, CountOpts{Cap: 5})
		assert.Equal(t, int64(5), got.N)
		assert.Greater(t, got.Estimate, int64(1_000), "the planner's estimate of about 10,001 rows")
	}

	got, limit = countOnce(t, db, small, CountOpts{Cap: 6})
	assert.Equal(t, JobCount{N: 6}, got)
	assert.Equal(t, "7", limit)

	for _, c := range []int64{0, -1} {
		got, limit = countOnce(t, db, small, CountOpts{Cap: c})
		assert.Equal(t, JobCount{N: 6}, got, "Cap %d", c)
		assert.Equal(t, "10001", limit, "Cap %d selects the default", c)
	}

	got, limit = countOnce(t, db, big, CountOpts{Cap: 200_000})
	assert.Equal(t, JobCount{N: 10_001}, got)
	assert.Equal(t, "100001", limit, "a cap above MaxCountCap counts to it")

	paged := JobQuery{
		Kinds: []string{"count.small"}, Sort: JobSortFinished, Cursor: "jq1.not-a-cursor", Limit: 3,
	}
	got, _ = countOnce(t, db, paged, CountOpts{})
	assert.Equal(t, JobCount{N: 6}, got, "a count ignores the sort, the cursor, and the limit")

	assert.Equal(t, 10_000, DefaultCountCap)
	assert.Equal(t, 100_000, MaxCountCap)
}

// TestCountJobsIsExactToTheCapThenEstimates runs the count suite on SQLite.
func TestCountJobsIsExactToTheCapThenEstimates(t *testing.T) {
	t.Parallel()
	countSuite(t, sqliteOpener)
}

// listAllPages lists every job q matches, two at a time, following each
// NextCursor, and returns their fixture keys in order.
func listAllPages(t *testing.T, db *gorm.DB, ids queryIDs, q JobQuery) []string {
	t.Helper()
	out := []string{}
	q.Limit = 2
	for range len(ids.id) + 1 {
		page, err := QueryJobs(context.Background(), db, q)
		require.NoError(t, err)
		out = append(out, ids.keys(t, page)...)
		if page.NextCursor == "" {
			return out
		}
		q.Cursor = page.NextCursor
	}
	t.Fatal("the pages never ended")
	return nil
}

// requireCountsWhatItLists asserts, for each case, that QueryJobs lists the
// case's jobs across its pages, read two at a time, and that CountJobs counts
// exactly them.
func requireCountsWhatItLists(t *testing.T, db *gorm.DB, ids queryIDs, cases []queryFilterCase) {
	t.Helper()
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			q := c.q(ids)
			listed := listAllPages(t, db, ids, q)
			assert.Equal(t, c.want, listed)
			n, err := CountJobs(context.Background(), db, q)
			require.NoError(t, err)
			assert.Equal(t, JobCount{N: int64(len(listed))}, n)
		})
	}
}

// TestLatestRunFiltersCountWhatTheyList proves CountJobs applies the latest-run
// filters exactly as QueryJobs does: for every case, it counts the jobs the
// query lists across its pages.
func TestLatestRunFiltersCountWhatTheyList(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	requireCountsWhatItLists(t, db, seedLatestRunFixture(t, db), latestRunFilterCases())
}
