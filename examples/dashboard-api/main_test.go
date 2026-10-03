package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/glebarez/sqlite"
	flywheel "github.com/mrz1836/go-flywheel"
	"github.com/mrz1836/go-foundation/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// testArgs is the job the handler tests enqueue.
type testArgs struct{}

// Kind implements flywheel.Args.
func (testArgs) Kind() string { return "dashboard.test" }

// dbSeq names each test's in-memory database.
//
//nolint:gochecknoglobals // per-test-binary sequence for database names
var dbSeq atomic.Uint64

// newTestDB opens a migrated in-memory SQLite database.
func newTestDB(t *testing.T) *gorm.DB {
	t.Helper()
	dsn := fmt.Sprintf("file:dashboard-api-test-%d?mode=memory&cache=shared", dbSeq.Add(1))
	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	require.NoError(t, err)
	t.Cleanup(func() {
		if sqlDB, err := db.DB(); err == nil {
			_ = sqlDB.Close()
		}
	})
	require.NoError(t, flywheel.Migrate(db))
	return db
}

// get serves one request through the handler and returns the recorder.
func get(t *testing.T, h http.Handler, target string) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequestWithContext(context.Background(), http.MethodGet, target, nil))
	return rec
}

// TestFinishedCursorKeepsAPositiveOffset pages /finished on a database whose
// jobs were written at +02:00, the zone a SQLite jobs row keeps: the cursor is
// the last job's finalized_at exactly as the JSON carried it, and it must work
// whether or not the client URL-encoded its '+'.
func TestFinishedCursorKeepsAPositiveOffset(t *testing.T) {
	t.Parallel()
	db := newTestDB(t)
	at := time.Date(2026, 10, 3, 12, 0, 0, 0, time.FixedZone("CEST", 2*3600))
	ctx := models.WithClock(context.Background(), models.NewFixedClock(at))
	for range 2 {
		id, err := flywheel.Insert(ctx, flywheel.NewClient(db), testArgs{}, flywheel.InsertOpts{})
		require.NoError(t, err)
		require.NoError(t, flywheel.CancelJob(ctx, db, id))
	}
	h := NewHandler(db)

	rec := get(t, h, "/finished")
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	var page []struct {
		ID          string `json:"id"`
		FinalizedAt string `json:"finalized_at"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &page))
	require.Len(t, page, 2)
	first := page[0]
	require.True(t, strings.HasSuffix(first.FinalizedAt, "+02:00"), first.FinalizedAt)

	for name, cursor := range map[string]string{
		"unencoded": first.FinalizedAt + "," + first.ID,
		"encoded":   strings.ReplaceAll(first.FinalizedAt, "+", "%2B") + "," + first.ID,
	} {
		rec := get(t, h, "/finished?before="+cursor)
		require.Equal(t, http.StatusOK, rec.Code, "%s: %s", name, rec.Body.String())
		var next []struct {
			ID string `json:"id"`
		}
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &next))
		require.Len(t, next, 1, name)
		assert.Equal(t, page[1].ID, next[0].ID, "%s: the cursor continues after the first job", name)
	}
}

// TestHandlerRejectsMalformedCursors answers a caller's malformed cursor with a
// 400 naming the expected shape, never a 500.
func TestHandlerRejectsMalformedCursors(t *testing.T) {
	t.Parallel()
	h := NewHandler(newTestDB(t))
	for _, target := range []string{
		"/finished?before=yesterday,abc",
		"/finished?before=2026-10-03T12:00:00Z",
		"/finished?before=2026-10-03T12:00:00Z,",
		"/finished?state=running",
		"/jobs/abc/runs?before_attempt=0",
		"/jobs/abc/runs?before_attempt=two",
		"/stats?since=-1h",
	} {
		rec := get(t, h, target)
		assert.Equal(t, http.StatusBadRequest, rec.Code, "%s: %s", target, rec.Body.String())
	}
	rec := get(t, h, "/jobs/abc/runs?before_attempt=2")
	assert.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
}
