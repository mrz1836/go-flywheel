package core

import (
	"cmp"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mrz1836/go-foundation/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/datatypes"
	"gorm.io/gorm"
)

// seedAuditEntry writes one job_audit row as given, with a fresh id and system
// actor when they are empty.
func seedAuditEntry(t testing.TB, db *gorm.DB, row jobAuditRow) {
	t.Helper()
	if row.ID == "" {
		row.ID = models.NewID()
	}
	if row.ActorType == "" {
		row.ActorType = string(ActorSystem)
	}
	require.NoError(t, db.Create(&row).Error)
}

// auditBase is the time the read tests' entries are stamped from.
//
//nolint:gochecknoglobals // shared fixture
var auditBase = time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)

// readAllAudit follows NextCursor from p's first page to its last, returning
// the entry ids in the order they were read.
func readAllAudit(t testing.TB, db *gorm.DB, p ListAuditParams) []string {
	t.Helper()
	var ids []string
	for pages := 0; ; pages++ {
		require.Less(t, pages, 20, "the pages end")
		page, err := ListAudit(context.Background(), db, p)
		require.NoError(t, err)
		require.NotNil(t, page.Entries)
		require.LessOrEqual(t, len(page.Entries), p.Limit)
		for _, e := range page.Entries {
			ids = append(ids, e.ID)
		}
		if page.NextCursor == "" {
			return ids
		}
		p.Cursor = page.NextCursor
	}
}

// assertListAuditPagesNeverSkipOrRepeat pages through one job's ten entries,
// seven of which share one created_at, three at a time in both orders: every
// entry appears exactly once, in (created_at, id) order.
func assertListAuditPagesNeverSkipOrRepeat(t *testing.T, db *gorm.DB) {
	t.Helper()
	// Ids out of insertion order, so the tie-break is id order and nothing else.
	ids := []string{"e-07", "e-02", "e-09", "e-04", "e-01", "e-08", "e-03", "e-06", "e-10", "e-05"}
	tie := auditBase.Add(time.Hour)
	type stamped struct {
		id string
		at time.Time
	}
	entries := make([]stamped, len(ids))
	for i, id := range ids {
		at := tie
		if i < 3 {
			at = auditBase.Add(time.Duration(i) * time.Minute)
		}
		entries[i] = stamped{id: id, at: at}
		seedAuditEntry(t, db, jobAuditRow{
			ID: id, CreatedAt: at, TargetType: string(AuditTargetJob), TargetID: "paged-job", Action: string(AuditRetried),
		})
	}
	seedAuditEntry(t, db, jobAuditRow{
		CreatedAt: tie, TargetType: string(AuditTargetJob), TargetID: "another-job", Action: string(AuditRetried),
	})
	slices.SortFunc(entries, func(a, b stamped) int {
		if c := a.at.Compare(b.at); c != 0 {
			return c
		}
		return cmp.Compare(a.id, b.id)
	})
	oldest := make([]string, len(entries))
	for i, e := range entries {
		oldest[i] = e.id
	}
	newest := slices.Clone(oldest)
	slices.Reverse(newest)

	p := ListAuditParams{TargetType: AuditTargetJob, TargetID: "paged-job", Limit: 3}
	assert.Equal(t, newest, readAllAudit(t, db, p), "the zero order is newest first")
	p.Order = AuditNewest
	assert.Equal(t, newest, readAllAudit(t, db, p))
	p.Order = AuditOldest
	assert.Equal(t, oldest, readAllAudit(t, db, p))
}

// TestListAuditPagesNeverSkipOrRepeat proves the keyset paging over (created_at,
// id) on SQLite.
func TestListAuditPagesNeverSkipOrRepeat(t *testing.T) {
	t.Parallel()
	assertListAuditPagesNeverSkipOrRepeat(t, newDB(t))
}

// TestListAuditReadsOneTargetOrTheWholeFeed proves each filter, the limits, and
// an entry's fields as stored.
func TestListAuditReadsOneTargetOrTheWholeFeed(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	ctx := context.Background()
	at := func(minutes int) time.Time { return auditBase.Add(time.Duration(minutes) * time.Minute) }
	job, periodic := string(AuditTargetJob), string(AuditTargetPeriodic)
	seedAuditEntry(t, db, jobAuditRow{
		ID: "a-cancelled", CreatedAt: at(1), TargetType: job, TargetID: "job-a", Action: string(AuditCancelled),
		ActorType: string(ActorUser), ActorID: "u-7", Reason: "stuck", Source: "api", RequestID: "req-1",
		Details: datatypes.JSON(`{"from_state": "available", "was_running": false}`),
	})
	seedAuditEntry(t, db, jobAuditRow{
		ID: "a-retried", CreatedAt: at(2), TargetType: job, TargetID: "job-a", Action: string(AuditRetried),
	})
	seedAuditEntry(t, db, jobAuditRow{
		ID: "a-paused", CreatedAt: at(3), TargetType: job, TargetID: "job-a", Action: string(AuditPaused),
		ActorType: string(ActorAgent), ActorID: "a-1",
	})
	seedAuditEntry(t, db, jobAuditRow{
		ID: "b-cancelled", CreatedAt: at(4), TargetType: job, TargetID: "job-b", Action: string(AuditCancelled),
		ActorType: string(ActorUser), ActorID: "u-8",
	})
	seedAuditEntry(t, db, jobAuditRow{
		ID: "b-replayed", CreatedAt: at(5), TargetType: job, TargetID: "job-b", Action: string(AuditReplayed),
		ActorType: string(ActorUser), ActorID: "u-7",
	})
	seedAuditEntry(t, db, jobAuditRow{
		ID: "n-deactivated", CreatedAt: at(6), TargetType: periodic, TargetID: "nightly", Action: string(AuditDeactivated),
		ActorType: string(ActorUser), ActorID: "u-7",
	})
	read := func(p ListAuditParams) []string {
		t.Helper()
		page, err := ListAudit(ctx, db, p)
		require.NoError(t, err)
		require.NotNil(t, page.Entries)
		ids := make([]string, len(page.Entries))
		for i, e := range page.Entries {
			ids[i] = e.ID
		}
		return ids
	}

	assert.Equal(t, []string{"a-paused", "a-retried", "a-cancelled"},
		read(ListAuditParams{TargetType: AuditTargetJob, TargetID: "job-a"}), "one job's history, newest first")
	assert.Equal(t, []string{"n-deactivated"}, read(ListAuditParams{TargetType: AuditTargetPeriodic}),
		"a target type alone")
	assert.Equal(t, []string{"b-cancelled", "a-retried", "a-cancelled"},
		read(ListAuditParams{Actions: []AuditAction{AuditCancelled, AuditRetried}}), "any of the actions")
	assert.Equal(t, []string{"n-deactivated", "b-replayed", "a-cancelled"},
		read(ListAuditParams{ActorType: ActorUser, ActorID: "u-7"}), "one actor's")
	assert.Equal(t, []string{"a-paused"}, read(ListAuditParams{ActorType: ActorAgent}))
	assert.Equal(t, []string{"a-retried", "a-paused", "b-cancelled"}, read(ListAuditParams{
		Since: at(2).In(time.FixedZone("UTC-5", -5*60*60)),
		Until: at(5).In(time.FixedZone("UTC+3", 3*60*60)),
		Order: AuditOldest,
	}), "[Since, Until), compared in UTC whatever zone the bounds carry")
	assert.Empty(t, read(ListAuditParams{TargetType: AuditTargetJob, TargetID: "nobody"}),
		"a read that matches nothing is an empty, non-nil page")

	page, err := ListAudit(ctx, db, ListAuditParams{TargetType: AuditTargetJob, TargetID: "job-a", Order: AuditOldest, Limit: 1})
	require.NoError(t, err)
	require.Len(t, page.Entries, 1)
	e := page.Entries[0]
	assert.Equal(t, "a-cancelled", e.ID)
	assert.True(t, e.CreatedAt.Equal(at(1)))
	assert.Equal(t, time.UTC, e.CreatedAt.Location())
	assert.Equal(t, AuditTargetJob, e.TargetType)
	assert.Equal(t, "job-a", e.TargetID)
	assert.Equal(t, AuditCancelled, e.Action)
	assert.Equal(t, Actor{Type: ActorUser, ID: "u-7", Reason: "stuck", Source: "api", RequestID: "req-1"}, e.Actor)
	assert.JSONEq(t, `{"from_state": "available", "was_running": false}`, string(e.Details))
	assert.NotEmpty(t, page.NextCursor, "more entries follow")

	page, err = ListAudit(ctx, db, ListAuditParams{TargetType: AuditTargetPeriodic, TargetID: "nightly"})
	require.NoError(t, err)
	require.Len(t, page.Entries, 1)
	assert.JSONEq(t, `{}`, string(page.Entries[0].Details), "an entry stored without details reads {}")
	assert.Empty(t, page.NextCursor, "nothing follows the last entry")

	rows := make([]jobAuditRow, 205)
	for i := range rows {
		rows[i] = jobAuditRow{
			ID: fmt.Sprintf("bulk-%03d", i), CreatedAt: at(100 + i), TargetType: job, TargetID: "bulk-job",
			Action: string(AuditRetried), ActorType: string(ActorSystem), Details: datatypes.JSON(`{}`),
		}
	}
	require.NoError(t, db.CreateInBatches(&rows, 50).Error)
	assert.Len(t, read(ListAuditParams{TargetType: AuditTargetJob, TargetID: "bulk-job"}), 50, "a zero Limit reads 50")
	assert.Len(t, read(ListAuditParams{TargetType: AuditTargetJob, TargetID: "bulk-job", Limit: -1}), 50)
	assert.Len(t, read(ListAuditParams{TargetType: AuditTargetJob, TargetID: "bulk-job", Limit: 500}), MaxReadLimit,
		"a Limit above the cap reads the cap")
}

// auditCursorFor encodes a cursor body as ListAudit's cursors are encoded.
func auditCursorFor(t testing.TB, body map[string]any) string {
	t.Helper()
	raw, err := json.Marshal(body)
	require.NoError(t, err)
	return "au1." + base64.RawURLEncoding.EncodeToString(raw)
}

// TestListAuditRefusesABadCursorOrFilter proves every malformed parameter is
// refused as a *ValidationError naming its field, before any query runs.
func TestListAuditRefusesABadCursorOrFilter(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	ctx := context.Background()
	for i := range 3 {
		seedAuditEntry(t, db, jobAuditRow{
			CreatedAt: auditBase.Add(time.Duration(i) * time.Minute), TargetType: string(AuditTargetJob),
			TargetID: "job-c", Action: string(AuditRetried),
		})
	}
	first, err := ListAudit(ctx, db, ListAuditParams{TargetType: AuditTargetJob, TargetID: "job-c", Limit: 1})
	require.NoError(t, err)
	require.NotEmpty(t, first.NextCursor)
	page, err := ListAudit(ctx, db, ListAuditParams{
		TargetType: AuditTargetJob, TargetID: "job-c", Limit: 1, Cursor: first.NextCursor,
	})
	require.NoError(t, err, "a cursor reads the next page in its own order")
	require.Len(t, page.Entries, 1)

	var queries atomic.Int32
	require.NoError(t, db.Callback().Query().Before("gorm:query").Register("test:count_queries",
		func(*gorm.DB) { queries.Add(1) }))

	at := auditBase.Format(time.RFC3339Nano)
	for _, tc := range []struct {
		name  string
		p     ListAuditParams
		field string
	}{
		{"a cursor from the other order", ListAuditParams{Order: AuditOldest, Cursor: first.NextCursor}, "cursor"},
		{"a string that is not a cursor", ListAuditParams{Cursor: "not-a-cursor"}, "cursor"},
		{"a cursor without its prefix", ListAuditParams{Cursor: first.NextCursor[len("au1."):]}, "cursor"},
		{"a cursor that is not base64", ListAuditParams{Cursor: "au1.!!!"}, "cursor"},
		{"a cursor that is not JSON", ListAuditParams{Cursor: "au1." + base64.RawURLEncoding.EncodeToString([]byte("{"))}, "cursor"},
		{"a cursor with no id", ListAuditParams{Cursor: auditCursorFor(t, map[string]any{"o": "newest", "t": at, "i": ""})}, "cursor"},
		{"a cursor with no time", ListAuditParams{Cursor: auditCursorFor(t, map[string]any{"o": "newest", "i": "x"})}, "cursor"},
		{"an unknown order", ListAuditParams{Order: "sideways"}, "order"},
		{"an unknown target type", ListAuditParams{TargetType: "galaxy"}, "target_type"},
		{"a target id without its type", ListAuditParams{TargetID: "job-c"}, "target_type"},
		{"an unknown actor type", ListAuditParams{ActorType: "robot"}, "actor_type"},
		{"an until equal to since", ListAuditParams{Since: auditBase, Until: auditBase}, "until"},
		{"an until before since", ListAuditParams{Since: auditBase, Until: auditBase.Add(-time.Second)}, "until"},
	} {
		_, err := ListAudit(ctx, db, tc.p)
		var ve *ValidationError
		require.ErrorAs(t, err, &ve, tc.name)
		require.ErrorIs(t, err, ErrValidation, tc.name)
		assert.Equal(t, tc.field, ve.Field, tc.name)
	}
	assert.Zero(t, queries.Load(), "every refusal comes before any query")

	_, err = ListAudit(ctx, nil, ListAuditParams{})
	require.EqualError(t, err, "flywheel: ListAudit: db is nil")
}
