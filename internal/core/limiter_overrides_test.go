package core

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/mrz1836/go-foundation/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// limiterBase is the fixed instant the limiter-override tests start from.
//
//nolint:gochecknoglobals // shared fixture
var limiterBase = time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)

// atLimiterTime is a context whose clock reads limiterBase plus d.
func atLimiterTime(d time.Duration) context.Context {
	return clockCtx(context.Background(), models.NewFixedClock(limiterBase.Add(d)))
}

// newTestDBLimiter builds a DBLimiter, failing the test on a refused config.
func newTestDBLimiter(t *testing.T, db *gorm.DB, cfg DBLimiterConfig) *DBLimiter {
	t.Helper()
	l, err := NewDBLimiter(db, cfg)
	require.NoError(t, err)
	return l
}

// heldPermits sums the permits resource's holds take, expired or not.
func heldPermits(t *testing.T, db *gorm.DB, resource string) int64 {
	t.Helper()
	var held int64
	require.NoError(t, db.Model(&limiterHoldRow{}).Where("resource = ?", resource).
		Select("COALESCE(SUM(n), 0)").Scan(&held).Error)
	return held
}

// limiterOverrideSuite proves each override replaces its configured value on
// the next Acquire: a lower concurrency cap, which never revokes a hold; a
// lower rate and burst; and, cleared, the configured values again. db carries
// the current schema.
func limiterOverrideSuite(t *testing.T, db *gorm.DB) {
	t.Helper()
	two, one := 2, 1

	// A concurrency cap of 4, lowered to 2.
	capped := newTestDBLimiter(t, db, DBLimiterConfig{MaxConcurrent: 4, HoldTTL: time.Minute})
	seed, err := capped.Acquire(atLimiterTime(0), "r", 1)
	require.NoError(t, err)
	capped.Release(atLimiterTime(0), seed)
	require.NoError(t, SetLimiterBudget(atLimiterTime(0), db, "r", LimiterBudget{MaxConcurrent: &two}))
	g, err := capped.Acquire(atLimiterTime(0), "r", 4)
	require.NoError(t, err)
	assert.Equal(t, 2, g.N, "the override caps the next Acquire")

	// Lowered below what is held: nothing new until holds fall below it, and the
	// two held permits stay held.
	require.NoError(t, SetLimiterBudget(atLimiterTime(0), db, "r", LimiterBudget{MaxConcurrent: &one}))
	denied, err := capped.Acquire(atLimiterTime(0), "r", 1)
	require.NoError(t, err)
	assert.Zero(t, denied.N, "a cap below what is held grants nothing")
	assert.EqualValues(t, 2, heldPermits(t, db, "r"), "and revokes nothing")
	capped.Release(atLimiterTime(0), g)
	after, err := capped.Acquire(atLimiterTime(0), "r", 4)
	require.NoError(t, err)
	assert.Equal(t, 1, after.N, "once the holds are released it grants up to the override")
	capped.Release(atLimiterTime(0), after)

	// Cleared: the configured cap applies on the next Acquire.
	require.NoError(t, SetLimiterBudget(atLimiterTime(0), db, "r", LimiterBudget{}))
	cleared, err := capped.Acquire(atLimiterTime(0), "r", 4)
	require.NoError(t, err)
	assert.Equal(t, 4, cleared.N, "the configured cap is back")

	// A rate of 10 per second with a burst of 10, lowered to 2 and 2.
	rated := newTestDBLimiter(t, db, DBLimiterConfig{Rate: 10, Interval: time.Second, Burst: 10})
	first, err := rated.Acquire(atLimiterTime(0), "rated", 1)
	require.NoError(t, err)
	require.Equal(t, 1, first.N)
	require.NoError(t, SetLimiterBudget(atLimiterTime(0), db, "rated", LimiterBudget{Rate: &two, Burst: &two}))
	g, err = rated.Acquire(atLimiterTime(time.Second), "rated", 10)
	require.NoError(t, err)
	assert.Equal(t, 2, g.N, "a second later the lower rate and burst grant 2, where 10 would have been granted")
}

// TestALimiterOverrideAppliesOnTheNextAcquire runs the suite on SQLite, then
// proves an override applies only where the limiter reads its row for it.
func TestALimiterOverrideAppliesOnTheNextAcquire(t *testing.T) {
	t.Parallel()

	t.Run("each override replaces its configured value", func(t *testing.T) {
		t.Parallel()
		limiterOverrideSuite(t, newDB(t))
	})

	t.Run("an override needs the setting its cap rests on", func(t *testing.T) {
		t.Parallel()
		db := newDB(t)
		one := 1
		// A rate override on a limiter with no Interval changes nothing.
		noInterval := newTestDBLimiter(t, db, DBLimiterConfig{MaxConcurrent: 3, HoldTTL: time.Minute})
		g, err := noInterval.Acquire(atLimiterTime(0), "no-interval", 1)
		require.NoError(t, err)
		noInterval.Release(atLimiterTime(0), g)
		require.NoError(t, SetLimiterBudget(atLimiterTime(0), db, "no-interval", LimiterBudget{Rate: &one}))
		g, err = noInterval.Acquire(atLimiterTime(0), "no-interval", 3)
		require.NoError(t, err)
		assert.Equal(t, 3, g.N, "without an Interval a rate override is not applied")

		// A concurrency override on a limiter with no HoldTTL changes nothing.
		noTTL := newTestDBLimiter(t, db, DBLimiterConfig{Rate: 5, Interval: time.Second})
		_, err = noTTL.Acquire(atLimiterTime(0), "no-ttl", 1)
		require.NoError(t, err)
		require.NoError(t, SetLimiterBudget(atLimiterTime(0), db, "no-ttl", LimiterBudget{MaxConcurrent: &one}))
		g, err = noTTL.Acquire(atLimiterTime(time.Minute), "no-ttl", 5)
		require.NoError(t, err)
		assert.Equal(t, 5, g.N, "without a HoldTTL a concurrency override is not applied")
	})

	t.Run("an override can turn a cap on", func(t *testing.T) {
		t.Parallel()
		db := newDB(t)
		two := 2
		// A HoldTTL with no cap reads its row on each Acquire, so an override
		// caps it.
		uncapped := newTestDBLimiter(t, db, DBLimiterConfig{HoldTTL: time.Minute})
		g, err := uncapped.Acquire(atLimiterTime(0), "ttl-only", 5)
		require.NoError(t, err)
		assert.Equal(t, 5, g.N, "uncapped, it grants the ask")
		require.NoError(t, SetLimiterBudget(atLimiterTime(0), db, "ttl-only", LimiterBudget{MaxConcurrent: &two}))
		g, err = uncapped.Acquire(atLimiterTime(0), "ttl-only", 5)
		require.NoError(t, err)
		assert.Equal(t, 2, g.N, "the override turns the concurrency cap on")
		assert.NotEmpty(t, g.Token, "and holds what it grants")

		// A limiter with neither cap nor Interval nor HoldTTL never reads a row.
		free := newTestDBLimiter(t, db, DBLimiterConfig{})
		g, err = free.Acquire(atLimiterTime(0), "ttl-only", 5)
		require.NoError(t, err)
		assert.Equal(t, 5, g.N, "it admits without a round trip, override or not")
	})
}

// TestSetLimiterBudgetRefusesBadInputBeforeWriting proves each refusal names
// its field and writes nothing.
func TestSetLimiterBudgetRefusesBadInputBeforeWriting(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	limiter := newTestDBLimiter(t, db, DBLimiterConfig{MaxConcurrent: 4, HoldTTL: time.Minute})
	_, err := limiter.Acquire(atLimiterTime(0), "r", 1)
	require.NoError(t, err)
	zero, negative, ok := 0, -1, 3

	for _, tc := range []struct {
		name     string
		ctx      context.Context
		resource string
		budget   LimiterBudget
		field    string
	}{
		{"an empty resource", atLimiterTime(0), "", LimiterBudget{MaxConcurrent: &ok}, "resource"},
		{"a concurrency cap of zero", atLimiterTime(0), "r", LimiterBudget{MaxConcurrent: &zero}, "max_concurrent"},
		{"a negative rate", atLimiterTime(0), "r", LimiterBudget{Rate: &negative}, "rate"},
		{"a burst of zero", atLimiterTime(0), "r", LimiterBudget{Burst: &zero}, "burst"},
		{"a resource no limiter has admitted against", atLimiterTime(0), "typo", LimiterBudget{MaxConcurrent: &ok}, "resource"},
		{
			"an actor the history can't record",
			WithActor(atLimiterTime(0), Actor{Type: "robot", ID: "x"}), "r",
			LimiterBudget{MaxConcurrent: &ok},
			"actor.type",
		},
	} {
		err := SetLimiterBudget(tc.ctx, db, tc.resource, tc.budget)
		var verr *ValidationError
		require.ErrorAs(t, err, &verr, tc.name)
		assert.Equal(t, tc.field, verr.Field, tc.name)
	}

	infos, err := ListLimiters(atLimiterTime(0), db)
	require.NoError(t, err)
	require.Len(t, infos, 1, "no refusal created a bucket")
	assert.Equal(t, LimiterBudget{}, infos[0].Overrides, "and none wrote an override")
	assert.Empty(t, auditEntriesFor(t, db, AuditTargetLimiter, "r"), "or an entry")
}

// auditEntriesFor reads target's control history.
func auditEntriesFor(t *testing.T, db *gorm.DB, target AuditTarget, id string) []AuditEntry {
	t.Helper()
	page, err := ListAudit(context.Background(), db, ListAuditParams{TargetType: target, TargetID: id})
	require.NoError(t, err)
	return page.Entries
}

// TestSetLimiterBudgetRecordsOneEntryPerChange proves the control history: one
// limits_set entry per change, with the overrides before and after, none for a
// call that changes nothing, and a hook that fails rolls the change back.
func TestSetLimiterBudgetRecordsOneEntryPerChange(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	limiter := newTestDBLimiter(t, db, DBLimiterConfig{MaxConcurrent: 4, HoldTTL: time.Minute})
	_, err := limiter.Acquire(atLimiterTime(0), "vendor", 1)
	require.NoError(t, err)
	operator := WithActor(atLimiterTime(0), Actor{Type: ActorUser, ID: "operator", Reason: "the vendor throttles us"})
	two, five := 2, 5

	require.NoError(t, SetLimiterBudget(operator, db, "vendor", LimiterBudget{MaxConcurrent: &two}))
	entries := auditEntriesFor(t, db, AuditTargetLimiter, "vendor")
	require.Len(t, entries, 1)
	assert.Equal(t, AuditLimitsSet, entries[0].Action)
	assert.Equal(t, "operator", entries[0].Actor.ID)
	assert.Equal(t, "the vendor throttles us", entries[0].Actor.Reason)
	assert.JSONEq(t, `{"from":{},"to":{"max_concurrent":2}}`, string(entries[0].Details))

	require.NoError(t, SetLimiterBudget(operator, db, "vendor", LimiterBudget{MaxConcurrent: &two}))
	assert.Len(t, auditEntriesFor(t, db, AuditTargetLimiter, "vendor"), 1, "the same budget again records nothing")

	require.NoError(t, SetLimiterBudget(operator, db, "vendor", LimiterBudget{Rate: &five}))
	entries = auditEntriesFor(t, db, AuditTargetLimiter, "vendor")
	require.Len(t, entries, 2)
	var details map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(entries[0].Details, &details))
	assert.JSONEq(t, `{"max_concurrent":2}`, string(details["from"]), "the newest entry holds the overrides before")
	assert.JSONEq(t, `{"rate":5}`, string(details["to"]), "and after")

	errHook := errors.New("the host's audit log is down")
	failing := WithAuditHook(operator, func(context.Context, *gorm.DB, []AuditEntry) error { return errHook })
	err = SetLimiterBudget(failing, db, "vendor", LimiterBudget{})
	require.ErrorIs(t, err, errHook)
	infos, err := ListLimiters(atLimiterTime(0), db)
	require.NoError(t, err)
	require.Len(t, infos, 1)
	require.NotNil(t, infos[0].Overrides.Rate, "the failed hook rolled the change back")
	assert.Equal(t, 5, *infos[0].Overrides.Rate)
	assert.Len(t, auditEntriesFor(t, db, AuditTargetLimiter, "vendor"), 2, "with its entry")
}

// TestListLimitersReportsTheOverrides proves each bucket's overrides read back,
// and a bucket with none reads none.
func TestListLimitersReportsTheOverrides(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	limiter := newTestDBLimiter(t, db, DBLimiterConfig{MaxConcurrent: 4, HoldTTL: time.Minute})
	for _, resource := range []string{"other", "r"} {
		g, err := limiter.Acquire(atLimiterTime(0), resource, 1)
		require.NoError(t, err)
		limiter.Release(atLimiterTime(0), g)
	}
	two, three, seven := 2, 3, 7
	require.NoError(t, SetLimiterBudget(atLimiterTime(0), db, "r",
		LimiterBudget{MaxConcurrent: &two, Rate: &three, Burst: &seven}))

	infos, err := ListLimiters(atLimiterTime(0), db)
	require.NoError(t, err)
	require.Len(t, infos, 2)
	assert.Equal(t, "other", infos[0].Resource)
	assert.Equal(t, LimiterBudget{}, infos[0].Overrides, "a resource with no override reads none")
	assert.Equal(t, "r", infos[1].Resource)
	assert.Equal(t, LimiterBudget{MaxConcurrent: &two, Rate: &three, Burst: &seven}, infos[1].Overrides)
	raw, err := json.Marshal(infos[0])
	require.NoError(t, err)
	assert.Contains(t, string(raw), `"overrides":{}`, "the field is always present")
}
