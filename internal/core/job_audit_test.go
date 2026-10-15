package core

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mrz1836/go-foundation/ctxutil"
	"github.com/mrz1836/go-foundation/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// auditRows reads targetID's job_audit rows straight from the table, oldest
// first, so the control tests prove the writer without depending on ListAudit.
func auditRows(t testing.TB, db *gorm.DB, targetID string) []jobAuditRow {
	t.Helper()
	var rows []jobAuditRow
	require.NoError(t, db.Where("target_id = ?", targetID).Order("created_at, id").Find(&rows).Error)
	return rows
}

// onlyAuditRow reads targetID's one job_audit row, failing unless there is
// exactly one.
func onlyAuditRow(t testing.TB, db *gorm.DB, targetID string) jobAuditRow {
	t.Helper()
	rows := auditRows(t, db, targetID)
	require.Len(t, rows, 1, "one entry for %s", targetID)
	return rows[0]
}

// auditDetails decodes an entry's details.
func auditDetails(t testing.TB, row jobAuditRow) map[string]any {
	t.Helper()
	var details map[string]any
	require.NoError(t, json.Unmarshal(row.Details, &details))
	return details
}

// retryKeys is the details a retry or replay records for opts, beside from_state.
func retryKeys(fromState JobState, force, allowRunning, resetAttempts bool, budget, delayMs float64) map[string]any {
	return map[string]any{
		"from_state": string(fromState), "force": force, "allow_running": allowRunning,
		"reset_attempts": resetAttempts, "budget": budget, "delay_ms": delayMs,
	}
}

// withKeys returns a copy of base with extra's keys added.
func withKeys(base, extra map[string]any) map[string]any {
	out := make(map[string]any, len(base)+len(extra))
	for k, v := range base {
		out[k] = v
	}
	for k, v := range extra {
		out[k] = v
	}
	return out
}

// seedScopedFamily writes parent's children for the scoped controls: three in
// state, one running, and one succeeded.
func seedScopedFamily(t testing.TB, db *gorm.DB, parent string, state JobState) {
	t.Helper()
	at := time.Now().UTC()
	for _, id := range []string{"-1", "-2", "-3"} {
		seedChild(t, db, parent+id, parent, "leaf", state, at)
	}
	seedChild(t, db, parent+"-running", parent, "leaf", StateRunning, at)
	seedChild(t, db, parent+"-done", parent, "leaf", StateSucceeded, at)
}

// assertScopedEntries asserts that each of parent's three changed children has
// one entry with action and details, and its running and succeeded children
// have none.
func assertScopedEntries(t *testing.T, db *gorm.DB, parent string, action AuditAction, details map[string]any) {
	t.Helper()
	for _, id := range []string{"-1", "-2", "-3"} {
		row := onlyAuditRow(t, db, parent+id)
		assert.Equal(t, string(AuditTargetJob), row.TargetType)
		assert.Equal(t, string(action), row.Action)
		assert.Equal(t, details, auditDetails(t, row), "%s%s", parent, id)
	}
	assert.Empty(t, auditRows(t, db, parent+"-running"), "a running child is left alone and gets no entry")
	assert.Empty(t, auditRows(t, db, parent+"-done"), "a terminal child is left alone and gets no entry")
}

// auditOperator is the actor the control tests run under.
//
//nolint:gochecknoglobals // shared fixture
var auditOperator = Actor{
	Type: ActorUser, ID: "u-7", Reason: "clear the outage backlog", Source: "api", RequestID: "req-1",
}

// assertEveryControlRecordsOneEntryPerChangedJob runs every control under an
// actor and a fixed clock and checks the entries each writes, one subtest per
// control. The subtests share db and seed their own rows.
func assertEveryControlRecordsOneEntryPerChangedJob(t *testing.T, db *gorm.DB) {
	t.Helper()
	// Five hours behind UTC, so an entry stamped in UTC is told apart.
	at := time.Date(2026, 1, 1, 7, 0, 0, 0, time.FixedZone("UTC-5", -5*60*60))
	ctx := clockCtx(WithActor(context.Background(), auditOperator), models.NewFixedClock(at))
	now := time.Now().UTC()

	t.Run("RetryJob", func(t *testing.T) {
		seedJob(t, db, jobRow{ID: "retry-1", Kind: "leaf", State: string(StateRetryable), ScheduledAt: now})
		require.NoError(t, RetryJob(ctx, db, "retry-1"))
		row := onlyAuditRow(t, db, "retry-1")
		assert.Equal(t, string(AuditRetried), row.Action)
		assert.Equal(t, retryKeys(StateRetryable, false, false, false, 0, 0), auditDetails(t, row))
	})

	t.Run("RetryJobWithOptions", func(t *testing.T) {
		seedJob(t, db, jobRow{ID: "retry-running", Kind: "leaf", State: string(StateRunning), Attempt: 1, ScheduledAt: now})
		require.NoError(t, RetryJobWithOptions(ctx, db, "retry-running", RetryOpts{AllowRunning: true}))
		row := onlyAuditRow(t, db, "retry-running")
		assert.Equal(t, string(AuditRetried), row.Action)
		assert.Equal(t, retryKeys(StateRunning, false, true, false, 0, 0), auditDetails(t, row))

		fin := now.Add(-time.Hour)
		seedJob(t, db, jobRow{
			ID: "retry-discarded", Kind: "leaf", State: string(StateDiscarded),
			Attempt: 3, MaxAttempts: 3, ScheduledAt: now, FinalizedAt: &fin,
		})
		require.NoError(t, RetryJobWithOptions(ctx, db, "retry-discarded", RetryOpts{
			Force: true, ResetAttempts: true, Budget: 3, Delay: 90 * time.Second,
		}))
		row = onlyAuditRow(t, db, "retry-discarded")
		assert.Equal(t, retryKeys(StateDiscarded, true, false, true, 3, 90000), auditDetails(t, row))
	})

	t.Run("CancelJob", func(t *testing.T) {
		seedJob(t, db, jobRow{ID: "cancel-1", Kind: "leaf", State: string(StateAvailable), ScheduledAt: now})
		require.NoError(t, CancelJob(ctx, db, "cancel-1"))
		row := onlyAuditRow(t, db, "cancel-1")
		assert.Equal(t, string(AuditTargetJob), row.TargetType)
		assert.Equal(t, "cancel-1", row.TargetID)
		assert.Equal(t, string(AuditCancelled), row.Action)
		assert.Equal(t, auditOperator, Actor{
			Type: ActorType(row.ActorType), ID: row.ActorID, Reason: row.Reason, Source: row.Source, RequestID: row.RequestID,
		}, "the actor's five values, exactly")
		assert.True(t, row.CreatedAt.Equal(at), "stamped with the control's clock reading: %s", row.CreatedAt)
		if db.Name() == "sqlite" {
			_, offset := row.CreatedAt.Zone()
			assert.Zero(t, offset, "stored in UTC, so SQLite's text comparison stays a time comparison")
		}
		assert.Equal(t, map[string]any{"from_state": "available", "was_running": false}, auditDetails(t, row))

		seedJob(t, db, jobRow{ID: "cancel-running", Kind: "leaf", State: string(StateRunning), Attempt: 1, ScheduledAt: now})
		require.NoError(t, CancelJob(ctx, db, "cancel-running"))
		row = onlyAuditRow(t, db, "cancel-running")
		assert.Equal(t, map[string]any{"from_state": "running", "was_running": true}, auditDetails(t, row))
	})

	scoped := map[string]any{"via": "parent"}
	t.Run("PauseByParent", func(t *testing.T) {
		seedScopedFamily(t, db, "pause-p", StateAvailable)
		res, err := PauseByParent(ctx, db, "pause-p", ScopeOpts{BatchSize: 2})
		require.NoError(t, err)
		assert.EqualValues(t, 3, res.Changed)
		assertScopedEntries(t, db, "pause-p", AuditPaused,
			withKeys(scoped, map[string]any{"from_state": "available", "parent_job_id": "pause-p"}))
	})

	t.Run("ResumeByParent", func(t *testing.T) {
		seedScopedFamily(t, db, "resume-p", StatePaused)
		res, err := ResumeByParent(ctx, db, "resume-p", ScopeOpts{BatchSize: 2})
		require.NoError(t, err)
		assert.EqualValues(t, 3, res.Changed)
		assertScopedEntries(t, db, "resume-p", AuditResumed,
			withKeys(scoped, map[string]any{"from_state": "paused", "parent_job_id": "resume-p"}))
	})

	t.Run("CancelByParent", func(t *testing.T) {
		seedScopedFamily(t, db, "cancel-p", StateAvailable)
		res, err := CancelByParent(ctx, db, "cancel-p", ScopeOpts{BatchSize: 2})
		require.NoError(t, err)
		assert.EqualValues(t, 3, res.Changed)
		assertScopedEntries(t, db, "cancel-p", AuditCancelled, withKeys(scoped, map[string]any{
			"from_state": "available", "parent_job_id": "cancel-p", "was_running": false,
		}))
	})

	// A replay ignores AllowRunning, so its entries record it false.
	replayOpts := ReplayOpts{RetryOpts: RetryOpts{AllowRunning: true, ResetAttempts: true, Budget: 2}}
	replayed := retryKeys(StateDiscarded, false, false, true, 2, 0)

	t.Run("ReplayByParent", func(t *testing.T) {
		seedKeyedJob(t, db, "replay-p-1", "replay.parent", "replay-p", StateDiscarded, "replay-p-key")
		seedKeyedJob(t, db, "replay-p-2", "replay.parent", "replay-p", StateDiscarded, "replay-p-key")
		seedKeyedJob(t, db, "replay-p-3", "replay.parent", "replay-p", StateDiscarded, "")
		seedKeyedJob(t, db, "replay-p-4", "replay.parent", "replay-p", StateDiscarded, "")
		res, err := ReplayByParent(ctx, db, "replay-p", replayOpts)
		require.NoError(t, err)
		assert.EqualValues(t, 3, res.Changed)
		assert.EqualValues(t, 1, res.SkippedActiveKey)
		for _, id := range []string{"replay-p-1", "replay-p-3", "replay-p-4"} {
			row := onlyAuditRow(t, db, id)
			assert.Equal(t, string(AuditReplayed), row.Action)
			assert.Equal(t, withKeys(replayed, map[string]any{"via": "parent", "parent_job_id": "replay-p"}),
				auditDetails(t, row), id)
		}
		assert.Empty(t, auditRows(t, db, "replay-p-2"), "a child left terminal for its key gets no entry")
	})

	t.Run("Replay", func(t *testing.T) {
		seedKeyedJob(t, db, "replay-k-1", "replay.kind", "replay-k", StateDiscarded, "replay-k-key")
		seedKeyedJob(t, db, "replay-k-2", "replay.kind", "replay-k", StateDiscarded, "replay-k-key")
		seedKeyedJob(t, db, "replay-k-3", "replay.kind", "replay-k", StateDiscarded, "")
		seedKeyedJob(t, db, "replay-k-4", "replay.kind", "replay-k", StateDiscarded, "")
		opts := replayOpts
		opts.Kinds = []string{"replay.kind"}
		res, err := Replay(ctx, db, opts)
		require.NoError(t, err)
		assert.EqualValues(t, 3, res.Changed)
		for _, id := range []string{"replay-k-1", "replay-k-3", "replay-k-4"} {
			row := onlyAuditRow(t, db, id)
			assert.Equal(t, string(AuditReplayed), row.Action)
			assert.Equal(t, withKeys(replayed, map[string]any{"via": "replay"}), auditDetails(t, row), id)
		}
		assert.Empty(t, auditRows(t, db, "replay-k-2"))
	})

	t.Run("SetPeriodicActive", func(t *testing.T) {
		require.NoError(t, UpsertPeriodic(ctx, db, PeriodicSpec{
			Slug: "nightly-report", Kind: "report", Every: time.Hour, Active: true,
		}))
		for i, active := range []bool{false, false, true} {
			step := clockCtx(ctx, models.NewFixedClock(at.Add(time.Duration(i+1)*time.Minute)))
			require.NoError(t, SetPeriodicActive(step, db, "nightly-report", active))
		}
		rows := auditRows(t, db, "nightly-report")
		require.Len(t, rows, 2, "only a flip is recorded")
		assert.Equal(t, string(AuditDeactivated), rows[0].Action)
		assert.Equal(t, string(AuditActivated), rows[1].Action)
		for _, row := range rows {
			assert.Equal(t, string(AuditTargetPeriodic), row.TargetType)
			assert.Equal(t, "nightly-report", row.TargetID, "a periodic is named by its slug")
			assert.Equal(t, map[string]any{}, auditDetails(t, row))
		}
	})

	t.Run("DeletePeriodic", func(t *testing.T) {
		require.NoError(t, UpsertPeriodic(ctx, db, PeriodicSpec{
			Slug: "weekly-report", Kind: "report", Every: time.Hour, Active: true,
		}))
		require.NoError(t, DeletePeriodic(ctx, db, "weekly-report"))
		row := onlyAuditRow(t, db, "weekly-report")
		assert.Equal(t, string(AuditTargetPeriodic), row.TargetType)
		assert.Equal(t, string(AuditDeleted), row.Action)
		assert.Equal(t, map[string]any{}, auditDetails(t, row))
	})

	t.Run("nothing changed", func(t *testing.T) {
		var hooked atomic.Int32
		quiet := WithAuditHook(ctx, func(context.Context, *gorm.DB, []AuditEntry) error {
			hooked.Add(1)
			return nil
		})
		before := countRows(t, db, &jobAuditRow{})
		seedChild(t, db, "idle-p-1", "idle-p", "leaf", StateSucceeded, now)
		seedChild(t, db, "idle-p-2", "idle-p", "leaf", StateCancelled, now)
		for name, control := range map[string]func(context.Context, *gorm.DB, string, ScopeOpts) (ScopeResult, error){
			"PauseByParent": PauseByParent, "ResumeByParent": ResumeByParent, "CancelByParent": CancelByParent,
		} {
			res, err := control(quiet, db, "idle-p", ScopeOpts{})
			require.NoError(t, err, name)
			assert.Zero(t, res.Changed, name)
		}
		res, err := ReplayByParent(quiet, db, "idle-p", ReplayOpts{})
		require.NoError(t, err)
		assert.Zero(t, res.Changed)
		require.NoError(t, UpsertPeriodic(quiet, db, PeriodicSpec{Slug: "idle-sched", Kind: "report", Every: time.Hour, Active: true}))
		require.NoError(t, SetPeriodicActive(quiet, db, "idle-sched", true), "already active")

		assert.Equal(t, before, countRows(t, db, &jobAuditRow{}), "a call that changes nothing records nothing")
		assert.Zero(t, hooked.Load(), "and runs no hook")
	})
}

// TestEveryControlRecordsOneEntryPerChangedJob proves each control writes one
// entry per job or schedule it changes, attributed to the context's actor, with
// the state the job changed from and the options the call used.
func TestEveryControlRecordsOneEntryPerChangedJob(t *testing.T) {
	t.Parallel()
	assertEveryControlRecordsOneEntryPerChangedJob(t, newDB(t))
}

// TestABatchRecordsOnlyTheJobsItChanged proves a batch records the jobs its
// guarded update changed, not the ones it selected: a row another write moves
// just before the update is skipped and gets no entry. A call that changes
// nothing, or is refused, writes no entry and runs no hook.
func TestABatchRecordsOnlyTheJobsItChanged(t *testing.T) {
	t.Parallel()

	t.Run("rows moved before the batch's update", func(t *testing.T) {
		t.Parallel()
		db := newDB(t)
		seedChildrenBulk(t, db, "P", 3, StateAvailable, time.Now().UTC())
		var fired atomic.Bool
		require.NoError(t, db.Callback().Update().Before("gorm:update").Register("test:finish_first",
			func(tx *gorm.DB) {
				if fired.Swap(true) {
					return
				}
				if _, err := tx.Statement.ConnPool.ExecContext(tx.Statement.Context,
					`UPDATE jobs SET state = 'succeeded' WHERE parent_job_id = 'P'`); err != nil {
					_ = tx.AddError(err)
				}
			}))

		res, err := CancelByParent(context.Background(), db, "P", ScopeOpts{})
		require.NoError(t, err)
		require.True(t, fired.Load())
		assert.Zero(t, res.Changed)
		assert.Zero(t, countRows(t, db, &jobAuditRow{}), "the batch changed no job, so it recorded none")
	})

	t.Run("one row moved before the batch's update", func(t *testing.T) {
		t.Parallel()
		db := newDB(t)
		seedChildrenBulk(t, db, "Q", 3, StateAvailable, time.Now().UTC())
		var fired atomic.Bool
		require.NoError(t, db.Callback().Update().Before("gorm:update").Register("test:finish_one_first",
			func(tx *gorm.DB) {
				if fired.Swap(true) {
					return
				}
				if _, err := tx.Statement.ConnPool.ExecContext(tx.Statement.Context,
					`UPDATE jobs SET state = 'succeeded' WHERE id = 'Q-available-1'`); err != nil {
					_ = tx.AddError(err)
				}
			}))

		res, err := CancelByParent(context.Background(), db, "Q", ScopeOpts{})
		require.NoError(t, err)
		require.True(t, fired.Load())
		assert.EqualValues(t, 2, res.Changed)
		assert.Empty(t, auditRows(t, db, "Q-available-1"), "the job the batch skipped gets no entry")
		for _, id := range []string{"Q-available-0", "Q-available-2"} {
			assert.Equal(t, string(AuditCancelled), onlyAuditRow(t, db, id).Action)
		}
	})

	t.Run("calls that change nothing", func(t *testing.T) {
		t.Parallel()
		db := newDB(t)
		var hooked atomic.Int32
		ctx := WithAuditHook(context.Background(), func(context.Context, *gorm.DB, []AuditEntry) error {
			hooked.Add(1)
			return nil
		})
		now := time.Now().UTC()

		seedChild(t, db, "T-1", "T", "leaf", StateSucceeded, now)
		seedChild(t, db, "T-2", "T", "leaf", StateDiscarded, now)
		res, err := CancelByParent(ctx, db, "T", ScopeOpts{})
		require.NoError(t, err)
		assert.Zero(t, res.Changed, "every child is terminal")

		fin := now.Add(-time.Minute)
		seedJob(t, db, jobRow{ID: "done-1", Kind: "leaf", State: string(StateSucceeded), ScheduledAt: now, FinalizedAt: &fin})
		require.ErrorIs(t, RetryJob(ctx, db, "done-1"), ErrJobTerminal)

		seedKeyedJob(t, db, "held-old", "leaf", "H", StateDiscarded, "held-key")
		seedKeyedJob(t, db, "held-new", "leaf", "H", StateAvailable, "held-key")
		err = RetryJobWithOptions(ctx, db, "held-old", RetryOpts{Force: true})
		var held *AlreadyEnqueuedError
		require.ErrorAs(t, err, &held, "a newer live job holds the key")

		assert.Zero(t, countRows(t, db, &jobAuditRow{}), "no call changed a job, so none recorded one")
		assert.Zero(t, hooked.Load(), "and none ran the hook")
	})
}

// TestARolledBackControlLeavesNoEntry proves an entry is written on the
// control's own transaction, a savepoint in a caller's: when the caller rolls
// back, the change and its entry go together.
func TestARolledBackControlLeavesNoEntry(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	now := time.Now().UTC()
	seedJob(t, db, jobRow{ID: "rb-cancel", Kind: "leaf", State: string(StateAvailable), ScheduledAt: now})
	seedJob(t, db, jobRow{ID: "rb-retry", Kind: "leaf", State: string(StateRetryable), ScheduledAt: now})
	ctx := WithActor(context.Background(), auditOperator)

	errRollback := errors.New("the caller rolls back")
	err := db.Transaction(func(tx *gorm.DB) error {
		require.NoError(t, CancelJob(ctx, tx, "rb-cancel"))
		require.NoError(t, RetryJob(ctx, tx, "rb-retry"))
		assert.EqualValues(t, 2, countRows(t, tx, &jobAuditRow{}), "both entries are on the caller's transaction")
		return errRollback
	})
	require.ErrorIs(t, err, errRollback)

	assert.Equal(t, string(StateAvailable), jobState(t, db, "rb-cancel"))
	assert.Equal(t, string(StateRetryable), jobState(t, db, "rb-retry"))
	assert.Zero(t, countRows(t, db, &jobAuditRow{}), "the rollback took the entries with the changes")
}

// TestAuditHookErrorRollsBackTheControl proves the hook runs on the control's
// transaction, after its entries: its error rolls back that transaction, change
// and entries alike, and is returned; a row the hook writes on the transaction
// commits or rolls back with the entry.
func TestAuditHookErrorRollsBackTheControl(t *testing.T) {
	t.Parallel()
	errHook := errors.New("the host's audit write failed")

	t.Run("CancelJob", func(t *testing.T) {
		t.Parallel()
		db := newDB(t)
		seedJob(t, db, jobRow{ID: "hook-1", Kind: "leaf", State: string(StateAvailable), ScheduledAt: time.Now().UTC()})
		ctx := WithAuditHook(context.Background(), func(context.Context, *gorm.DB, []AuditEntry) error {
			return errHook
		})

		err := CancelJob(ctx, db, "hook-1")
		require.ErrorIs(t, err, errHook)
		assert.Equal(t, string(StateAvailable), jobState(t, db, "hook-1"), "the cancel rolled back")
		assert.Zero(t, countRows(t, db, &jobAuditRow{}), "and its entry with it")
	})

	t.Run("CancelByParent", func(t *testing.T) {
		t.Parallel()
		db := newDB(t)
		seedChildrenBulk(t, db, "HB", 4, StateAvailable, time.Now().UTC())
		var calls atomic.Int32
		ctx := WithAuditHook(context.Background(), func(_ context.Context, _ *gorm.DB, entries []AuditEntry) error {
			assert.Len(t, entries, 2, "the hook gets its batch's entries")
			if calls.Add(1) == 2 {
				return errHook
			}
			return nil
		})

		res, err := CancelByParent(ctx, db, "HB", ScopeOpts{BatchSize: 2})
		require.ErrorIs(t, err, errHook)
		assert.EqualValues(t, 2, res.Changed, "the first batch committed")
		assert.EqualValues(t, 2, countChildrenInState(t, db, "HB", StateCancelled))
		assert.EqualValues(t, 2, countChildrenInState(t, db, "HB", StateAvailable), "the second batch rolled back")
		assert.EqualValues(t, 2, countRows(t, db, &jobAuditRow{}), "only the committed batch's entries remain")
	})

	t.Run("HostRow", func(t *testing.T) {
		t.Parallel()
		db := newDB(t)
		require.NoError(t, db.Exec(`CREATE TABLE host_audit (id text PRIMARY KEY, entry_id text NOT NULL)`).Error)
		now := time.Now().UTC()
		seedJob(t, db, jobRow{ID: "host-ok", Kind: "leaf", State: string(StateAvailable), ScheduledAt: now})
		seedJob(t, db, jobRow{ID: "host-fail", Kind: "leaf", State: string(StateAvailable), ScheduledAt: now})
		hook := func(fail bool) AuditHook {
			return func(_ context.Context, tx *gorm.DB, entries []AuditEntry) error {
				for _, e := range entries {
					if err := tx.Exec(`INSERT INTO host_audit (id, entry_id) VALUES (?, ?)`, e.TargetID, e.ID).Error; err != nil {
						return err
					}
				}
				if fail {
					return errHook
				}
				return nil
			}
		}
		hostRows := func(id string) []string {
			var ids []string
			require.NoError(t, db.Raw(`SELECT entry_id FROM host_audit WHERE id = ?`, id).Scan(&ids).Error)
			return ids
		}

		require.NoError(t, CancelJob(WithAuditHook(context.Background(), hook(false)), db, "host-ok"))
		entry := onlyAuditRow(t, db, "host-ok")
		assert.Equal(t, []string{entry.ID}, hostRows("host-ok"), "the host's row commits with the entry it was given")

		err := CancelJob(WithAuditHook(context.Background(), hook(true)), db, "host-fail")
		require.ErrorIs(t, err, errHook)
		assert.Empty(t, hostRows("host-fail"), "the host's row rolls back with the control")
		assert.Empty(t, auditRows(t, db, "host-fail"))
		assert.Equal(t, string(StateAvailable), jobState(t, db, "host-fail"))
	})
}

// TestAControlWithoutAnActorIsRecordedAsSystem proves a control called with no
// actor is recorded as the system, with the context's request id, and an
// actor's empty request id takes the context's.
func TestAControlWithoutAnActorIsRecordedAsSystem(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	now := time.Now().UTC()
	seedJob(t, db, jobRow{ID: "sys-1", Kind: "leaf", State: string(StateAvailable), ScheduledAt: now})
	seedJob(t, db, jobRow{ID: "sys-2", Kind: "leaf", State: string(StateAvailable), ScheduledAt: now})
	ctx := ctxutil.WithRequestID(context.Background(), "req-9")

	_, ok := ActorFrom(ctx)
	assert.False(t, ok, "no actor on the context")
	require.NoError(t, CancelJob(ctx, db, "sys-1"))
	row := onlyAuditRow(t, db, "sys-1")
	assert.Equal(t, string(ActorSystem), row.ActorType)
	assert.Empty(t, row.ActorID)
	assert.Empty(t, row.Reason)
	assert.Empty(t, row.Source)
	assert.Equal(t, "req-9", row.RequestID)

	operator := Actor{Type: ActorUser, ID: "u-7"}
	actorCtx := WithActor(ctx, operator)
	got, ok := ActorFrom(actorCtx)
	require.True(t, ok)
	assert.Equal(t, operator, got)
	require.NoError(t, CancelJob(actorCtx, db, "sys-2"))
	row = onlyAuditRow(t, db, "sys-2")
	assert.Equal(t, string(ActorUser), row.ActorType)
	assert.Equal(t, "u-7", row.ActorID)
	assert.Equal(t, "req-9", row.RequestID, "an empty request id takes the context's")
}

// invalidActor is an actor a control refuses, the field it names, and the value
// the error must never quote.
type invalidActor struct {
	name  string
	actor Actor
	field string
	value string
}

// invalidActors returns one actor of each kind the history can't record.
func invalidActors() []invalidActor {
	reason := strings.Repeat("r", 1025)
	source := strings.Repeat("s", 65)
	return []invalidActor{
		{name: "an unknown type", actor: Actor{Type: "robot", ID: "r-1"}, field: "actor.type", value: "robot"},
		{name: "the zero actor", actor: Actor{}, field: "actor.type"},
		{name: "a reason of 1,025 bytes", actor: Actor{Type: ActorUser, Reason: reason}, field: "actor.reason", value: reason},
		{name: "a source of 65 bytes", actor: Actor{Type: ActorAgent, Source: source}, field: "actor.source", value: source},
	}
}

// boundaryActor carries a reason and a source at exactly their limits.
func boundaryActor() Actor {
	return Actor{Type: ActorUser, ID: "u-7", Reason: strings.Repeat("r", 1024), Source: strings.Repeat("s", 64)}
}

// assertRefusedActor asserts err refuses bad as a *ValidationError naming its
// field, without quoting the value.
func assertRefusedActor(t *testing.T, err error, bad invalidActor) {
	t.Helper()
	var ve *ValidationError
	require.ErrorAs(t, err, &ve, bad.name)
	require.ErrorIs(t, err, ErrValidation)
	assert.Equal(t, bad.field, ve.Field, bad.name)
	if bad.value != "" {
		assert.NotContains(t, err.Error(), bad.value, "the error never quotes the value")
	}
}

// assertBoundaryStored asserts row carries the boundary actor's reason and
// source whole.
func assertBoundaryStored(t *testing.T, row jobAuditRow) {
	t.Helper()
	assert.Len(t, row.Reason, 1024, "a reason at the limit is stored whole")
	assert.Len(t, row.Source, 64, "a source at the limit is stored whole")
}

// TestAnInvalidActorRefusesTheControl proves an actor the history can't record
// refuses the control before any SQL runs: nothing is read or written, and the
// limits themselves are accepted.
func TestAnInvalidActorRefusesTheControl(t *testing.T) {
	t.Parallel()
	assert.Equal(t, 1024, MaxReasonBytes)
	assert.Equal(t, 64, MaxActorSourceBytes)
	now := time.Now().UTC()

	t.Run("CancelJob", func(t *testing.T) {
		t.Parallel()
		db := newDB(t)
		seedJob(t, db, jobRow{ID: "inv-c", Kind: "leaf", State: string(StateAvailable), ScheduledAt: now})
		for _, bad := range invalidActors() {
			assertRefusedActor(t, CancelJob(WithActor(context.Background(), bad.actor), db, "inv-c"), bad)
			assertRefusedActor(t, CancelJob(WithActor(context.Background(), bad.actor), db, "no-such-job"), bad)
		}
		assert.Equal(t, string(StateAvailable), jobState(t, db, "inv-c"))
		assert.Zero(t, countRows(t, db, &jobAuditRow{}))

		require.NoError(t, CancelJob(WithActor(context.Background(), boundaryActor()), db, "inv-c"))
		assertBoundaryStored(t, onlyAuditRow(t, db, "inv-c"))
	})

	t.Run("CancelByParent", func(t *testing.T) {
		t.Parallel()
		db := newDB(t)
		seedChildrenBulk(t, db, "inv-p", 2, StateAvailable, now)
		for _, bad := range invalidActors() {
			_, err := CancelByParent(WithActor(context.Background(), bad.actor), db, "inv-p", ScopeOpts{})
			assertRefusedActor(t, err, bad)
		}
		assert.EqualValues(t, 2, countChildrenInState(t, db, "inv-p", StateAvailable))
		assert.Zero(t, countRows(t, db, &jobAuditRow{}))

		res, err := CancelByParent(WithActor(context.Background(), boundaryActor()), db, "inv-p", ScopeOpts{})
		require.NoError(t, err)
		assert.EqualValues(t, 2, res.Changed)
		assertBoundaryStored(t, onlyAuditRow(t, db, "inv-p-available-0"))
	})

	t.Run("ReplayByParent", func(t *testing.T) {
		t.Parallel()
		db := newDB(t)
		seedKeyedJob(t, db, "inv-r-1", "leaf", "inv-r", StateDiscarded, "")
		seedKeyedJob(t, db, "inv-r-2", "leaf", "inv-r", StateDiscarded, "")
		for _, bad := range invalidActors() {
			_, err := ReplayByParent(WithActor(context.Background(), bad.actor), db, "inv-r", ReplayOpts{})
			assertRefusedActor(t, err, bad)
		}
		assert.EqualValues(t, 2, countChildrenInState(t, db, "inv-r", StateDiscarded))
		assert.Zero(t, countRows(t, db, &jobAuditRow{}))

		res, err := ReplayByParent(WithActor(context.Background(), boundaryActor()), db, "inv-r", ReplayOpts{})
		require.NoError(t, err)
		assert.EqualValues(t, 2, res.Changed)
		assertBoundaryStored(t, onlyAuditRow(t, db, "inv-r-1"))
	})

	t.Run("SetPeriodicActive", func(t *testing.T) {
		t.Parallel()
		db := newDB(t)
		require.NoError(t, UpsertPeriodic(context.Background(), db, PeriodicSpec{
			Slug: "inv-s", Kind: "report", Every: time.Hour, Active: true,
		}))
		for _, bad := range invalidActors() {
			assertRefusedActor(t, SetPeriodicActive(WithActor(context.Background(), bad.actor), db, "inv-s", false), bad)
		}
		views, err := ListPeriodics(context.Background(), db)
		require.NoError(t, err)
		require.Len(t, views, 1)
		assert.True(t, views[0].Active, "the refused calls changed nothing")
		assert.Zero(t, countRows(t, db, &jobAuditRow{}))

		require.NoError(t, SetPeriodicActive(WithActor(context.Background(), boundaryActor()), db, "inv-s", false))
		assertBoundaryStored(t, onlyAuditRow(t, db, "inv-s"))
	})
}
