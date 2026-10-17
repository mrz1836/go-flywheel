package core

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mrz1836/go-foundation/ctxutil"
	"github.com/mrz1836/go-foundation/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// bulkDay is when the bulk fixtures' jobs were created: their ids are UUIDv7
// shaped at this day plus their index in milliseconds, so id order is seed
// order.
//
//nolint:gochecknoglobals // shared fixture instant
var bulkDay = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

// bulkID is the id of the bulk fixtures' job number i.
func bulkID(i int) string { return idAt(bulkDay.Add(time.Duration(i)*time.Millisecond), i) }

// bulkJob is one job a bulk test seeds, at index i of its fixture.
type bulkJob struct {
	i      int
	kind   string
	queue  string
	state  JobState
	parent string
	key    string // its UniqueActiveKey, when set
}

// seedBulkJob writes j: created, updated, and scheduled at its id's time, with
// attempt 1 of 5, finalized then when its state is terminal. It returns the id.
func seedBulkJob(t testing.TB, db *gorm.DB, j bulkJob) string {
	t.Helper()
	at := bulkDay.Add(time.Duration(j.i) * time.Millisecond)
	row := jobRow{
		ID: bulkID(j.i), Kind: orString(j.kind, "k"), Queue: orString(j.queue, "q"), State: string(j.state),
		Attempt: 1, MaxAttempts: 5, CreatedAt: at, UpdatedAt: at, ScheduledAt: at,
	}
	if isTerminalStateString(string(j.state)) {
		row.FinalizedAt = &at
	}
	if j.parent != "" {
		parent := j.parent
		row.ParentJobID = &parent
	}
	if j.key != "" {
		key := j.key
		row.UniqueActiveKey = &key
	}
	seedJob(t, db, row)
	return row.ID
}

// seedBulkJobs writes n jobs of kind k on queue q in state, at indexes from,
// from+1, …, and returns their ids in id order.
func seedBulkJobs(t testing.TB, db *gorm.DB, from, n int, state JobState) []string {
	t.Helper()
	ids := make([]string, n)
	for k := range n {
		ids[k] = seedBulkJob(t, db, bulkJob{i: from + k, state: state})
	}
	return ids
}

// jobsSnapshot reads every jobs row, soft-deleted ones too, in id order, so a
// test proves a call changed none of them.
func jobsSnapshot(t testing.TB, db *gorm.DB) []jobRow {
	t.Helper()
	var rows []jobRow
	require.NoError(t, db.Unscoped().Model(&jobRow{}).Order("id").Find(&rows).Error)
	return rows
}

// bulkRefusal is a spec BulkJobs refuses before it reads anything: with a
// *ValidationError naming field, or with an error matching is.
type bulkRefusal struct {
	name  string
	ctx   context.Context
	spec  BulkSpec
	field string
	is    error
}

// bulkRefusals are the specs BulkJobs refuses, one per rule, then pairs that pin
// the order the rules apply in.
func bulkRefusals() []bulkRefusal {
	at := bulkDay.Add(time.Hour)
	bad := WithActor(context.Background(), Actor{Type: "robot"})
	cases := []bulkRefusal{
		{name: "no_query_and_no_ids", spec: BulkSpec{Action: BulkCancel}, is: ErrBulkUnbounded},
		{name: "a_retry_with_no_query", spec: BulkSpec{Action: BulkRetry, Retry: RetryOpts{Force: true}}, is: ErrBulkUnbounded},
		{name: "a_delete_with_no_query", spec: BulkSpec{Action: BulkDelete}, is: ErrBulkUnbounded},
		{name: "a_reschedule_with_no_query", spec: BulkSpec{Action: BulkReschedule, RescheduleAt: at}, is: ErrBulkUnbounded},
		{name: "a_pause_with_no_query", spec: BulkSpec{Action: BulkPause}, is: ErrBulkUnbounded},
		{name: "a_resume_with_no_query", spec: BulkSpec{Action: BulkResume}, is: ErrBulkUnbounded},
		{name: "a_query_of_sort_cursor_and_limit_alone", spec: BulkSpec{
			Action: BulkCancel, Query: JobQuery{Sort: JobSortOldest, Cursor: "jq1.x", Limit: 5},
		}, is: ErrBulkUnbounded},
		{name: "an_empty_id_list", spec: BulkSpec{Action: BulkCancel, IDs: []string{}}, is: ErrBulkUnbounded},

		{name: "no_action", spec: BulkSpec{Query: byKind}, field: "action"},
		{name: "an_unknown_action", spec: BulkSpec{Action: "archive", Query: byKind}, field: "action"},
		{name: "a_negative_max_jobs", spec: BulkSpec{Action: BulkCancel, Query: byKind, MaxJobs: -1}, field: "max_jobs"},
		{
			name: "max_jobs_above_the_cap", field: "max_jobs",
			spec: BulkSpec{Action: BulkCancel, Query: byKind, MaxJobs: MaxBulkJobs + 1},
		},
		{
			name: "more_ids_than_max_jobs", field: "ids",
			spec: BulkSpec{Action: BulkCancel, IDs: []string{"one", "two", "three"}, MaxJobs: 2},
		},
		{
			name: "more_ids_than_the_default_cap", field: "ids",
			spec: BulkSpec{Action: BulkCancel, IDs: missingIDs(DefaultBulkMaxJobs + 1)},
		},
		{name: "ids_and_a_query", spec: BulkSpec{Action: BulkCancel, IDs: []string{"one"}, Query: byKind}, field: "ids"},
		{name: "a_reschedule_without_a_time", spec: BulkSpec{Action: BulkReschedule, Query: byKind}, field: "reschedule_at"},
		{
			name: "a_time_off_a_reschedule", field: "reschedule_at",
			spec: BulkSpec{Action: BulkCancel, Query: byKind, RescheduleAt: at},
		},
		{
			name: "retry_options_off_a_retry", field: "retry",
			spec: BulkSpec{Action: BulkCancel, Query: byKind, Retry: RetryOpts{Force: true}},
		},
		{
			name: "allow_running_off_a_retry", field: "retry",
			spec: BulkSpec{Action: BulkPause, Query: byKind, Retry: RetryOpts{AllowRunning: true}},
		},
		{name: "a_stagger_on_a_cancel", spec: BulkSpec{Action: BulkCancel, Query: byKind, Stagger: time.Minute}, field: "stagger"},
		{name: "a_stagger_on_a_delete", spec: BulkSpec{Action: BulkDelete, Query: byKind, Stagger: time.Minute}, field: "stagger"},
		{name: "a_stagger_on_a_pause", spec: BulkSpec{Action: BulkPause, Query: byKind, Stagger: time.Minute}, field: "stagger"},
		{name: "a_stagger_on_a_resume", spec: BulkSpec{Action: BulkResume, Query: byKind, Stagger: time.Minute}, field: "stagger"},
		{
			name: "a_state_query_jobs_refuses", field: "states",
			spec: BulkSpec{Action: BulkCancel, Query: JobQuery{States: []JobState{"lost"}}},
		},
		{
			name: "a_priority_range_query_jobs_refuses", field: "priority",
			spec: BulkSpec{Action: BulkCancel, Query: JobQuery{PriorityMin: intp(5), PriorityMax: intp(1)}},
		},
		{
			name: "more_query_ids_than_query_jobs_takes", field: "ids",
			spec: BulkSpec{Action: BulkCancel, Query: JobQuery{IDs: missingIDs(MaxReadLimit + 1)}},
		},
		{
			name: "error_contains_on_sqlite", is: ErrUnsupportedDialect,
			spec: BulkSpec{Action: BulkRetry, Query: JobQuery{
				ErrorContains: "refused", CreatedFrom: bulkDay, CreatedTo: bulkDay.Add(time.Hour),
			}},
		},

		// The order: the actor, the action, MaxJobs, IDs, the action's options, the
		// query, and only then the bound.
		{name: "an_invalid_actor_before_the_bound", ctx: bad, spec: BulkSpec{Action: BulkCancel}, field: "actor.type"},
		{name: "the_action_before_max_jobs", spec: BulkSpec{Action: "archive", MaxJobs: -1}, field: "action"},
		{
			name: "max_jobs_before_the_ids", field: "max_jobs",
			spec: BulkSpec{Action: BulkCancel, MaxJobs: -1, IDs: []string{"one"}, Query: byKind},
		},
		{
			name: "the_ids_before_the_options", field: "ids",
			spec: BulkSpec{Action: BulkReschedule, IDs: []string{"one"}, Query: byKind},
		},
		{
			name: "the_options_before_the_query", field: "stagger",
			spec: BulkSpec{Action: BulkCancel, Stagger: time.Minute, Query: JobQuery{States: []JobState{"lost"}}},
		},
	}
	for _, a := range invalidActors() {
		cases = append(cases, bulkRefusal{
			name: "an_actor_with_" + a.name, ctx: WithActor(context.Background(), a.actor),
			spec: BulkSpec{Action: BulkCancel, Query: byKind}, field: a.field,
		})
	}
	return cases
}

// missingIDs returns n distinct ids no job has.
func missingIDs(n int) []string {
	ids := make([]string, n)
	for i := range ids {
		ids[i] = fmt.Sprintf("missing-%06d", i)
	}
	return ids
}

// TestBulkRefusesAnUnboundedSpec proves BulkJobs refuses, before it runs any
// statement, a spec bounded by neither a filter nor ids (ErrBulkUnbounded), each
// spec it can't run (a *ValidationError naming the field), a query QueryJobs
// refuses (with QueryJobs' own error), and an actor the history can't record;
// and that no refusal changes a job or writes an entry.
func TestBulkRefusesAnUnboundedSpec(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	seedBulkJobs(t, db, 0, 3, StateAvailable)
	seedBulkJob(t, db, bulkJob{i: 3, state: StateDiscarded})
	before := jobsSnapshot(t, db)

	for _, c := range bulkRefusals() {
		t.Run(c.name, func(t *testing.T) {
			ctx := c.ctx
			if ctx == nil {
				ctx = context.Background()
			}
			recorded, rec := recordStatements(db)
			res, err := BulkJobs(ctx, recorded, c.spec)
			if c.field != "" {
				requireFieldRefusal(t, err, c.field)
			} else {
				require.ErrorIs(t, err, c.is)
				require.NotErrorIs(t, err, ErrValidation, "a bound or a dialect refusal is no *ValidationError")
			}
			assert.Equal(t, BulkResult{}, res, "a refusal reports nothing")
			assert.Empty(t, rec.stmt, "a refusal runs no statement")
		})
	}
	assert.Equal(t, before, jobsSnapshot(t, db), "no refusal changes a job")
	assert.Zero(t, countRows(t, db, &jobAuditRow{}), "no refusal writes an entry")
}

// TestBulkTreatsEveryJobQueryFilterAsABound proves the bound check knows every
// JobQuery field: each filter set alone bounds a spec, and Sort, Cursor, and
// Limit, which page a read rather than filter it, never do. A field added to
// JobQuery fails it until the check names it.
func TestBulkTreatsEveryJobQueryFilterAsABound(t *testing.T) {
	t.Parallel()
	paging := map[string]bool{"Sort": true, "Cursor": true, "Limit": true}
	typ := reflect.TypeFor[JobQuery]()
	for i := range typ.NumField() {
		f := typ.Field(i)
		t.Run(f.Name, func(t *testing.T) {
			var q JobQuery
			v := reflect.ValueOf(&q).Elem().Field(i)
			switch v.Kind() {
			case reflect.Slice:
				if f.Type == reflect.TypeFor[json.RawMessage]() {
					v.Set(reflect.ValueOf(json.RawMessage(`{"n":1}`)))
				} else {
					v.Set(reflect.MakeSlice(f.Type, 1, 1))
				}
			case reflect.String:
				v.SetString("x")
			case reflect.Int:
				v.SetInt(1)
			case reflect.Pointer:
				v.Set(reflect.New(f.Type.Elem()))
			case reflect.Struct:
				require.Equal(t, reflect.TypeFor[time.Time](), f.Type, "a struct field is a time")
				v.Set(reflect.ValueOf(bulkDay))
			default:
				t.Fatalf("JobQuery.%s is a %s, which this guard does not set", f.Name, v.Kind())
			}
			assert.Equal(t, !paging[f.Name], jobQueryFilters(q), "JobQuery.%s set alone", f.Name)
		})
	}
	assert.False(t, jobQueryFilters(JobQuery{}), "the zero query filters nothing")
}

// byKind selects the bulk fixtures' jobs of kind k.
//
//nolint:gochecknoglobals // shared fixture query
var byKind = JobQuery{Kinds: []string{"k"}}

// seedDryRunFixture writes thirty jobs of kind k, interleaved so the newest is
// running: five running, five succeeded, and twenty discarded. Beside them it
// writes three discarded jobs of another kind and a soft-deleted discarded job
// of kind k, newer than all of them, which a spec selecting kind k never
// matches. It returns the thirty ids in id order.
func seedDryRunFixture(t *testing.T, db *gorm.DB) []string {
	t.Helper()
	ids := make([]string, 30)
	for i := range ids {
		state := StateDiscarded
		switch i % 6 {
		case 2:
			state = StateSucceeded
		case 5:
			state = StateRunning
		}
		ids[i] = seedBulkJob(t, db, bulkJob{i: i, state: state})
	}
	for i := 30; i < 33; i++ {
		seedBulkJob(t, db, bulkJob{i: i, kind: "other", state: StateDiscarded})
	}
	gone := seedBulkJob(t, db, bulkJob{i: 33, state: StateDiscarded})
	require.NoError(t, db.Delete(&jobRow{}, "id = ?", gone).Error)
	return ids
}

// newestFirst returns the last n of ids, which are in id order, newest first.
func newestFirst(ids []string, n int) []string {
	out := make([]string, 0, n)
	for i := len(ids) - 1; i >= 0 && len(out) < n; i-- {
		out = append(out, ids[i])
	}
	return out
}

// TestBulkDryRunChangesNothing proves a dry run reports what an apply would do
// and does none of it: the matches past a soft-deleted job and another kind, the
// jobs it would change and the ones it would skip by reason, by state and by
// kind, a sample of the newest it would change read as views, the newest id it
// matched, and with MaxJobs, a capped walk of the first MaxJobs.
func TestBulkDryRunChangesNothing(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	ids := seedDryRunFixture(t, db)
	var changeable []string
	for i, id := range ids {
		if i%6 != 5 {
			changeable = append(changeable, id)
		}
	}
	before := jobsSnapshot(t, db)
	ctx := context.Background()
	forced := BulkSpec{Query: byKind, Action: BulkRetry, Retry: RetryOpts{Force: true}, DryRun: true}

	for _, size := range []int{0, 7} {
		t.Run(fmt.Sprintf("a_forced_retry_in_batches_of_%d", size), func(t *testing.T) {
			spec := forced
			spec.BatchSize = size
			res, err := BulkJobs(ctx, db, spec)
			require.NoError(t, err)
			assert.EqualValues(t, 30, res.Matched)
			assert.False(t, res.MatchedCapped)
			assert.EqualValues(t, 25, res.Changed, "every discarded and succeeded job")
			assert.Equal(t, map[BulkSkipReason]int64{BulkSkipRunning: 5}, res.Skipped)
			assert.Equal(t, map[JobState]int64{StateDiscarded: 20, StateRunning: 5, StateSucceeded: 5}, res.ByState)
			assert.Equal(t, map[string]int64{"k": 30}, res.ByKind)
			assert.Equal(t, newestFirst(changeable, 20), viewIDs(res.Sample), "the 20 newest it would change")
			for _, view := range res.Sample {
				want, err := FindJob(ctx, db, view.ID)
				require.NoError(t, err)
				assert.Equal(t, want, view, "a sample job is its view")
			}
			assert.Equal(t, ids[29], res.ResumeAfter, "the newest id it matched, a running job the sample leaves out")
			assert.True(t, res.Done)
			assert.Zero(t, res.Batches, "a dry run runs no transaction")
		})
	}

	t.Run("a_retry_without_force", func(t *testing.T) {
		spec := forced
		spec.Retry = RetryOpts{}
		res, err := BulkJobs(ctx, db, spec)
		require.NoError(t, err)
		assert.EqualValues(t, 30, res.Matched)
		assert.Zero(t, res.Changed)
		assert.Equal(t, map[BulkSkipReason]int64{BulkSkipRunning: 5, BulkSkipTerminal: 25}, res.Skipped,
			"a retry changes a terminal job only with Force")
		assert.Empty(t, res.Sample)
		assert.True(t, res.Done)
	})

	t.Run("capped_at_max_jobs", func(t *testing.T) {
		spec := forced
		spec.MaxJobs = 10
		spec.BatchSize = 4
		res, err := BulkJobs(ctx, db, spec)
		require.NoError(t, err)
		assert.EqualValues(t, 10, res.Matched)
		assert.True(t, res.MatchedCapped)
		assert.EqualValues(t, 9, res.Changed, "it walks the first ten matches")
		assert.Equal(t, map[BulkSkipReason]int64{BulkSkipRunning: 1}, res.Skipped)
		assert.Equal(t, map[JobState]int64{StateDiscarded: 7, StateRunning: 1, StateSucceeded: 2}, res.ByState)
		assert.Equal(t, newestFirst(changeable[:9], 9), viewIDs(res.Sample))
		assert.False(t, res.Done, "a capped walk is not done")
		assert.Equal(t, ids[9], res.ResumeAfter, "the tenth match by id")
	})

	assert.Equal(t, before, jobsSnapshot(t, db), "a dry run changes no job")
	assert.Zero(t, countRows(t, db, &jobAuditRow{}), "a dry run writes no entry")
}

// bulkChanges marks, in a skip table, a state the action changes.
const bulkChanges BulkSkipReason = ""

// bulkSkipCase is one action and, for a job in each state, the reason it skips
// that job, or bulkChanges.
type bulkSkipCase struct {
	name  string
	spec  BulkSpec
	skips map[JobState]BulkSkipReason
}

// bulkSkipCases is every action's table over every state.
func bulkSkipCases() []bulkSkipCase {
	live := func(waiting, paused, terminal BulkSkipReason) map[JobState]BulkSkipReason {
		return map[JobState]BulkSkipReason{
			StateAvailable: waiting, StateRetryable: waiting, StateScheduled: waiting, StatePaused: paused,
			StateRunning:   BulkSkipRunning,
			StateSucceeded: terminal, StateCancelled: terminal, StateDiscarded: terminal,
		}
	}
	at := bulkDay.Add(48 * time.Hour)
	return []bulkSkipCase{
		{"cancel", BulkSpec{Action: BulkCancel}, live(bulkChanges, bulkChanges, BulkSkipTerminal)},
		{"retry", BulkSpec{Action: BulkRetry}, live(bulkChanges, bulkChanges, BulkSkipTerminal)},
		{"forced_retry", BulkSpec{Action: BulkRetry, Retry: RetryOpts{Force: true}}, live(bulkChanges, bulkChanges, bulkChanges)},
		{"delete", BulkSpec{Action: BulkDelete}, live(BulkSkipNotTerminal, BulkSkipNotTerminal, bulkChanges)},
		{"reschedule", BulkSpec{Action: BulkReschedule, RescheduleAt: at}, live(bulkChanges, bulkChanges, BulkSkipTerminal)},
		{"pause", BulkSpec{Action: BulkPause}, live(bulkChanges, BulkSkipState, BulkSkipTerminal)},
		{"resume", BulkSpec{Action: BulkResume}, live(BulkSkipState, bulkChanges, BulkSkipTerminal)},
	}
}

// everyState is every job state, in the order the skip table seeds them.
func everyState() []JobState {
	return []JobState{
		StateAvailable, StateRetryable, StateScheduled, StatePaused, StateRunning,
		StateSucceeded, StateCancelled, StateDiscarded,
	}
}

// assertBulkChanged asserts the job a bulk action changed from before ended as
// the action leaves it.
func assertBulkChanged(t *testing.T, db *gorm.DB, spec BulkSpec, before jobRow) {
	t.Helper()
	if spec.Action == BulkDelete {
		var left int64
		require.NoError(t, db.Unscoped().Model(&jobRow{}).Where("id = ?", before.ID).Count(&left).Error)
		assert.Zero(t, left, "a deleted %s job is gone", before.State)
		return
	}
	after := jobRowByID(t, db, before.ID)
	switch spec.Action {
	case BulkCancel:
		assert.Equal(t, string(StateCancelled), after.State)
		assert.NotNil(t, after.FinalizedAt, "a cancel stamps finalized_at")
		assert.Nil(t, after.LeaseToken)
	case BulkRetry:
		assert.Equal(t, string(StateAvailable), after.State)
		assert.Nil(t, after.FinalizedAt, "a retry clears finalized_at")
	case BulkReschedule:
		assert.Equal(t, before.State, after.State, "a reschedule keeps the state")
		assert.True(t, after.ScheduledAt.Equal(spec.RescheduleAt), "due at the new time")
	case BulkPause:
		assert.Equal(t, string(StatePaused), after.State)
	case BulkResume:
		assert.Equal(t, string(StateAvailable), after.State)
		assert.True(t, after.ScheduledAt.Equal(before.ScheduledAt), "a resume keeps scheduled_at")
	}
}

// TestBulkSkipsEachJobForItsReason proves every action, over a job in every
// state, puts each job in exactly one of Changed and the skip reasons as its
// table says, that a dry run classifies them exactly as the apply does and
// changes nothing, and that the apply changes each job it counts as changed the
// way its single-job control does and leaves every other job as it was.
func TestBulkSkipsEachJobForItsReason(t *testing.T) {
	t.Parallel()
	for _, c := range bulkSkipCases() {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			db := newDB(t)
			ctx := context.Background()
			for i, state := range everyState() {
				seedBulkJob(t, db, bulkJob{i: i, state: state})
			}
			before := jobsSnapshot(t, db)
			var changed int64
			skipped := map[BulkSkipReason]int64{}
			byState := map[JobState]int64{}
			for _, state := range everyState() {
				byState[state] = 1
				if reason := c.skips[state]; reason == bulkChanges {
					changed++
				} else {
					skipped[reason]++
				}
			}

			spec := c.spec
			spec.Query = byKind
			spec.DryRun = true
			dry, err := BulkJobs(ctx, db, spec)
			require.NoError(t, err)
			assert.Equal(t, before, jobsSnapshot(t, db), "the dry run changes nothing")

			spec.DryRun = false
			res, err := BulkJobs(ctx, db, spec)
			require.NoError(t, err)
			for _, r := range []BulkResult{dry, res} {
				assert.EqualValues(t, len(everyState()), r.Matched)
				assert.Equal(t, changed, r.Changed)
				assert.Equal(t, skipped, r.Skipped)
				assert.Equal(t, byState, r.ByState)
				assert.Equal(t, map[string]int64{"k": int64(len(everyState()))}, r.ByKind)
				assert.True(t, r.Done)
			}

			for _, row := range before {
				if c.skips[JobState(row.State)] == bulkChanges {
					assertBulkChanged(t, db, c.spec, row)
					continue
				}
				assert.Equal(t, row, jobRowByID(t, db, row.ID), "a skipped %s job is unchanged", row.State)
			}
		})
	}
}

// TestBulkApplyRefusesMoreThanMaxJobs proves an apply never acts on more than
// MaxJobs jobs: one whose first count finds more refuses with ErrBulkTooLarge
// and changes nothing; one whose selection grows past MaxJobs while it walks
// stops there with ErrBulkTooLarge and keeps the batches it committed; and one
// that matches exactly MaxJobs is done.
func TestBulkApplyRefusesMoreThanMaxJobs(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	t.Run("more_matches_than_max_jobs_changes_nothing", func(t *testing.T) {
		t.Parallel()
		db := newDB(t)
		seedBulkJobs(t, db, 0, 12, StateAvailable)
		before := jobsSnapshot(t, db)
		res, err := BulkJobs(ctx, db, BulkSpec{Query: byKind, Action: BulkCancel, MaxJobs: 10})
		require.ErrorIs(t, err, ErrBulkTooLarge)
		assert.EqualValues(t, 10, res.Matched)
		assert.True(t, res.MatchedCapped)
		assert.Zero(t, res.Changed)
		assert.False(t, res.Done)
		assert.Equal(t, before, jobsSnapshot(t, db), "a refused apply changes nothing")
		assert.Zero(t, countRows(t, db, &jobAuditRow{}))
	})

	t.Run("a_selection_that_grows_past_max_jobs_stops_there", func(t *testing.T) {
		t.Parallel()
		db := newDB(t)
		ids := seedBulkJobs(t, db, 0, 10, StateAvailable)
		// The first batch's update commits a newer matching job beside its own
		// changes, as a concurrent enqueue would.
		var grown atomic.Bool
		late := bulkID(50)
		require.NoError(t, db.Callback().Update().After("gorm:update").Register("test:grow_selection",
			func(tx *gorm.DB) {
				if grown.Swap(true) {
					return
				}
				row := jobRow{
					ID: late, Kind: "k", Queue: "q", State: string(StateAvailable), MaxAttempts: 5,
					CreatedAt: bulkDay, UpdatedAt: bulkDay, ScheduledAt: bulkDay, Args: []byte("{}"),
				}
				if err := tx.Session(&gorm.Session{NewDB: true}).Create(&row).Error; err != nil {
					_ = tx.AddError(err)
				}
			}))

		res, err := BulkJobs(ctx, db, BulkSpec{Query: byKind, Action: BulkCancel, MaxJobs: 10, BatchSize: 5})
		require.ErrorIs(t, err, ErrBulkTooLarge)
		require.True(t, grown.Load())
		assert.EqualValues(t, 10, res.Matched)
		assert.EqualValues(t, 10, res.Changed, "the committed batches stay")
		assert.Equal(t, 2, res.Batches)
		assert.False(t, res.Done)
		assert.Equal(t, ids[9], res.ResumeAfter)
		for _, id := range ids {
			assert.Equal(t, string(StateCancelled), jobState(t, db, id))
		}
		assert.Equal(t, string(StateAvailable), jobState(t, db, late), "the job past MaxJobs is never acted on")
	})

	t.Run("exactly_max_jobs_matches_is_done", func(t *testing.T) {
		t.Parallel()
		db := newDB(t)
		seedBulkJobs(t, db, 0, 10, StateAvailable)
		res, err := BulkJobs(ctx, db, BulkSpec{Query: byKind, Action: BulkCancel, MaxJobs: 10, BatchSize: 5})
		require.NoError(t, err)
		assert.EqualValues(t, 10, res.Matched)
		assert.False(t, res.MatchedCapped)
		assert.EqualValues(t, 10, res.Changed)
		assert.True(t, res.Done)
	})
}

// bulkMaxIDSuite proves MaxID pins an apply to what a preview matched: twenty
// available jobs of kind k with a paused one among them, previewed, then either
// a newer matching job inserted, which the apply bounded by the preview's
// ResumeAfter never matches, or the paused job, older than the ceiling, moved
// into the selection, which makes the apply refuse with ErrBulkTooLarge and
// change nothing.
func bulkMaxIDSuite(t *testing.T, open dbOpener) {
	t.Helper()
	ctx := context.Background()
	query := JobQuery{Kinds: []string{"k"}, States: []JobState{StateAvailable}}
	seed := func(t *testing.T, db *gorm.DB) ([]string, string) {
		t.Helper()
		var ids []string
		for i := range 21 {
			if i == 10 {
				continue
			}
			ids = append(ids, seedBulkJob(t, db, bulkJob{i: i, state: StateAvailable}))
		}
		return ids, seedBulkJob(t, db, bulkJob{i: 10, state: StatePaused})
	}
	preview := func(t *testing.T, db *gorm.DB, maxID string) BulkResult {
		t.Helper()
		res, err := BulkJobs(ctx, db, BulkSpec{Query: query, Action: BulkCancel, DryRun: true, MaxID: maxID})
		require.NoError(t, err)
		return res
	}

	t.Run("a_job_inserted_after_the_preview_is_never_matched", func(t *testing.T) {
		t.Parallel()
		db := open(t)
		ids, paused := seed(t, db)
		seen := preview(t, db, "")
		require.EqualValues(t, 20, seen.Matched)
		require.Equal(t, ids[19], seen.ResumeAfter, "the preview's ceiling is the newest id it matched")

		late := seedBulkJob(t, db, bulkJob{i: 21, state: StateAvailable})
		assert.EqualValues(t, 20, preview(t, db, seen.ResumeAfter).Matched, "MaxID bounds a dry run too")
		res, err := BulkJobs(ctx, db, BulkSpec{
			Query: query, Action: BulkCancel, MaxID: seen.ResumeAfter, MaxJobs: int(seen.Matched), BatchSize: 6,
		})
		require.NoError(t, err)
		assert.EqualValues(t, 20, res.Matched)
		assert.EqualValues(t, 20, res.Changed, "it acts on exactly the previewed jobs")
		assert.True(t, res.Done)
		for _, id := range ids {
			assert.Equal(t, string(StateCancelled), jobState(t, db, id))
		}
		assert.Equal(t, string(StateAvailable), jobState(t, db, late), "a job above MaxID is never acted on")
		assert.Equal(t, string(StatePaused), jobState(t, db, paused))
	})

	t.Run("a_job_that_moves_in_below_the_ceiling_refuses_the_apply", func(t *testing.T) {
		t.Parallel()
		db := open(t)
		_, paused := seed(t, db)
		seen := preview(t, db, "")
		require.EqualValues(t, 20, seen.Matched)

		require.NoError(t, db.Model(&jobRow{}).Where("id = ?", paused).Update("state", string(StateAvailable)).Error)
		before := jobsSnapshot(t, db)
		res, err := BulkJobs(ctx, db, BulkSpec{
			Query: query, Action: BulkCancel, MaxID: seen.ResumeAfter, MaxJobs: int(seen.Matched),
		})
		require.ErrorIs(t, err, ErrBulkTooLarge, "the selection grew since the preview")
		assert.EqualValues(t, 20, res.Matched)
		assert.True(t, res.MatchedCapped)
		assert.Zero(t, res.Changed)
		assert.Equal(t, before, jobsSnapshot(t, db), "the refused apply changes nothing")
		assert.Zero(t, countRows(t, db, &jobAuditRow{}))
	})
}

// TestBulkNeverMatchesAJobAboveMaxID runs the id-ceiling suite on SQLite.
func TestBulkNeverMatchesAJobAboveMaxID(t *testing.T) {
	t.Parallel()
	bulkMaxIDSuite(t, sqliteOpener)
}

// seedRunningBulkJob writes job i of kind k running its first attempt, under a
// lease, with that attempt's run started.
func seedRunningBulkJob(t testing.TB, db *gorm.DB, i int) string {
	t.Helper()
	id := seedBulkJob(t, db, bulkJob{i: i, state: StateRunning})
	until := bulkDay.Add(24 * time.Hour)
	token := "lease-" + id
	require.NoError(t, db.Model(&jobRow{}).Where("id = ?", id).
		Updates(map[string]any{"leased_until": until, "lease_token": token}).Error)
	seedBulkRun(t, db, id, 1, OutcomeStarted)
	return id
}

// seedBulkRun writes job id's run of attempt with outcome.
func seedBulkRun(t testing.TB, db *gorm.DB, id string, attempt int, outcome RunOutcome) {
	t.Helper()
	at := bulkDay.Add(time.Minute)
	row := jobRunRow{
		ID: fmt.Sprintf("run-%s-%d", id, attempt), JobID: id, Attempt: attempt, Kind: "k", Queue: "q",
		ExecutorID: "exec", StartedAt: at, Outcome: string(outcome), CreatedAt: at,
	}
	if outcome != OutcomeStarted {
		finished := at.Add(time.Second)
		row.FinishedAt = &finished
	}
	seedRun(t, db, row)
}

// TestBulkNeverActsOnARunningJob proves no action changes a running job, a
// forced retry that asks for AllowRunning included, each skipped as running in
// a dry run and an apply; and that a forced retry skips as running a job
// cancelled while its claimed attempt still runs, and retries a cancelled job
// whose attempt returned and one never claimed.
func TestBulkNeverActsOnARunningJob(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	at := bulkDay.Add(48 * time.Hour)
	cases := []struct {
		name  string
		spec  BulkSpec
		other JobState
	}{
		{"cancel", BulkSpec{Action: BulkCancel}, StateAvailable},
		{"retry", BulkSpec{Action: BulkRetry}, StateRetryable},
		{"forced_retry_asking_for_allow_running", BulkSpec{
			Action: BulkRetry, Retry: RetryOpts{Force: true, AllowRunning: true},
		}, StateDiscarded},
		{"delete", BulkSpec{Action: BulkDelete}, StateSucceeded},
		{"reschedule", BulkSpec{Action: BulkReschedule, RescheduleAt: at}, StateScheduled},
		{"pause", BulkSpec{Action: BulkPause}, StateAvailable},
		{"resume", BulkSpec{Action: BulkResume}, StatePaused},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			db := newDB(t)
			running := []string{seedRunningBulkJob(t, db, 0), seedRunningBulkJob(t, db, 1)}
			seedBulkJob(t, db, bulkJob{i: 2, state: c.other})
			before := map[string]jobRow{}
			for _, id := range running {
				before[id] = jobRowByID(t, db, id)
			}
			spec := c.spec
			spec.Query = byKind
			for _, dry := range []bool{true, false} {
				spec.DryRun = dry
				res, err := BulkJobs(ctx, db, spec)
				require.NoError(t, err)
				assert.Equal(t, map[BulkSkipReason]int64{BulkSkipRunning: 2}, res.Skipped, "dry run %v", dry)
				assert.EqualValues(t, 1, res.Changed, "dry run %v", dry)
			}
			for _, id := range running {
				assert.Equal(t, before[id], jobRowByID(t, db, id), "a running job is unchanged, column by column")
				assert.EqualValues(t, 1, runCount(t, db, id), "its running attempt's run is kept")
			}
		})
	}

	t.Run("a_forced_retry_skips_a_job_cancelled_while_its_attempt_still_runs", func(t *testing.T) {
		t.Parallel()
		db := newDB(t)
		fenced := seedRunningBulkJob(t, db, 0)
		require.NoError(t, CancelJob(ctx, db, fenced))
		require.Equal(t, string(StateCancelled), jobState(t, db, fenced))
		returned := seedBulkJob(t, db, bulkJob{i: 1, state: StateCancelled})
		seedBulkRun(t, db, returned, 1, OutcomeCancelled)
		unclaimed := seedBulkJob(t, db, bulkJob{i: 2, state: StateCancelled})
		require.NoError(t, db.Model(&jobRow{}).Where("id = ?", unclaimed).Update("attempt", 0).Error)
		before := jobRowByID(t, db, fenced)

		spec := BulkSpec{Query: byKind, Action: BulkRetry, Retry: RetryOpts{Force: true, AllowRunning: true}}
		for _, dry := range []bool{true, false} {
			spec.DryRun = dry
			res, err := BulkJobs(ctx, db, spec)
			require.NoError(t, err)
			assert.Equal(t, map[BulkSkipReason]int64{BulkSkipRunning: 1}, res.Skipped,
				"the job whose attempt still runs counts as running, dry run %v", dry)
			assert.EqualValues(t, 2, res.Changed, "dry run %v", dry)
			assert.Equal(t, map[JobState]int64{StateCancelled: 3}, res.ByState)
		}
		assert.Equal(t, before, jobRowByID(t, db, fenced), "a retry never starts a second attempt beside one still running")
		assert.Equal(t, string(StateAvailable), jobState(t, db, returned), "a cancelled job whose attempt returned is retried")
		assert.Equal(t, string(StateAvailable), jobState(t, db, unclaimed), "a cancelled job never claimed is retried")
	})
}

// TestBulkForcedRetryNeverRevivesAJobThatLeftTheSelection proves a batch
// re-reads its candidates through the selection under its lock: a selected
// discarded job that a concurrent retry ran to succeeded between the batch's
// read and its transaction stays succeeded, with no entry and in no bucket,
// while every other selected job is retried once.
func TestBulkForcedRetryNeverRevivesAJobThatLeftTheSelection(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	ids := seedBulkJobs(t, db, 0, 6, StateDiscarded)
	left := ids[2]
	var moved atomic.Bool
	ctx := withBulkHooks(context.Background(), bulkHooks{beforeTx: func(batch []string) {
		if moved.Swap(true) {
			return
		}
		assert.Contains(t, batch, left, "the batch read the job before it left")
		finished := bulkDay.Add(time.Hour)
		assert.NoError(t, db.Model(&jobRow{}).Where("id = ?", left).
			Updates(map[string]any{"state": string(StateSucceeded), "finalized_at": finished}).Error)
	}})

	res, err := BulkJobs(ctx, db, BulkSpec{
		Query:  JobQuery{Kinds: []string{"k"}, States: []JobState{StateDiscarded}},
		Action: BulkRetry, Retry: RetryOpts{Force: true},
	})
	require.NoError(t, err)
	require.True(t, moved.Load(), "the hook ran between the batch's read and its transaction")
	assert.EqualValues(t, 6, res.Matched)
	assert.EqualValues(t, 5, res.Changed, "every other selected job is retried")
	assert.Empty(t, res.Skipped, "the job that left the selection is counted in no bucket")
	assert.Equal(t, map[JobState]int64{StateDiscarded: 5}, res.ByState)
	assert.Equal(t, 1, res.Batches)
	assert.Equal(t, string(StateSucceeded), jobState(t, db, left), "a job that left the selection is never revived")
	assert.Empty(t, auditRows(t, db, left), "and gets no entry")
	for _, id := range ids {
		if id != left {
			assert.Equal(t, string(StateAvailable), jobState(t, db, id))
		}
	}
}

// bulkResumeSuite proves an interrupted apply resumes where it stopped: a
// cancel of 1,000 jobs in batches of 100 whose context is cancelled after its
// third batch commits returns 300 changed, not done, its ResumeAfter the 300th
// id, and an error wrapping context.Canceled; resumed with that as After, it
// changes the other 700 and is done, so each job is cancelled exactly once; and
// a run resumed from the end finds nothing.
func bulkResumeSuite(t *testing.T, open dbOpener) {
	t.Helper()
	db := open(t)
	ids := seedBulkJobs(t, db, 0, 1000, StateAvailable)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	batches := 0
	hooked := withBulkHooks(ctx, bulkHooks{afterBatch: func(BulkResult) {
		if batches++; batches == 3 {
			cancel()
		}
	}})

	spec := BulkSpec{Query: byKind, Action: BulkCancel, BatchSize: 100}
	res, err := BulkJobs(hooked, db, spec)
	require.ErrorIs(t, err, context.Canceled)
	assert.Contains(t, err.Error(), "flywheel: bulk cancel cancelled after 300 changed")
	assert.EqualValues(t, 1000, res.Matched)
	assert.EqualValues(t, 300, res.Changed)
	assert.Empty(t, res.Skipped)
	assert.Equal(t, 3, res.Batches)
	assert.False(t, res.Done)
	assert.Equal(t, ids[299], res.ResumeAfter, "the last id the run passed")
	var cancelled int64
	require.NoError(t, db.Model(&jobRow{}).Where("state = ?", string(StateCancelled)).Count(&cancelled).Error)
	assert.EqualValues(t, 300, cancelled, "the committed batches stay")

	spec.After = res.ResumeAfter
	rest, err := BulkJobs(context.Background(), db, spec)
	require.NoError(t, err)
	assert.EqualValues(t, 700, rest.Matched, "the jobs past After")
	assert.EqualValues(t, 700, rest.Changed)
	assert.Empty(t, rest.Skipped, "no job is passed twice: a repeat would be skipped as terminal")
	assert.Equal(t, 7, rest.Batches)
	assert.True(t, rest.Done)
	assert.Equal(t, ids[999], rest.ResumeAfter)
	for _, id := range ids {
		require.Equal(t, string(StateCancelled), jobState(t, db, id))
	}

	spec.After = rest.ResumeAfter
	end, err := BulkJobs(context.Background(), db, spec)
	require.NoError(t, err)
	assert.Zero(t, end.Matched)
	assert.True(t, end.Done)
	assert.Equal(t, ids[999], end.ResumeAfter, "a run that passes nothing keeps its After")
}

// assertBulkActsOnWhatQueryJobsLists proves BulkJobs selects exactly what
// QueryJobs lists for q on db: a forced retry's dry run matches every listed
// job and would change all but the running ones, its sample those, newest
// first; and its apply, under a clock that stamps what it changes, retries
// exactly those.
func assertBulkActsOnWhatQueryJobsLists(t *testing.T, db *gorm.DB, q JobQuery) {
	t.Helper()
	ctx := context.Background()
	listQuery := q
	listQuery.Limit = MaxReadLimit
	page, err := QueryJobs(ctx, db, listQuery)
	require.NoError(t, err)
	require.Empty(t, page.NextCursor, "one page lists every match")
	var changeable []string
	running := int64(0)
	for _, item := range page.Items {
		if item.State == string(StateRunning) {
			running++
			continue
		}
		changeable = append(changeable, item.ID)
	}
	skipped := map[BulkSkipReason]int64{}
	if running > 0 {
		skipped[BulkSkipRunning] = running
	}

	spec := BulkSpec{Query: q, Action: BulkRetry, Retry: RetryOpts{Force: true}, DryRun: true, BatchSize: 3}
	dry, err := BulkJobs(ctx, db, spec)
	require.NoError(t, err)
	assert.EqualValues(t, len(page.Items), dry.Matched, "it matches what QueryJobs lists")
	assert.EqualValues(t, len(changeable), dry.Changed)
	assert.Equal(t, skipped, dry.Skipped)
	if len(changeable) == 0 {
		changeable = []string{}
	}
	assert.Equal(t, changeable, append([]string{}, viewIDs(dry.Sample)...), "newest first, as QueryJobs lists them")

	stamp := time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC)
	spec.DryRun = false
	res, err := BulkJobs(clockCtx(ctx, models.NewFixedClock(stamp)), db, spec)
	require.NoError(t, err)
	assert.EqualValues(t, len(changeable), res.Changed)
	var rows []jobRow
	require.NoError(t, db.Model(&jobRow{}).Select("id", "updated_at").Order("id DESC").Find(&rows).Error)
	acted := []string{}
	for _, r := range rows {
		if r.UpdatedAt.Equal(stamp) {
			acted = append(acted, r.ID)
		}
	}
	assert.Equal(t, changeable, acted, "it retries exactly the listed jobs that aren't running")
}

// TestBulkRetryNeverPutsASecondLiveJobOnAKey proves a forced retry never puts
// a second live job on a UniqueActiveKey: of two discarded jobs sharing a free
// key the first by id is retried and the other skipped, and a discarded job
// whose key a live job holds is skipped, each as active_key, with no
// duplicate-key error, whether the pair shares a batch or not; and a dry run
// counts it the same.
func TestBulkRetryNeverPutsASecondLiveJobOnAKey(t *testing.T) {
	t.Parallel()
	for _, size := range []int{0, 1} {
		t.Run(fmt.Sprintf("in_batches_of_%d", size), func(t *testing.T) {
			t.Parallel()
			db := newDB(t)
			ctx := context.Background()
			first := seedBulkJob(t, db, bulkJob{i: 0, state: StateDiscarded, key: "report-7"})
			second := seedBulkJob(t, db, bulkJob{i: 1, state: StateDiscarded, key: "report-7"})
			held := seedBulkJob(t, db, bulkJob{i: 2, state: StateDiscarded, key: "report-8"})
			holder := seedBulkJob(t, db, bulkJob{i: 3, kind: "other", state: StateAvailable, key: "report-8"})
			plain := seedBulkJob(t, db, bulkJob{i: 4, state: StateDiscarded})

			spec := BulkSpec{Query: byKind, Action: BulkRetry, Retry: RetryOpts{Force: true}, BatchSize: size}
			for _, dry := range []bool{true, false} {
				spec.DryRun = dry
				res, err := BulkJobs(ctx, db, spec)
				require.NoError(t, err, "dry run %v: no duplicate-key error escapes", dry)
				assert.EqualValues(t, 2, res.Changed, "dry run %v", dry)
				assert.Equal(t, map[BulkSkipReason]int64{BulkSkipActiveKey: 2}, res.Skipped, "dry run %v", dry)
			}
			assert.Equal(t, string(StateAvailable), jobState(t, db, first), "the first of the pair goes live")
			assert.Equal(t, string(StateDiscarded), jobState(t, db, second), "the second stays terminal")
			assert.Equal(t, string(StateDiscarded), jobState(t, db, held), "a key a live job holds stays held")
			assert.Equal(t, string(StateAvailable), jobState(t, db, holder))
			assert.Equal(t, string(StateAvailable), jobState(t, db, plain), "an unkeyed job is retried")
		})
	}
}

// TestBulkStaggerSpreadsDueTimesAcrossTheWindow proves a staggered retry or
// reschedule makes job i of n, by id across batches, due at base +
// Stagger·i/n, base being the call's now plus Retry.Delay for a retry and
// RescheduleAt for a reschedule; and that without a stagger every job is due at
// base.
func TestBulkStaggerSpreadsDueTimesAcrossTheWindow(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	cases := []struct {
		name  string
		state JobState
		spec  BulkSpec
		base  time.Time
		step  time.Duration
	}{
		{"a_retry", StateDiscarded, BulkSpec{
			Action: BulkRetry, Retry: RetryOpts{Force: true}, Stagger: 10 * time.Minute,
		}, now, time.Minute},
		{"a_delayed_retry", StateRetryable, BulkSpec{
			Action: BulkRetry, Retry: RetryOpts{Delay: time.Hour}, Stagger: 20 * time.Minute,
		}, now.Add(time.Hour), 2 * time.Minute},
		{"a_reschedule", StateScheduled, BulkSpec{
			Action: BulkReschedule, RescheduleAt: now.Add(2 * time.Hour), Stagger: 10 * time.Minute,
		}, now.Add(2 * time.Hour), time.Minute},
		{"a_retry_without_a_stagger", StateDiscarded, BulkSpec{
			Action: BulkRetry, Retry: RetryOpts{Force: true, Delay: time.Hour},
		}, now.Add(time.Hour), 0},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			db := newDB(t)
			ids := seedBulkJobs(t, db, 0, 10, c.state)
			spec := c.spec
			spec.Query = byKind
			spec.BatchSize = 3
			res, err := BulkJobs(clockCtx(context.Background(), models.NewFixedClock(now)), db, spec)
			require.NoError(t, err)
			require.EqualValues(t, 10, res.Changed)
			for i, id := range ids {
				want := c.base.Add(time.Duration(i) * c.step)
				got := jobRowByID(t, db, id).ScheduledAt
				assert.True(t, got.Equal(want), "job %d is due at %s, not %s", i, want, got)
			}
		})
	}
}

// TestBulkResumesWithoutRepeatingAJob runs the resume suite on SQLite.
func TestBulkResumesWithoutRepeatingAJob(t *testing.T) {
	t.Parallel()
	bulkResumeSuite(t, sqliteOpener)
}

// TestBulkAppliesEveryJobQueryFilter proves a bulk selection is exactly what
// QueryJobs lists for every JobQuery filter, alone and together, the latest-run
// filters included; and that a bulk delete removes its jobs' finish-log
// entries, then their runs, then their rows, in retention's order, and keeps
// their history.
func TestBulkAppliesEveryJobQueryFilter(t *testing.T) {
	t.Parallel()
	for _, c := range queryFilterCases() {
		if c.name == "no_filter" {
			continue // ErrBulkUnbounded
		}
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			db := newDB(t)
			assertBulkActsOnWhatQueryJobsLists(t, db, c.q(seedQueryFixture(t, db)))
		})
	}
	for _, c := range latestRunFilterCases() {
		t.Run("latest_run_"+c.name, func(t *testing.T) {
			t.Parallel()
			db := newDB(t)
			assertBulkActsOnWhatQueryJobsLists(t, db, c.q(seedLatestRunFixture(t, db)))
		})
	}

	t.Run("a_delete_in_retentions_order_keeping_the_history", func(t *testing.T) {
		t.Parallel()
		db := newDB(t)
		ctx := context.Background()
		targets := []string{
			seedBulkJob(t, db, bulkJob{i: 0, state: StateDiscarded}),
			seedBulkJob(t, db, bulkJob{i: 1, state: StateSucceeded}),
		}
		neighbor := seedBulkJob(t, db, bulkJob{i: 2, kind: "other", state: StateDiscarded})
		for _, id := range append([]string{neighbor}, targets...) {
			seedBulkRun(t, db, id, 1, OutcomeError)
		}
		require.NoError(t, db.Exec(fmt.Sprintf(finishLogInsert, "job_runs", "")).Error)
		require.NoError(t, recordAudit(ctx, db, auditOperator, bulkDay, []auditChange{
			{target: AuditTargetJob, targetID: targets[0], action: AuditRetried},
			{target: AuditTargetJob, targetID: targets[1], action: AuditRetried},
		}))
		var tables []string
		require.NoError(t, db.Callback().Delete().After("gorm:delete").Register("test:delete_order",
			func(tx *gorm.DB) { tables = append(tables, tx.Statement.Table) }))

		res, err := BulkJobs(ctx, db, BulkSpec{Query: byKind, Action: BulkDelete})
		require.NoError(t, err)
		assert.EqualValues(t, 2, res.Changed)
		assert.Equal(t, []string{"job_run_finishes", "job_runs", "jobs"}, tables,
			"finish-log entries, then runs, then the jobs")
		for _, id := range targets {
			var left int64
			require.NoError(t, db.Unscoped().Model(&jobRow{}).Where("id = ?", id).Count(&left).Error)
			assert.Zero(t, left, "the job is hard-deleted")
			assert.Zero(t, runCount(t, db, id), "its runs are deleted")
			assert.Empty(t, finishEntries(t, db, fmt.Sprintf("run-%s-1", id)), "its finish-log entry is deleted")
			entries := auditRows(t, db, id)
			require.NotEmpty(t, entries)
			assert.Equal(t, string(AuditRetried), entries[0].Action, "the job's history outlives it")
		}
		assert.Equal(t, string(StateDiscarded), jobState(t, db, neighbor), "another job is untouched")
		assert.EqualValues(t, 1, runCount(t, db, neighbor))
		assert.Len(t, finishEntries(t, db, fmt.Sprintf("run-%s-1", neighbor)), 1)
	})
}

// bulkAuditActor is the actor the bulk audit cases run under.
//
//nolint:gochecknoglobals // shared fixture
var bulkAuditActor = Actor{Type: ActorUser, ID: "u1", Reason: "replay the outage", Source: "console", RequestID: "req-bulk"}

// TestBulkAuditsEveryChangedJobWithItsOperation proves each job a bulk action
// changes gets exactly one entry, in its batch's transaction: the action, the
// context's actor and reason, the call's clock reading, and details holding
// via bulk, the operation id, the state it changed from, and the action's own
// keys; a skipped job gets none. An audit hook that fails on the second batch
// rolls that batch back, its jobs unchanged and with no entries, and the call
// returns the first batch's result with the hook's error.
func TestBulkAuditsEveryChangedJobWithItsOperation(t *testing.T) {
	t.Parallel()
	at := time.Date(2026, 4, 1, 9, 0, 0, 0, time.UTC)
	ctx := clockCtx(WithActor(context.Background(), bulkAuditActor), models.NewFixedClock(at))
	op := map[string]any{"via": "bulk", "operation_id": "op-1"}
	cases := []struct {
		name    string
		spec    BulkSpec
		from    JobState
		action  AuditAction
		details func(t *testing.T, db *gorm.DB, before jobRow) map[string]any
	}{
		{"retry", BulkSpec{Action: BulkRetry, Retry: RetryOpts{
			Force: true, AllowRunning: true, ResetAttempts: true, Budget: 3, Delay: time.Minute,
		}}, StateDiscarded, AuditRetried, func(*testing.T, *gorm.DB, jobRow) map[string]any {
			return withKeys(retryKeys(StateDiscarded, true, false, true, 3, 60000), op)
		}},
		{
			"cancel",
			BulkSpec{Action: BulkCancel},
			StateAvailable, AuditCancelled,
			func(*testing.T, *gorm.DB, jobRow) map[string]any {
				return withKeys(map[string]any{"from_state": "available", "was_running": false}, op)
			},
		},
		{
			"delete",
			BulkSpec{Action: BulkDelete},
			StateSucceeded, AuditDeleted,
			func(*testing.T, *gorm.DB, jobRow) map[string]any {
				return withKeys(map[string]any{"from_state": "succeeded", "kind": "k", "state": "succeeded"}, op)
			},
		},
		{
			"reschedule",
			BulkSpec{Action: BulkReschedule, RescheduleAt: at.Add(time.Hour), Stagger: 9 * time.Minute},
			StateScheduled, AuditRescheduled, func(t *testing.T, db *gorm.DB, before jobRow) map[string]any {
				t.Helper()
				return withKeys(map[string]any{
					"from_state": "scheduled",
					"from":       before.ScheduledAt.UTC().Format(time.RFC3339Nano),
					"to":         jobRowByID(t, db, before.ID).ScheduledAt.UTC().Format(time.RFC3339Nano),
				}, op)
			},
		},
		{
			"pause",
			BulkSpec{Action: BulkPause},
			StateRetryable, AuditPaused,
			func(*testing.T, *gorm.DB, jobRow) map[string]any {
				return withKeys(map[string]any{"from_state": "retryable"}, op)
			},
		},
		{
			"resume",
			BulkSpec{Action: BulkResume},
			StatePaused, AuditResumed,
			func(*testing.T, *gorm.DB, jobRow) map[string]any {
				return withKeys(map[string]any{"from_state": "paused"}, op)
			},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			db := newDB(t)
			ids := seedBulkJobs(t, db, 0, 3, c.from)
			running := seedRunningBulkJob(t, db, 3)
			before := map[string]jobRow{}
			for _, id := range ids {
				before[id] = jobRowByID(t, db, id)
			}
			spec := c.spec
			spec.Query = byKind
			spec.OperationID = "op-1"
			spec.BatchSize = 2
			res, err := BulkJobs(ctx, db, spec)
			require.NoError(t, err)
			require.EqualValues(t, 3, res.Changed)
			assert.Equal(t, 2, res.Batches)
			for _, id := range ids {
				row := onlyAuditRow(t, db, id)
				assert.Equal(t, string(AuditTargetJob), row.TargetType)
				assert.Equal(t, string(c.action), row.Action)
				assert.Equal(t, string(ActorUser), row.ActorType)
				assert.Equal(t, "u1", row.ActorID)
				assert.Equal(t, "replay the outage", row.Reason)
				assert.Equal(t, "console", row.Source)
				assert.Equal(t, "req-bulk", row.RequestID)
				assert.True(t, row.CreatedAt.Equal(at), "stamped with the call's clock reading")
				assert.Equal(t, c.details(t, db, before[id]), auditDetails(t, row), "%s's details", id)
			}
			assert.Empty(t, auditRows(t, db, running), "a skipped job gets no entry")
			assert.EqualValues(t, 3, countRows(t, db, &jobAuditRow{}), "one entry per changed job")
		})
	}

	t.Run("a_hook_that_fails_on_the_second_batch_rolls_it_back", func(t *testing.T) {
		t.Parallel()
		db := newDB(t)
		ids := seedBulkJobs(t, db, 0, 4, StateAvailable)
		errHook := errors.New("the host's record failed")
		var calls int
		var first []AuditEntry
		hooked := WithAuditHook(ctx, func(_ context.Context, tx *gorm.DB, entries []AuditEntry) error {
			if calls++; calls == 2 {
				return errHook
			}
			first = entries
			return nil
		})
		res, err := BulkJobs(hooked, db, BulkSpec{Query: byKind, Action: BulkCancel, OperationID: "op-1", BatchSize: 2})
		require.ErrorIs(t, err, errHook)
		assert.Equal(t, 2, calls, "the hook runs once per batch")
		assert.EqualValues(t, 2, res.Changed, "the first batch's result")
		assert.Equal(t, 1, res.Batches)
		assert.False(t, res.Done)
		assert.Equal(t, ids[1], res.ResumeAfter, "the second batch is not passed")
		require.Len(t, first, 2)
		assert.Equal(t, []string{ids[0], ids[1]}, []string{first[0].TargetID, first[1].TargetID})
		for _, id := range ids[:2] {
			assert.Equal(t, string(StateCancelled), jobState(t, db, id))
			assert.Len(t, auditRows(t, db, id), 1)
		}
		for _, id := range ids[2:] {
			assert.Equal(t, string(StateAvailable), jobState(t, db, id), "the failed batch rolled back")
			assert.Empty(t, auditRows(t, db, id), "with its entries")
		}
	})
}

// seedBarrierFamily writes parent, a succeeded job on queue "parents" that
// declared a barrier whose stored spec makes its continuation a "rollup" job on
// queue "work" with priority 50, three attempts, and the tag "rollup"; and its
// children on queue "work", one per state, at indexes from from. It returns the
// children's ids.
func seedBarrierFamily(t *testing.T, db *gorm.DB, parent string, from int, states ...JobState) []string {
	t.Helper()
	kind := "rollup"
	spec := `{"args":{"report":"weekly"},"queue":"work","executor_class":"","priority":50,"max_attempts":3,"tags":["rollup"]}`
	finished := bulkDay
	seedJob(t, db, jobRow{
		ID: parent, Kind: "fanout", Queue: "parents", State: string(StateSucceeded), Attempt: 1, MaxAttempts: 5,
		CreatedAt: bulkDay, UpdatedAt: bulkDay, ScheduledAt: bulkDay, FinalizedAt: &finished,
		BarrierKind: &kind, BarrierSpec: []byte(spec),
	})
	ids := make([]string, len(states))
	for k, state := range states {
		ids[k] = seedBulkJob(t, db, bulkJob{i: from + k, kind: "leaf", queue: "work", state: state, parent: parent})
	}
	return ids
}

// TestBulkCancelOfTheLastPendingChildrenFiresTheirParentsBarrier proves a bulk
// cancel that ends a barrier's last pending children enqueues its continuation
// once, from the spec the barrier stored, with the context's request id and no
// hook metadata, and never for a parent with a child still running; that with
// no MaxID and a selection that matches the continuation, later batches never
// cancel the continuation the run enqueued; and that a run ending at MaxJobs is
// not refused for it.
func TestBulkCancelOfTheLastPendingChildrenFiresTheirParentsBarrier(t *testing.T) {
	t.Parallel()
	ctx := ctxutil.WithRequestID(context.Background(), "req-bulk")
	work := JobQuery{Queues: []string{"work"}}

	t.Run("the_continuation_is_enqueued_once_from_the_stored_spec", func(t *testing.T) {
		t.Parallel()
		db := newDB(t)
		children := seedBarrierFamily(t, db, "parent-a", 0, StateAvailable, StateScheduled, StatePaused)
		other := seedBarrierFamily(t, db, "parent-b", 3, StateAvailable, StateRunning)
		res, err := BulkJobs(ctx, db, BulkSpec{Query: work, Action: BulkCancel, MaxID: other[1], BatchSize: 2})
		require.NoError(t, err)
		assert.EqualValues(t, 4, res.Changed)
		assert.Equal(t, map[BulkSkipReason]int64{BulkSkipRunning: 1}, res.Skipped)
		for _, id := range children {
			assert.Equal(t, string(StateCancelled), jobState(t, db, id))
		}

		require.EqualValues(t, 1, countJobsOfKind(t, db, "rollup"), "one continuation: parent-b's child still runs")
		cont := jobRowOfKind(t, db, "rollup")
		require.NotNil(t, cont.UniqueKey)
		assert.Equal(t, "barrier:parent-a", *cont.UniqueKey)
		require.NotNil(t, cont.ParentJobID)
		assert.Equal(t, "parent-a", *cont.ParentJobID)
		assert.Equal(t, "work", cont.Queue, "the queue the spec stored")
		assert.Equal(t, 50, cont.Priority)
		assert.Equal(t, 3, cont.MaxAttempts, "the attempts the spec stored")
		assert.JSONEq(t, `["rollup"]`, string(cont.Tags))
		assert.JSONEq(t, `{"report":"weekly"}`, string(cont.Args))
		assert.JSONEq(t, `{"request_id":"req-bulk"}`, string(cont.Metadata), "the request id, and no hook's metadata")
		assert.Equal(t, string(StateAvailable), cont.State)
		assert.Nil(t, jobRowByID(t, db, "parent-a").BarrierKind, "the fired barrier is cleared")
		assert.NotNil(t, jobRowByID(t, db, "parent-b").BarrierKind, "a barrier with a child still running stays armed")
	})

	t.Run("later_batches_never_cancel_the_continuation_the_run_enqueued", func(t *testing.T) {
		t.Parallel()
		db := newDB(t)
		seedBarrierFamily(t, db, "parent-a", 0, StateAvailable, StateAvailable, StateAvailable)
		res, err := BulkJobs(ctx, db, BulkSpec{Query: work, Action: BulkCancel, BatchSize: 3})
		require.NoError(t, err)
		assert.EqualValues(t, 3, res.Changed)
		assert.Empty(t, res.Skipped)
		assert.True(t, res.Done)
		require.EqualValues(t, 1, countJobsOfKind(t, db, "rollup"))
		assert.Equal(t, string(StateAvailable), jobRowOfKind(t, db, "rollup").State,
			"the continuation sorts after every match, and the run leaves it out")
	})

	t.Run("a_run_that_ends_at_max_jobs_is_not_refused_for_its_continuation", func(t *testing.T) {
		t.Parallel()
		db := newDB(t)
		seedBarrierFamily(t, db, "parent-a", 0, StateAvailable, StateAvailable, StateAvailable)
		res, err := BulkJobs(ctx, db, BulkSpec{Query: work, Action: BulkCancel, MaxJobs: 3, BatchSize: 3})
		require.NoError(t, err)
		assert.EqualValues(t, 3, res.Changed)
		assert.True(t, res.Done)
		assert.Equal(t, string(StateAvailable), jobRowOfKind(t, db, "rollup").State)
	})
}

// TestBulkActsOnExactlyTheGivenIDs proves a spec of IDs selects exactly those
// jobs, each once however often it is named and in whatever order, never a
// missing or soft-deleted one, within After and MaxID, in batches across its
// chunks of ids; and that a dry run counts it the same.
func TestBulkActsOnExactlyTheGivenIDs(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	ctx := context.Background()
	ids := seedBulkJobs(t, db, 0, 8, StateAvailable)
	gone := seedBulkJob(t, db, bulkJob{i: 8, state: StateAvailable})
	require.NoError(t, db.Delete(&jobRow{}, "id = ?", gone).Error)
	untouched := seedBulkJob(t, db, bulkJob{i: 9, state: StateAvailable})

	named := []string{ids[6], ids[1], "missing-000001", ids[1], ids[0], gone, ids[3], ids[7], ids[5]}
	spec := BulkSpec{IDs: named, Action: BulkPause, BatchSize: 2, After: ids[0], MaxID: ids[6]}
	for _, dry := range []bool{true, false} {
		spec.DryRun = dry
		res, err := BulkJobs(ctx, db, spec)
		require.NoError(t, err)
		assert.EqualValues(t, 4, res.Matched, "ids 1, 3, 5, and 6: past After, at or below MaxID, once each")
		assert.EqualValues(t, 4, res.Changed, "dry run %v", dry)
		assert.Empty(t, res.Skipped)
		assert.True(t, res.Done)
	}
	for k, id := range ids {
		want := string(StateAvailable)
		if k == 1 || k == 3 || k == 5 || k == 6 {
			want = string(StatePaused)
		}
		assert.Equal(t, want, jobState(t, db, id), "job %d", k)
	}
	assert.Equal(t, string(StateAvailable), jobState(t, db, untouched), "a job not named is never acted on")
	assert.Equal(t, string(StateAvailable), unscopedJobRow(t, db, gone).State, "a soft-deleted job never matches")
}
