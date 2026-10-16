package core

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/mrz1836/go-foundation/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/datatypes"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// queryDay0 is the first of the query fixture's three created days.
//
//nolint:gochecknoglobals // shared fixture instant
var queryDay0 = time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)

// queryDay returns the start of the fixture's created day n.
func queryDay(n int) time.Time { return queryDay0.Add(time.Duration(n) * 24 * time.Hour) }

// idAt returns a UUIDv7-shaped id whose time is t's millisecond, seq telling
// apart ids of one millisecond, so a test controls exactly where a created
// window's id bounds fall.
func idAt(t time.Time, seq int) string {
	h := fmt.Sprintf("%012x", t.UnixMilli())
	return fmt.Sprintf("%s-%s-7000-8000-%012x", h[:8], h[8:], seq)
}

// recordStatements returns a session of db that records every statement it runs,
// so a test can prove a refusal ran none.
func recordStatements(db *gorm.DB) (*gorm.DB, *sqlRecorder) {
	rec := &sqlRecorder{Interface: logger.Discard}
	return db.Session(&gorm.Session{Logger: rec}), rec
}

// queryJob is one job of the filter fixture. Its id and created_at are its
// created day plus minute; scheduled_at is its created time unless set.
type queryJob struct {
	key       string
	day       int
	minute    int
	kind      string
	queue     string
	state     JobState
	tags      []string
	child     bool // a child of the fixture's parent, "p"
	priority  int
	attempt   int
	scheduled time.Time
	finalized time.Time
	args      string
	metadata  string
}

// queryFixture is the filter fixture: three kinds, two queues, every state,
// tags, a parent and its children, priorities, attempts, and three created days.
// Newest first, its keys read b4 a4 c3 b3 a3 c2 c1 b2 b1 a2 a1 p.
//
//nolint:gochecknoglobals // shared fixture
var queryFixture = []queryJob{
	{
		key: "p", day: 0, minute: 1, kind: "a", queue: "q1", state: StateSucceeded, priority: 100, attempt: 1,
		finalized: queryDay(0).Add(time.Hour), args: `{"n":0}`,
	},
	{
		key: "a1", day: 0, minute: 2, kind: "a", queue: "q1", state: StateAvailable, tags: []string{"x"}, priority: 100,
		args: `{"person_id":"p1","n":1}`, metadata: `{"source":"fixture"}`,
	},
	{
		key: "a2", day: 0, minute: 3, kind: "a", queue: "q2", state: StateRunning, tags: []string{"y"}, child: true,
		priority: 50, attempt: 1, args: `{"person_id":"p2"}`,
	},
	{
		key: "b1", day: 0, minute: 4, kind: "b", queue: "q1", state: StateRetryable, tags: []string{"x", "y"}, child: true,
		priority: 10, attempt: 2, scheduled: queryDay(3).Add(time.Hour),
	},
	{
		key: "b2", day: 1, minute: 0, kind: "b", queue: "q2", state: StateScheduled, priority: 100,
		scheduled: queryDay(3),
	},
	{
		key: "c1", day: 1, minute: 2, kind: "c", queue: "q1", state: StatePaused, tags: []string{"z"}, child: true,
		priority: 200,
	},
	{
		key: "c2", day: 1, minute: 3, kind: "c", queue: "q2", state: StateSucceeded, priority: 100, attempt: 1,
		finalized: queryDay(1).Add(time.Hour),
	},
	{
		key: "a3", day: 1, minute: 4, kind: "a", queue: "q1", state: StateCancelled, tags: []string{"x"}, priority: 100,
		attempt: 1, finalized: queryDay(1).Add(2 * time.Hour), args: `{"person_id":"p1"}`,
	},
	{
		key: "b3", day: 2, minute: 1, kind: "b", queue: "q1", state: StateDiscarded, tags: []string{"y"}, priority: 100,
		attempt: 3, finalized: queryDay(2).Add(time.Hour),
	},
	{
		key: "c3", day: 2, minute: 2, kind: "c", queue: "q2", state: StateDiscarded, child: true, priority: 5,
		attempt: 5, finalized: queryDay(2).Add(2 * time.Hour),
	},
	{key: "a4", day: 2, minute: 3, kind: "a", queue: "q2", state: StateAvailable, priority: 100},
	{
		key: "b4", day: 2, minute: 4, kind: "b", queue: "q1", state: StateSucceeded, tags: []string{"z", "x"},
		priority: 75, attempt: 1, finalized: queryDay(2).Add(3 * time.Hour),
	},
}

// queryIDs maps the fixture's keys to the ids it seeded, and back.
type queryIDs struct {
	id  map[string]string
	key map[string]string
}

// keys returns the fixture keys of a page's items, in page order.
func (ids queryIDs) keys(t *testing.T, page JobPage) []string {
	t.Helper()
	out := make([]string, len(page.Items))
	for i, item := range page.Items {
		k, ok := ids.key[item.ID]
		require.True(t, ok, "an item the fixture did not seed: %s", item.ID)
		out[i] = k
	}
	return out
}

// seedQueryFixture writes the filter fixture and the runs its latest-run cases
// read: p's successful attempt; b1's first attempt, though the job names its
// second; b3's three attempts, the last failing with a long first line of
// two-byte runes; and c3's fifth, failing with a short first line under 4 KiB
// of detail.
func seedQueryFixture(t *testing.T, db *gorm.DB) queryIDs {
	t.Helper()
	ids := queryIDs{id: map[string]string{}, key: map[string]string{}}
	for i, j := range queryFixture {
		ids.id[j.key] = idAt(queryDay(j.day).Add(time.Duration(j.minute)*time.Minute), i)
		ids.key[ids.id[j.key]] = j.key
	}
	for _, j := range queryFixture {
		created := queryDay(j.day).Add(time.Duration(j.minute) * time.Minute)
		row := jobRow{
			ID: ids.id[j.key], Kind: j.kind, Queue: j.queue, State: string(j.state), Priority: j.priority,
			Attempt: j.attempt, MaxAttempts: 25, CreatedAt: created, UpdatedAt: created, ScheduledAt: created,
			Args: datatypes.JSON(j.args), Metadata: datatypes.JSON(j.metadata),
		}
		if !j.scheduled.IsZero() {
			row.ScheduledAt = j.scheduled
		}
		if !j.finalized.IsZero() {
			finalized := j.finalized
			row.FinalizedAt = &finalized
		}
		if j.child {
			parent := ids.id["p"]
			row.ParentJobID = &parent
		}
		if len(j.tags) > 0 {
			tags, err := json.Marshal(j.tags)
			require.NoError(t, err)
			row.Tags = datatypes.JSON(tags)
		}
		seedJob(t, db, row)
	}

	run := func(key string, attempt int, outcome RunOutcome, class ErrorClass, msg string) {
		at := queryDay(3)
		r := jobRunRow{
			ID: fmt.Sprintf("run-%s-%d", key, attempt), JobID: ids.id[key], Attempt: attempt, Kind: "fixture",
			ExecutorID: "exec", StartedAt: at, Outcome: string(outcome), CreatedAt: at,
		}
		if outcome != OutcomeStarted {
			finished := at.Add(1500 * time.Millisecond)
			duration := 1500
			r.FinishedAt = &finished
			r.DurationMs = &duration
		}
		if class != "" {
			c := string(class)
			r.ErrorClass = &c
		}
		if msg != "" {
			r.ErrorMessage = &msg
		}
		seedRun(t, db, r)
	}
	run("p", 1, OutcomeSuccess, "", "")
	run("b1", 1, OutcomeError, ErrorTransient, "first try")
	run("b3", 1, OutcomeError, ErrorTransient, "one")
	run("b3", 2, OutcomeError, ErrorTransient, "two")
	run("b3", 3, OutcomeError, ErrorPermanent, "x"+strings.Repeat("é", 200)+"\nsecond line")
	run("c3", 5, OutcomeError, ErrorPermanent, "boom: first line\n"+strings.Repeat("s", 4096-len("boom: first line\n")))
	return ids
}

// queryFilterCase is one query of the filter fixture and the keys it lists,
// newest first.
type queryFilterCase struct {
	name string
	q    func(ids queryIDs) JobQuery
	want []string
}

// intp returns a pointer to v, for JobQuery's optional bounds.
func intp(v int) *int { return &v }

// queryFilterCases are the filter fixture's queries: each field alone, then
// several together.
func queryFilterCases() []queryFilterCase {
	sub := func(d time.Duration) time.Time { return queryDay(1).Add(d) }
	return []queryFilterCase{
		{
			"no_filter", func(queryIDs) JobQuery { return JobQuery{} },
			[]string{"b4", "a4", "c3", "b3", "a3", "c2", "c1", "b2", "b1", "a2", "a1", "p"},
		},
		{
			"states", func(queryIDs) JobQuery { return JobQuery{States: []JobState{StateDiscarded, StateRetryable}} },
			[]string{"c3", "b3", "b1"},
		},
		{
			"one_state", func(queryIDs) JobQuery { return JobQuery{States: []JobState{StateSucceeded}} },
			[]string{"b4", "c2", "p"},
		},
		{
			"one_kind", func(queryIDs) JobQuery { return JobQuery{Kinds: []string{"a"}} },
			[]string{"a4", "a3", "a2", "a1", "p"},
		},
		{
			"kinds", func(queryIDs) JobQuery { return JobQuery{Kinds: []string{"a", "c"}} },
			[]string{"a4", "c3", "a3", "c2", "c1", "a2", "a1", "p"},
		},
		{
			"queues", func(queryIDs) JobQuery { return JobQuery{Queues: []string{"q2"}} },
			[]string{"a4", "c3", "c2", "b2", "a2"},
		},
		{
			"one_tag", func(queryIDs) JobQuery { return JobQuery{Tags: []string{"x"}} },
			[]string{"b4", "a3", "b1", "a1"},
		},
		{
			"any_of_the_tags", func(queryIDs) JobQuery { return JobQuery{Tags: []string{"y", "z"}} },
			[]string{"b4", "b3", "c1", "b1", "a2"},
		},
		{"ids", func(ids queryIDs) JobQuery {
			return JobQuery{IDs: []string{ids.id["a1"], ids.id["c3"], "no-such-job"}}
		}, []string{"c3", "a1"}},
		{
			"parent", func(ids queryIDs) JobQuery { return JobQuery{ParentJobID: ids.id["p"]} },
			[]string{"c3", "c1", "b1", "a2"},
		},
		{
			"priority_min", func(queryIDs) JobQuery { return JobQuery{PriorityMin: intp(50)} },
			[]string{"b4", "a4", "b3", "a3", "c2", "c1", "b2", "a2", "a1", "p"},
		},
		{
			"priority_max", func(queryIDs) JobQuery { return JobQuery{PriorityMax: intp(50)} },
			[]string{"c3", "b1", "a2"},
		},
		{
			"priority_range_inclusive", func(queryIDs) JobQuery { return JobQuery{PriorityMin: intp(50), PriorityMax: intp(100)} },
			[]string{"b4", "a4", "b3", "a3", "c2", "b2", "a2", "a1", "p"},
		},
		{
			"attempt_range_inclusive", func(queryIDs) JobQuery { return JobQuery{AttemptMin: intp(1), AttemptMax: intp(2)} },
			[]string{"b4", "a3", "c2", "b1", "a2", "p"},
		},
		{
			"attempt_min", func(queryIDs) JobQuery { return JobQuery{AttemptMin: intp(3)} },
			[]string{"c3", "b3"},
		},
		{
			"created_window", func(queryIDs) JobQuery { return JobQuery{CreatedFrom: queryDay(1), CreatedTo: queryDay(2)} },
			[]string{"a3", "c2", "c1", "b2"},
		},
		{
			"created_from", func(queryIDs) JobQuery { return JobQuery{CreatedFrom: queryDay(2)} },
			[]string{"b4", "a4", "c3", "b3"},
		},
		{
			"created_to", func(queryIDs) JobQuery { return JobQuery{CreatedTo: queryDay(1)} },
			[]string{"b1", "a2", "a1", "p"},
		},
		// b2's id is the first millisecond of day 1: a bound inside that millisecond
		// is that millisecond's, so it is listed from it and not before it.
		{"created_from_within_a_millisecond", func(queryIDs) JobQuery {
			return JobQuery{CreatedFrom: sub(500 * time.Microsecond), CreatedTo: queryDay(2)}
		}, []string{"a3", "c2", "c1", "b2"}},
		{
			"created_to_within_a_millisecond", func(queryIDs) JobQuery { return JobQuery{CreatedTo: sub(500 * time.Microsecond)} },
			[]string{"b1", "a2", "a1", "p"},
		},
		{
			"scheduled_from", func(queryIDs) JobQuery { return JobQuery{ScheduledFrom: queryDay(3)} },
			[]string{"b2", "b1"},
		},
		{"scheduled_window_half_open", func(queryIDs) JobQuery {
			return JobQuery{ScheduledFrom: queryDay(3), ScheduledTo: queryDay(3).Add(time.Hour)}
		}, []string{"b2"}},
		{
			"finalized_from", func(queryIDs) JobQuery { return JobQuery{FinalizedFrom: queryDay(2)} },
			[]string{"b4", "c3", "b3"},
		},
		{
			"finalized_window", func(queryIDs) JobQuery { return JobQuery{FinalizedFrom: queryDay(1), FinalizedTo: queryDay(2)} },
			[]string{"a3", "c2"},
		},
		{"several_fields", func(queryIDs) JobQuery {
			return JobQuery{
				Kinds: []string{"a", "b"}, Queues: []string{"q1"},
				States: []JobState{StateAvailable, StateRetryable, StateSucceeded},
			}
		}, []string{"b4", "b1", "a1", "p"}},
		{"several_fields_and_a_tag", func(queryIDs) JobQuery {
			return JobQuery{
				Kinds: []string{"a", "b"}, Queues: []string{"q1"}, Tags: []string{"x"},
				States: []JobState{StateAvailable, StateRetryable, StateSucceeded},
			}
		}, []string{"b4", "b1", "a1"}},
		{"kind_parent_and_created", func(ids queryIDs) JobQuery {
			return JobQuery{Kinds: []string{"c"}, ParentJobID: ids.id["p"], CreatedFrom: queryDay(1)}
		}, []string{"c3", "c1"}},
	}
}

// runQueryFilterCases runs each case against the seeded fixture.
func runQueryFilterCases(t *testing.T, db *gorm.DB, ids queryIDs, cases []queryFilterCase) {
	t.Helper()
	ctx := context.Background()
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			page, err := QueryJobs(ctx, db, c.q(ids))
			require.NoError(t, err)
			assert.Equal(t, c.want, ids.keys(t, page))
			assert.Empty(t, page.NextCursor, "one page holds every match")
		})
	}
}

// queryFilterSuite proves QueryJobs applies every filter on the database open
// returns: fields AND together, a field's values OR together, and each item
// carries its view, its metadata, and its latest run's summary.
func queryFilterSuite(t *testing.T, open dbOpener) {
	t.Helper()
	db := open(t)
	ids := seedQueryFixture(t, db)
	ctx := context.Background()

	runQueryFilterCases(t, db, ids, queryFilterCases())

	t.Run("each_item_carries_its_view", func(t *testing.T) {
		page, err := QueryJobs(ctx, db, JobQuery{IDs: []string{ids.id["b1"]}})
		require.NoError(t, err)
		require.Len(t, page.Items, 1)
		item := page.Items[0]
		assert.Equal(t, ids.id["b1"], item.ID)
		assert.Equal(t, "b", item.Kind)
		assert.Equal(t, "q1", item.Queue)
		assert.Equal(t, string(StateRetryable), item.State)
		assert.Equal(t, ids.id["p"], item.ParentJobID)
		assert.Equal(t, 2, item.Attempt)
		assert.Equal(t, 10, item.Priority)
		assert.Equal(t, []string{"x", "y"}, item.Tags)
		assert.True(t, item.EnqueuedAt.Equal(queryDay(0).Add(4*time.Minute)))
		assert.True(t, item.ScheduledAt.Equal(queryDay(3).Add(time.Hour)))
	})

	t.Run("metadata_as_stored", func(t *testing.T) {
		page, err := QueryJobs(ctx, db, JobQuery{IDs: []string{ids.id["a1"], ids.id["p"]}})
		require.NoError(t, err)
		require.Len(t, page.Items, 2)
		assert.JSONEq(t, `{"source":"fixture"}`, string(page.Items[0].Metadata))
		assert.JSONEq(t, `{}`, string(page.Items[1].Metadata))
	})

	t.Run("latest_run_is_the_attempt_the_job_names", func(t *testing.T) {
		page, err := QueryJobs(ctx, db, JobQuery{IDs: []string{
			ids.id["p"], ids.id["a2"], ids.id["b1"], ids.id["b3"], ids.id["c3"],
		}})
		require.NoError(t, err)
		runs := map[string]*RunSummary{}
		for _, item := range page.Items {
			runs[ids.key[item.ID]] = item.LatestRun
		}
		require.Len(t, runs, 5)

		require.NotNil(t, runs["p"])
		assert.Equal(t, "run-p-1", runs["p"].RunID)
		assert.Equal(t, 1, runs["p"].Attempt)
		assert.Equal(t, string(OutcomeSuccess), runs["p"].Outcome)
		assert.Empty(t, runs["p"].ErrorClass)
		assert.Empty(t, runs["p"].Error)
		assert.True(t, runs["p"].StartedAt.Equal(queryDay(3)))
		require.NotNil(t, runs["p"].FinishedAt)
		assert.True(t, runs["p"].FinishedAt.Equal(queryDay(3).Add(1500*time.Millisecond)))
		require.NotNil(t, runs["p"].DurationMs)
		assert.Equal(t, 1500, *runs["p"].DurationMs)

		assert.Nil(t, runs["a2"], "a claim whose run was never written reads as no run")
		assert.Nil(t, runs["b1"], "the run of an earlier attempt is not the attempt the job names")

		require.NotNil(t, runs["b3"])
		assert.Equal(t, "run-b3-3", runs["b3"].RunID)
		assert.Equal(t, 3, runs["b3"].Attempt)
		assert.Equal(t, string(OutcomeError), runs["b3"].Outcome)
		assert.Equal(t, string(ErrorPermanent), runs["b3"].ErrorClass)
		assert.Equal(t, "x"+strings.Repeat("é", 127), runs["b3"].Error,
			"the first line, cut at 256 bytes on a rune boundary")
		assert.True(t, utf8.ValidString(runs["b3"].Error))

		require.NotNil(t, runs["c3"])
		assert.Equal(t, 5, runs["c3"].Attempt)
		assert.Equal(t, "boom: first line", runs["c3"].Error, "the error's first line alone")
	})

	t.Run("metadata_empty_or_null_reads_as_an_empty_object", func(t *testing.T) {
		if db.Name() == "postgres" {
			// A host's schema may allow a NULL metadata.
			require.NoError(t, db.Exec(`ALTER TABLE jobs ALTER COLUMN metadata DROP NOT NULL`).Error)
			require.NoError(t, db.Exec(`UPDATE jobs SET metadata = NULL WHERE id = ?`, ids.id["a4"]).Error)
		} else {
			require.NoError(t, db.Exec(`UPDATE jobs SET metadata = '' WHERE id = ?`, ids.id["a4"]).Error)
		}
		page, err := QueryJobs(ctx, db, JobQuery{IDs: []string{ids.id["a4"]}})
		require.NoError(t, err)
		require.Len(t, page.Items, 1)
		assert.Equal(t, `{}`, string(page.Items[0].Metadata))
	})

	t.Run("created_window_reads_the_id_not_created_at", func(t *testing.T) {
		db := open(t)
		// A simulated clock writes created_at; the id still carries the wall clock.
		simulated := clockCtx(context.Background(), models.NewSimulatedClock(queryDay(1).Add(time.Hour)))
		id, err := Enqueue(simulated, NewClient(db), "query.sim", []byte(`{}`), InsertOpts{})
		require.NoError(t, err)
		job, err := FindJob(ctx, db, id)
		require.NoError(t, err)
		require.True(t, job.EnqueuedAt.Equal(queryDay(1).Add(time.Hour)), "created_at is the simulated clock's")

		page, err := QueryJobs(ctx, db, JobQuery{CreatedFrom: queryDay(1), CreatedTo: queryDay(2)})
		require.NoError(t, err)
		assert.Empty(t, page.Items, "the job's id is not in the window its created_at is")

		page, err = QueryJobs(ctx, db, JobQuery{CreatedFrom: time.Now().Add(-time.Hour)})
		require.NoError(t, err)
		require.Len(t, page.Items, 1, "its id puts it in the last hour")
		assert.Equal(t, id, page.Items[0].ID)
	})

	t.Run("soft_deleted_jobs_never_match", func(t *testing.T) {
		require.NoError(t, db.Delete(&jobRow{ID: ids.id["c2"]}).Error)
		page, err := QueryJobs(ctx, db, JobQuery{Kinds: []string{"c"}})
		require.NoError(t, err)
		assert.Equal(t, []string{"c3", "c1"}, ids.keys(t, page))
	})
}

// TestQueryJobsAppliesEveryFilter runs the filter suite on SQLite.
func TestQueryJobsAppliesEveryFilter(t *testing.T) {
	t.Parallel()
	queryFilterSuite(t, sqliteOpener)
}

// tiedJob is one job of the paging fixture.
type tiedJob struct {
	id        string
	state     JobState
	scheduled time.Time
	finalized *time.Time
}

// seedTiedJobs writes the paging fixture: 23 terminal jobs sharing one
// finalized_at and 23 waiting jobs sharing one scheduled_at, each tie spread
// across three states, and six more of each with times of their own on either
// side of the tie.
func seedTiedJobs(t *testing.T, db *gorm.DB) []tiedJob {
	t.Helper()
	tieFinal := queryDay(0).Add(10 * time.Hour)
	tieSched := queryDay(0).Add(20 * time.Hour)
	terminal := []JobState{StateSucceeded, StateDiscarded, StateCancelled}
	waiting := []JobState{StateAvailable, StateRetryable, StateScheduled}
	var jobs []tiedJob
	seq := 0
	add := func(state JobState, scheduled time.Time, finalized *time.Time) {
		created := queryDay(0).Add(time.Duration(seq) * time.Second)
		j := tiedJob{id: idAt(created, seq), state: state, scheduled: scheduled, finalized: finalized}
		seq++
		seedJob(t, db, jobRow{
			ID: j.id, Kind: "tie", State: string(state), MaxAttempts: 1, CreatedAt: created, UpdatedAt: created,
			ScheduledAt: scheduled, FinalizedAt: finalized,
		})
		jobs = append(jobs, j)
	}
	for i := range 23 {
		at := tieFinal
		add(terminal[i%3], queryDay(0), &at)
		add(waiting[i%3], tieSched, nil)
	}
	for i := range 6 {
		offset := time.Duration(i-3) * time.Minute
		if offset >= 0 {
			offset += time.Minute
		}
		at := tieFinal.Add(offset)
		add(terminal[i%3], queryDay(0), &at)
		add(waiting[i%3], tieSched.Add(offset), nil)
	}
	return jobs
}

// expectedOrder returns the ids of jobs that keep returns true for, in the
// order sort lists them.
func expectedOrder(jobs []tiedJob, sortBy JobSort, keep func(tiedJob) bool) []string {
	var sel []tiedJob
	for _, j := range jobs {
		if keep(j) {
			sel = append(sel, j)
		}
	}
	sort.Slice(sel, func(a, b int) bool {
		x, y := sel[a], sel[b]
		switch sortBy {
		case JobSortOldest:
			return x.id < y.id
		case JobSortFinished:
			if !x.finalized.Equal(*y.finalized) {
				return x.finalized.After(*y.finalized)
			}
			return x.id > y.id
		case JobSortScheduled:
			if !x.scheduled.Equal(y.scheduled) {
				return x.scheduled.Before(y.scheduled)
			}
			return x.id < y.id
		default:
			return x.id > y.id
		}
	})
	out := make([]string, len(sel))
	for i, j := range sel {
		out[i] = j.id
	}
	return out
}

// pageAll reads every page of q at limit, checking each page against the limit
// and the cursor rules, and returns the ids in the order the pages listed them.
func pageAll(t *testing.T, db *gorm.DB, q JobQuery, limit, total int) []string {
	t.Helper()
	q.Limit = limit
	var got []string
	for range total/limit + 2 {
		page, err := QueryJobs(context.Background(), db, q)
		require.NoError(t, err)
		require.LessOrEqual(t, len(page.Items), limit, "a page never holds more than its limit")
		for _, item := range page.Items {
			got = append(got, item.ID)
		}
		if page.NextCursor == "" {
			return got
		}
		require.Len(t, page.Items, limit, "only the last page is short")
		require.True(t, strings.HasPrefix(page.NextCursor, "jq1."), "the cursor is prefixed jq1.: %s", page.NextCursor)
		q.Cursor = page.NextCursor
	}
	t.Fatalf("paging %d jobs at %d a page never reached an empty cursor", total, limit)
	return nil
}

// queryPagingSuite proves every sort pages through ties without skipping or
// repeating a row, on the database open returns.
func queryPagingSuite(t *testing.T, open dbOpener) {
	t.Helper()
	db := open(t)
	jobs := seedTiedJobs(t, db)
	isState := func(states ...JobState) func(tiedJob) bool {
		return func(j tiedJob) bool {
			for _, s := range states {
				if j.state == s {
					return true
				}
			}
			return false
		}
	}
	all := func(tiedJob) bool { return true }
	terminal := TerminalStates()
	waiting := []JobState{StateAvailable, StateRetryable, StateScheduled}

	cases := []struct {
		name string
		q    JobQuery
		keep func(tiedJob) bool
	}{
		{"newest", JobQuery{}, all},
		{"newest_by_name", JobQuery{Sort: JobSortNewest}, all},
		{"oldest", JobQuery{Sort: JobSortOldest}, all},
		{
			"newest_of_some_states",
			JobQuery{States: []JobState{StateSucceeded, StateAvailable}},
			isState(StateSucceeded, StateAvailable),
		},
		{"finished", JobQuery{Sort: JobSortFinished, States: terminal}, isState(terminal...)},
		{"finished_with_a_state_named_twice", JobQuery{
			Sort: JobSortFinished, States: []JobState{StateSucceeded, StateSucceeded, StateDiscarded},
		}, isState(StateSucceeded, StateDiscarded)},
		{"scheduled", JobQuery{Sort: JobSortScheduled, States: waiting}, isState(waiting...)},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			sortBy := c.q.Sort
			want := expectedOrder(jobs, sortBy, c.keep)
			require.NotEmpty(t, want)
			got := pageAll(t, db, c.q, 5, len(want))
			assert.Equal(t, want, got, "every match exactly once, in the sort's order")
		})
	}
}

// TestQueryJobsPagesNeverSkipOrRepeatTiedRows runs the paging suite on SQLite.
func TestQueryJobsPagesNeverSkipOrRepeatTiedRows(t *testing.T) {
	t.Parallel()
	queryPagingSuite(t, sqliteOpener)
}

// TestQueryJobsReturnsAtMost200 proves the page limit: 50 when none is given,
// and never more than MaxReadLimit.
func TestQueryJobsReturnsAtMost200(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	ctx := context.Background()
	items := make([]BatchItem, 250)
	for i := range items {
		items[i] = BatchItem{Kind: "query.many", Args: []byte(`{}`)}
	}
	_, err := InsertMany(ctx, NewClient(db), items, BatchOpts{})
	require.NoError(t, err)

	for _, c := range []struct {
		limit, want int
	}{{0, 50}, {-5, 50}, {7, 7}, {200, 200}, {1000, 200}} {
		page, err := QueryJobs(ctx, db, JobQuery{Limit: c.limit})
		require.NoError(t, err)
		assert.Len(t, page.Items, c.want, "Limit %d", c.limit)
		assert.NotEmpty(t, page.NextCursor, "Limit %d: more jobs follow", c.limit)
	}
	assert.Equal(t, 200, MaxReadLimit)
}

// requireFieldRefusal asserts err is a *ValidationError naming field.
func requireFieldRefusal(t *testing.T, err error, field string) {
	t.Helper()
	require.Error(t, err)
	require.ErrorIs(t, err, ErrValidation)
	var ve *ValidationError
	require.True(t, errors.As(err, &ve), "a *ValidationError: %v", err)
	assert.Equal(t, field, ve.Field, "the refusal names its field: %v", err)
}

// encodeTestCursor builds a jq1. cursor around body, for the malformed cases.
func encodeTestCursor(body string) string {
	return "jq1." + base64.RawURLEncoding.EncodeToString([]byte(body))
}

// TestQueryJobsRefusesACursorFromAnotherSort proves a cursor is bound to its
// sort and refused, malformed or from another sort, before any statement runs.
func TestQueryJobsRefusesACursorFromAnotherSort(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	ctx := context.Background()
	for i, state := range []JobState{StateSucceeded, StateSucceeded, StateAvailable} {
		at := queryDay(0).Add(time.Duration(i) * time.Minute)
		row := jobRow{
			ID: idAt(at, i), Kind: "query.cursor", State: string(state), MaxAttempts: 1,
			CreatedAt: at, UpdatedAt: at, ScheduledAt: at,
		}
		if state == StateSucceeded {
			row.FinalizedAt = &at
		}
		seedJob(t, db, row)
	}

	newest, err := QueryJobs(ctx, db, JobQuery{Limit: 1})
	require.NoError(t, err)
	require.NotEmpty(t, newest.NextCursor)
	next, err := QueryJobs(ctx, db, JobQuery{Limit: 1, Cursor: newest.NextCursor})
	require.NoError(t, err, "a cursor reads the next page of its own sort")
	require.Len(t, next.Items, 1)

	finished, err := QueryJobs(ctx, db, JobQuery{Sort: JobSortFinished, States: []JobState{StateSucceeded}, Limit: 1})
	require.NoError(t, err)
	require.NotEmpty(t, finished.NextCursor)

	refused := []struct {
		name string
		q    JobQuery
	}{
		{"newest_cursor_on_finished", JobQuery{
			Sort: JobSortFinished, States: []JobState{StateSucceeded}, Cursor: newest.NextCursor,
		}},
		{"newest_cursor_on_oldest", JobQuery{Sort: JobSortOldest, Cursor: newest.NextCursor}},
		{"finished_cursor_on_newest", JobQuery{Cursor: finished.NextCursor}},
		{"finished_cursor_on_scheduled", JobQuery{
			Sort: JobSortScheduled, States: []JobState{StateAvailable}, Cursor: finished.NextCursor,
		}},
		{"not_a_cursor", JobQuery{Cursor: "nope"}},
		{"empty_body", JobQuery{Cursor: "jq1."}},
		{"bad_base64", JobQuery{Cursor: "jq1.%%%"}},
		{"another_version", JobQuery{Cursor: "jq2." + strings.TrimPrefix(newest.NextCursor, "jq1.")}},
		{"not_an_object", JobQuery{Cursor: encodeTestCursor(`[]`)}},
		{"no_id", JobQuery{Cursor: encodeTestCursor(`{"s":"newest"}`)}},
		{"no_time_on_a_time_sort", JobQuery{
			Sort: JobSortFinished, States: []JobState{StateSucceeded}, Cursor: encodeTestCursor(`{"s":"finished","id":"x"}`),
		}},
	}
	for _, c := range refused {
		t.Run(c.name, func(t *testing.T) {
			recorded, rec := recordStatements(db)
			_, err := QueryJobs(ctx, recorded, c.q)
			requireFieldRefusal(t, err, "cursor")
			assert.Empty(t, rec.stmt, "a refused cursor runs no statement")
		})
	}
}

// TestQueryJobsRefusesAnInvalidQueryNamingTheField proves each refusal names its
// field and runs no statement, and that a count refuses the same filter.
func TestQueryJobsRefusesAnInvalidQueryNamingTheField(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	ctx := context.Background()
	at := queryDay(1)
	ids := make([]string, MaxReadLimit+1)
	for i := range ids {
		ids[i] = fmt.Sprintf("id-%d", i)
	}
	cases := []struct {
		name   string
		q      JobQuery
		field  string
		filter bool // a filter refusal, which a count shares
	}{
		{"unknown_state", JobQuery{States: []JobState{"sideways"}}, "states", true},
		{"unknown_sort", JobQuery{Sort: "sideways"}, "sort", false},
		{"priority_min_above_max", JobQuery{PriorityMin: intp(5), PriorityMax: intp(1)}, "priority", true},
		{"attempt_min_above_max", JobQuery{AttemptMin: intp(3), AttemptMax: intp(2)}, "attempt", true},
		{"empty_created_window", JobQuery{CreatedFrom: at, CreatedTo: at}, "created", true},
		{"inverted_created_window", JobQuery{CreatedFrom: at, CreatedTo: at.Add(-time.Second)}, "created", true},
		{"inverted_scheduled_window", JobQuery{ScheduledFrom: at, ScheduledTo: at.Add(-time.Second)}, "scheduled", true},
		{"empty_finalized_window", JobQuery{FinalizedFrom: at, FinalizedTo: at}, "finalized", true},
		{"too_many_ids", JobQuery{IDs: ids}, "ids", true},
		{"finished_without_states", JobQuery{Sort: JobSortFinished}, "states", false},
		{"finished_with_a_waiting_state", JobQuery{
			Sort: JobSortFinished, States: []JobState{StateSucceeded, StateRunning},
		}, "states", false},
		{"scheduled_without_states", JobQuery{Sort: JobSortScheduled}, "states", false},
		{"scheduled_with_a_terminal_state", JobQuery{
			Sort: JobSortScheduled, States: []JobState{StateAvailable, StateSucceeded},
		}, "states", false},
		{"malformed_cursor", JobQuery{Cursor: "garbage"}, "cursor", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			recorded, rec := recordStatements(db)
			_, err := QueryJobs(ctx, recorded, c.q)
			requireFieldRefusal(t, err, c.field)
			if c.filter {
				_, err = CountJobs(ctx, recorded, c.q)
				requireFieldRefusal(t, err, c.field)
			}
			assert.Empty(t, rec.stmt, "a refused query runs no statement")
		})
	}
}

// TestContainmentNeedsABoundedWindow proves a containment filter is refused
// unless it is a JSON object searched within a created window of at most
// MaxContainmentWindow, and that exactly that window passes: on SQLite a filter
// that passes is then refused for its dialect alone.
func TestContainmentNeedsABoundedWindow(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	from := queryDay(0)
	object := json.RawMessage(`{"person_id":"p1"}`)
	cases := []struct {
		name  string
		ctx   context.Context
		q     func(c json.RawMessage) JobQuery
		value json.RawMessage
	}{
		{
			"no_created_from", context.Background(),
			func(c json.RawMessage) JobQuery { return JobQuery{ArgsContains: c, CreatedTo: from} }, object,
		},
		{"a_millisecond_too_wide", context.Background(), func(c json.RawMessage) JobQuery {
			return JobQuery{ArgsContains: c, CreatedFrom: from, CreatedTo: from.Add(MaxContainmentWindow + time.Millisecond)}
		}, object},
		{
			"open_ended_past_the_window", clockCtx(context.Background(), models.NewFixedClock(from.Add(8*24*time.Hour))),
			func(c json.RawMessage) JobQuery { return JobQuery{ArgsContains: c, CreatedFrom: from} }, object,
		},
		{"an_array", context.Background(), func(c json.RawMessage) JobQuery {
			return JobQuery{ArgsContains: c, CreatedFrom: from, CreatedTo: from.Add(time.Hour)}
		}, json.RawMessage(`[1]`)},
		{"a_string", context.Background(), func(c json.RawMessage) JobQuery {
			return JobQuery{ArgsContains: c, CreatedFrom: from, CreatedTo: from.Add(time.Hour)}
		}, json.RawMessage(`"x"`)},
		{"not_json", context.Background(), func(c json.RawMessage) JobQuery {
			return JobQuery{ArgsContains: c, CreatedFrom: from, CreatedTo: from.Add(time.Hour)}
		}, json.RawMessage(`{"person_id":`)},
	}
	for _, c := range cases {
		t.Run("args/"+c.name, func(t *testing.T) {
			_, err := QueryJobs(c.ctx, db, c.q(c.value))
			requireFieldRefusal(t, err, "args_contains")
			_, err = CountJobs(c.ctx, db, c.q(c.value))
			requireFieldRefusal(t, err, "args_contains")
		})
		t.Run("metadata/"+c.name, func(t *testing.T) {
			q := c.q(c.value)
			q.MetadataContains, q.ArgsContains = q.ArgsContains, nil
			_, err := QueryJobs(c.ctx, db, q)
			requireFieldRefusal(t, err, "metadata_contains")
		})
	}

	t.Run("exactly_the_window_passes", func(t *testing.T) {
		_, err := QueryJobs(context.Background(), db, JobQuery{
			ArgsContains: object, CreatedFrom: from, CreatedTo: from.Add(MaxContainmentWindow),
		})
		require.ErrorIs(t, err, ErrUnsupportedDialect, "the window passes; SQLite then refuses the filter")
		require.NotErrorIs(t, err, ErrValidation)

		atEdge := clockCtx(context.Background(), models.NewFixedClock(from.Add(MaxContainmentWindow)))
		_, err = QueryJobs(atEdge, db, JobQuery{MetadataContains: object, CreatedFrom: from})
		require.ErrorIs(t, err, ErrUnsupportedDialect, "an open end reads the clock's now")
		require.NotErrorIs(t, err, ErrValidation)
	})
	assert.Equal(t, 7*24*time.Hour, time.Duration(MaxContainmentWindow))
}

// TestContainmentIsRefusedOnSQLite proves a valid containment filter is refused
// on SQLite, which cannot express it, before any statement runs.
func TestContainmentIsRefusedOnSQLite(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	ctx := context.Background()
	q := JobQuery{
		ArgsContains: json.RawMessage(`{"person_id":"p1"}`),
		CreatedFrom:  queryDay(0), CreatedTo: queryDay(1),
	}
	recorded, rec := recordStatements(db)
	_, err := QueryJobs(ctx, recorded, q)
	require.ErrorIs(t, err, ErrUnsupportedDialect)
	require.NotErrorIs(t, err, ErrValidation)
	_, err = CountJobs(ctx, recorded, q)
	require.ErrorIs(t, err, ErrUnsupportedDialect)
	require.NotErrorIs(t, err, ErrValidation)

	q.ArgsContains, q.MetadataContains = nil, json.RawMessage(`{"source":"x"}`)
	_, err = CountJobsWithOptions(ctx, recorded, q, CountOpts{Cap: 5})
	require.ErrorIs(t, err, ErrUnsupportedDialect)
	assert.Empty(t, rec.stmt, "the refusal runs no statement")
}

// The latest-run fixture's stored fingerprints.
const (
	latestFPOne   = "aaaaaaaaaaaaaaaa"
	latestFPTwo   = "bbbbbbbbbbbbbbbb"
	latestFPThree = "cccccccccccccccc"
)

// latestRun is one attempt of a latest-run fixture job.
type latestRun struct {
	outcome     RunOutcome
	class       ErrorClass
	fingerprint string
	message     string
}

// latestRunJob is one job of the latest-run fixture: its key, state, and
// attempt counter, and its runs in attempt order from 1.
type latestRunJob struct {
	key     string
	state   JobState
	attempt int
	runs    []latestRun
}

// latestRunFixture puts each filter's match on a job's latest attempt and its
// decoys on earlier ones: a's first attempt failed as b's only one did, c's
// first failed as the error text cases read, and running's earlier failures sit
// under an attempt still running. legacy's failure was stored by a release
// before fingerprints. The pairs pct/pct2, under/under2, and slash/slash2 differ
// only where an unescaped %, _, or \ would match the other. Newest first, its
// keys read running slash2 slash under2 under pct2 pct c ok legacy b a never.
//
//nolint:gochecknoglobals // shared fixture
var latestRunFixture = []latestRunJob{
	{key: "never", state: StateAvailable},
	{key: "a", state: StateRetryable, attempt: 2, runs: []latestRun{
		{OutcomeError, ErrorPermanent, latestFPOne, "connection refused by upstream"},
		{OutcomeError, ErrorTransient, latestFPTwo, "Connection Refused: retry later"},
	}},
	{key: "b", state: StateDiscarded, attempt: 1, runs: []latestRun{
		{OutcomeError, ErrorPermanent, latestFPOne, "deadline exceeded"},
	}},
	{key: "legacy", state: StateRetryable, attempt: 1, runs: []latestRun{
		{OutcomeError, ErrorPermanent, "", "connection refused"},
	}},
	{key: "ok", state: StateSucceeded, attempt: 1, runs: []latestRun{{outcome: OutcomeSuccess}}},
	{key: "c", state: StateRetryable, attempt: 2, runs: []latestRun{
		{OutcomeError, ErrorPermanent, latestFPOne, "connection refused"},
		{OutcomeTimeout, ErrorTimeout, latestFPThree, "deadline exceeded"},
	}},
	{key: "pct", state: StateRetryable, attempt: 1, runs: []latestRun{{OutcomeError, ErrorValidation, latestFPThree, "rate 100% reached"}}},
	{key: "pct2", state: StateRetryable, attempt: 1, runs: []latestRun{{OutcomeError, ErrorValidation, latestFPThree, "rate 1000 reached"}}},
	{key: "under", state: StateRetryable, attempt: 1, runs: []latestRun{{OutcomeError, ErrorValidation, latestFPThree, "a_b failed"}}},
	{key: "under2", state: StateRetryable, attempt: 1, runs: []latestRun{{OutcomeError, ErrorValidation, latestFPThree, "axb failed"}}},
	{key: "slash", state: StateRetryable, attempt: 1, runs: []latestRun{{OutcomeError, ErrorValidation, latestFPThree, `C:\temp failed`}}},
	{key: "slash2", state: StateRetryable, attempt: 1, runs: []latestRun{{OutcomeError, ErrorValidation, latestFPThree, `C:temp failed`}}},
	{key: "running", state: StateRunning, attempt: 3, runs: []latestRun{
		{OutcomeError, ErrorPermanent, latestFPOne, "connection refused"},
		{OutcomeError, ErrorPermanent, latestFPOne, "connection refused"},
		{outcome: OutcomeStarted},
	}},
}

// seedLatestRunFixture writes the latest-run fixture, each job a minute after
// the last on the query fixture's first day, its runs beside it.
func seedLatestRunFixture(t *testing.T, db *gorm.DB) queryIDs {
	t.Helper()
	ids := queryIDs{id: map[string]string{}, key: map[string]string{}}
	for i, j := range latestRunFixture {
		created := queryDay(0).Add(time.Duration(i) * time.Minute)
		id := idAt(created, i)
		ids.id[j.key], ids.key[id] = id, j.key
		seedJob(t, db, jobRow{
			ID: id, Kind: "latest", Queue: defaultQueue, State: string(j.state), Attempt: j.attempt, MaxAttempts: 25,
			CreatedAt: created, UpdatedAt: created, ScheduledAt: created,
		})
		for n, r := range j.runs {
			at := created.Add(time.Duration(n+1) * time.Second)
			row := jobRunRow{
				ID: fmt.Sprintf("latest-%s-%d", j.key, n+1), JobID: id, Attempt: n + 1, Kind: "latest", Queue: defaultQueue,
				ExecutorID: "exec", StartedAt: at, Outcome: string(r.outcome), CreatedAt: at,
			}
			if r.class != "" {
				c := string(r.class)
				row.ErrorClass = &c
			}
			if r.fingerprint != "" {
				fp := r.fingerprint
				row.ErrorFingerprint = &fp
			}
			if r.message != "" {
				m := r.message
				row.ErrorMessage = &m
			}
			seedRun(t, db, row)
		}
	}
	return ids
}

// latestRunFilterCases are the latest-run filters both dialects run, each
// listing its keys newest first.
func latestRunFilterCases() []queryFilterCase {
	classes := func(cs ...ErrorClass) func(queryIDs) JobQuery {
		return func(queryIDs) JobQuery { return JobQuery{ErrorClasses: cs} }
	}
	fingerprint := func(fp string) func(queryIDs) JobQuery {
		return func(queryIDs) JobQuery { return JobQuery{Fingerprint: fp} }
	}
	return []queryFilterCase{
		{"one_class", classes(ErrorPermanent), []string{"legacy", "b"}},
		{"another_class", classes(ErrorTransient), []string{"a"}},
		{"any_of_the_classes", classes(ErrorTransient, ErrorTimeout), []string{"c", "a"}},
		{
			"a_class_every_escape_case_shares", classes(ErrorValidation),
			[]string{"slash2", "slash", "under2", "under", "pct2", "pct"},
		},
		{"a_fingerprint_only_an_earlier_attempt_or_a_running_job_also_had", fingerprint(latestFPOne), []string{"b"}},
		{"a_latest_fingerprint", fingerprint(latestFPTwo), []string{"a"}},
		{
			"a_fingerprint_shared_by_many", fingerprint(latestFPThree),
			[]string{"slash2", "slash", "under2", "under", "pct2", "pct", "c"},
		},
		{"a_fingerprint_no_run_stores", fingerprint("0123456789abcdef"), []string{}},
		{
			"a_fingerprint_the_legacy_run_would_have", fingerprint(ErrorFingerprint("latest", ErrorPermanent, "connection refused")),
			[]string{},
		},
		{"a_class_and_a_fingerprint", func(queryIDs) JobQuery {
			return JobQuery{ErrorClasses: []ErrorClass{ErrorPermanent}, Fingerprint: latestFPOne}
		}, []string{"b"}},
		{"a_class_and_a_state", func(queryIDs) JobQuery {
			return JobQuery{ErrorClasses: []ErrorClass{ErrorPermanent}, States: []JobState{StateDiscarded}}
		}, []string{"b"}},
		{"a_fingerprint_and_a_kind", func(queryIDs) JobQuery {
			return JobQuery{Fingerprint: latestFPTwo, Kinds: []string{"latest"}}
		}, []string{"a"}},
		{"a_fingerprint_and_another_kind", func(queryIDs) JobQuery {
			return JobQuery{Fingerprint: latestFPTwo, Kinds: []string{"other"}}
		}, []string{}},
	}
}

// TestLatestRunFiltersMatchOnlyTheLatestAttempt proves ErrorClasses and
// Fingerprint match a job by the run of the attempt it names: an earlier
// attempt's failure never matches, a job never claimed or whose attempt still
// runs matches none, and a run stored without a fingerprint never matches one.
func TestLatestRunFiltersMatchOnlyTheLatestAttempt(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	runQueryFilterCases(t, db, seedLatestRunFixture(t, db), latestRunFilterCases())
}

// TestErrorContainsNeedsABoundedWindowAndIsRefusedOnSQLite proves ErrorContains
// searches only a created window of at most MaxContainmentWindow, refused
// before any statement as error_contains, and that SQLite, which cannot run it,
// refuses it with ErrUnsupportedDialect after those checks.
func TestErrorContainsNeedsABoundedWindowAndIsRefusedOnSQLite(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	now := queryDay(10)
	ctx := models.WithClock(context.Background(), models.NewFixedClock(now))

	refused := map[string]JobQuery{
		"no_created_from":          {ErrorContains: "refused"},
		"a_window_over_seven_days": {ErrorContains: "refused", CreatedFrom: queryDay(0), CreatedTo: queryDay(8)},
		"an_open_end_too_far_back": {ErrorContains: "refused", CreatedFrom: now.Add(-MaxContainmentWindow - time.Minute)},
	}
	for name, q := range refused {
		t.Run(name, func(t *testing.T) {
			session, rec := recordStatements(db)
			_, err := QueryJobs(ctx, session, q)
			requireFieldRefusal(t, err, "error_contains")
			_, err = CountJobs(ctx, session, q)
			requireFieldRefusal(t, err, "error_contains")
			assert.Empty(t, rec.stmt, "a refusal runs no statement")
		})
	}

	for name, q := range map[string]JobQuery{
		"a_bounded_window": {ErrorContains: "refused", CreatedFrom: queryDay(0), CreatedTo: queryDay(7)},
		"an_open_end":      {ErrorContains: "refused", CreatedFrom: now.Add(-MaxContainmentWindow)},
	} {
		t.Run("sqlite_refuses_"+name, func(t *testing.T) {
			session, rec := recordStatements(db)
			_, err := QueryJobs(ctx, session, q)
			require.ErrorIs(t, err, ErrUnsupportedDialect)
			assert.NotErrorIs(t, err, ErrValidation, "the window is valid; only the dialect is refused")
			_, err = CountJobs(ctx, session, q)
			require.ErrorIs(t, err, ErrUnsupportedDialect)
			assert.Empty(t, rec.stmt, "a refusal runs no statement")
		})
	}
}

// TestLatestRunFiltersRefuseABadRequest proves an unknown error class and a
// fingerprint that is not 16 lowercase hex characters are refused before any
// statement runs, each naming its field, by QueryJobs and CountJobs alike.
func TestLatestRunFiltersRefuseABadRequest(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	ctx := context.Background()
	for name, c := range map[string]struct {
		q     JobQuery
		field string
	}{
		"an_unknown_class":          {JobQuery{ErrorClasses: []ErrorClass{ErrorPermanent, "fatal"}}, "error_classes"},
		"an_empty_class":            {JobQuery{ErrorClasses: []ErrorClass{""}}, "error_classes"},
		"a_short_fingerprint":       {JobQuery{Fingerprint: "abc123"}, "fingerprint"},
		"a_long_fingerprint":        {JobQuery{Fingerprint: latestFPOne + "0"}, "fingerprint"},
		"an_uppercase_fingerprint":  {JobQuery{Fingerprint: strings.ToUpper(latestFPOne)}, "fingerprint"},
		"a_fingerprint_not_in_hex":  {JobQuery{Fingerprint: "gggggggggggggggg"}, "fingerprint"},
		"a_class_beside_valid_ones": {JobQuery{Kinds: []string{"latest"}, ErrorClasses: []ErrorClass{"Permanent"}}, "error_classes"},
	} {
		t.Run(name, func(t *testing.T) {
			session, rec := recordStatements(db)
			_, err := QueryJobs(ctx, session, c.q)
			requireFieldRefusal(t, err, c.field)
			_, err = CountJobs(ctx, session, c.q)
			requireFieldRefusal(t, err, c.field)
			assert.Empty(t, rec.stmt, "a refusal runs no statement")
		})
	}
}
