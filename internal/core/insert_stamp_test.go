package core

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mrz1836/go-foundation/ctxutil"
	"github.com/mrz1836/go-foundation/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// stampTestWorker runs fn for its kind, or nothing when fn is nil. Its args stay
// raw JSON, so one type serves every kind a test registers.
type stampTestWorker struct {
	kind string
	fn   func(ctx context.Context, job *Job[json.RawMessage]) (Result, error)
}

func (w stampTestWorker) Kind() string { return w.kind }

func (w stampTestWorker) Work(ctx context.Context, job *Job[json.RawMessage]) (Result, error) {
	if w.fn == nil {
		return Result{}, nil
	}
	return w.fn(ctx, job)
}

// stampRunner builds a single-slot Runner over db's own dialect, polling queues,
// whose follow-ups and barrier continuations take client's options.
func stampRunner(t *testing.T, db *gorm.DB, reg *Registry, client *Client, queues ...string) *Runner {
	t.Helper()
	r, err := NewRunner(RunnerConfig{
		DB:            db,
		Driver:        driverForTest(t, db),
		Registry:      reg,
		Client:        client,
		Queues:        queues,
		ExecutorClass: "local",
		ClaimAnyClass: true,
		Concurrency:   1,
		PollInterval:  2 * time.Millisecond,
	})
	require.NoError(t, err)
	return r
}

// drainWithin runs r until no job it polls is in flight, under a deadline, so a
// job that can never finish fails the test instead of hanging it.
func drainWithin(r *Runner) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	return r.RunUntilIdle(ctx)
}

// requireValidation asserts err carries a *ValidationError for field.
func requireValidation(t *testing.T, err error, field string) {
	t.Helper()
	var ve *ValidationError
	require.ErrorAs(t, err, &ve)
	assert.Equal(t, field, ve.Field)
	require.ErrorIs(t, err, ErrValidation)
}

// jobRowOfKind reads the one job of kind.
func jobRowOfKind(t *testing.T, db *gorm.DB, kind string) jobRow {
	t.Helper()
	var rows []jobRow
	require.NoError(t, db.Model(&jobRow{}).Where("kind = ?", kind).Find(&rows).Error)
	require.Len(t, rows, 1, "one job of kind %q", kind)
	return rows[0]
}

// TestInsertRefusesOversizedMetadataAndBadTags proves every insert path stores
// tags in order with duplicates dropped and metadata merged over the request id,
// and refuses, writing nothing, what it would not store safely.
func TestInsertRefusesOversizedMetadataAndBadTags(t *testing.T) {
	t.Parallel()

	t.Run("tags", func(t *testing.T) {
		t.Parallel()

		t.Run("kept_in_order_without_duplicates", func(t *testing.T) {
			t.Parallel()
			db := newDB(t)
			ctx := ctxutil.WithRequestID(context.Background(), "req-tags")
			id, err := Enqueue(ctx, NewClient(db), "stamp.tags", []byte(`{}`), InsertOpts{
				Tags: []string{"b", "a", "b"}, Metadata: map[string]any{"k": "v"},
			})
			require.NoError(t, err)
			row := jobRowByID(t, db, id)
			assert.JSONEq(t, `["b","a"]`, string(row.Tags))
			assert.JSONEq(t, `{"k":"v","request_id":"req-tags"}`, string(row.Metadata))
			view, err := FindJob(ctx, db, id)
			require.NoError(t, err)
			assert.Equal(t, []string{"b", "a"}, view.Tags)
		})

		t.Run("kept_at_the_limits", func(t *testing.T) {
			t.Parallel()
			db := newDB(t)
			seventeen := make([]string, 0, MaxTags+1)
			for i := range MaxTags {
				seventeen = append(seventeen, fmt.Sprintf("t%02d", i))
			}
			seventeen = append(seventeen, "t03") // a repeat is dropped before the count
			longest := strings.Repeat("x", MaxTagBytes)
			for _, tc := range []struct{ tags, want []string }{
				{seventeen, seventeen[:MaxTags]},
				{[]string{longest}, []string{longest}},
				{[]string{"ünïcode"}, []string{"ünïcode"}},
			} {
				id, err := Enqueue(context.Background(), NewClient(db), "stamp.tags", []byte(`{}`), InsertOpts{Tags: tc.tags})
				require.NoError(t, err)
				view, err := FindJob(context.Background(), db, id)
				require.NoError(t, err)
				assert.Equal(t, tc.want, view.Tags)
			}
		})

		seventeen := make([]string, MaxTags+1)
		for i := range seventeen {
			seventeen[i] = fmt.Sprintf("t%02d", i)
		}
		for name, tags := range map[string][]string{
			"seventeen_distinct": seventeen,
			"empty":              {"ok", ""},
			"65_bytes":           {strings.Repeat("x", MaxTagBytes+1)},
			"control_character":  {"bell\a"},
			"newline":            {"two\nlines"},
			"invalid_utf8":       {"bad\xff"},
		} {
			t.Run("refused_"+name, func(t *testing.T) {
				t.Parallel()
				db := newDB(t)
				_, err := Enqueue(context.Background(), NewClient(db), "stamp.tags", []byte(`{}`), InsertOpts{Tags: tags})
				requireValidation(t, err, "tags")
				for _, tag := range tags {
					if tag != "" {
						assert.NotContains(t, err.Error(), tag, "a refusal names the rule, never the tag")
					}
				}
				assert.Zero(t, countJobsOfKind(t, db, "stamp.tags"), "a refused insert writes nothing")
			})
		}
	})

	t.Run("metadata", func(t *testing.T) {
		t.Parallel()
		// {"pad":"<n x>"} marshals to n+10 bytes.
		pad := func(merged int) map[string]any { return map[string]any{"pad": strings.Repeat("x", merged-10)} }

		t.Run("kept_at_the_limit", func(t *testing.T) {
			t.Parallel()
			db := newDB(t)
			id, err := Enqueue(context.Background(), NewClient(db), "stamp.meta", []byte(`{}`),
				InsertOpts{Metadata: pad(MaxMetadataBytes)})
			require.NoError(t, err)
			assert.Len(t, jobRowByID(t, db, id).Metadata, MaxMetadataBytes)
		})

		hookPad := func(context.Context, InsertInfo) map[string]any { return pad(MaxMetadataBytes + 1) }
		for name, insert := range map[string]func(db *gorm.DB) error{
			"merged_8193_bytes": func(db *gorm.DB) error {
				_, err := Enqueue(context.Background(), NewClient(db), "stamp.meta", []byte(`{}`),
					InsertOpts{Metadata: pad(MaxMetadataBytes + 1)})
				return err
			},
			"unencodable": func(db *gorm.DB) error {
				_, err := Enqueue(context.Background(), NewClient(db), "stamp.meta", []byte(`{}`),
					InsertOpts{Metadata: map[string]any{"ch": make(chan int)}})
				return err
			},
			"a_hook_result_past_the_limit": func(db *gorm.DB) error {
				c := NewClientWithOptions(db, ClientOpts{InsertHook: hookPad})
				_, err := Enqueue(context.Background(), c, "stamp.meta", []byte(`{}`), InsertOpts{})
				return err
			},
		} {
			t.Run("refused_"+name, func(t *testing.T) {
				t.Parallel()
				db := newDB(t)
				requireValidation(t, insert(db), "metadata")
				assert.Zero(t, countJobsOfKind(t, db, "stamp.meta"), "a refused insert writes nothing")
			})
		}
	})

	t.Run("insert_many", func(t *testing.T) {
		t.Parallel()
		db := newDB(t)
		ctx := context.Background()
		items := make([]BatchItem, 10)
		for i := range items {
			items[i] = BatchItem{Kind: "stamp.many", Args: []byte(`{}`), Opts: InsertOpts{Tags: []string{"ok"}}}
		}
		items[6].Opts.Tags = []string{""}
		res, err := InsertMany(ctx, NewClient(db), items, BatchOpts{ChunkSize: 2})
		requireValidation(t, err, "tags")
		assert.Contains(t, err.Error(), "item 6")
		assert.Zero(t, res.Inserted)
		assert.Zero(t, countJobsOfKind(t, db, "stamp.many"), "no chunk is written before every item is checked")

		_, err = InsertManyTyped(ctx, NewClient(db), []stampArgs{{N: 1}, {N: 2}},
			InsertOpts{Metadata: map[string]any{"ch": make(chan int)}}, BatchOpts{})
		requireValidation(t, err, "metadata")
		assert.Zero(t, countJobsOfKind(t, db, stampArgs{}.Kind()))
	})

	t.Run("follow_up", func(t *testing.T) {
		t.Parallel()

		t.Run("stored", func(t *testing.T) {
			t.Parallel()
			db := newDB(t)
			ctx := context.Background()
			reg := NewRegistry()
			Register(reg, stampTestWorker{kind: "stamp.spawner", fn: func(context.Context, *Job[json.RawMessage]) (Result, error) {
				return Result{FollowUps: []FollowUp{{
					Kind: "stamp.child", Tags: []string{"child", "child"}, Metadata: map[string]any{"k": 1},
				}}}, nil
			}})
			Register(reg, stampTestWorker{kind: "stamp.child"})
			_, err := Enqueue(ctx, NewClient(db), "stamp.spawner", []byte(`{}`), InsertOpts{RequestID: "req-spawner"})
			require.NoError(t, err)
			require.NoError(t, drainWithin(stampRunner(t, db, reg, nil, "default")))

			child := jobRowOfKind(t, db, "stamp.child")
			assert.JSONEq(t, `["child"]`, string(child.Tags))
			assert.JSONEq(t, `{"k":1,"request_id":"req-spawner"}`, string(child.Metadata))
		})

		t.Run("refused_fails_the_finalize", func(t *testing.T) {
			t.Parallel()
			db := newDB(t)
			ctx := context.Background()
			reg := NewRegistry()
			Register(reg, stampTestWorker{kind: "stamp.spawner", fn: func(context.Context, *Job[json.RawMessage]) (Result, error) {
				return Result{FollowUps: []FollowUp{{Kind: "stamp.child", Tags: []string{strings.Repeat("x", MaxTagBytes+1)}}}}, nil
			}})
			Register(reg, stampTestWorker{kind: "stamp.child"})
			id, err := Enqueue(ctx, NewClient(db), "stamp.spawner", []byte(`{}`), InsertOpts{})
			require.NoError(t, err)
			err = drainWithin(stampRunner(t, db, reg, nil, "default"))
			requireValidation(t, err, "tags")
			assert.Equal(t, string(StateRunning), jobRowByID(t, db, id).State, "the finalize rolled back")
			assert.Zero(t, countJobsOfKind(t, db, "stamp.child"))
		})
	})
}

// stampArgs is a typed payload for the generic insert paths.
type stampArgs struct{ N int }

func (stampArgs) Kind() string { return "stamp.typed" }

// TestNeitherHookNorCallerReplacesTheRequestID proves the request id is the one
// key of a job's metadata no caller and no hook can set: the call's own id
// stands over both, the context's applies when the call names none, and with
// neither the metadata carries no request_id at all.
func TestNeitherHookNorCallerReplacesTheRequestID(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	hook := func(context.Context, InsertInfo) map[string]any {
		return map[string]any{"request_id": "y", "k": "hook", "h": 1}
	}
	c := NewClientWithOptions(db, ClientOpts{InsertHook: hook})
	caller := map[string]any{"request_id": "x", "k": "caller"}
	withID := ctxutil.WithRequestID(context.Background(), "ctx-id")

	for name, tc := range map[string]struct {
		ctx       context.Context
		requestID string
		want      string
	}{
		"the_call_names_one":      {ctx: withID, requestID: "r1", want: `{"h":1,"k":"hook","request_id":"r1"}`},
		"the_context_carries_one": {ctx: withID, want: `{"h":1,"k":"hook","request_id":"ctx-id"}`},
		"no_request_id_anywhere":  {ctx: context.Background(), want: `{"h":1,"k":"hook"}`},
	} {
		id, err := Enqueue(tc.ctx, c, "stamp.rid", []byte(`{}`), InsertOpts{RequestID: tc.requestID, Metadata: caller})
		require.NoError(t, err, name)
		assert.JSONEq(t, tc.want, string(jobRowByID(t, db, id).Metadata), name)
	}
}

// hookRecorder is an InsertHook that records every call and stamps the row with
// the source it was called for.
type hookRecorder struct {
	mu    sync.Mutex
	calls []InsertInfo
}

func (r *hookRecorder) hook(_ context.Context, in InsertInfo) map[string]any {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, in)
	return map[string]any{"stamped": "hook", "source": string(in.Source)}
}

// list returns the recorded calls.
func (r *hookRecorder) list() []InsertInfo {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.calls)
}

// bySource returns the recorded calls of one source.
func (r *hookRecorder) bySource(src InsertSource) []InsertInfo {
	var out []InsertInfo
	for _, in := range r.list() {
		if in.Source == src {
			out = append(out, in)
		}
	}
	return out
}

// hookedClient returns a recorder and a Client whose InsertHook it is.
func hookedClient(db *gorm.DB) (*hookRecorder, *Client) {
	rec := &hookRecorder{}
	return rec, NewClientWithOptions(db, ClientOpts{InsertHook: rec.hook})
}

// assertStamped asserts the job's metadata carries the hook's keys for src.
func assertStamped(t *testing.T, db *gorm.DB, id string, src InsertSource) {
	t.Helper()
	var meta map[string]any
	require.NoError(t, json.Unmarshal(jobRowByID(t, db, id).Metadata, &meta))
	assert.Equal(t, "hook", meta["stamped"], "job %s carries the hook's keys", id)
	assert.Equal(t, string(src), meta["source"])
}

// hookPathCase is one insert path the hook must reach, run on a fresh database.
type hookPathCase struct {
	name string
	run  func(t *testing.T, db *gorm.DB)
}

// runHookCases runs each case in parallel on a database open returns.
func runHookCases(t *testing.T, open dbOpener, cases []hookPathCase) {
	t.Helper()
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			c.run(t, open(t))
		})
	}
}

// hookClientCases are the four inserts through a Client.
func hookClientCases() []hookPathCase {
	return []hookPathCase{
		{"Insert", func(t *testing.T, db *gorm.DB) {
			rec, c := hookedClient(db)
			parent := "parent-of-the-insert"
			id, err := Insert(context.Background(), c, stampArgs{N: 1}, InsertOpts{Parent: &parent})
			require.NoError(t, err)
			assert.Equal(t, []InsertInfo{{
				Kind: "stamp.typed", Queue: "default", Source: InsertSourceClient, ParentJobID: parent,
			}}, rec.list())
			assertStamped(t, db, id, InsertSourceClient)
		}},
		{"Enqueue", func(t *testing.T, db *gorm.DB) {
			rec, c := hookedClient(db)
			id, err := Enqueue(context.Background(), c, "stamp.raw", []byte(`{}`), InsertOpts{Queue: "q1"})
			require.NoError(t, err)
			assert.Equal(t, []InsertInfo{{Kind: "stamp.raw", Queue: "q1", Source: InsertSourceClient}}, rec.list())
			assertStamped(t, db, id, InsertSourceClient)
		}},
		{"InsertMany", func(t *testing.T, db *gorm.DB) {
			rec, c := hookedClient(db)
			items := make([]BatchItem, 3)
			for i := range items {
				items[i] = BatchItem{Kind: "stamp.many", Args: []byte(`{}`), Opts: InsertOpts{Queue: "bulk"}}
			}
			res, err := InsertMany(context.Background(), c, items, BatchOpts{})
			require.NoError(t, err)
			require.Equal(t, 3, res.Inserted)
			require.Len(t, rec.list(), 3, "one call per row")
			for i, in := range rec.list() {
				assert.Equal(t, InsertInfo{Kind: "stamp.many", Queue: "bulk", Source: InsertSourceClient}, in)
				assertStamped(t, db, res.IDs[i], InsertSourceClient)
			}
		}},
		{"InsertManyTyped", func(t *testing.T, db *gorm.DB) {
			rec, c := hookedClient(db)
			res, err := InsertManyTyped(context.Background(), c, []stampArgs{{N: 1}, {N: 2}}, InsertOpts{}, BatchOpts{})
			require.NoError(t, err)
			require.Len(t, rec.list(), 2, "one call per row")
			for i, in := range rec.list() {
				assert.Equal(t, InsertInfo{Kind: "stamp.typed", Queue: "default", Source: InsertSourceClient}, in)
				assertStamped(t, db, res.IDs[i], InsertSourceClient)
			}
		}},
	}
}

// runSpawningJob runs, through a Runner whose Client is hooked, a job that
// returns a parented and an unparented follow-up and a barrier over the
// parented one, then lets the children and the continuation finish. It returns
// the recorder and the spawning job's id.
func runSpawningJob(t *testing.T, db *gorm.DB) (*hookRecorder, string) {
	t.Helper()
	ctx := context.Background()
	rec, client := hookedClient(db)
	reg := NewRegistry()
	Register(reg, stampTestWorker{kind: "stamp.parent", fn: func(context.Context, *Job[json.RawMessage]) (Result, error) {
		return Result{
			FollowUps: []FollowUp{{Kind: "stamp.child", Parent: true}, {Kind: "stamp.loose"}},
			Barrier:   &Barrier{Kind: "stamp.cont"},
		}, nil
	}})
	for _, kind := range []string{"stamp.child", "stamp.loose", "stamp.cont"} {
		Register(reg, stampTestWorker{kind: kind})
	}
	parentID, err := Enqueue(ctx, NewClient(db), "stamp.parent", []byte(`{}`), InsertOpts{RequestID: "req-parent"})
	require.NoError(t, err)
	require.NoError(t, drainWithin(stampRunner(t, db, reg, client, "default")))
	require.Equal(t, string(StateSucceeded), jobRowOfKind(t, db, "stamp.cont").State)
	return rec, parentID
}

// hookFollowUpCase proves a finalize's follow-ups are stamped as spawned by the
// finishing job.
func hookFollowUpCase(t *testing.T, db *gorm.DB) {
	rec, parentID := runSpawningJob(t, db)
	byKind := map[string]InsertInfo{}
	for _, in := range rec.bySource(InsertSourceFollowUp) {
		byKind[in.Kind] = in
	}
	require.Len(t, byKind, 2, "one call per follow-up")
	assert.Equal(t, InsertInfo{
		Kind: "stamp.child", Queue: "default", Source: InsertSourceFollowUp, SpawnedBy: parentID, ParentJobID: parentID,
	}, byKind["stamp.child"])
	assert.Equal(t, InsertInfo{
		Kind: "stamp.loose", Queue: "default", Source: InsertSourceFollowUp, SpawnedBy: parentID,
	}, byKind["stamp.loose"])
	for _, kind := range []string{"stamp.child", "stamp.loose"} {
		row := jobRowOfKind(t, db, kind)
		assert.JSONEq(t, `{"request_id":"req-parent","stamped":"hook","source":"follow_up"}`, string(row.Metadata))
	}
}

// hookFinalizeBarrierCase proves the continuation a child's finalize fires is
// stamped as spawned by that child.
func hookFinalizeBarrierCase(t *testing.T, db *gorm.DB) {
	rec, parentID := runSpawningJob(t, db)
	child := jobRowOfKind(t, db, "stamp.child")
	assert.Equal(t, []InsertInfo{{
		Kind: "stamp.cont", Queue: "default", Source: InsertSourceBarrier, SpawnedBy: child.ID, ParentJobID: parentID,
	}}, rec.bySource(InsertSourceBarrier))
	assertStamped(t, db, jobRowOfKind(t, db, "stamp.cont").ID, InsertSourceBarrier)
}

// hookPeriodicCase proves a Scheduler's periodic fire is stamped with its
// schedule's slug.
func hookPeriodicCase(t *testing.T, db *gorm.DB) {
	rec, client := hookedClient(db)
	t0 := time.Now().UTC().Truncate(time.Second)
	require.NoError(t, UpsertPeriodic(clockCtx(context.Background(), models.NewFixedClock(t0)), db, PeriodicSpec{
		Slug: "stamp-every-minute", Kind: "stamp.periodic", Every: time.Minute, Active: true,
	}))
	s := newSchedulerCfg(t, SchedulerConfig{DB: db, Client: client})
	n, err := s.Tick(clockCtx(context.Background(), models.NewFixedClock(t0.Add(90*time.Second))))
	require.NoError(t, err)
	require.Equal(t, 1, n)
	assert.Equal(t, []InsertInfo{{
		Kind: "stamp.periodic", Queue: "periodic", Source: InsertSourcePeriodic, PeriodicSlug: "stamp-every-minute",
	}}, rec.list())
	assertStamped(t, db, jobRowOfKind(t, db, "stamp.periodic").ID, InsertSourcePeriodic)
}

// hookSweepBarrierCase proves the continuation the lease sweep fires, by
// discarding a parent's last pending children on their last crashed attempt,
// is stamped as spawned by the last of them in reclaim order.
func hookSweepBarrierCase(t *testing.T, db *gorm.DB) {
	rec, client := hookedClient(db)
	d := driverForTest(t, db)
	s := newSchedulerCfg(t, SchedulerConfig{DB: db, Client: client, Driver: d})
	ctx := context.Background()
	parentID := declareBarrierParent(t, d, db, ctx, "stamp.child", "stamp.cont", 2)
	require.NoError(t, db.Model(&jobRow{}).Where("parent_job_id = ?", parentID).Update("max_attempts", 1).Error)

	first, _ := crashAttempt(t, ctx, d, "default")
	time.Sleep(10 * time.Millisecond) // the second lease expires strictly later
	second, _ := crashAttempt(t, ctx, d, "default")
	firstLease, secondLease := jobRowByID(t, db, first.ID).LeasedUntil, jobRowByID(t, db, second.ID).LeasedUntil
	require.True(t, secondLease.After(*firstLease), "the second crash is reclaimed last")

	sweepPastTheLease(t, s)
	assert.Equal(t, []InsertInfo{{
		Kind: "stamp.cont", Queue: "default", Source: InsertSourceBarrier, SpawnedBy: second.ID, ParentJobID: parentID,
	}}, rec.bySource(InsertSourceBarrier))
	assertStamped(t, db, jobRowOfKind(t, db, "stamp.cont").ID, InsertSourceBarrier)
}

// controlBarrierParent declares a barrier over one child, due an hour from now
// so no runner claims it before a control ends it, and returns the parent and
// child ids.
func controlBarrierParent(t *testing.T, db *gorm.DB) (string, string) {
	t.Helper()
	ctx := context.Background()
	d := driverForTest(t, db)
	ids := seedClaimable(t, db, 1)
	parent := claimOne(t, d, ctx)
	require.Equal(t, ids[0], parent.ID)
	later := time.Now().Add(time.Hour)
	finalizeJob(t, d, ctx, parent, Result{
		FollowUps: []FollowUp{{Kind: "stamp.child", Parent: true, ScheduleAt: &later}},
		Barrier:   &Barrier{Kind: "stamp.cont"},
	}, nil)
	return parent.ID, jobRowOfKind(t, db, "stamp.child").ID
}

// assertUnstampedContinuation asserts the parent's continuation carries the
// request id alone.
func assertUnstampedContinuation(t *testing.T, db *gorm.DB, requestID string) {
	t.Helper()
	cont := jobRowOfKind(t, db, "stamp.cont")
	assert.JSONEq(t, fmt.Sprintf(`{"request_id":%q}`, requestID), string(cont.Metadata))
}

// hookControlCases end a parent's last pending child with a control: each fires
// the continuation with no hook.
func hookControlCases() []hookPathCase {
	return []hookPathCase{
		{"CancelJob", func(t *testing.T, db *gorm.DB) {
			_, childID := controlBarrierParent(t, db)
			require.NoError(t, CancelJob(ctxutil.WithRequestID(context.Background(), "req-control"), db, childID))
			assertUnstampedContinuation(t, db, "req-control")
		}},
		{"CancelByParent", func(t *testing.T, db *gorm.DB) {
			parentID, _ := controlBarrierParent(t, db)
			_, err := CancelByParent(ctxutil.WithRequestID(context.Background(), "req-control"), db, parentID, ScopeOpts{})
			require.NoError(t, err)
			assertUnstampedContinuation(t, db, "req-control")
		}},
		{"in_worker", func(t *testing.T, db *gorm.DB) {
			_, childID := controlBarrierParent(t, db)
			rec, client := hookedClient(db)
			reg := NewRegistry()
			Register(reg, stampTestWorker{kind: "stamp.canceller", fn: func(ctx context.Context, job *Job[json.RawMessage]) (Result, error) {
				var target struct{ Child string }
				if err := json.Unmarshal(job.Args, &target); err != nil {
					return Result{}, err
				}
				return Result{}, CancelJob(ctx, db, target.Child)
			}})
			Register(reg, stampTestWorker{kind: "stamp.cont"})
			_, err := Enqueue(context.Background(), NewClient(db), "stamp.canceller",
				[]byte(fmt.Sprintf(`{"Child":%q}`, childID)), InsertOpts{RequestID: "req-worker"})
			require.NoError(t, err)
			require.NoError(t, drainWithin(stampRunner(t, db, reg, client, "default")))
			assert.Equal(t, string(StateCancelled), jobRowByID(t, db, childID).State)
			assert.Empty(t, rec.list(), "a worker's own context carries no Client, and the continuation inserts nothing")
			assertUnstampedContinuation(t, db, "req-worker")
		}},
	}
}

// TestInsertHookStampsEveryInsertPath proves one hook on a Client reaches every
// insert a host makes and every one the runtime makes for it: inserts through
// the Client, follow-ups and continuations through RunnerConfig.Client, and
// periodic fires and the sweep's continuations through SchedulerConfig.Client.
// A continuation a control fires runs no hook.
func TestInsertHookStampsEveryInsertPath(t *testing.T) {
	t.Parallel()
	t.Run("client", func(t *testing.T) {
		t.Parallel()
		runHookCases(t, sqliteOpener, hookClientCases())
	})
	runHookCases(t, sqliteOpener, []hookPathCase{
		{"follow_up", hookFollowUpCase},
		{"finalize_barrier", hookFinalizeBarrierCase},
		{"periodic", hookPeriodicCase},
		{"sweep_barrier", hookSweepBarrierCase},
	})
	t.Run("control", func(t *testing.T) {
		t.Parallel()
		runHookCases(t, sqliteOpener, hookControlCases())
	})
}
