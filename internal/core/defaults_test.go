package core

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mrz1836/go-foundation/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/datatypes"
	"gorm.io/gorm"
)

// routing is the part of a jobs row a kind's defaults can decide.
type routing struct {
	Queue         string
	Priority      int
	MaxAttempts   int
	TimeoutMs     int // 0 when the row carries no timeout
	ExecutorClass string
}

// runtimeRouting is what a row gets when nothing names a value: the runtime's
// own defaults.
func runtimeRouting() routing {
	return routing{Queue: defaultQueue, Priority: defaultPriority, MaxAttempts: defaultMaxAttempts}
}

// welcomeRouting is the routing welcomeWorker's Defaults declare.
func welcomeRouting() routing {
	return routing{Queue: "mail", Priority: 40, MaxAttempts: 4, TimeoutMs: 30_000, ExecutorClass: "ecs"}
}

// routingOf reads the routing of the job with id.
func routingOf(t testing.TB, db *gorm.DB, id string) routing {
	t.Helper()
	var row jobRow
	require.NoError(t, db.Model(&jobRow{}).Where("id = ?", id).First(&row).Error)
	return routingOfRow(row)
}

// routingOfRow is a row's routing.
func routingOfRow(row jobRow) routing {
	r := routing{
		Queue: row.Queue, Priority: row.Priority, MaxAttempts: row.MaxAttempts, ExecutorClass: row.ExecutorClass,
	}
	if row.TimeoutMs != nil {
		r.TimeoutMs = *row.TimeoutMs
	}
	return r
}

// defaultsArgs names its own kind, so one args type reaches every typed insert
// path for any kind. The kind is not part of the payload.
type defaultsArgs struct {
	kind string
	User string `json:"user"`
}

func (a defaultsArgs) Kind() string { return a.kind }

// welcomeWorker declares Defaults for the kind "mail.welcome".
type welcomeWorker struct{}

func (welcomeWorker) Kind() string { return "mail.welcome" }

func (welcomeWorker) Work(context.Context, *Job[defaultsArgs]) (Result, error) { return Result{}, nil }

// Defaults declares the five fields an insert takes from it, and fields it must
// never take: a unique key, a unique active key, a schedule, a parent, and a
// request id.
func (welcomeWorker) Defaults() InsertOpts {
	later := time.Now().Add(30 * 24 * time.Hour)
	parent := "defaults-parent"
	return InsertOpts{
		Queue: "mail", Priority: 40, MaxAttempts: 4, Timeout: 30 * time.Second, ExecutorClass: "ecs",
		UniqueKey: "welcome", UniqueActiveKey: "welcome-live", ScheduleAt: &later, Parent: &parent,
		RequestID: "request-from-defaults",
	}
}

// welcomeRegistry registers welcomeWorker alone.
func welcomeRegistry() *Registry {
	reg := NewRegistry()
	Register(reg, welcomeWorker{})
	return reg
}

// assertNothingElseFromDefaults checks that the row with id took none of the
// fields welcomeWorker's Defaults declare but an insert never reads.
func assertNothingElseFromDefaults(t testing.TB, db *gorm.DB, id string) {
	t.Helper()
	var row jobRow
	require.NoError(t, db.Model(&jobRow{}).Where("id = ?", id).First(&row).Error)
	assert.Nil(t, row.UniqueKey, "a kind's defaults carry no unique key")
	assert.Nil(t, row.UniqueActiveKey, "nor a unique active key")
	if row.ParentJobID != nil {
		assert.NotEqual(t, "defaults-parent", *row.ParentJobID, "nor a parent")
	}
	assert.True(t, row.ScheduledAt.Before(time.Now().Add(7*24*time.Hour)), "nor a schedule")
	assert.NotContains(t, string(row.Metadata), "request-from-defaults", "nor a request id")
}

// insertPath is one way a producer inserts a job through a Client.
type insertPath struct {
	name   string
	insert func(t *testing.T, ctx context.Context, c *Client, kind string, opts InsertOpts) string
}

// defaultsInsertPaths are every insert a Client makes: Insert, Enqueue (on its
// own connection and on the caller's transaction), InsertMany, and
// InsertManyTyped.
func defaultsInsertPaths() []insertPath {
	payload := []byte(`{"user":"u1"}`)
	return []insertPath{
		{name: "Insert", insert: func(t *testing.T, ctx context.Context, c *Client, kind string, opts InsertOpts) string {
			id, err := Insert(ctx, c, defaultsArgs{kind: kind, User: "u1"}, opts)
			require.NoError(t, err)
			return id
		}},
		{name: "Enqueue", insert: func(t *testing.T, ctx context.Context, c *Client, kind string, opts InsertOpts) string {
			id, err := Enqueue(ctx, c, kind, payload, opts)
			require.NoError(t, err)
			return id
		}},
		{name: "Enqueue on a transaction", insert: func(t *testing.T, ctx context.Context, c *Client, kind string, opts InsertOpts) string {
			var id string
			require.NoError(t, c.WriteDB().Transaction(func(tx *gorm.DB) error {
				opts.Tx = tx
				var err error
				id, err = Enqueue(ctx, c, kind, payload, opts)
				return err
			}))
			return id
		}},
		{name: "InsertMany", insert: func(t *testing.T, ctx context.Context, c *Client, kind string, opts InsertOpts) string {
			res, err := InsertMany(ctx, c, []BatchItem{{Kind: kind, Args: payload, Opts: opts}}, BatchOpts{})
			require.NoError(t, err)
			require.Equal(t, 1, res.Inserted)
			return res.IDs[0]
		}},
		{name: "InsertManyTyped", insert: func(t *testing.T, ctx context.Context, c *Client, kind string, opts InsertOpts) string {
			res, err := InsertManyTyped(ctx, c, []defaultsArgs{{kind: kind, User: "u1"}}, opts, BatchOpts{})
			require.NoError(t, err)
			require.Equal(t, 1, res.Inserted)
			return res.IDs[0]
		}},
	}
}

// defaultsInsertCase is one insert and the routing its row must get.
type defaultsInsertCase struct {
	name     string
	registry bool // the Client is built with ClientOpts.Registry
	kind     string
	opts     InsertOpts
	want     routing
}

// defaultsInsertCases cover a call that names nothing, one that names some
// fields, one that names every field, a kind the registry does not register,
// and a Client built without a registry.
func defaultsInsertCases() []defaultsInsertCase {
	return []defaultsInsertCase{
		{
			name: "zero options take the worker's defaults", registry: true, kind: "mail.welcome",
			want: welcomeRouting(),
		},
		{
			name: "the caller's fields win and the rest come from the defaults", registry: true, kind: "mail.welcome",
			opts: InsertOpts{Queue: "urgent", MaxAttempts: 9},
			want: routing{Queue: "urgent", Priority: 40, MaxAttempts: 9, TimeoutMs: 30_000, ExecutorClass: "ecs"},
		},
		{
			name: "a caller that names every field keeps every one", registry: true, kind: "mail.welcome",
			opts: InsertOpts{Queue: "urgent", Priority: 7, MaxAttempts: 9, Timeout: 5 * time.Second, ExecutorClass: "gpu"},
			want: routing{Queue: "urgent", Priority: 7, MaxAttempts: 9, TimeoutMs: 5_000, ExecutorClass: "gpu"},
		},
		{
			name: "a kind the registry does not register takes the runtime's defaults", registry: true,
			kind: "mail.stray", want: runtimeRouting(),
		},
		{
			name: "a client without a registry applies no defaults", registry: false, kind: "mail.welcome",
			want: runtimeRouting(),
		},
	}
}

// defaultsInsertSuite runs every case on every insert path. Each case inserts
// twice: the worker's Defaults name a unique key, which an insert never takes,
// so both rows land.
func defaultsInsertSuite(t *testing.T, open dbOpener) {
	t.Helper()
	for _, path := range defaultsInsertPaths() {
		t.Run(path.name, func(t *testing.T) {
			t.Parallel()
			db := open(t)
			ctx := context.Background()
			for _, tc := range defaultsInsertCases() {
				t.Run(tc.name, func(t *testing.T) {
					c := NewClient(db)
					if tc.registry {
						c = NewClientWithOptions(db, ClientOpts{Registry: welcomeRegistry()})
					}
					first := path.insert(t, ctx, c, tc.kind, tc.opts)
					second := path.insert(t, ctx, c, tc.kind, tc.opts)
					require.NotEmpty(t, first)
					require.NotEmpty(t, second)
					require.NotEqual(t, first, second, "both inserts land")
					for _, id := range []string{first, second} {
						assert.Equal(t, tc.want, routingOf(t, db, id))
						assertNothingElseFromDefaults(t, db, id)
					}
				})
			}
		})
	}
}

// TestDefaultsSeedEveryInsertUnderTheCallersOptions proves a Client built with
// ClientOpts.Registry fills each field a call leaves zero (queue, priority, max
// attempts, timeout, and executor class) from the kind's Defaults, on Insert,
// Enqueue, InsertMany, and InsertManyTyped; that a field the call sets wins; and
// that every other field of Defaults is ignored. A kind the registry does not
// register, and any kind through a Client without a registry, take the runtime's
// defaults.
func TestDefaultsSeedEveryInsertUnderTheCallersOptions(t *testing.T) {
	t.Parallel()
	defaultsInsertSuite(t, sqliteOpener)
}

// countingDefaultsWorker counts its Defaults calls.
type countingDefaultsWorker struct{ calls *atomic.Int32 }

func (countingDefaultsWorker) Kind() string { return "mail.counted" }

func (countingDefaultsWorker) Work(context.Context, *Job[defaultsArgs]) (Result, error) {
	return Result{}, nil
}

func (w countingDefaultsWorker) Defaults() InsertOpts {
	w.calls.Add(1)
	return InsertOpts{Queue: "counted"}
}

// TestRegisterReadsAWorkersDefaultsOnce proves Defaults is called once, when the
// worker is registered, and not again for each insert.
func TestRegisterReadsAWorkersDefaultsOnce(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	calls := &atomic.Int32{}
	reg := NewRegistry()
	Register(reg, countingDefaultsWorker{calls: calls})
	c := NewClientWithOptions(db, ClientOpts{Registry: reg})

	for range 3 {
		id, err := Enqueue(context.Background(), c, "mail.counted", []byte(`{}`), InsertOpts{})
		require.NoError(t, err)
		assert.Equal(t, "counted", routingOf(t, db, id).Queue)
	}
	assert.EqualValues(t, 1, calls.Load(), "Defaults is read at registration only")
}

// defaultsPeriodicSuite fires one periodic of kind "mail.welcome" through a
// Scheduler whose Client carries the registry, and one through a Scheduler whose
// Client does not. The periodic names its queue, so the job keeps it; every
// other field comes from the worker's Defaults, or from the runtime without a
// registry.
func defaultsPeriodicSuite(t *testing.T, open dbOpener) {
	t.Helper()
	cases := []struct {
		name     string
		registry bool
		want     routing
	}{
		{
			name: "a client with a registry", registry: true,
			want: routing{Queue: "periodic", Priority: 40, MaxAttempts: 4, TimeoutMs: 30_000, ExecutorClass: "ecs"},
		},
		{
			name: "a client without a registry", registry: false,
			want: routing{Queue: "periodic", Priority: defaultPriority, MaxAttempts: defaultMaxAttempts},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			db := open(t)
			c := NewClient(db)
			if tc.registry {
				c = NewClientWithOptions(db, ClientOpts{Registry: welcomeRegistry()})
			}
			s := newSchedulerCfg(t, SchedulerConfig{DB: db, Client: c, Driver: driverForTest(t, db)})

			declared := time.Now().UTC().Truncate(time.Second)
			require.NoError(t, UpsertPeriodic(clockCtx(context.Background(), models.NewFixedClock(declared)), db, PeriodicSpec{
				Slug: "welcome-digest", Kind: "mail.welcome", Queue: "periodic", Every: time.Minute, Active: true,
			}))
			enqueued, err := s.Tick(clockCtx(context.Background(), models.NewFixedClock(declared.Add(90*time.Second))))
			require.NoError(t, err)
			require.Equal(t, 1, enqueued)

			var row jobRow
			require.NoError(t, db.Where("kind = ?", "mail.welcome").First(&row).Error)
			assert.Equal(t, tc.want, routingOfRow(row))
			require.NotNil(t, row.UniqueKey)
			assert.Contains(t, *row.UniqueKey, "welcome-digest@", "the fire keeps its own bucketed key")
		})
	}
}

// TestDefaultsReachAPeriodicFire proves a Scheduler's fires take the kind's
// defaults through a Client built with a registry, under the periodic's queue.
func TestDefaultsReachAPeriodicFire(t *testing.T) {
	t.Parallel()
	defaultsPeriodicSuite(t, sqliteOpener)
}

// fanOutWorker returns three follow-ups: one of kind "mail.welcome" that names
// nothing, one of the same kind that names its queue, priority, and executor
// class, and one of a kind the Runner does not register.
type fanOutWorker struct{}

func (fanOutWorker) Kind() string { return "defaults.fan_out" }

func (fanOutWorker) Work(context.Context, *Job[defaultsArgs]) (Result, error) {
	return Result{FollowUps: []FollowUp{
		{Kind: "mail.welcome", Args: map[string]string{"case": "bare"}, Parent: true},
		{
			Kind: "mail.welcome", Args: map[string]string{"case": "routed"}, Parent: true,
			Queue: "urgent", Priority: 7, ExecutorClass: "gpu",
		},
		{Kind: "mail.stray", Args: map[string]string{"case": "stray"}, Parent: true},
	}}, nil
}

// pollOneOn runs one claim on queue to completion through a Runner over reg.
func pollOneOn(t *testing.T, db *gorm.DB, reg *Registry, queue string) {
	t.Helper()
	r := rwRunner(t, db, reg, func(cfg *RunnerConfig) {
		cfg.Driver = driverForTest(t, db)
		cfg.Queues = []string{queue}
	})
	claimed, err := r.pollOnce(context.Background())
	require.NoError(t, err)
	require.Equal(t, 1, claimed)
}

// childByCase reads the follow-up whose args carry case c.
func childByCase(t testing.TB, db *gorm.DB, parentID, c string) jobRow {
	t.Helper()
	var rows []jobRow
	require.NoError(t, db.Where("parent_job_id = ?", parentID).Find(&rows).Error)
	for _, row := range rows {
		var args map[string]string
		require.NoError(t, json.Unmarshal(row.Args, &args))
		if args["case"] == c {
			return row
		}
	}
	require.Failf(t, "no follow-up", "parent %s has no follow-up for case %q", parentID, c)
	return jobRow{}
}

// defaultsFollowUpSuite runs fanOutWorker through a Runner whose registry
// registers "mail.welcome", and reads the three follow-ups its finalize inserted.
func defaultsFollowUpSuite(t *testing.T, db *gorm.DB) {
	t.Helper()
	ctx := context.Background()
	reg := welcomeRegistry()
	Register(reg, fanOutWorker{})
	parentID, err := Enqueue(ctx, NewClient(db), "defaults.fan_out", []byte(`{}`), InsertOpts{Queue: "fan-out"})
	require.NoError(t, err)

	pollOneOn(t, db, reg, "fan-out")

	bare := childByCase(t, db, parentID, "bare")
	assert.Equal(t, welcomeRouting(), routingOfRow(bare), "a follow-up's zero fields take its kind's defaults")
	routed := childByCase(t, db, parentID, "routed")
	assert.Equal(t, routing{Queue: "urgent", Priority: 7, MaxAttempts: 4, TimeoutMs: 30_000, ExecutorClass: "gpu"},
		routingOfRow(routed), "a follow-up's own fields win")
	stray := childByCase(t, db, parentID, "stray")
	assert.Equal(t, runtimeRouting(), routingOfRow(stray), "a kind the Runner does not register gets no defaults")

	for _, child := range []jobRow{bare, routed, stray} {
		assert.Nil(t, child.UniqueKey, "a follow-up takes no unique key from its kind's defaults")
		require.NotNil(t, child.ParentJobID)
		assert.Equal(t, parentID, *child.ParentJobID, "nor a parent")
		assert.True(t, child.ScheduledAt.Before(time.Now().Add(7*24*time.Hour)), "nor a schedule")
	}

	// InsertChild, the Driver's single-row seam, applies the defaults of the
	// registry its context carries, and none without one.
	d := driverForTest(t, db)
	fu := FollowUp{Kind: "mail.welcome", Args: map[string]string{"case": "seam"}, Parent: true}
	require.NoError(t, d.InsertChild(withKindRegistry(ctx, reg), db, fu, "seam-parent"))
	fu.Args = map[string]string{"case": "bare-seam"}
	require.NoError(t, d.InsertChild(ctx, db, fu, "seam-parent"))
	assert.Equal(t, welcomeRouting(), routingOfRow(childByCase(t, db, "seam-parent", "seam")))
	assert.Equal(t, runtimeRouting(), routingOfRow(childByCase(t, db, "seam-parent", "bare-seam")))
}

// TestDefaultsReachFollowUps proves a Runner fills a follow-up's zero fields
// from its kind's Defaults in its own registry, keeps the fields the follow-up
// names, and applies none to a kind it does not register.
func TestDefaultsReachFollowUps(t *testing.T) {
	t.Parallel()
	defaultsFollowUpSuite(t, newDB(t))
}

// rollupWorker is a barrier continuation kind that declares Defaults.
type rollupWorker struct{}

func (rollupWorker) Kind() string { return "report.rollup" }

func (rollupWorker) Work(context.Context, *Job[defaultsArgs]) (Result, error) { return Result{}, nil }

func (rollupWorker) Defaults() InsertOpts {
	return InsertOpts{Queue: "reports", Priority: 30, MaxAttempts: 3, Timeout: 20 * time.Second, ExecutorClass: "batch"}
}

// plainRollupWorker is a barrier continuation kind with no Defaults.
type plainRollupWorker struct{}

func (plainRollupWorker) Kind() string { return "report.plain" }

func (plainRollupWorker) Work(context.Context, *Job[defaultsArgs]) (Result, error) {
	return Result{}, nil
}

// barrierChildWorker runs the barrier tests' children, of kind "child".
type barrierChildWorker struct{}

func (barrierChildWorker) Kind() string { return "child" }

func (barrierChildWorker) Work(context.Context, *Job[map[string]int]) (Result, error) {
	return Result{}, nil
}

// coordinatorWorker spawns two children of kind "child" and declares barrier
// over them.
type coordinatorWorker struct{ barrier Barrier }

func (coordinatorWorker) Kind() string { return "defaults.coordinator" }

func (w coordinatorWorker) Work(context.Context, *Job[defaultsArgs]) (Result, error) {
	b := w.barrier
	return Result{FollowUps: barrierFollowUps("child", 2), Barrier: &b}, nil
}

// coordinatorRouting is the spawning job's routing in the barrier tests.
func coordinatorRouting() routing {
	return routing{Queue: "coord", Priority: 60, MaxAttempts: defaultMaxAttempts, ExecutorClass: "coord-pool"}
}

// declareBarrierThroughARunner enqueues a coordinator on its own queue and runs
// it through a Runner whose registry registers both continuation kinds, so the
// barrier is declared in a finalize that knows their defaults. Its two children
// wait on the default queue. It returns the coordinator's id.
func declareBarrierThroughARunner(t *testing.T, db *gorm.DB, b Barrier) string {
	t.Helper()
	cr := coordinatorRouting()
	parentID, err := Enqueue(context.Background(), NewClient(db), "defaults.coordinator", []byte(`{}`), InsertOpts{
		Queue: cr.Queue, Priority: cr.Priority, ExecutorClass: ExecutorClass(cr.ExecutorClass),
	})
	require.NoError(t, err)

	reg := NewRegistry()
	Register(reg, coordinatorWorker{barrier: b})
	Register(reg, rollupWorker{})
	Register(reg, plainRollupWorker{})
	pollOneOn(t, db, reg, cr.Queue)
	require.Len(t, pendingChildIDs(t, db, parentID, StateAvailable), 2)
	return parentID
}

// barrierFiringPath is one way a generation's last pending child becomes
// terminal. None of them runs under a context that carries a registry.
type barrierFiringPath struct {
	name string
	fire func(t *testing.T, ctx context.Context, d Driver, db *gorm.DB, parentID string)
}

// barrierFiringPaths are the four paths that fire a barrier: a child's
// finalize, CancelByParent, CancelJob, and the lease sweep's discard.
func barrierFiringPaths() []barrierFiringPath {
	return []barrierFiringPath{
		{name: "a child's finalize", fire: func(t *testing.T, ctx context.Context, d Driver, _ *gorm.DB, _ string) {
			finalizeJob(t, d, ctx, claimOne(t, d, ctx), Result{}, nil)
			finalizeJob(t, d, ctx, claimOne(t, d, ctx), Result{}, nil)
		}},
		{name: "CancelByParent", fire: func(t *testing.T, ctx context.Context, _ Driver, db *gorm.DB, parentID string) {
			_, err := CancelByParent(ctx, db, parentID, ScopeOpts{})
			require.NoError(t, err)
		}},
		{name: "CancelJob", fire: func(t *testing.T, ctx context.Context, _ Driver, db *gorm.DB, parentID string) {
			for _, id := range pendingChildIDs(t, db, parentID, StateAvailable) {
				require.NoError(t, CancelJob(ctx, db, id))
			}
		}},
		{name: "the sweep's discard", fire: func(t *testing.T, ctx context.Context, d Driver, db *gorm.DB, parentID string) {
			require.NoError(t, db.Model(&jobRow{}).Where("parent_job_id = ?", parentID).
				Update("max_attempts", 1).Error)
			finalizeJob(t, d, ctx, claimOne(t, d, ctx), Result{}, nil)
			crashAttempt(t, ctx, d, "default")
			sweepPastTheLease(t, crashScheduler(t, db, d))
		}},
	}
}

// continuationOf reads the one continuation of kind its parent's barrier
// enqueued, and checks it is keyed on and joined to the parent.
func continuationOf(t testing.TB, db *gorm.DB, kind, parentID string) jobRow {
	t.Helper()
	var rows []jobRow
	require.NoError(t, db.Where("kind = ?", kind).Find(&rows).Error)
	require.Len(t, rows, 1, "the barrier enqueues one continuation")
	cont := rows[0]
	require.NotNil(t, cont.UniqueKey)
	assert.Equal(t, "barrier:"+parentID, *cont.UniqueKey)
	require.NotNil(t, cont.ParentJobID)
	assert.Equal(t, parentID, *cont.ParentJobID)
	return cont
}

// defaultsBarrierSuite declares each barrier through a Runner, fires it on each
// path, and checks the continuation's routing, which must be the same row
// whichever path fired it.
func defaultsBarrierSuite(t *testing.T, open dbOpener) {
	t.Helper()
	cr := coordinatorRouting()
	cases := []struct {
		name    string
		barrier Barrier
		want    routing
	}{
		{
			name:    "an empty barrier takes the continuation kind's defaults",
			barrier: Barrier{Kind: "report.rollup"},
			want:    routing{Queue: "reports", Priority: 30, MaxAttempts: 3, TimeoutMs: 20_000, ExecutorClass: "batch"},
		},
		{
			name:    "the barrier's own fields win",
			barrier: Barrier{Kind: "report.rollup", Queue: "x", Priority: 9, ExecutorClass: "y"},
			want:    routing{Queue: "x", Priority: 9, MaxAttempts: 3, TimeoutMs: 20_000, ExecutorClass: "y"},
		},
		{
			name:    "a kind without defaults takes the spawning job's routing",
			barrier: Barrier{Kind: "report.plain"},
			want: routing{
				Queue: cr.Queue, Priority: cr.Priority, MaxAttempts: defaultMaxAttempts, ExecutorClass: cr.ExecutorClass,
			},
		},
	}
	for _, tc := range cases {
		for _, path := range barrierFiringPaths() {
			t.Run(tc.name+"/"+path.name, func(t *testing.T) {
				t.Parallel()
				db := open(t)
				d := driverForTest(t, db)
				ctx := context.Background()
				parentID := declareBarrierThroughARunner(t, db, tc.barrier)

				path.fire(t, ctx, d, db, parentID)
				assert.Equal(t, tc.want, routingOfRow(continuationOf(t, db, tc.barrier.Kind, parentID)))
			})
		}
	}
}

// TestDefaultsReachBarrierContinuationsOnEveryFiringPath proves a barrier's
// continuation is resolved when the barrier is declared: the Barrier's own
// field, then the continuation kind's default, then the spawning job's routing,
// then the runtime's default. Its max attempts and timeout are stored with it,
// so a child's finalize, CancelByParent, CancelJob, and the sweep's discard,
// none of which knows the registry, build the same row.
func TestDefaultsReachBarrierContinuationsOnEveryFiringPath(t *testing.T) {
	t.Parallel()
	defaultsBarrierSuite(t, sqliteOpener)
}

// legacyBarrierSpec is a barrier spec as a release that stored only the routing
// wrote it.
const legacyBarrierSpec = `{"args":{"n":1},"queue":"legacy","executor_class":"legacy-pool","priority":45}`

// seedLegacyBarrier writes a succeeded parent holding legacyBarrierSpec for the
// continuation kind "report.rollup", and one pending child of kind "child" on
// the default queue. It returns the parent's id.
func seedLegacyBarrier(t *testing.T, db *gorm.DB) string {
	t.Helper()
	at := time.Now().Add(-time.Minute)
	parentID, childID, contKind := "legacy-parent", "legacy-child", "report.rollup"
	seedJob(t, db, jobRow{
		ID: parentID, Kind: "defaults.coordinator", Queue: "coord", State: string(StateSucceeded),
		MaxAttempts: defaultMaxAttempts, CreatedAt: at, UpdatedAt: at, ScheduledAt: at, FinalizedAt: &at,
		BarrierKind: &contKind, BarrierSpec: datatypes.JSON(legacyBarrierSpec),
	})
	seedJob(t, db, jobRow{
		ID: childID, Kind: "child", Queue: defaultQueue, State: string(StateAvailable), ParentJobID: &parentID,
		MaxAttempts: 5, CreatedAt: at, UpdatedAt: at, ScheduledAt: at,
	})
	return parentID
}

// legacyBarrierSuite fires a spec stored before the spec carried attempts and a
// timeout, once through a Runner whose registry declares the continuation
// kind's defaults and once through CancelByParent. Both build the row the spec
// always built: its own routing, with the runtime's attempts and no timeout.
func legacyBarrierSuite(t *testing.T, open dbOpener) {
	t.Helper()
	paths := []struct {
		name string
		fire func(t *testing.T, db *gorm.DB, parentID string)
	}{
		{name: "a child's finalize under a Runner that knows the kind's defaults", fire: func(t *testing.T, db *gorm.DB, _ string) {
			reg := NewRegistry()
			Register(reg, barrierChildWorker{})
			Register(reg, rollupWorker{})
			pollOneOn(t, db, reg, defaultQueue)
		}},
		{name: "CancelByParent", fire: func(t *testing.T, db *gorm.DB, parentID string) {
			_, err := CancelByParent(context.Background(), db, parentID, ScopeOpts{})
			require.NoError(t, err)
		}},
	}
	for _, path := range paths {
		t.Run(path.name, func(t *testing.T) {
			t.Parallel()
			db := open(t)
			parentID := seedLegacyBarrier(t, db)

			path.fire(t, db, parentID)
			cont := continuationOf(t, db, "report.rollup", parentID)
			assert.Equal(t,
				routing{Queue: "legacy", Priority: 45, MaxAttempts: defaultMaxAttempts, ExecutorClass: "legacy-pool"},
				routingOfRow(cont))
			assert.JSONEq(t, `{"n":1}`, string(cont.Args))
		})
	}
}

// TestABarrierSpecStoredWithoutDefaultsBuildsItsOldRow proves a barrier spec
// that carries no attempts or timeout, as every spec stored before the spec
// carried them, builds the continuation it always built, even when the
// finalize that fires it knows the continuation kind's defaults.
func TestABarrierSpecStoredWithoutDefaultsBuildsItsOldRow(t *testing.T) {
	t.Parallel()
	legacyBarrierSuite(t, sqliteOpener)
}
