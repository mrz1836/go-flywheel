package node

import (
	"bytes"
	"context"
	"log/slog"
	"os"
	"strconv"
	"testing"
	"time"

	ft "github.com/mrz1836/go-flywheel/flywheeltest"
	core "github.com/mrz1836/go-flywheel/internal/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// registryTestInterval is the RegistryInterval the registration tests run with.
const registryTestInterval = 50 * time.Millisecond

// mailArgs and mailWorker are a second kind, on a second queue, so a node's
// kinds are a union over its runners.
type mailArgs struct{}

func (mailArgs) Kind() string { return "node.mail" }

type mailWorker struct{ release chan struct{} }

func (*mailWorker) Kind() string { return "node.mail" }

// Work holds its slot until the test releases it, so a heartbeat can see it in
// flight.
func (w *mailWorker) Work(ctx context.Context, _ *core.Job[mailArgs]) (core.Result, error) {
	select {
	case <-w.release:
	case <-ctx.Done():
	}
	return core.Result{}, nil
}

// thisExecutor is the executor identity every runner of this process writes:
// host:pid, with "unknown" for a host that cannot be read.
func thisExecutor(t *testing.T) string {
	t.Helper()
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "unknown"
	}
	return host + ":" + strconv.Itoa(os.Getpid())
}

// nodeRun is a Node running on its own goroutine: finished closes when Run
// returns, with its result in err.
type nodeRun struct {
	finished chan struct{}
	err      error
}

// startNode runs n until the test cancels it, or the test ends.
func startNode(t *testing.T, n *Node) (context.CancelFunc, *nodeRun) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	run := &nodeRun{finished: make(chan struct{})}
	go func() {
		defer close(run.finished)
		run.err = n.Run(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-run.finished:
		case <-time.After(5 * time.Second):
		}
	})
	return cancel, run
}

// awaitRun waits for Run's result.
func awaitRun(t *testing.T, run *nodeRun) error {
	t.Helper()
	select {
	case <-run.finished:
		return run.err
	case <-time.After(5 * time.Second):
		t.Fatal("node.Run did not return")
		return nil
	}
}

// onlyNode waits until ListNodes lists exactly one node that has not stopped
// and returns it.
func onlyNode(t *testing.T, db *gorm.DB) core.NodeInfo {
	t.Helper()
	var nodes []core.NodeInfo
	require.Eventually(t, func() bool {
		var err error
		nodes, err = core.ListNodes(context.Background(), db, core.ListNodesParams{})
		return err == nil && len(nodes) == 1
	}, 5*time.Second, 5*time.Millisecond, "the node registers")
	return nodes[0]
}

// TestANodeHeartbeatsAndStopsCleanly proves a registering Node writes its row
// before its components run — its executor identity, host, version, commit,
// runners, the union of their kinds, and whether it runs a Scheduler — then
// heartbeats it with its runners' in-flight counts, and stamps its stop time
// when Run returns, after which it is listed only back to a StoppedSince.
func TestANodeHeartbeatsAndStopsCleanly(t *testing.T) {
	t.Parallel()
	db := ft.NewWALFileDB(t)
	success := core.NewRegistry()
	core.Register(success, &ft.SuccessWorker{})
	mail := core.NewRegistry()
	mw := &mailWorker{release: make(chan struct{})}
	core.Register(mail, mw)
	mailRunner := sqliteRunner(db, mail)
	mailRunner.Queues = []string{"mail"}
	mailRunner.ExecutorClass = "ecs"
	mailRunner.Concurrency = 1

	var logs bytes.Buffer
	n, err := NewNode(NodeConfig{
		Runners:          []core.RunnerConfig{sqliteRunner(db, success), mailRunner},
		Scheduler:        &core.SchedulerConfig{DB: db, Client: core.NewClient(db), Driver: core.NewSQLiteDriver(db)},
		Version:          "1.4.0",
		Commit:           "abc1234",
		RegistryInterval: registryTestInterval,
		Logger:           slog.New(slog.NewTextHandler(&logs, nil)),
	})
	require.NoError(t, err)
	before := time.Now().UTC().Add(-time.Second)
	cancel, done := startNode(t, n)

	node := onlyNode(t, db)
	assert.NotEmpty(t, node.ID)
	assert.Equal(t, thisExecutor(t), node.ExecutorID, "the executor identity its runners write")
	host, _ := os.Hostname()
	assert.Equal(t, host, node.Host)
	assert.Equal(t, "1.4.0", node.Version)
	assert.Equal(t, "abc1234", node.Commit)
	assert.True(t, node.StartedAt.After(before))
	assert.True(t, node.Scheduler)
	assert.False(t, node.Stale)
	assert.Nil(t, node.StoppedAt)
	assert.Equal(t, []core.NodeRunner{
		{Queues: []string{"default", "periodic"}, Concurrency: 1},
		{Queues: []string{"mail"}, ExecutorClass: "ecs", Concurrency: 1},
	}, node.Runners, "a zero Concurrency reports the one slot the runner runs")
	assert.Equal(t, []string{"node.mail", "test.success"}, node.Kinds, "the sorted union of the runners' kinds")

	first := node.HeartbeatAt
	require.Eventually(t, func() bool { return onlyNode(t, db).HeartbeatAt.After(first) }, 5*time.Second,
		5*time.Millisecond, "the heartbeat advances")

	_, err = core.Insert(context.Background(), core.NewClient(db), mailArgs{}, core.InsertOpts{Queue: "mail"})
	require.NoError(t, err)
	require.Eventually(t, func() bool { return onlyNode(t, db).Runners[1].InFlight == 1 }, 5*time.Second,
		5*time.Millisecond, "a heartbeat reports the runner's job in flight")
	close(mw.release)
	require.Eventually(t, func() bool { return onlyNode(t, db).Runners[1].InFlight == 0 }, 5*time.Second,
		5*time.Millisecond, "and its slot free again")

	cancel()
	require.NoError(t, awaitRun(t, done))
	assert.NotContains(t, logs.String(), "flywheel: node registry", "a healthy node's registry writes all succeed")
	nodes, err := core.ListNodes(context.Background(), db, core.ListNodesParams{})
	require.NoError(t, err)
	assert.Empty(t, nodes, "a stopped node is not listed by default")
	nodes, err = core.ListNodes(context.Background(), db, core.ListNodesParams{StoppedSince: before})
	require.NoError(t, err)
	require.Len(t, nodes, 1, "and is listed back to a StoppedSince before its stop")
	assert.Equal(t, node.ID, nodes[0].ID)
	require.NotNil(t, nodes[0].StoppedAt)
	assert.False(t, nodes[0].StoppedAt.Before(nodes[0].StartedAt))
	assert.False(t, nodes[0].Stale)
}

// TestANodeRefusesToStartWithoutTheNodeTable proves a Node with
// RegistryInterval set stops before any component starts against a schema
// without job_nodes, naming the table and the fix, while a Node without it
// writes no row and needs no table. db carries the current schema; the test
// drops the table.
func TestANodeRefusesToStartWithoutTheNodeTable(t *testing.T) {
	t.Parallel()
	reg := core.NewRegistry()
	core.Register(reg, &ft.SuccessWorker{})

	quiet := ft.NewWALFileDB(t)
	n, err := NewNode(NodeConfig{Runners: []core.RunnerConfig{sqliteRunner(quiet, reg)}})
	require.NoError(t, err)
	cancel, done := startNode(t, n)
	id, err := core.Insert(context.Background(), core.NewClient(quiet), ft.SuccessArgs{V: "x"}, core.InsertOpts{})
	require.NoError(t, err)
	ft.WaitForJobState(t, quiet, id, string(core.StateSucceeded), 5*time.Second)
	cancel()
	require.NoError(t, awaitRun(t, done))
	var rows int64
	require.NoError(t, quiet.Table("job_nodes").Count(&rows).Error)
	assert.Zero(t, rows, "a Node without RegistryInterval writes no row")

	db := ft.NewDB(t)
	require.NoError(t, db.Exec("DROP TABLE job_nodes").Error)
	held, err := core.Insert(context.Background(), core.NewClient(db), ft.SuccessArgs{V: "y"}, core.InsertOpts{})
	require.NoError(t, err)
	registering, err := NewNode(NodeConfig{
		Runners: []core.RunnerConfig{sqliteRunner(db, reg)}, RegistryInterval: registryTestInterval,
	})
	require.NoError(t, err)
	ctx, cancelRun := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelRun()
	err = registering.Run(ctx)
	require.ErrorIs(t, err, core.ErrSchemaOutdated)
	assert.Contains(t, err.Error(), "job_nodes (table)", "the error names the missing table")
	assert.Contains(t, err.Error(), "flywheel migrate", "and how to fix it")
	assert.NoError(t, ctx.Err(), "it stopped at once")
	assert.Equal(t, string(core.StateAvailable), ft.JobState(t, db, held), "no runner started")

	unregistered, err := NewNode(NodeConfig{Runners: []core.RunnerConfig{sqliteRunner(db, reg)}})
	require.NoError(t, err)
	cancel, done = startNode(t, unregistered)
	ft.WaitForJobState(t, db, held, string(core.StateSucceeded), 5*time.Second)
	cancel()
	require.NoError(t, awaitRun(t, done), "a Node that does not register needs no job_nodes")
}

// TestTheNodeWritesEveryColumnTheRuntimeDeclares holds the node's own model of
// job_nodes to the runtime's: the table Migrate creates has exactly the columns
// the node writes, and a table built from the node's model has every column the
// runtime declares.
func TestTheNodeWritesEveryColumnTheRuntimeDeclares(t *testing.T) {
	t.Parallel()
	db := ft.NewDB(t)
	stmt := &gorm.Statement{DB: db}
	require.NoError(t, stmt.Parse(&nodeRow{}))
	rows, err := db.Raw("SELECT * FROM job_nodes WHERE 1 = 0").Rows()
	require.NoError(t, err)
	columns, err := rows.Columns()
	require.NoError(t, err)
	require.NoError(t, rows.Close())
	require.NoError(t, rows.Err())
	assert.ElementsMatch(t, columns, stmt.Schema.DBNames, "the node writes every column of job_nodes, and no other")

	bare := ft.NewBareSQLite(t)
	require.NoError(t, bare.AutoMigrate(&nodeRow{}))
	drift, err := core.InspectSchema(context.Background(), bare)
	require.NoError(t, err)
	for _, d := range drift {
		assert.NotEqual(t, "job_nodes", d.Table, "the node's model lacks %s", d)
	}
}

// TestRunsJoinTheirNodeByExecutorAndLifetime proves the documented rule joins a
// run to the node that ran it: an earlier process's stopped row shares the
// node's executor identity, and the run's start falls inside the current node's
// span and outside the earlier one's.
func TestRunsJoinTheirNodeByExecutorAndLifetime(t *testing.T) {
	t.Parallel()
	db := ft.NewWALFileDB(t)
	now := time.Now().UTC()
	oldStart, oldStop := now.Add(-2*time.Hour), now.Add(-time.Hour)
	require.NoError(t, db.Create(&nodeRow{
		ID: "earlier-process", ExecutorID: thisExecutor(t), Host: "earlier", StartedAt: oldStart,
		HeartbeatAt: oldStop, StoppedAt: &oldStop,
	}).Error)

	reg := core.NewRegistry()
	core.Register(reg, &ft.SuccessWorker{})
	n, err := NewNode(NodeConfig{
		Runners: []core.RunnerConfig{sqliteRunner(db, reg)}, RegistryInterval: registryTestInterval,
	})
	require.NoError(t, err)
	_, _ = startNode(t, n)
	current := onlyNode(t, db)
	id, err := core.Insert(context.Background(), core.NewClient(db), ft.SuccessArgs{V: "x"}, core.InsertOpts{})
	require.NoError(t, err)
	ft.WaitForJobState(t, db, id, string(core.StateSucceeded), 5*time.Second)
	runs, err := core.ListRuns(context.Background(), db, id, core.ListRunsParams{})
	require.NoError(t, err)
	require.Len(t, runs, 1)
	run := runs[0]
	require.Eventually(t, func() bool { return !onlyNode(t, db).HeartbeatAt.Before(run.StartedAt) }, 5*time.Second,
		5*time.Millisecond, "a heartbeat after the run started")

	nodes, err := core.ListNodes(context.Background(), db, core.ListNodesParams{StoppedSince: oldStart})
	require.NoError(t, err)
	var owners []string
	for _, node := range nodes {
		if node.ExecutorID != run.ExecutorID {
			continue
		}
		end := node.HeartbeatAt
		if node.StoppedAt != nil {
			end = *node.StoppedAt
		}
		if !run.StartedAt.Before(node.StartedAt) && !run.StartedAt.After(end) {
			owners = append(owners, node.ID)
		}
	}
	assert.Equal(t, current.ExecutorID, run.ExecutorID, "the run's executor is the node's")
	assert.Len(t, nodes, 2, "both process lifetimes share the executor identity")
	assert.Equal(t, []string{current.ID}, owners, "exactly the current node's span covers the run")
}

// TestRequestDrainStopsClaimsWithinOneHeartbeat proves a drain request reaches
// a running node within one heartbeat: it stops every runner's claims, so no
// attempt starts for a job enqueued after it reported draining; it reports
// drained only once its last job in flight finishes; it keeps heartbeating; and
// the request is recorded once, with its actor and reason.
func TestRequestDrainStopsClaimsWithinOneHeartbeat(t *testing.T) {
	t.Parallel()
	db := ft.NewWALFileDB(t)
	success := &ft.SuccessWorker{}
	mw := &mailWorker{release: make(chan struct{})}
	reg := core.NewRegistry()
	core.Register(reg, success)
	core.Register(reg, mw)
	runner := sqliteRunner(db, reg)
	runner.Queues = []string{"default", "mail"}
	n, err := NewNode(NodeConfig{Runners: []core.RunnerConfig{runner}, RegistryInterval: registryTestInterval})
	require.NoError(t, err)
	cancel, run := startNode(t, n)
	node := onlyNode(t, db)

	_, err = core.Insert(context.Background(), core.NewClient(db), mailArgs{}, core.InsertOpts{Queue: "mail"})
	require.NoError(t, err)
	require.Eventually(t, func() bool { return onlyNode(t, db).Runners[0].InFlight == 1 }, 5*time.Second,
		5*time.Millisecond, "a job is in flight when the drain is requested")

	retire := core.WithActor(context.Background(), core.Actor{Type: core.ActorUser, ID: "u1", Reason: "retire host"})
	require.NoError(t, core.RequestDrain(retire, db, node.ID))
	require.Eventually(t, func() bool { return onlyNode(t, db).Draining }, 5*time.Second, 5*time.Millisecond,
		"the node reports draining within a heartbeat")
	draining := onlyNode(t, db)
	assert.False(t, draining.Drained, "not drained while its job is in flight")
	require.NotNil(t, draining.DrainRequestedAt)

	late, err := core.Insert(context.Background(), core.NewClient(db), ft.SuccessArgs{V: "late"}, core.InsertOpts{})
	require.NoError(t, err)
	close(mw.release)
	require.Eventually(t, func() bool { return onlyNode(t, db).Drained }, 5*time.Second, 5*time.Millisecond,
		"drained once nothing is in flight")
	beat := onlyNode(t, db).HeartbeatAt
	require.Eventually(t, func() bool { return onlyNode(t, db).HeartbeatAt.After(beat) }, 5*time.Second,
		5*time.Millisecond, "a drained node keeps heartbeating")
	time.Sleep(4 * registryTestInterval)
	assert.Equal(t, string(core.StateAvailable), ft.JobState(t, db, late), "no attempt starts after the drain")
	assert.Zero(t, success.Calls.Load())

	page, err := core.ListAudit(context.Background(), db, core.ListAuditParams{
		TargetType: core.AuditTargetNode, TargetID: node.ID,
	})
	require.NoError(t, err)
	require.Len(t, page.Entries, 1)
	assert.Equal(t, core.AuditDrainRequested, page.Entries[0].Action)
	assert.Equal(t, "u1", page.Entries[0].Actor.ID)
	assert.Equal(t, "retire host", page.Entries[0].Actor.Reason)

	cancel()
	require.NoError(t, awaitRun(t, run))
	nodes, err := core.ListNodes(context.Background(), db, core.ListNodesParams{StoppedSince: node.StartedAt})
	require.NoError(t, err)
	require.Len(t, nodes, 1)
	assert.NotNil(t, nodes[0].StoppedAt, "a drained node still stamps its stop")
}

// TestNodeRowsArePrunedOnlyPastNodeRetention proves a registering Node that
// runs a Scheduler deletes, at start, the rows of nodes whose heartbeat is
// older than the Scheduler's NodeRetention — seven days when zero — and keeps
// the others; and that a registering Node without a Scheduler deletes nothing.
func TestNodeRowsArePrunedOnlyPastNodeRetention(t *testing.T) {
	t.Parallel()
	seed := func(t *testing.T, db *gorm.DB) {
		t.Helper()
		now := time.Now().UTC()
		for id, age := range map[string]time.Duration{"eight-days": 8 * 24 * time.Hour, "six-days": 6 * 24 * time.Hour} {
			beat := now.Add(-age)
			require.NoError(t, db.Create(&nodeRow{
				ID: id, ExecutorID: "gone:1", Host: "gone", StartedAt: beat.Add(-time.Hour), HeartbeatAt: beat,
			}).Error)
		}
	}
	remaining := func(t *testing.T, db *gorm.DB) []string {
		t.Helper()
		var ids []string
		require.NoError(t, db.Model(&nodeRow{}).Where("id IN ?", []string{"eight-days", "six-days"}).
			Order("id").Pluck("id", &ids).Error)
		return ids
	}
	scheduler := func(db *gorm.DB, retention time.Duration) *core.SchedulerConfig {
		return &core.SchedulerConfig{
			DB: db, Client: core.NewClient(db), Driver: core.NewSQLiteDriver(db), NodeRetention: retention,
		}
	}
	for _, tc := range []struct {
		name      string
		scheduler func(*gorm.DB) *core.SchedulerConfig
		want      []string
	}{
		{
			"a Scheduler with the default retention", func(db *gorm.DB) *core.SchedulerConfig { return scheduler(db, 0) },
			[]string{"six-days"},
		},
		{"a Scheduler keeping 400 days", func(db *gorm.DB) *core.SchedulerConfig {
			return scheduler(db, 400*24*time.Hour)
		}, []string{"eight-days", "six-days"}},
		{"no Scheduler", func(*gorm.DB) *core.SchedulerConfig { return nil }, []string{"eight-days", "six-days"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			db := ft.NewWALFileDB(t)
			seed(t, db)
			n, err := NewNode(NodeConfig{
				Runners:          []core.RunnerConfig{sqliteRunner(db, core.NewRegistry())},
				Scheduler:        tc.scheduler(db),
				RegistryInterval: registryTestInterval,
			})
			require.NoError(t, err)
			_, _ = startNode(t, n)
			// The seeded rows never stopped, so the running node is the one that
			// is not theirs.
			heartbeat := func() time.Time {
				nodes, err := core.ListNodes(context.Background(), db, core.ListNodesParams{})
				require.NoError(t, err)
				for _, node := range nodes {
					if node.ID != "eight-days" && node.ID != "six-days" {
						return node.HeartbeatAt
					}
				}
				return time.Time{}
			}
			require.Eventually(t, func() bool { return !heartbeat().IsZero() }, 5*time.Second, 5*time.Millisecond,
				"the node registers")
			beat := heartbeat()
			require.Eventually(t, func() bool { return heartbeat().After(beat) }, 5*time.Second,
				5*time.Millisecond, "the node is past its start")
			assert.Equal(t, tc.want, remaining(t, db))
		})
	}
}
