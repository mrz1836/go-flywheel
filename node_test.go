package flywheel

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// newWALFileDB opens a fresh file-backed SQLite database in WAL mode — the same
// configuration the local daemon uses — so a Node's runner can write while the
// test polls the DB to observe progress, without hitting shared-cache LOCKED
// errors. Migrate stands up the full schema.
func newWALFileDB(t *testing.T) *gorm.DB {
	t.Helper()
	dsn := "file:" + t.TempDir() + "/flywheel.db?_journal_mode=WAL&_busy_timeout=5000&_txlock=immediate&_synchronous=NORMAL"
	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	require.NoError(t, err)
	t.Cleanup(func() {
		if sqlDB, derr := db.DB(); derr == nil {
			_ = sqlDB.Close()
		}
	})
	require.NoError(t, Migrate(db))
	return db
}

// freeAddr reserves and releases an ephemeral loopback port, returning its
// address for a server to bind. The brief reserve/release window is tolerated by
// the callers' connect-retry loops.
func freeAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := ln.Addr().String()
	require.NoError(t, ln.Close())
	return addr
}

// waitForJobState polls until jobID reaches state or the deadline elapses. It is
// the deterministic way to observe a Node's infinite Run loop making progress.
func waitForJobState(t *testing.T, db *gorm.DB, jobID, state string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if jobState(t, db, jobID) == state {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("job %s did not reach state %q within %s (last: %q)", jobID, state, timeout, jobState(t, db, jobID))
}

// requireEventually200 polls url until it returns 200 or the deadline elapses.
func requireEventually200(t *testing.T, url string, timeout time.Duration) {
	t.Helper()
	client := &http.Client{Timeout: 200 * time.Millisecond}
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		resp, err := client.Get(url)
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("GET %s did not return 200 within %s", url, timeout)
}

// sqliteRunner is a one-line RunnerConfig for a Node test over db.
func sqliteRunner(db *gorm.DB, reg *Registry) RunnerConfig {
	return RunnerConfig{
		DB: db, Driver: NewSQLiteDriver(db), Registry: reg,
		Queues: []string{"default", "periodic"}, ClaimAnyClass: true,
		PollInterval: 5 * time.Millisecond,
	}
}

func TestNewNodeRequiresRunner(t *testing.T) {
	t.Parallel()
	_, err := NewNode(NodeConfig{})
	require.ErrorIs(t, err, errNodeNeedsRunner)
}

func TestNewNodeRejectsInvalidRunnerConfig(t *testing.T) {
	t.Parallel()
	_, err := NewNode(NodeConfig{Runners: []RunnerConfig{{ /* missing DB */ }}})
	require.ErrorIs(t, err, errRunnerNeedsDB, "an invalid runner config surfaces through NewNode")
}

func TestNewNodeRejectsSchedulerWithoutDBAndClient(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	reg := NewRegistry()
	_, err := NewNode(NodeConfig{
		Runners:   []RunnerConfig{sqliteRunner(db, reg)},
		Scheduler: &SchedulerConfig{}, // nil DB and Client
	})
	require.ErrorIs(t, err, errNodeSchedulerConfig)
}

func TestNodeRunDrainsRunnerOnContextCancel(t *testing.T) {
	t.Parallel()
	db := newWALFileDB(t)
	reg := NewRegistry()
	w := &successWorker{}
	Register(reg, w)

	node, err := NewNode(NodeConfig{Runners: []RunnerConfig{sqliteRunner(db, reg)}})
	require.NoError(t, err)

	id, err := Insert(context.Background(), NewClient(db), successArgs{V: "x"}, InsertOpts{})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	runErr := make(chan error, 1)
	go func() { runErr <- node.Run(ctx) }()

	waitForJobState(t, db, id, string(StateSucceeded), 3*time.Second)
	cancel()

	select {
	case err := <-runErr:
		require.NoError(t, err, "a context-cancel drain returns nil")
	case <-time.After(3 * time.Second):
		t.Fatal("node.Run did not return after cancel")
	}
	assert.EqualValues(t, 1, w.calls.Load())
}

func TestNodeProcessesEnqueuedBatch(t *testing.T) {
	t.Parallel()
	db := newWALFileDB(t)
	reg := NewRegistry()
	w := &successWorker{}
	Register(reg, w)

	const jobs = 20
	ids := make([]string, jobs)
	for i := range ids {
		id, err := Insert(context.Background(), NewClient(db), successArgs{V: fmt.Sprintf("v%d", i)}, InsertOpts{})
		require.NoError(t, err)
		ids[i] = id
	}

	// A single SQLite runner (SQLite serializes writers, so one is the supported
	// shape) drains the whole batch. Multiple-runner concurrency is exercised on
	// Postgres in the integration suite.
	node, err := NewNode(NodeConfig{Runners: []RunnerConfig{sqliteRunner(db, reg)}})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	runErr := make(chan error, 1)
	go func() { runErr <- node.Run(ctx) }()

	for _, id := range ids {
		waitForJobState(t, db, id, string(StateSucceeded), 5*time.Second)
	}
	cancel()
	require.NoError(t, <-runErr)
	assert.EqualValues(t, jobs, w.calls.Load(), "every job ran exactly once")
}

func TestNodeRunsSchedulerEnqueuesPeriodic(t *testing.T) {
	t.Parallel()
	db := newWALFileDB(t)
	reg := NewRegistry()
	w := &successWorker{}
	Register(reg, w)

	// A periodic that was due a minute ago must fire immediately once the Node's
	// scheduler ticks; the Node's runner then processes the enqueued job.
	installPeriodic(t, db, "node-sched", "test.success", time.Now().Add(-time.Minute), true)

	node, err := NewNode(NodeConfig{
		Runners: []RunnerConfig{sqliteRunner(db, reg)},
		Scheduler: &SchedulerConfig{
			DB: db, Client: NewClient(db),
			TickInterval: 5 * time.Millisecond, SweepInterval: 50 * time.Millisecond,
		},
	})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	runErr := make(chan error, 1)
	go func() { runErr <- node.Run(ctx) }()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && w.calls.Load() == 0 {
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	require.NoError(t, <-runErr)
	assert.Positive(t, w.calls.Load(), "the scheduler enqueued a periodic job that the runner processed")
}

// blockingArgs/blockingWorker park inside Work until released, modeling a worker
// that does not observe ctx cancellation — used to exercise the drain timeout.
type blockingArgs struct{}

func (blockingArgs) Kind() string { return "test.blocking" }

type blockingWorker struct {
	started chan struct{}
	release chan struct{}
	done    chan struct{}
}

func (*blockingWorker) Kind() string { return "test.blocking" }
func (w *blockingWorker) Work(_ context.Context, _ *Job[blockingArgs]) (Result, error) {
	close(w.started)
	<-w.release
	close(w.done)
	return Result{}, nil
}

func TestNodeRunHonorsDrainTimeout(t *testing.T) {
	t.Parallel()
	db := newWALFileDB(t)
	reg := NewRegistry()
	w := &blockingWorker{started: make(chan struct{}), release: make(chan struct{}), done: make(chan struct{})}
	Register(reg, w)

	node, err := NewNode(NodeConfig{
		Runners:      []RunnerConfig{sqliteRunner(db, reg)},
		DrainTimeout: 100 * time.Millisecond,
	})
	require.NoError(t, err)

	_, err = Insert(context.Background(), NewClient(db), blockingArgs{}, InsertOpts{})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	start := time.Now()
	runErr := make(chan error, 1)
	go func() { runErr <- node.Run(ctx) }()

	<-w.started // the worker is in-flight and will not return until released
	cancel()    // request shutdown while the worker is blocked

	// Run must return on the drain timeout rather than waiting for the stuck
	// worker, which never observes the cancellation.
	select {
	case err := <-runErr:
		require.NoError(t, err, "Run returns after the drain timeout even with a stuck worker")
		assert.Less(t, time.Since(start), time.Second, "Run returns near the 100ms drain timeout, not blocked on the worker")
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return within the drain timeout")
	}

	// Release the stuck worker so its goroutine unwinds before the DB is torn
	// down. The post-cancel finalize is a no-op the lease sweep would recover.
	close(w.release)
	<-w.done
}

func TestNodeHealthMuxRoutes(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		path      string
		readiness func(context.Context) error
		wantCode  int
		wantBody  string
	}{
		{"healthz is always 200", "/healthz", nil, http.StatusOK, "ok"},
		{"readyz with nil readiness is ready", "/readyz", nil, http.StatusOK, "ready"},
		{"readyz with passing readiness is ready", "/readyz", func(context.Context) error { return nil }, http.StatusOK, "ready"},
		{"readyz with failing readiness is unavailable", "/readyz", func(context.Context) error { return errors.New("db down") }, http.StatusServiceUnavailable, "unavailable"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rec := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodGet, tc.path, nil)
			healthMux(tc.readiness).ServeHTTP(rec, req)
			assert.Equal(t, tc.wantCode, rec.Code)
			assert.Equal(t, tc.wantBody, rec.Body.String())
		})
	}
}

func TestNodeServeHealthEndToEnd(t *testing.T) {
	t.Parallel()
	db := newWALFileDB(t)
	reg := NewRegistry()
	Register(reg, &successWorker{})
	addr := freeAddr(t)

	node, err := NewNode(NodeConfig{
		Runners: []RunnerConfig{sqliteRunner(db, reg)},
		Health:  HealthConfig{Addr: addr},
	})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	runErr := make(chan error, 1)
	go func() { runErr <- node.Run(ctx) }()

	base := "http://" + addr
	requireEventually200(t, base+"/healthz", 3*time.Second)

	// The default readiness pings the open DB, so /readyz is 200.
	client := &http.Client{Timeout: 500 * time.Millisecond}
	resp, err := client.Get(base + "/readyz")
	require.NoError(t, err)
	_ = resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	cancel()
	require.NoError(t, <-runErr)

	// After the node drains, the health server is stopped.
	_, err = client.Get(base + "/healthz")
	require.Error(t, err, "the health server is stopped after the node drains")
}

func TestDBPingerReadiness(t *testing.T) {
	t.Parallel()
	require.NoError(t, dbPinger(nil)(context.Background()), "a nil db is always ready")

	db := newDB(t)
	require.NoError(t, dbPinger(db)(context.Background()), "an open db pings ready")

	sqlDB, err := db.DB()
	require.NoError(t, err)
	require.NoError(t, sqlDB.Close())
	require.Error(t, dbPinger(db)(context.Background()), "a closed db fails readiness")
}
