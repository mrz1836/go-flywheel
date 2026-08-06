package node

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	ft "github.com/mrz1836/go-flywheel/flywheeltest"
	core "github.com/mrz1836/go-flywheel/internal/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

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

// sqliteRunner is a one-line core.RunnerConfig for a Node test over db.
func sqliteRunner(db *gorm.DB, reg *core.Registry) core.RunnerConfig {
	return core.RunnerConfig{
		DB: db, Driver: core.NewSQLiteDriver(db), Registry: reg,
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
	_, err := NewNode(NodeConfig{Runners: []core.RunnerConfig{{ /* missing DB */ }}})
	require.ErrorContains(t, err, "runner config requires DB", "an invalid runner config surfaces through NewNode")
}

func TestNewNodeRejectsSchedulerWithoutDBAndClient(t *testing.T) {
	t.Parallel()
	db := ft.NewDB(t)
	reg := core.NewRegistry()
	_, err := NewNode(NodeConfig{
		Runners:   []core.RunnerConfig{sqliteRunner(db, reg)},
		Scheduler: &core.SchedulerConfig{}, // nil DB, Client, and Driver
	})
	// The scheduler's own constructor is the authority, so the error names the
	// first missing field rather than a generic "node scheduler config" verdict.
	require.ErrorContains(t, err, "scheduler config requires DB")
}

// TestNewNodePropagatesASchedulerConstructionError proves the propagation for a
// config NewNode itself never inspected: DB and Client are present, and only the
// Driver is missing. Before the scheduler owned its validation this reached
// core.NewSchedulerWithConfig unchecked and a Node was built around a scheduler that
// could not sweep.
func TestNewNodePropagatesASchedulerConstructionError(t *testing.T) {
	t.Parallel()
	db := ft.NewDB(t)
	reg := core.NewRegistry()
	_, err := NewNode(NodeConfig{
		Runners:   []core.RunnerConfig{sqliteRunner(db, reg)},
		Scheduler: &core.SchedulerConfig{DB: db, Client: core.NewClient(db)}, // nil Driver
	})
	require.ErrorContains(t, err, "scheduler config requires Driver")
}

func TestNodeRunDrainsRunnerOnContextCancel(t *testing.T) {
	t.Parallel()
	db := ft.NewWALFileDB(t)
	reg := core.NewRegistry()
	w := &ft.SuccessWorker{}
	core.Register(reg, w)

	node, err := NewNode(NodeConfig{Runners: []core.RunnerConfig{sqliteRunner(db, reg)}})
	require.NoError(t, err)

	id, err := core.Insert(context.Background(), core.NewClient(db), ft.SuccessArgs{V: "x"}, core.InsertOpts{})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	runErr := make(chan error, 1)
	go func() { runErr <- node.Run(ctx) }()

	ft.WaitForJobState(t, db, id, string(core.StateSucceeded), 3*time.Second)
	cancel()

	select {
	case err := <-runErr:
		require.NoError(t, err, "a context-cancel drain returns nil")
	case <-time.After(3 * time.Second):
		t.Fatal("node.Run did not return after cancel")
	}
	assert.EqualValues(t, 1, w.Calls.Load())
}

func TestNodeProcessesEnqueuedBatch(t *testing.T) {
	t.Parallel()
	db := ft.NewWALFileDB(t)
	reg := core.NewRegistry()
	w := &ft.SuccessWorker{}
	core.Register(reg, w)

	const jobs = 20
	ids := make([]string, jobs)
	for i := range ids {
		id, err := core.Insert(context.Background(), core.NewClient(db), ft.SuccessArgs{V: fmt.Sprintf("v%d", i)}, core.InsertOpts{})
		require.NoError(t, err)
		ids[i] = id
	}

	// A single SQLite runner (SQLite serializes writers, so one is the supported
	// shape) drains the whole batch. Multiple-runner concurrency is exercised on
	// Postgres in the integration suite.
	node, err := NewNode(NodeConfig{Runners: []core.RunnerConfig{sqliteRunner(db, reg)}})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	runErr := make(chan error, 1)
	go func() { runErr <- node.Run(ctx) }()

	for _, id := range ids {
		ft.WaitForJobState(t, db, id, string(core.StateSucceeded), 5*time.Second)
	}
	cancel()
	require.NoError(t, <-runErr)
	assert.EqualValues(t, jobs, w.Calls.Load(), "every job ran exactly once")
}

func TestNodeRunsSchedulerEnqueuesPeriodic(t *testing.T) {
	t.Parallel()
	db := ft.NewWALFileDB(t)
	reg := core.NewRegistry()
	w := &ft.SuccessWorker{}
	core.Register(reg, w)

	// A periodic that was due a minute ago must fire immediately once the Node's
	// scheduler ticks; the Node's runner then processes the enqueued job.
	ft.InstallPeriodic(t, db, "node-sched", "test.success", time.Now().Add(-time.Minute), true)

	node, err := NewNode(NodeConfig{
		Runners: []core.RunnerConfig{sqliteRunner(db, reg)},
		Scheduler: &core.SchedulerConfig{
			DB: db, Client: core.NewClient(db), Driver: core.NewSQLiteDriver(db),
			TickInterval: 5 * time.Millisecond, SweepInterval: 50 * time.Millisecond,
		},
	})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	runErr := make(chan error, 1)
	go func() { runErr <- node.Run(ctx) }()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && w.Calls.Load() == 0 {
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	require.NoError(t, <-runErr)
	assert.Positive(t, w.Calls.Load(), "the scheduler enqueued a periodic job that the runner processed")
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
func (w *blockingWorker) Work(_ context.Context, _ *core.Job[blockingArgs]) (core.Result, error) {
	close(w.started)
	<-w.release
	close(w.done)
	return core.Result{}, nil
}

func TestNodeRunHonorsDrainTimeout(t *testing.T) {
	t.Parallel()
	db := ft.NewWALFileDB(t)
	reg := core.NewRegistry()
	w := &blockingWorker{started: make(chan struct{}), release: make(chan struct{}), done: make(chan struct{})}
	core.Register(reg, w)

	node, err := NewNode(NodeConfig{
		Runners:      []core.RunnerConfig{sqliteRunner(db, reg)},
		DrainTimeout: 100 * time.Millisecond,
	})
	require.NoError(t, err)

	_, err = core.Insert(context.Background(), core.NewClient(db), blockingArgs{}, core.InsertOpts{})
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
			healthMux(tc.readiness, nil).ServeHTTP(rec, req)
			assert.Equal(t, tc.wantCode, rec.Code)
			assert.Equal(t, tc.wantBody, rec.Body.String())
		})
	}
}

func TestNodeHealthMuxServesMetricsWhenHandlerSet(t *testing.T) {
	t.Parallel()
	stub := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("metrics-body"))
	})
	mux := healthMux(nil, stub)

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "metrics-body", rec.Body.String(), "the supplied metrics handler is served at /metrics")

	// Liveness is unaffected by a metrics handler being present.
	health := httptest.NewRecorder()
	mux.ServeHTTP(health, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	assert.Equal(t, http.StatusOK, health.Code)
}

func TestNodeHealthMuxMetricsIs404WhenHandlerNil(t *testing.T) {
	t.Parallel()
	mux := healthMux(nil, nil)

	metrics := httptest.NewRecorder()
	mux.ServeHTTP(metrics, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	assert.Equal(t, http.StatusNotFound, metrics.Code, "no handler leaves /metrics unrouted")

	// /healthz still serves with no metrics handler.
	health := httptest.NewRecorder()
	mux.ServeHTTP(health, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	assert.Equal(t, http.StatusOK, health.Code)
}

func TestNodeServeHealthEndToEnd(t *testing.T) {
	t.Parallel()
	db := ft.NewWALFileDB(t)
	reg := core.NewRegistry()
	core.Register(reg, &ft.SuccessWorker{})
	addr := ft.FreeAddr(t)

	node, err := NewNode(NodeConfig{
		Runners: []core.RunnerConfig{sqliteRunner(db, reg)},
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

func TestNodeServeMetricsEndToEnd(t *testing.T) {
	t.Parallel()
	db := ft.NewWALFileDB(t)
	reg := core.NewRegistry()
	core.Register(reg, &ft.SuccessWorker{})
	addr := ft.FreeAddr(t)

	metrics := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("flywheel_queue_ready 0\n"))
	})
	node, err := NewNode(NodeConfig{
		Runners: []core.RunnerConfig{sqliteRunner(db, reg)},
		Health:  HealthConfig{Addr: addr, MetricsHandler: metrics},
	})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	runErr := make(chan error, 1)
	go func() { runErr <- node.Run(ctx) }()

	base := "http://" + addr
	requireEventually200(t, base+"/metrics", 3*time.Second)

	client := &http.Client{Timeout: 500 * time.Millisecond}
	resp, err := client.Get(base + "/metrics")
	require.NoError(t, err)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	_ = resp.Body.Close()
	assert.Contains(t, string(body), "flywheel_queue_ready", "the Node serves the supplied metrics handler at /metrics")

	// /healthz still serves alongside /metrics.
	requireEventually200(t, base+"/healthz", time.Second)

	cancel()
	require.NoError(t, <-runErr)
}

func TestDBPingerReadiness(t *testing.T) {
	t.Parallel()
	require.NoError(t, dbPinger(nil)(context.Background()), "a nil db is always ready")

	db := ft.NewDB(t)
	require.NoError(t, dbPinger(db)(context.Background()), "an open db pings ready")

	sqlDB, err := db.DB()
	require.NoError(t, err)
	require.NoError(t, sqlDB.Close())
	require.Error(t, dbPinger(db)(context.Background()), "a closed db fails readiness")
}

// TestNodeRunSurfacesHealthListenError proves a Node whose health server cannot
// bind its address fails fast: serveHealth returns the listen error, the fail
// closure records it and tears down the siblings, and Run returns that first
// error through drainErrors.
func TestNodeRunSurfacesHealthListenError(t *testing.T) {
	t.Parallel()
	db := ft.NewWALFileDB(t)
	reg := core.NewRegistry()
	core.Register(reg, &ft.SuccessWorker{})

	node, err := NewNode(NodeConfig{
		Runners: []core.RunnerConfig{{
			DB: db, Driver: core.NewSQLiteDriver(db), Registry: reg,
			Queues: []string{"default"}, ExecutorClass: "local", Concurrency: 1,
			PollInterval: time.Hour, // keep the runner quiet so the health error is the first
		}},
		Health: HealthConfig{Addr: "256.256.256.256:99999"}, // an unbindable address
	})
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err = node.Run(ctx)
	require.ErrorContains(t, err, "health server", "the health listen failure is surfaced as the node's first error")
}
