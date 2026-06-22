package flywheel

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"gorm.io/gorm"
)

// defaultHealthShutdownTimeout bounds the health server's graceful shutdown.
const defaultHealthShutdownTimeout = 5 * time.Second

// HealthConfig configures a Node's optional in-process health server. It uses
// only net/http from the standard library, so enabling it adds no dependency. A
// zero value (empty Addr) disables the server.
type HealthConfig struct {
	// Addr is the listen address (e.g. ":8080"). Empty disables the server.
	Addr string
	// Readiness reports whether the node is ready to serve work. When nil, the
	// Node installs a default that pings the first runner's database. /healthz is
	// always a shallow liveness 200; /readyz gates its 200 on Readiness.
	Readiness func(ctx context.Context) error
	// ShutdownTimeout bounds the health server's graceful shutdown. Optional;
	// defaults to five seconds.
	ShutdownTimeout time.Duration
}

// NodeConfig declares everything a Node runs: one or more runners, an optional
// scheduler, and an optional health server. It is the one-call replacement for a
// hand-wired runner + scheduler + health-server + signal-drain main().
type NodeConfig struct {
	// Runners are the dispatch loops this Node hosts. Each may claim a different
	// set of queues at a different concurrency for a different executor class. At
	// least one is required.
	Runners []RunnerConfig
	// Scheduler, when non-nil, runs periodic ticks plus the stuck-lease sweep.
	// Leave it nil on a pure worker node where another process owns scheduling.
	Scheduler *SchedulerConfig
	// Health configures the optional liveness/readiness server.
	Health HealthConfig
	// Logger logs the Node's own lifecycle events. Optional; defaults to
	// slog.Default(). It does not override a RunnerConfig.Logger.
	Logger *slog.Logger
	// DrainTimeout bounds how long Run waits for in-flight work after ctx is
	// cancelled before returning regardless. Zero waits for the natural,
	// lease-bounded drain.
	DrainTimeout time.Duration
}

// Node is a self-contained job-runtime process: it owns its runners, an optional
// scheduler, and an optional health server, starts them together, and drains
// cleanly when its context is cancelled. It turns the ~100 lines of lifecycle
// boilerplate every host re-implements into a single Run call.
type Node struct {
	cfg       NodeConfig
	runners   []*Runner
	scheduler *Scheduler
	logger    *slog.Logger
}

// NewNode validates cfg and constructs the Node. Each runner is built through
// NewRunner (so the SQLite concurrency-1 guard and every zero-value default
// still apply) and the scheduler, when configured, through NewSchedulerWithConfig.
func NewNode(cfg NodeConfig) (*Node, error) {
	if len(cfg.Runners) == 0 {
		return nil, errNodeNeedsRunner
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}

	runners := make([]*Runner, 0, len(cfg.Runners))
	for i := range cfg.Runners {
		r, err := NewRunner(cfg.Runners[i])
		if err != nil {
			return nil, fmt.Errorf("flywheel: node runner[%d]: %w", i, err)
		}
		runners = append(runners, r)
	}

	var scheduler *Scheduler
	if cfg.Scheduler != nil {
		if cfg.Scheduler.DB == nil || cfg.Scheduler.Client == nil {
			return nil, errNodeSchedulerConfig
		}
		scheduler = NewSchedulerWithConfig(*cfg.Scheduler)
	}

	if cfg.Health.Addr != "" && cfg.Health.Readiness == nil {
		cfg.Health.Readiness = dbPinger(cfg.Runners[0].DB)
	}

	return &Node{cfg: cfg, runners: runners, scheduler: scheduler, logger: logger}, nil
}

// Run starts every component and blocks until ctx is cancelled and all
// components have drained (lease-bounded, or DrainTimeout if it is shorter). It
// returns the first non-cancellation error any component reported, or nil on a
// clean drain. Signal handling stays with the caller: pass a context from
// signal.NotifyContext.
func (n *Node) Run(ctx context.Context) error {
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wg sync.WaitGroup
	errCh := make(chan error, len(n.runners)+2)
	// fail records a component's first real (non-cancellation) error and tears
	// down its siblings by cancelling the shared run context.
	fail := func(err error) {
		if err != nil && !errors.Is(err, context.Canceled) {
			errCh <- err
			cancel()
		}
	}

	for i := range n.runners {
		r := n.runners[i]
		wg.Add(1)
		go func() {
			defer wg.Done()
			fail(r.Run(runCtx))
		}()
	}
	if n.scheduler != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			fail(n.scheduler.Run(runCtx))
		}()
	}
	if n.cfg.Health.Addr != "" {
		wg.Add(1)
		go func() {
			defer wg.Done()
			fail(n.serveHealth(runCtx))
		}()
	}

	n.logger.InfoContext(ctx, "flywheel: node started",
		"runners", len(n.runners),
		"scheduler", n.scheduler != nil,
		"health_addr", n.cfg.Health.Addr)

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()

	select {
	case <-done:
	case <-ctx.Done():
		n.awaitDrain(ctx, done)
	}

	if firstErr := drainErrors(errCh); firstErr != nil {
		n.logger.ErrorContext(ctx, "flywheel: node stopped with error", "error", firstErr)
		return firstErr
	}
	n.logger.InfoContext(ctx, "flywheel: node drained and stopped")
	return nil
}

// awaitDrain waits for every component goroutine to exit after a shutdown
// signal, bounded by DrainTimeout when one is set.
func (n *Node) awaitDrain(ctx context.Context, done <-chan struct{}) {
	if n.cfg.DrainTimeout <= 0 {
		<-done
		return
	}
	select {
	case <-done:
	case <-time.After(n.cfg.DrainTimeout):
		n.logger.WarnContext(ctx, "flywheel: node drain timed out; some in-flight jobs may not have finished",
			"drain_timeout", n.cfg.DrainTimeout.String())
	}
}

// serveHealth runs the liveness/readiness server until ctx is cancelled, then
// shuts it down gracefully. A listen failure is returned (and stops the Node);
// a clean shutdown returns nil.
func (n *Node) serveHealth(ctx context.Context) error {
	shutdownTimeout := n.cfg.Health.ShutdownTimeout
	if shutdownTimeout <= 0 {
		shutdownTimeout = defaultHealthShutdownTimeout
	}
	srv := &http.Server{
		Addr:              n.cfg.Health.Addr,
		Handler:           healthMux(n.cfg.Health.Readiness),
		ReadHeaderTimeout: shutdownTimeout,
	}

	shutdownDone := make(chan struct{})
	go func() {
		defer close(shutdownDone)
		<-ctx.Done()
		// ctx is already cancelled; the graceful shutdown needs a fresh deadline.
		shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			n.logger.ErrorContext(ctx, "flywheel: health server shutdown", "error", err)
		}
	}()

	if err := srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("flywheel: health server: %w", err)
	}
	<-shutdownDone
	return nil
}

// healthMux builds the health server's routes: /healthz is an unconditional
// liveness 200, /readyz returns 200 only when readiness (if set) reports nil.
func healthMux(readiness func(ctx context.Context) error) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		if readiness != nil {
			if err := readiness(r.Context()); err != nil {
				w.WriteHeader(http.StatusServiceUnavailable)
				_, _ = w.Write([]byte("unavailable"))
				return
			}
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ready"))
	})
	return mux
}

// dbPinger returns a readiness check that pings db. A nil db yields a check that
// always reports ready — there is nothing to probe.
func dbPinger(db *gorm.DB) func(context.Context) error {
	return func(ctx context.Context) error {
		if db == nil {
			return nil
		}
		sqlDB, err := db.DB()
		if err != nil {
			return fmt.Errorf("flywheel: readiness: %w", err)
		}
		if err := sqlDB.PingContext(ctx); err != nil {
			return fmt.Errorf("flywheel: readiness ping: %w", err)
		}
		return nil
	}
}

// drainErrors non-blockingly collects the first error buffered on errCh. It does
// not close the channel, so it is safe even if a component is still shutting down
// after a drain timeout.
func drainErrors(errCh <-chan error) error {
	var first error
	for {
		select {
		case err := <-errCh:
			if first == nil {
				first = err
			}
		default:
			return first
		}
	}
}
