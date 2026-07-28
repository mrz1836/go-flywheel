// Command split-executors runs one registry across two executors: a
// long-running worker process and a bounded, invocation-scoped burst process.
//
// It is one program with three modes so the pieces stay side by side, but the
// point is that they are separate deployments sharing one database and one
// registry:
//
//	go run ./examples/split-executors enqueue   # produce some work
//	go run ./examples/split-executors worker    # the long-running pool (Ctrl+C to stop)
//	go run ./examples/split-executors burst     # the bounded pool, one invocation
//
// The shape it demonstrates:
//
//   - One Registry, built once by newRegistry and compiled into both binaries.
//     Both processes know every kind; ExecutorClass decides who runs what.
//   - The worker process runs a Node (runners + scheduler) until its context is
//     cancelled. It claims the "worker" class.
//   - The burst process builds a bare Runner claiming the "burst" class, derives
//     a deadline that reserves a teardown margin below its own budget, and calls
//     RunUntilIdle. That is the shape of a Lambda or a Kubernetes Job.
//   - The trap: the scheduler runs on exactly one process. Two schedulers means
//     two sweeps.
//
// It uses a shared SQLite file so both modes run on one machine with no setup. A
// real split deployment uses PostgreSQL, whose SKIP LOCKED claim is what makes
// two pools safe at concurrency above 1.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/glebarez/sqlite"
	flywheel "github.com/mrz1836/go-flywheel"
	"gorm.io/gorm"
)

// The two executor classes this deployment routes between. A class is a
// free-form label, not an enum: these strings are the whole routing contract
// between the producer, the two pools, and the jobs table.
const (
	// classWorker is the long-running pool: steady, latency-tolerant work.
	classWorker flywheel.ExecutorClass = "worker"
	// classBurst is the bounded pool: work that should drain inside one
	// invocation's budget.
	classBurst flywheel.ExecutorClass = "burst"
)

// invocationBudget is the wall-clock the bounded process is given — on a real
// platform this is the invocation timeout, read from the environment rather than
// hardcoded.
const invocationBudget = 30 * time.Second

// teardownMargin is reserved below invocationBudget so RunUntilIdle is cancelled
// while the process still has time to finalize the attempt in flight, flush
// logs, and return. A deadline set *at* the platform's own limit gets the
// process killed mid-finalize instead.
const teardownMargin = 5 * time.Second

// dbFile is the shared database both processes claim from.
const dbFile = "file:split-executors.db?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)&_txlock=immediate"

// ReindexArgs is the payload of a reindex job — steady background work for the
// long-running pool.
type ReindexArgs struct {
	Collection string `json:"collection"`
}

// Kind names the worker for these args.
func (ReindexArgs) Kind() string { return "reindex" }

// ReindexWorker handles reindex jobs.
type ReindexWorker struct{}

// Kind is the stable worker name.
func (ReindexWorker) Kind() string { return "reindex" }

// Work does the reindex and records what it did on the attempt's audit row.
// Result.Output lands in job_runs.output and is readable through
// flywheel.ListRuns, so this worker needs no lifecycle table of its own.
func (ReindexWorker) Work(ctx context.Context, job *flywheel.Job[ReindexArgs]) (flywheel.Result, error) {
	job.Logger.InfoContext(ctx, "reindexing", "collection", job.Args.Collection, "run_id", job.RunID)
	return flywheel.Result{
		Output: map[string]any{"collection": job.Args.Collection, "documents": 128},
	}, nil
}

// ThumbnailArgs is the payload of a thumbnail job — short, bursty work sized for
// a bounded invocation.
type ThumbnailArgs struct {
	AssetID string `json:"asset_id"`
}

// Kind names the worker for these args.
func (ThumbnailArgs) Kind() string { return "thumbnail" }

// ThumbnailWorker handles thumbnail jobs.
type ThumbnailWorker struct{}

// Kind is the stable worker name.
func (ThumbnailWorker) Kind() string { return "thumbnail" }

// Work renders the thumbnail and records the result on the attempt.
func (ThumbnailWorker) Work(ctx context.Context, job *flywheel.Job[ThumbnailArgs]) (flywheel.Result, error) {
	job.Logger.InfoContext(ctx, "rendering thumbnail", "asset", job.Args.AssetID, "run_id", job.RunID)
	return flywheel.Result{Output: map[string]any{"asset_id": job.Args.AssetID, "bytes": 8192}}, nil
}

// newRegistry builds the registry both processes share. It is one function, in
// one package, compiled into both binaries: the registry is not per-pool, and a
// kind registered here is dispatchable by whichever pool claims its job.
func newRegistry() *flywheel.Registry {
	reg := flywheel.NewRegistry()
	flywheel.Register(reg, ReindexWorker{})
	flywheel.Register(reg, ThumbnailWorker{})
	return reg
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	mode := "worker"
	if len(os.Args) > 1 {
		mode = os.Args[1]
	}

	db, err := openDB()
	if err != nil {
		logger.Error("open db", "error", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	switch mode {
	case "enqueue":
		err = enqueueWork(ctx, db, logger)
	case "worker":
		err = runWorkerProcess(ctx, db, logger)
	case "burst":
		err = runBurstProcess(ctx, db, logger)
	default:
		err = fmt.Errorf("unknown mode %q: want enqueue, worker, or burst", mode)
	}
	if err != nil {
		logger.Error(mode, "error", err)
		os.Exit(1)
	}
}

// openDB opens the shared database and installs the schema. This example is the
// library-owned install: one SQLite file, no other writer, no migration tool. A
// host whose application schema shares this database installs the tables from
// its own loader and calls flywheel.InstallIndexes instead.
func openDB() (*gorm.DB, error) {
	db, err := gorm.Open(sqlite.Open(dbFile), &gorm.Config{})
	if err != nil {
		return nil, fmt.Errorf("open: %w", err)
	}
	if sqlDB, derr := db.DB(); derr == nil {
		sqlDB.SetMaxOpenConns(1)
	}
	if err := flywheel.Migrate(db); err != nil {
		return nil, fmt.Errorf("migrate: %w", err)
	}
	return db, nil
}

// enqueueWork produces one job for each routing case, which is the clearest way
// to see what ExecutorClass decides.
func enqueueWork(ctx context.Context, db *gorm.DB, logger *slog.Logger) error {
	client := flywheel.NewClient(db)

	// Pinned to the long-running pool: only a runner whose ExecutorClass is
	// "worker" (or one with ClaimAnyClass) can claim it.
	if _, err := flywheel.Insert(
		ctx, client,
		ReindexArgs{Collection: "documents"},
		flywheel.InsertOpts{ExecutorClass: classWorker},
	); err != nil {
		return fmt.Errorf("enqueue reindex: %w", err)
	}

	// Pinned to the bounded pool.
	if _, err := flywheel.Insert(
		ctx, client,
		ThumbnailArgs{AssetID: "asset-1"},
		flywheel.InsertOpts{ExecutorClass: classBurst},
	); err != nil {
		return fmt.Errorf("enqueue thumbnail: %w", err)
	}

	// Unpinned: flywheel.AnyClass is the empty default, and a job carrying it is
	// claimable by *either* pool — whichever polls first. Leave a job unpinned
	// unless it genuinely needs one pool's hardware, credentials, or budget.
	if _, err := flywheel.Insert(
		ctx, client,
		ThumbnailArgs{AssetID: "asset-2"},
		flywheel.InsertOpts{ExecutorClass: flywheel.AnyClass},
	); err != nil {
		return fmt.Errorf("enqueue unpinned thumbnail: %w", err)
	}

	logger.InfoContext(ctx, "enqueued three jobs: one worker-pinned, one burst-pinned, one unpinned")
	return nil
}

// runWorkerProcess is the long-running deployment: a Node with one runner on the
// worker class, plus the scheduler.
//
// The scheduler belongs here and nowhere else. It owns the periodic ticks and
// the stuck-lease sweep that reclaims jobs whose executor died — running a
// second one in the burst process would double every tick and race every sweep.
// One process, one scheduler; every other process leaves SchedulerConfig nil.
func runWorkerProcess(ctx context.Context, db *gorm.DB, logger *slog.Logger) error {
	// One driver, shared by this process's runner and its scheduler.
	driver := flywheel.NewSQLiteDriver(db)

	node, err := flywheel.NewNode(flywheel.NodeConfig{
		Runners: []flywheel.RunnerConfig{{
			DB:            db,
			Driver:        driver,
			Registry:      newRegistry(),
			Queues:        []string{"default", "periodic"},
			ExecutorClass: classWorker,
			Concurrency:   1, // SQLite serializes writers; Postgres runs this higher
			Logger:        logger,
		}},
		Scheduler: &flywheel.SchedulerConfig{DB: db, Client: flywheel.NewClient(db), Driver: driver},
		Logger:    logger,
	})
	if err != nil {
		return fmt.Errorf("build node: %w", err)
	}

	logger.InfoContext(ctx, "worker pool running; press Ctrl+C to stop", "class", string(classWorker))
	if err := node.Run(ctx); err != nil {
		return fmt.Errorf("node: %w", err)
	}
	return nil
}

// runBurstProcess is the bounded deployment: no Node, no scheduler, one bare
// Runner drained under a deadline.
func runBurstProcess(ctx context.Context, db *gorm.DB, logger *slog.Logger) error {
	runner, err := flywheel.NewRunner(flywheel.RunnerConfig{
		DB:            db,
		Driver:        flywheel.NewSQLiteDriver(db),
		Registry:      newRegistry(), // the same registry the worker process has
		Queues:        []string{"default"},
		ExecutorClass: classBurst,
		Concurrency:   1,
		Logger:        logger,
	})
	if err != nil {
		return fmt.Errorf("build runner: %w", err)
	}

	// Reserve the teardown margin below the invocation's own budget, so the
	// deadline fires while there is still time to finalize cleanly.
	budget, cancel := context.WithTimeout(ctx, invocationBudget-teardownMargin)
	defer cancel()

	// RunUntilIdle returns nil only when *no job is in a non-terminal state* —
	// not merely when this runner found nothing to claim. A job another pool is
	// still running, or one waiting out a retry backoff, keeps it looping. So the
	// three outcomes are distinct and worth branching on:
	err = runner.RunUntilIdle(budget)
	switch {
	case err == nil:
		logger.InfoContext(ctx, "queue fully drained: every job reached a terminal state")
		return nil
	case errors.Is(err, context.DeadlineExceeded):
		// The budget was spent with work still outstanding. That is a normal
		// outcome for a bounded invocation, not a failure: the leftover jobs stay
		// claimable and the next invocation picks them up.
		logger.InfoContext(ctx, "invocation budget spent with work outstanding; exiting cleanly")
		return nil
	case errors.Is(err, context.Canceled):
		logger.InfoContext(ctx, "shutdown signal received; exiting cleanly")
		return nil
	default:
		// Anything else is a real failure — a driver error, an unregistered kind
		// — and should fail the invocation.
		return fmt.Errorf("run until idle: %w", err)
	}
}
