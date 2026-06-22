// Command sqlite-quickstart is a minimal flywheel daemon over a local SQLite
// file. It registers a worker, enqueues one job, and runs the runtime (runner +
// scheduler) until interrupted. It is the smallest "bolt flywheel onto an app"
// example.
//
//	go run ./examples/sqlite-quickstart
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/glebarez/sqlite"
	flywheel "github.com/mrz1836/go-flywheel"
	"gorm.io/gorm"
)

// GreetArgs is the typed payload of a greet job.
type GreetArgs struct {
	Name string `json:"name"`
}

// Kind names the worker for these args.
func (GreetArgs) Kind() string { return "greet" }

// GreetWorker handles greet jobs.
type GreetWorker struct{}

// Kind is the stable worker name.
func (GreetWorker) Kind() string { return "greet" }

// Work logs a greeting; a real worker would do durable side-effect work here.
func (GreetWorker) Work(ctx context.Context, job *flywheel.Job[GreetArgs]) (flywheel.Result, error) {
	job.Logger.InfoContext(ctx, "hello "+job.Args.Name)
	return flywheel.Result{}, nil
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	// WAL mode + a single writer connection is the robust local-SQLite setup.
	db, err := gorm.Open(sqlite.Open("file:flywheel.db?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)&_txlock=immediate"), &gorm.Config{})
	if err != nil {
		logger.Error("open db", "error", err)
		os.Exit(1)
	}
	if sqlDB, derr := db.DB(); derr == nil {
		sqlDB.SetMaxOpenConns(1)
	}
	if err := flywheel.Migrate(db); err != nil {
		logger.Error("migrate", "error", err)
		os.Exit(1)
	}

	reg := flywheel.NewRegistry()
	flywheel.Register(reg, GreetWorker{})

	if _, err := flywheel.Insert(context.Background(), flywheel.NewClient(db),
		GreetArgs{Name: "world"}, flywheel.InsertOpts{}); err != nil {
		logger.Error("enqueue", "error", err)
		os.Exit(1)
	}

	node, err := flywheel.NewNode(flywheel.NodeConfig{
		Runners: []flywheel.RunnerConfig{{
			DB: db, Driver: flywheel.NewSQLiteDriver(db), Registry: reg,
			Queues: []string{"default", "periodic"}, Concurrency: 1, ClaimAnyClass: true, Logger: logger,
		}},
		Scheduler: &flywheel.SchedulerConfig{DB: db, Client: flywheel.NewClient(db)},
		Logger:    logger,
	})
	if err != nil {
		logger.Error("build node", "error", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	logger.Info("flywheel quickstart running; press Ctrl+C to stop")
	if err := node.Run(ctx); err != nil {
		logger.Error("node", "error", err)
		os.Exit(1)
	}
}
