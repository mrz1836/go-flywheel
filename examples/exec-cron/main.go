// Command exec-cron shows flywheel replacing cron. It registers the generic
// ExecWorker and schedules a shell command to run on an interval — durably, with
// retries and a full per-run audit trail — without writing any job-specific Go.
//
//	go run ./examples/exec-cron
//
// Inspect the audit trail with the flywheel CLI against the same database:
//
//	flywheel jobs ls --db flywheel-cron.db
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	flywheel "github.com/mrz1836/go-flywheel"
	"github.com/mrz1836/go-flywheel/workers"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	db, err := gorm.Open(sqlite.Open("file:flywheel-cron.db?_journal_mode=WAL&_busy_timeout=5000&_txlock=immediate"), &gorm.Config{})
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
	flywheel.Register(reg, workers.ExecWorker{})

	// Replace a crontab line like:
	//   */1 * * * * sh -c "date '+%T' && echo healthy"
	// with a durable, retried, audited periodic job.
	args := []byte(`{"command":"sh","args":["-c","date '+%T' && echo healthy"]}`)
	if err := flywheel.UpsertPeriodic(context.Background(), db, flywheel.PeriodicSpec{
		Slug:         "health-check",
		Kind:         workers.ExecKind,
		Every:        time.Minute,
		ArgsTemplate: args,
		Active:       true,
	}); err != nil {
		logger.Error("schedule", "error", err)
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
	logger.Info("flywheel exec-cron running; a shell command runs every minute. Ctrl+C to stop")
	if err := node.Run(ctx); err != nil {
		logger.Error("node", "error", err)
		os.Exit(1)
	}
}
