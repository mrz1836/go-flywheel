// Command local-tasks shows flywheel running local developer tasks durably: a
// shell script, a Python script, and a magex/mage build target — each registered
// as a typed worker, enqueued once immediately, and (for the shell task) scheduled
// to repeat. Every run is retried on failure and recorded — captured stdout/stderr,
// exit code, and timing — in the job_runs audit trail. It is the "replace my shell
// cron jobs" example.
//
//	go run ./examples/local-tasks
//
// The example logs each run's outcome live. To inspect the full captured output and
// history, point the flywheel CLI at the same database with a one-line config:
//
//	echo 'db: {sqlite: local-tasks.db}' > flywheel.yaml
//	flywheel jobs ls                 # recent jobs and their state
//	flywheel jobs inspect <id> --json  # full stdout/stderr, exit code, run history
package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"syscall"
	"time"

	"github.com/glebarez/sqlite"
	flywheel "github.com/mrz1836/go-flywheel"
	"github.com/mrz1836/go-flywheel/workers"
	"gorm.io/gorm"
	gormlogger "gorm.io/gorm/logger"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	// WAL mode + a single writer connection is the robust local-SQLite setup. The
	// silent GORM logger keeps the demo output to flywheel's own structured logs.
	db, err := gorm.Open(
		sqlite.Open("file:local-tasks.db?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)&_txlock=immediate"),
		&gorm.Config{Logger: gormlogger.Default.LogMode(gormlogger.Silent)},
	)
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

	// Register the three local-execution workers. Each is a thin, typed wrapper over
	// the shared exec engine, so output capture, timeouts, retries, and failure
	// classification come for free — you only describe what to run.
	reg := flywheel.NewRegistry()
	flywheel.Register(reg, workers.ShellWorker{})
	flywheel.Register(reg, workers.PythonWorker{})
	flywheel.Register(reg, workers.MageWorker{})

	ctx := context.Background()
	client := flywheel.NewClient(db)
	dir := scriptsDir()

	// 1) A shell script. sh is universal, so this always runs. Enqueue one now for
	//    instant feedback, and schedule it every 30s as a durable cron replacement.
	shellHello := workers.ShellArgs{Script: filepath.Join(dir, "hello.sh"), Args: []string{"flywheel"}, TimeoutSeconds: 30}
	if _, err := flywheel.Insert(ctx, client, shellHello, flywheel.InsertOpts{}); err != nil {
		logger.Error("enqueue shell", "error", err)
		os.Exit(1)
	}
	if err := flywheel.UpsertPeriodic(ctx, db, flywheel.PeriodicSpec{
		Slug:         "hello-shell",
		Kind:         workers.ShellKind,
		Queue:        "periodic",
		Every:        30 * time.Second,
		ArgsTemplate: mustJSON(workers.ShellArgs{Script: filepath.Join(dir, "hello.sh"), Args: []string{"cron"}}),
		Active:       true,
	}); err != nil {
		logger.Error("schedule shell", "error", err)
		os.Exit(1)
	}

	// 2) A Python script. Enqueue only when an interpreter is present so the demo
	//    never shows a spurious failure. The worker resolves python3, then python.
	if hasTool("python3") || hasTool("python") {
		pyHello := workers.PythonArgs{Script: filepath.Join(dir, "hello.py"), Args: []string{"flywheel"}, TimeoutSeconds: 30}
		if _, err := flywheel.Insert(ctx, client, pyHello, flywheel.InsertOpts{}); err != nil {
			logger.Error("enqueue python", "error", err)
			os.Exit(1)
		}
	} else {
		logger.Warn("skipping python job: no python3/python on PATH")
	}

	// 3) A magex build target. magex ships 150+ built-in commands and needs no
	//    magefile; `magex help` is a safe, read-only target. Enqueue only when the
	//    binary is present.
	if hasTool("magex") {
		mageHelp := workers.MageArgs{Targets: []string{"help"}, TimeoutSeconds: 60}
		if _, err := flywheel.Insert(ctx, client, mageHelp, flywheel.InsertOpts{}); err != nil {
			logger.Error("enqueue mage", "error", err)
			os.Exit(1)
		}
	} else {
		logger.Warn("skipping mage job: magex not on PATH (install: go install github.com/mrz1836/mage-x/cmd/magex@latest)")
	}

	node, err := flywheel.NewNode(flywheel.NodeConfig{
		Runners: []flywheel.RunnerConfig{{
			DB: db, Driver: flywheel.NewSQLiteDriver(db), Registry: reg,
			Queues: []string{"default", "periodic"}, Concurrency: 1, ClaimAnyClass: true,
			Logger: logger, Observer: runLogObserver{log: logger},
		}},
		Scheduler: &flywheel.SchedulerConfig{DB: db, Client: client},
		Logger:    logger,
	})
	if err != nil {
		logger.Error("build node", "error", err)
		os.Exit(1)
	}

	runCtx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	logger.Info("flywheel local-tasks running: shell now + every 30s; python & mage once. Watch outcomes below; Ctrl+C to stop.")
	if err := node.Run(runCtx); err != nil {
		logger.Error("node", "error", err)
		os.Exit(1)
	}
}

// runLogObserver logs each attempt's outcome so the demo shows live progress. A
// real deployment would implement flywheel.Observer against its own metrics or
// tracing stack; the core imports none.
type runLogObserver struct{ log *slog.Logger }

func (runLogObserver) OnClaim(context.Context, flywheel.ClaimEvent) {}
func (o runLogObserver) OnStart(ctx context.Context, ev flywheel.JobEvent) {
	o.log.InfoContext(ctx, "job started", "kind", ev.Kind, "attempt", ev.Attempt)
}

func (o runLogObserver) OnFinish(ctx context.Context, ev flywheel.FinishEvent) {
	o.log.InfoContext(ctx, "job finished",
		"kind", ev.Kind, "outcome", ev.Outcome, "duration_ms", ev.Duration.Milliseconds())
}

func (o runLogObserver) OnRetry(ctx context.Context, ev flywheel.RetryEvent) {
	o.log.WarnContext(ctx, "job retry scheduled", "kind", ev.Kind, "next_attempt", ev.NextAttempt, "delay", ev.Delay.String())
}

// scriptsDir resolves the bundled scripts directory from this source file so the
// example runs from any working directory.
func scriptsDir() string {
	_, file, _, _ := runtime.Caller(0)
	return filepath.Join(filepath.Dir(file), "scripts")
}

// hasTool reports whether name is resolvable on PATH.
func hasTool(name string) bool {
	_, err := exec.LookPath(name)
	return err == nil
}

// mustJSON marshals v for an ArgsTemplate; the input is a known-good struct, so an
// error is a programming bug.
func mustJSON(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}
