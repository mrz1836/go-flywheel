package flywheel_test

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/glebarez/sqlite"
	flywheel "github.com/mrz1836/go-flywheel"
	"gorm.io/gorm"
)

// EmailArgs is a job's typed arguments. Its Kind method names the worker that
// handles it, and the struct is JSON-serialized as the job payload.
type EmailArgs struct {
	To      string `json:"to"`
	Subject string `json:"subject"`
}

// Kind names the worker for these args.
func (EmailArgs) Kind() string { return "send_email" }

// EmailWorker handles EmailArgs jobs.
type EmailWorker struct{}

// Kind is the stable worker name, matching EmailArgs.Kind.
func (EmailWorker) Kind() string { return "send_email" }

// Work runs one job. It receives the decoded args and a logger pre-tagged with
// the job id, kind, and run id.
func (EmailWorker) Work(ctx context.Context, job *flywheel.Job[EmailArgs]) (flywheel.Result, error) {
	job.Logger.InfoContext(ctx, "sending email", "to", job.Args.To, "subject", job.Args.Subject)
	return flywheel.Result{}, nil
}

// ExampleRegister registers a typed worker so the runtime can dispatch its kind.
func ExampleRegister() {
	reg := flywheel.NewRegistry()
	flywheel.Register(reg, EmailWorker{})
	fmt.Println("registered")
	// Output: registered
}

// ExampleInsert enqueues a typed job onto a SQLite-backed queue.
func ExampleInsert() {
	db, _ := gorm.Open(sqlite.Open("file:example-insert?mode=memory&cache=shared"), &gorm.Config{})
	_ = flywheel.Migrate(db)

	id, err := flywheel.Insert(context.Background(), flywheel.NewClient(db),
		EmailArgs{To: "a@example.com", Subject: "hi"}, flywheel.InsertOpts{})
	if err != nil {
		panic(err)
	}
	fmt.Println(id != "")
	// Output: true
}

// ExampleNewNode wires a complete job-runtime daemon — a runner, the periodic
// scheduler, and a health server — and runs it until SIGINT/SIGTERM. This is the
// whole daemon: define workers, register them, and let the Node own the
// runner/scheduler/health/drain lifecycle.
func ExampleNewNode() {
	db, _ := gorm.Open(sqlite.Open("flywheel.db"), &gorm.Config{})
	_ = flywheel.Migrate(db)

	reg := flywheel.NewRegistry()
	flywheel.Register(reg, EmailWorker{})

	node, err := flywheel.NewNode(flywheel.NodeConfig{
		Runners: []flywheel.RunnerConfig{{
			DB:       db,
			Driver:   flywheel.NewSQLiteDriver(db),
			Registry: reg,
			Queues:   []string{"default", "periodic"},
			// SQLite is single-writer: keep Concurrency at 1 and claim every class.
			Concurrency:   1,
			ClaimAnyClass: true,
		}},
		Scheduler: &flywheel.SchedulerConfig{DB: db, Client: flywheel.NewClient(db)},
		Health:    flywheel.HealthConfig{Addr: ":8080"},
		Logger:    slog.New(slog.NewJSONHandler(os.Stdout, nil)),
	})
	if err != nil {
		panic(err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	_ = node.Run(ctx)
}

// ExampleUpsertPeriodic declares a periodic job in code: run "send_email" every
// day at 02:00. Reconciling the same slug on startup is idempotent, so it is safe
// to call on every boot.
func ExampleUpsertPeriodic() {
	db, _ := gorm.Open(sqlite.Open("flywheel.db"), &gorm.Config{})
	_ = flywheel.Migrate(db)

	err := flywheel.UpsertPeriodic(context.Background(), db, flywheel.PeriodicSpec{
		Slug:   "nightly-report",
		Kind:   "send_email",
		Cron:   "0 2 * * *",
		Active: true,
	})
	if err != nil {
		panic(err)
	}
}
