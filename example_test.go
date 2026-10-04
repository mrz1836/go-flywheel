package flywheel_test

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
	"gorm.io/gorm/logger"
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

// ExampleAlreadyEnqueuedError finds the job an insert collided with: a second
// insert under the same UniqueActiveKey collides, and the error names the job
// holding the key, so the caller can join that job instead of dropping the
// request. (The silent logger keeps GORM's duplicate-key log line out of the
// example's output.)
func ExampleAlreadyEnqueuedError() {
	db, _ := gorm.Open(sqlite.Open("file:example-already-enqueued?mode=memory&cache=shared"),
		&gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	_ = flywheel.Migrate(db)
	ctx, client := context.Background(), flywheel.NewClient(db)
	opts := flywheel.InsertOpts{UniqueActiveKey: "report:acct-42"}

	first, err := flywheel.Insert(ctx, client, EmailArgs{To: "a@example.com"}, opts)
	if err != nil {
		panic(err)
	}

	_, err = flywheel.Insert(ctx, client, EmailArgs{To: "a@example.com"}, opts)
	var dup *flywheel.AlreadyEnqueuedError
	if errors.As(err, &dup) {
		fmt.Println(dup.ExistingID == first, dup.Key, errors.Is(err, flywheel.ErrAlreadyEnqueued))
	}
	// Output: true report:acct-42 true
}

// ExampleWaitForJob is enqueue-or-join, then wait: start the report unless one
// for this account is already in flight, join whichever job is, and block until
// it finishes — at most two minutes — before reading what it produced. It needs a
// running Node to work the job, so it has no checked output.
func ExampleWaitForJob() {
	db, _ := gorm.Open(sqlite.Open("flywheel.db"), &gorm.Config{})
	ctx, client := context.Background(), flywheel.NewClient(db)

	id, err := flywheel.Insert(ctx, client, EmailArgs{To: "ops@example.com", Subject: "report"},
		flywheel.InsertOpts{UniqueActiveKey: "report:acct-42"})
	var dup *flywheel.AlreadyEnqueuedError
	switch {
	case errors.As(err, &dup) && dup.ExistingID != "":
		id = dup.ExistingID // a report is already in flight: join it
	case err != nil:
		panic(err)
	}

	waitCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	job, err := flywheel.WaitForJob(waitCtx, db, id)
	if err != nil {
		panic(err) // still running at the deadline, or the job is gone
	}
	if job.State != string(flywheel.StateSucceeded) {
		panic("report " + job.State)
	}
	run, _, _ := flywheel.LatestRun(ctx, db, id)
	fmt.Println(string(run.Output))
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
		// The scheduler needs its own Driver (the dialect + observability seam);
		// SchedulerConfig requires it explicitly, unlike the NewScheduler shorthand.
		Scheduler: &flywheel.SchedulerConfig{DB: db, Client: flywheel.NewClient(db), Driver: flywheel.NewSQLiteDriver(db)},
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
// to call on every boot. A host running a Scheduler will usually declare its
// schedules in SchedulerConfig.Periodics instead, which applies them the same way
// on start and re-creates any that go missing while it runs.
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

// ExampleSchedulerConfig_periodics declares a host's periodic jobs on its
// Scheduler. Run applies them on start — an unchanged schedule keeps its cadence
// across restarts — and re-creates any whose row goes missing while it runs, such
// as after a database restore, logging each re-creation once at warn. To retire
// one, remove it from Periodics first, then delete its row.
func ExampleSchedulerConfig_periodics() {
	db, _ := gorm.Open(sqlite.Open("flywheel.db"), &gorm.Config{})
	_ = flywheel.Migrate(db)

	reg := flywheel.NewRegistry()
	flywheel.Register(reg, EmailWorker{})
	driver := flywheel.NewSQLiteDriver(db)

	node, err := flywheel.NewNode(flywheel.NodeConfig{
		Runners: []flywheel.RunnerConfig{{
			DB: db, Driver: driver, Registry: reg,
			Queues: []string{"default", "periodic"}, Concurrency: 1, ClaimAnyClass: true,
		}},
		Scheduler: &flywheel.SchedulerConfig{
			DB: db, Client: flywheel.NewClient(db), Driver: driver,
			Periodics: []flywheel.PeriodicSpec{{
				Slug:         "nightly-report",
				Kind:         "send_email",
				Cron:         "0 2 * * *",
				ArgsTemplate: []byte(`{"to":"ops@example.com","subject":"nightly report"}`),
				Active:       true, // the zero value declares the schedule inactive
			}},
			// How often a missing schedule is re-created. Zero selects one minute;
			// a negative value turns it off, leaving only the apply on start.
			ReconcileInterval: 5 * time.Minute,
		},
	})
	if err != nil {
		panic(err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	_ = node.Run(ctx)
}
