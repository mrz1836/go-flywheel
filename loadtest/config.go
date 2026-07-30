//go:build loadtest

package loadtest

import (
	"fmt"
	"time"
)

// Workload is the shape of a run.
type Workload string

// Recognized Workload values.
const (
	// WorkloadEnqueueOnly seeds through the producer API and stops: the
	// measurement is insert throughput, so nothing may drain concurrently.
	WorkloadEnqueueOnly Workload = "enqueue"
	// WorkloadDrainOnly bulk-seeds first, then measures claim and finalize
	// against a full queue. It is the shape the published baseline uses.
	WorkloadDrainOnly Workload = "drain"
	// WorkloadSteady enqueues and drains concurrently, so the queue depth is the
	// difference between two rates rather than a starting condition.
	WorkloadSteady Workload = "steady"
	// WorkloadFanOut has each seeded job enqueue children on success: one parent,
	// many children.
	//
	// The name is deliberate. "One parent, many children" is fan-out; a fan-in
	// would be many jobs converging on one, which needs a join primitive the
	// runtime does not have. Naming this shape "fan-in" would put a promise in
	// every committed JSON report that the runtime cannot keep.
	WorkloadFanOut Workload = "fan-out"
	// WorkloadBarrier is fan-out plus a fan-in barrier: each seeded parent fans out
	// children and declares a continuation that the runtime enqueues once the whole
	// generation reaches a terminal state. It is what measures the barrier's
	// per-child completion count and the one-continuation-per-parent tail — the
	// join primitive the fan-out mix's name deliberately did not promise, now that
	// the runtime has it.
	WorkloadBarrier Workload = "barrier"
	// WorkloadMixedSpeed gives a minority of jobs a much longer work duration.
	//
	// It is the most informative of the five: a runner's poll waits for the whole
	// claimed batch before claiming again, so one slow member holds its entire
	// batch — and at Workers: 8 with 10% slow jobs, most batches contain one.
	WorkloadMixedSpeed Workload = "mixed-speed"
	// WorkloadFairness seeds Parents synthetic parents, each with Children ready leaf
	// children, all available at once — the shape that exposes head-of-line blocking.
	// Under strict FIFO the first parent's whole batch drains before the second gets a
	// claim; the Fairness strategy bands the children's priorities at enqueue so the
	// claim interleaves the parents instead. It is what measures A4 (window share) and
	// A5 (non-starvation).
	WorkloadFairness Workload = "fairness"
)

// Valid reports whether w is a recognized Workload.
func (w Workload) Valid() bool {
	switch w {
	case WorkloadEnqueueOnly, WorkloadDrainOnly, WorkloadSteady, WorkloadFanOut,
		WorkloadBarrier, WorkloadMixedSpeed, WorkloadFairness:
		return true
	default:
		return false
	}
}

// FairnessStrategy selects how the fairness mix assigns its children's priorities.
type FairnessStrategy string

// Recognized FairnessStrategy values.
const (
	// FairnessFIFO is the default: every child carries the same priority, so the
	// claim's (priority, scheduled_at) order is strict FIFO and the first parent's
	// whole batch drains before the second is seen. It is the baseline the banding
	// is measured against.
	FairnessFIFO FairnessStrategy = ""
	// FairnessRoundRobinParent bands the children's priorities at enqueue: a parent's
	// n-th child shares a priority band with every other parent's n-th child, so the
	// shipped claim interleaves the parents at O(batch) with no claim-path change.
	// This is the mechanism the README documents; the ranked-CTE alternative it
	// replaces was measured O(ready) and disqualifying (see BENCHMARKS).
	FairnessRoundRobinParent FairnessStrategy = "round-robin-parent"
)

// Valid reports whether s is a recognized FairnessStrategy.
func (s FairnessStrategy) Valid() bool {
	switch s {
	case FairnessFIFO, FairnessRoundRobinParent:
		return true
	default:
		return false
	}
}

// IndexCondition selects which indexes the target schema carries. The two
// conditions differ in exactly one variable — both install the same tables from
// the same models, and both apply the runtime's own IndexSet — so a delta
// between them is attributable to the index set and to nothing else.
type IndexCondition string

// Recognized IndexCondition values.
const (
	// IndexesFull is the production schema: every index the runtime installs.
	IndexesFull IndexCondition = "full"
	// IndexesCorrectness installs only the correctness-bearing indexes, which is
	// what quantifies the performance indexes. It is a measurement condition, not
	// a supported deployment.
	IndexesCorrectness IndexCondition = "correctness-only"
)

// Valid reports whether c is a recognized IndexCondition.
func (c IndexCondition) Valid() bool {
	switch c {
	case IndexesFull, IndexesCorrectness:
		return true
	default:
		return false
	}
}

// StorageCondition selects the storage parameters the target's jobs table
// carries. It mirrors IndexCondition: the two conditions differ in exactly one
// variable, so a delta between them is attributable to the storage parameters
// and to nothing else.
type StorageCondition string

// Recognized StorageCondition values.
const (
	// StorageDefault leaves PostgreSQL's defaults in place: fillfactor 100 and
	// the cluster-wide autovacuum thresholds. It is what a host gets today.
	StorageDefault StorageCondition = "default"
	// StorageTuned applies a lower fillfactor and a per-table autovacuum scale
	// factor to jobs.
	//
	// job_runs is deliberately untouched: it is append-only, so it has no update
	// churn for either setting to act on.
	StorageTuned StorageCondition = "tuned"
)

// Valid reports whether c is a recognized StorageCondition.
func (c StorageCondition) Valid() bool {
	switch c {
	case StorageDefault, StorageTuned:
		return true
	default:
		return false
	}
}

// LimiterKind selects the pre-claim admission gate a run wraps its runners in.
type LimiterKind string

// Recognized LimiterKind values.
const (
	// LimiterNone is the default: the runners claim ungated.
	LimiterNone LimiterKind = "none"
	// LimiterTokenBucket wraps each runner in an in-process TokenBucket. Correct
	// for one process; the harness runs one, so it is the right choice here.
	LimiterTokenBucket LimiterKind = "token-bucket"
	// LimiterDB wraps each runner in a shared DBLimiter over the target schema.
	LimiterDB LimiterKind = "db"
)

// Valid reports whether k is a recognized LimiterKind.
func (k LimiterKind) Valid() bool {
	switch k {
	case LimiterNone, LimiterTokenBucket, LimiterDB:
		return true
	default:
		return false
	}
}

// gated reports whether the run installs an admission limiter.
func (k LimiterKind) gated() bool {
	return k == LimiterTokenBucket || k == LimiterDB
}

// limiterConnections is the extra connection budget the limiter needs: a shared
// DBLimiter opens its own pool, one connection per runner's serial Acquire, so it
// never queues behind the work pool. The in-process gate needs none.
func (c Config) limiterConnections() int {
	if c.Limiter == LimiterDB {
		return c.Runners
	}
	return 0
}

// Defaults applied by validate to a zero-valued field.
const (
	defaultRunners        = 1
	defaultWorkers        = 1
	defaultSampleInterval = time.Second
	defaultTimeout        = 30 * time.Minute
	defaultQueue          = "default"
	defaultExecutorClass  = "loadtest"
	// defaultTerminalSeedAge backdates the seeded terminal jobs far enough that
	// any plausible retention window is older than none of them.
	defaultTerminalSeedAge = 90 * 24 * time.Hour
	// defaultReplayMaxAttempts is the small seeded budget a replay run applies when
	// MaxAttempts is left zero, so a fail-child exhausts to discarded in three
	// attempts rather than the runtime default of twenty-five.
	defaultReplayMaxAttempts = 3
	// defaultReplayBudget is how many attempts a replay restores when ReplayBudget
	// is left zero.
	defaultReplayBudget = 3
	// defaultFairnessParents and defaultFairnessChildren size the fairness mix when
	// the flags are left zero: two parents is the smallest set that can starve, and
	// a deep child batch is what makes the head-of-line blocking visible.
	defaultFairnessParents  = 2
	defaultFairnessChildren = 10_000
)

// maxConnections is the connection budget one run may plan for.
//
// postgres:17-alpine — the image the integration workflow boots and the version
// the local target runs — defaults to max_connections = 100, of which three are
// reserved for superuser connections. Overshooting does not degrade: the pool
// blocks, the run stalls, and the latency histograms record the stall as if it
// were the runtime's. Rejecting the configuration up front, with the arithmetic
// in the message, is the difference between a bad config and a bad number.
const maxConnections = 90

// schedulerConnections is the scheduler pool's size: one connection per
// maintenance activity that can be in flight at once — the periodic tick and
// the lease sweep — so neither queues behind the other.
const schedulerConnections = 2

// overheadConnections is what the run needs beyond the work pool: the admin pool
// that owns CREATE/DROP SCHEMA (2), the probe pool the sampler and the
// completion check share (1), the scheduler's own pool (schedulerConnections),
// and one spare.
const overheadConnections = 3 + schedulerConnections + 1

// Config declares one run. The zero value is not runnable: DSN and Jobs are
// required. Every other field has a default, applied by validate.
type Config struct {
	// DSN is the PostgreSQL target. Required. It is never written to a report:
	// see redactDSN.
	DSN string
	// Jobs is the number of jobs seeded before the run. Required, positive.
	Jobs int
	// Seed makes a run reproducible: every generated value derives from it. Two
	// runs with equal Config produce byte-identical workloads. Zero is a valid
	// seed, not "unset" — there is no wall-clock fallback anywhere in the harness.
	Seed int64
	// Runners is the number of independent Runner loops; Workers is the
	// per-runner concurrency. Total in-flight capacity is Runners × Workers, and
	// the work pool is sized so every one of them can hold a connection at once.
	Runners int
	Workers int
	// Mix selects the workload shape.
	Mix Workload
	// Children overrides how many children each parent fans out in the fan-out and
	// barrier mixes. Zero selects the mix's own default (fanOutChildren). It is the
	// knob that turns a 100k-child parent — the rollup's subject at 1M — into a
	// scenario, and that sizes a barrier-bearing generation.
	Children int
	// Indexes selects the schema condition.
	Indexes IndexCondition
	// WorkDuration is the simulated per-job work time; WorkJitter spreads it. A
	// zero WorkDuration makes the worker a no-op, which isolates the database
	// path — the only configuration in which the reported throughput is the
	// runtime's own ceiling rather than the workload's.
	WorkDuration time.Duration
	WorkJitter   time.Duration
	// Lease overrides the runners' LeaseDuration. Zero derives it from the work
	// duration (see leaseFor), which is the right default for a run measuring
	// throughput: no ordinary job is ever reclaimed mid-flight.
	//
	// Setting it is how a scenario makes the lease *shorter* than the work, which
	// is the only way to exercise renewal — a job that never outlives its lease
	// never asks the heartbeat for anything.
	Lease time.Duration
	// Heartbeat is the runners' lease-renewal interval, passed through verbatim:
	// zero derives it from the lease, negative disables renewal. Disabling it is
	// what makes a with/without comparison of the heartbeat's write cost a
	// same-binary A/B rather than a comparison across two builds.
	Heartbeat time.Duration
	// SampleInterval is the cadence of the storage and OS sampler.
	SampleInterval time.Duration
	// Timeout bounds the whole run. A drain that cannot finish — a fault that
	// stalls it, a queue that outruns its runners — must end as a reported
	// timeout rather than a hung process.
	Timeout time.Duration
	// Queue and ExecutorClass are the routing labels the run uses. They default
	// to a dedicated class so a harness runner never claims a job some other
	// process left in the target database.
	Queue         string
	ExecutorClass string
	// Faults, when non-nil, injects a failure on the schedule the Fault declares.
	Faults Fault

	// Duration bounds a steady run in time rather than by exhausting a
	// population. It applies only to the steady mix; zero preserves today's
	// behavior byte for byte.
	//
	// A duration-bounded steady run is a *closed loop*: one job is enqueued for
	// every job that retires, so the number of jobs in a non-terminal state stays
	// at Jobs for the whole run.
	//
	// That bounds the *working set*, not the table. A retired job stays in jobs
	// as a terminal row, so total row count still climbs at the drain rate unless
	// retention is removing them. A storage measurement that wants growth to mean
	// churn rather than accumulation therefore needs both halves: this flag to
	// hold the working set steady, and RetentionMaxAge set short enough to keep
	// the terminal tail bounded. With only this flag, table growth is the sum of
	// both effects and neither is separable from the other.
	//
	// The consequence to state wherever the numbers are published:
	// EnqueueThroughput equals DrainThroughput under a closed loop *by
	// construction*, and neither is a ceiling. Compared against an open-loop
	// drain baseline it reads as a regression, and it is not one.
	Duration time.Duration

	// RetentionMaxAge enables the scheduler's retention sweep for the run.
	// RetentionInterval is its cadence and RetentionBatchSize its per-transaction
	// bound; both default when zero, and retention stays off unless
	// RetentionMaxAge is set.
	RetentionMaxAge    time.Duration
	RetentionInterval  time.Duration
	RetentionBatchSize int

	// SweepBatchSize bounds the lease sweep's per-transaction batch. Zero selects
	// the runtime's own default, which is itself a bound.
	SweepBatchSize int

	// Limiter selects the pre-claim admission gate the runners consult before
	// claiming: LimiterNone (the default), LimiterTokenBucket (in-process), or
	// LimiterDB (shared across processes through the target). Rate, Burst, and
	// MaxConcurrent parameterize it, per second.
	//
	// It is the mechanism plan-08's A1 measures against WorkerSnooze: a gated
	// runner never claims work it cannot run, so job_runs grows at the completion
	// rate rather than the snooze rate.
	Limiter       LimiterKind
	Rate          int
	Burst         int
	MaxConcurrent int

	// WorkerSnooze, when positive, is the claim-then-snooze baseline the gate is
	// measured against: the worker holds completions to this many per second with
	// its own in-process bucket, returning Result{Snooze} when over budget — which
	// spends a full claim cycle and a job_runs row per deferral. It is mutually
	// exclusive with Limiter: the two are the alternatives being compared.
	WorkerSnooze int

	// Storage selects the storage-parameter condition applied to jobs.
	Storage StorageCondition

	// TerminalSeed writes this many already-finalized jobs, each with a finished
	// run row, backdated by TerminalSeedAge before the run starts.
	//
	// It exists because retention has nothing to delete otherwise. The bulk seed
	// path writes no finalized_at and no job_runs at all, so a retention pass
	// against a freshly seeded run removes zero rows however long it runs — which
	// would look like a working retention sweep with an empty backlog rather than
	// a measurement of nothing.
	TerminalSeed    int
	TerminalSeedAge time.Duration

	// FailFraction is the share of the fan-out cohort's children that fail
	// transiently, so a replay has failures to recover. It applies to the fan-out
	// mix and must be in [0,1); zero preserves the all-succeed workload. A run with
	// a positive fraction seeds its children directly under one parent with
	// MaxAttempts each, rather than fanning out at runtime — the runtime gives a
	// follow-up no way to carry a small budget, and a small budget is what lets a
	// fail-child exhaust to discarded in a few attempts.
	FailFraction float64
	// MaxAttempts overrides the seeded jobs' retry budget. Zero selects the
	// runtime's own default (25); a replay run defaults it small so the fail cohort
	// exhausts quickly.
	MaxAttempts int

	// MaxRetryBackoff caps the runners' exponential retry delay, threaded to
	// RunnerConfig.MaxRetryBackoff verbatim. Zero leaves the runtime's own default
	// (one minute) in place. It is the knob the downstream-outage measurement
	// varies: a longer cap spreads the same attempt budget across a longer outage.
	MaxRetryBackoff time.Duration

	// Parents is how many synthetic parents the fairness mix seeds, each with
	// Children ready leaf children. It applies only to the fairness mix and must be
	// at least two — one parent cannot starve itself.
	Parents int
	// Fairness selects the fairness mix's priority-assignment strategy: FairnessFIFO
	// (the strict-FIFO baseline) or FairnessRoundRobinParent (priority banding). It
	// is an enqueue-time property, not a runner setting — the claim path is
	// untouched — so a run's claim query is byte-identical whichever is chosen.
	Fairness FairnessStrategy

	// Replay runs a replay phase after the initial drain: the discarded children are
	// replayed under their parent with a restored budget, then the run awaits a
	// second drain and asserts the cohort re-converges — every job terminal, no
	// succeeded child re-run, and no job run more times than its restored budget
	// allows. It requires the fan-out mix and a positive FailFraction.
	Replay bool
	// ReplayStagger spreads the replayed cohort's scheduled_at across this window,
	// instead of making every replayed job immediately claimable. Zero replays them
	// all at once.
	ReplayStagger time.Duration
	// ReplayBudget is the number of attempts the replay restores (the ResetAttempts
	// Budget). Zero selects a small default. It is the ceiling the re-convergence
	// check holds the replayed cohort to: no job runs more than its restored budget.
	ReplayBudget int
}

// connections reports the connection budget this configuration plans for: the
// work pool, sized to Runners × (Workers + 1) so every runner can hold one
// claim connection plus one per concurrent worker finalize, the fixed overhead
// of the admin and probe pools, and a shared limiter's own pool.
func (c Config) connections() int {
	return c.Runners*(c.Workers+1) + overheadConnections + c.limiterConnections()
}

// validate returns a normalized copy of c with defaults applied, or an error
// naming the field at fault.
//
// It returns a copy rather than mutating in place so a caller's Config is never
// silently rewritten — the benchmarks derive one config from another and mutate
// a single field, and that idiom is only safe if validation has no side effects
// on the source.
//
//nolint:gocognit,gocyclo // a flat sequence of independent field checks and defaults
func (c Config) validate() (Config, error) {
	if c.DSN == "" {
		return Config{}, fmt.Errorf("loadtest: %w", ErrNoDSN)
	}
	if c.Jobs <= 0 {
		return Config{}, fmt.Errorf("loadtest: Jobs must be positive, got %d: %w", c.Jobs, ErrInvalidConfig)
	}
	if c.Runners < 0 || c.Workers < 0 {
		return Config{}, fmt.Errorf(
			"loadtest: Runners and Workers must not be negative, got %d and %d: %w",
			c.Runners, c.Workers, ErrInvalidConfig,
		)
	}
	if c.Children < 0 {
		return Config{}, fmt.Errorf("loadtest: Children must not be negative, got %d: %w", c.Children, ErrInvalidConfig)
	}
	if c.Runners == 0 {
		c.Runners = defaultRunners
	}
	if c.Workers == 0 {
		c.Workers = defaultWorkers
	}
	if c.Mix == "" {
		c.Mix = WorkloadDrainOnly
	}
	if !c.Mix.Valid() {
		return Config{}, fmt.Errorf("loadtest: unknown mix %q: %w", c.Mix, ErrInvalidConfig)
	}
	if c.Indexes == "" {
		c.Indexes = IndexesFull
	}
	if !c.Indexes.Valid() {
		return Config{}, fmt.Errorf("loadtest: unknown index condition %q: %w", c.Indexes, ErrInvalidConfig)
	}
	if c.WorkDuration < 0 || c.WorkJitter < 0 {
		return Config{}, fmt.Errorf(
			"loadtest: WorkDuration and WorkJitter must not be negative, got %s and %s: %w",
			c.WorkDuration, c.WorkJitter, ErrInvalidConfig,
		)
	}
	if c.Lease < 0 {
		return Config{}, fmt.Errorf(
			"loadtest: Lease must not be negative, got %s: %w", c.Lease, ErrInvalidConfig,
		)
	}
	if c.SampleInterval <= 0 {
		c.SampleInterval = defaultSampleInterval
	}
	if c.Timeout <= 0 {
		c.Timeout = defaultTimeout
	}
	if c.Storage == "" {
		c.Storage = StorageDefault
	}
	if !c.Storage.Valid() {
		return Config{}, fmt.Errorf(
			"loadtest: unknown storage condition %q: %w", c.Storage, ErrInvalidConfig,
		)
	}
	if c.Duration < 0 || c.RetentionMaxAge < 0 || c.RetentionInterval < 0 || c.TerminalSeedAge < 0 {
		return Config{}, fmt.Errorf(
			"loadtest: Duration, RetentionMaxAge, RetentionInterval and TerminalSeedAge "+
				"must not be negative: %w", ErrInvalidConfig,
		)
	}
	if c.TerminalSeed < 0 || c.RetentionBatchSize < 0 || c.SweepBatchSize < 0 {
		return Config{}, fmt.Errorf(
			"loadtest: TerminalSeed, RetentionBatchSize and SweepBatchSize must not be negative: %w",
			ErrInvalidConfig,
		)
	}
	if c.Duration > 0 && c.Mix != WorkloadSteady {
		return Config{}, fmt.Errorf(
			"loadtest: Duration applies only to the steady mix, got %q: a fixed population drains "+
				"when it is empty, not when a clock says so: %w",
			c.Mix, ErrInvalidConfig,
		)
	}
	// A duration at or past the timeout is the trap this rule exists to close.
	// Timeout defaults to 30 minutes and Run wraps the whole run in it, so a
	// -duration longer than an unset -timeout dies partway with a non-zero exit
	// and a truncated artifact — after having already spent the time.
	if c.Duration > 0 && c.Duration >= c.Timeout {
		return Config{}, fmt.Errorf(
			"loadtest: Duration %s must be less than Timeout %s: the run is wrapped in Timeout, so a "+
				"Duration at or past it is killed before it can finish (raise -timeout above -duration, "+
				"leaving room for seeding and teardown): %w",
			c.Duration, c.Timeout, ErrInvalidConfig,
		)
	}
	if c.RetentionMaxAge > 0 && c.TerminalSeed == 0 && c.Mix != WorkloadSteady {
		return Config{}, fmt.Errorf(
			"loadtest: RetentionMaxAge is set but nothing will be finalized old enough to prune: "+
				"seed a terminal backlog with TerminalSeed, or use the steady mix: %w",
			ErrInvalidConfig,
		)
	}
	if c.TerminalSeed > 0 && c.TerminalSeedAge == 0 {
		c.TerminalSeedAge = defaultTerminalSeedAge
	}
	if c.FailFraction < 0 || c.FailFraction >= 1 {
		return Config{}, fmt.Errorf(
			"loadtest: FailFraction %.3f is not in [0,1): 1 would fail every child and leave nothing "+
				"to succeed: %w", c.FailFraction, ErrInvalidConfig,
		)
	}
	if c.MaxAttempts < 0 || c.ReplayBudget < 0 || c.ReplayStagger < 0 {
		return Config{}, fmt.Errorf(
			"loadtest: MaxAttempts, ReplayBudget and ReplayStagger must not be negative: %w", ErrInvalidConfig,
		)
	}
	if c.MaxRetryBackoff < 0 {
		return Config{}, fmt.Errorf(
			"loadtest: MaxRetryBackoff must not be negative, got %s: %w", c.MaxRetryBackoff, ErrInvalidConfig,
		)
	}
	if !c.Fairness.Valid() {
		return Config{}, fmt.Errorf("loadtest: unknown fairness strategy %q: %w", c.Fairness, ErrInvalidConfig)
	}
	if c.Parents < 0 {
		return Config{}, fmt.Errorf("loadtest: Parents must not be negative, got %d: %w", c.Parents, ErrInvalidConfig)
	}
	if c.Mix == WorkloadFairness {
		if c.Parents == 0 {
			c.Parents = defaultFairnessParents
		}
		if c.Parents < 2 {
			return Config{}, fmt.Errorf(
				"loadtest: the fairness mix needs Parents >= 2, got %d: one parent cannot starve itself: %w",
				c.Parents, ErrInvalidConfig,
			)
		}
		if c.Children == 0 {
			c.Children = defaultFairnessChildren
		}
	} else if c.Fairness != FairnessFIFO {
		return Config{}, fmt.Errorf(
			"loadtest: -fairness applies only to the fairness mix, got %q: %w", c.Mix, ErrInvalidConfig,
		)
	}
	if c.Replay {
		// A replay recovers failed children under a parent, so it needs both a
		// lineage to scope to (the fan-out mix) and failures to recover (a positive
		// fail fraction). Without either there is nothing to replay.
		if c.Mix != WorkloadFanOut {
			return Config{}, fmt.Errorf(
				"loadtest: Replay requires the fan-out mix (it replays a parent's children), got %q: %w",
				c.Mix, ErrInvalidConfig,
			)
		}
		if c.FailFraction <= 0 {
			return Config{}, fmt.Errorf(
				"loadtest: Replay needs failures to recover: set a positive FailFraction: %w", ErrInvalidConfig,
			)
		}
		if c.MaxAttempts == 0 {
			c.MaxAttempts = defaultReplayMaxAttempts
		}
		if c.ReplayBudget == 0 {
			c.ReplayBudget = defaultReplayBudget
		}
	}
	if c.Queue == "" {
		c.Queue = defaultQueue
	}
	if c.ExecutorClass == "" {
		c.ExecutorClass = defaultExecutorClass
	}

	if c.Limiter == "" {
		c.Limiter = LimiterNone
	}
	if !c.Limiter.Valid() {
		return Config{}, fmt.Errorf("loadtest: unknown limiter %q: %w", c.Limiter, ErrInvalidConfig)
	}
	if c.Rate < 0 || c.Burst < 0 || c.MaxConcurrent < 0 || c.WorkerSnooze < 0 {
		return Config{}, fmt.Errorf(
			"loadtest: Rate, Burst, MaxConcurrent and WorkerSnooze must not be negative: %w", ErrInvalidConfig,
		)
	}
	if c.Limiter.gated() && c.WorkerSnooze > 0 {
		return Config{}, fmt.Errorf(
			"loadtest: -limiter and -worker-snooze are the two alternatives being compared, not a "+
				"combination: set one or the other: %w", ErrInvalidConfig,
		)
	}
	if c.Limiter.gated() && c.Rate <= 0 && c.MaxConcurrent <= 0 {
		return Config{}, fmt.Errorf(
			"loadtest: -limiter %q admits everything with neither -rate nor -max-concurrent set: %w",
			c.Limiter, ErrInvalidConfig,
		)
	}

	if c.Faults != nil {
		if c.Faults.At() <= 0 || c.Faults.At() >= 1 {
			return Config{}, fmt.Errorf(
				"loadtest: fault fraction %.3f is not in (0,1): a fault at 0 fires before the run starts "+
					"and one at 1 fires after it ends: %w",
				c.Faults.At(), ErrInvalidConfig,
			)
		}
		// The fault validates against the normalized config, so it sees the
		// defaulted Runners count rather than the caller's zero.
		if err := c.Faults.Validate(c); err != nil {
			return Config{}, err
		}
	}

	if n := c.connections(); n > maxConnections {
		return Config{}, fmt.Errorf(
			"loadtest: Runners×(Workers+1)+overhead+limiter = %d×%d+%d+%d = %d exceeds the %d-connection "+
				"budget (postgres defaults to max_connections=100): %w",
			c.Runners, c.Workers+1, overheadConnections, c.limiterConnections(), n, maxConnections, ErrTooManyConnections,
		)
	}
	return c, nil
}
