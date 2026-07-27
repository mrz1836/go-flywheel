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
	// WorkloadMixedSpeed gives a minority of jobs a much longer work duration.
	//
	// It is the most informative of the five: a runner's poll waits for the whole
	// claimed batch before claiming again, so one slow member holds its entire
	// batch — and at Workers: 8 with 10% slow jobs, most batches contain one.
	WorkloadMixedSpeed Workload = "mixed-speed"
)

// Valid reports whether w is a recognized Workload.
func (w Workload) Valid() bool {
	switch w {
	case WorkloadEnqueueOnly, WorkloadDrainOnly, WorkloadSteady, WorkloadFanOut, WorkloadMixedSpeed:
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

// Defaults applied by validate to a zero-valued field.
const (
	defaultRunners        = 1
	defaultWorkers        = 1
	defaultSampleInterval = time.Second
	defaultTimeout        = 30 * time.Minute
	defaultQueue          = "default"
	defaultExecutorClass  = "loadtest"
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

// overheadConnections is what the run needs beyond the work pool: the admin pool
// that owns CREATE/DROP SCHEMA, the probe pool the sampler and the completion
// check share, and one spare.
const overheadConnections = 4

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
}

// connections reports the connection budget this configuration plans for: the
// work pool, sized to Runners × (Workers + 1) so every runner can hold one
// claim connection plus one per concurrent worker finalize, and the fixed
// overhead of the admin and probe pools.
func (c Config) connections() int {
	return c.Runners*(c.Workers+1) + overheadConnections
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
	if c.Queue == "" {
		c.Queue = defaultQueue
	}
	if c.ExecutorClass == "" {
		c.ExecutorClass = defaultExecutorClass
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
			"loadtest: Runners×(Workers+1)+%d = %d×%d+%d = %d exceeds the %d-connection budget "+
				"(postgres defaults to max_connections=100): %w",
			overheadConnections, c.Runners, c.Workers+1, overheadConnections, n, maxConnections, ErrTooManyConnections,
		)
	}
	return c, nil
}
