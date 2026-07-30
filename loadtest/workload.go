//go:build loadtest

package loadtest

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"math/rand/v2"
	"time"

	flywheel "github.com/mrz1836/go-flywheel"
)

// Random streams. Each generated quantity draws from its own PCG stream, keyed
// by the run seed, so adding a new quantity later cannot shift the values of the
// existing ones — a single shared stream would make every published digest
// change the moment the generator grew a field.
const (
	streamWork     uint64 = 1
	streamPayload  uint64 = 2
	streamPriority uint64 = 3
	streamID       uint64 = 4
	// streamFail domain-separates the per-job fail decision. It is applied as a
	// hash of the job ordinal rather than a sequential draw, because a fail-fanout
	// cohort's children are decided by ordinal — the seed path and any runtime path
	// must agree on a given child without depending on draw order. Keeping it a
	// distinct stream means adding the fail dimension does not shift the values the
	// other quantities draw.
	streamFail uint64 = 5
)

// replayParentID is the synthetic parent the fail-fanout cohort's children are
// seeded under, so ReplayByParent has a lineage to scope to. No parent row is
// written for it: parent_job_id carries no foreign key, and the replay reasons
// about the children that reference it.
const replayParentID = "lt-replay-parent"

// Workload shape constants.
const (
	// fanOutChildren is how many children each parent enqueues in the fan-out
	// mix. With one parent per ten jobs, a run's total job count still equals
	// Config.Jobs, so the drain target means the same thing across mixes.
	fanOutChildren = 9
	// slowFraction is the share of jobs that are slow in the mixed-speed mix.
	//
	// Ten percent is chosen against the runner's batching, not arbitrarily: a
	// poll waits for its whole claimed batch before claiming again, so at
	// Workers: 8 the chance a batch contains at least one slow job is
	// 1 − 0.9⁸ ≈ 57%. Most batches therefore run at the slow job's speed, which
	// is the effect this mix exists to expose.
	slowFraction = 0.10
	// slowFactor multiplies the base work duration for a slow job.
	slowFactor = 20
	// mixedSpeedBaseWork is the base duration the mixed-speed mix uses when the
	// config sets none. A bimodal distribution needs a nonzero mode.
	mixedSpeedBaseWork = time.Millisecond
	// payloadBytes is the width of each job's filler args.
	//
	// A jobs row whose args are `{}` is not the row a production database
	// stores, and row width drives page count, which drives every storage number
	// this harness reports.
	payloadBytes = 192
	// priorityBands is how many distinct priorities the generator spreads jobs
	// across. The claim orders by priority, so a single-valued column would let
	// the index do less work than it does in production.
	priorityBands = 5
)

// jobSpec is one generated job, decided before anything concurrent starts.
//
// It carries no id. Ids are minted by whichever seed path runs, and the two
// paths mint them differently on purpose — see seed.go — so keeping them out of
// the spec is what lets the digest describe the workload rather than the
// insertion.
type jobSpec struct {
	// N is the job's ordinal in the workload.
	N int
	// WorkNanos is how long this job's worker sleeps.
	WorkNanos int64
	// Priority is the job's claim priority.
	Priority int
	// Children is how many follow-ups this job enqueues on success.
	Children int
	// Payload is deterministic filler that gives the row a realistic width.
	Payload string
	// Fail marks a job whose worker returns a transient error every attempt, so it
	// exhausts its budget to discarded. It is how a replay run manufactures the
	// failures it then recovers.
	Fail bool
}

// mixPlan is the shape decision for one run: how many rows to generate, and the
// parameters the generator varies them with. It is separated from generate so
// the arithmetic that decides "how many parents, how many children" is testable
// without producing a workload.
type mixPlan struct {
	// Rows is how many jobs the seeder inserts.
	Rows int
	// Children is how many follow-ups each seeded job enqueues on success.
	Children int
	// BaseWork and SlowWork are the two work durations the mix draws from;
	// SlowShare is the probability of drawing the slow one.
	BaseWork, SlowWork time.Duration
	SlowShare          float64
	// Bulk reports whether this mix seeds through the bulk path.
	Bulk bool
	// Barrier reports whether each parent declares a fan-in barrier over its
	// children, which the runtime completes with one continuation per parent.
	Barrier bool
	// ParentID, when non-empty, seeds the rows directly as leaf children of that
	// parent rather than fanning them out at runtime. It is how a replay cohort gets
	// a lineage to scope ReplayByParent to and a controllable per-child budget.
	ParentID string
}

// TotalJobs reports how many jobs the run will process, parents plus children,
// plus one barrier continuation per parent when the mix declares a barrier. It is
// what the drain fraction is measured against, so a barrier run's denominator
// counts the continuation the runtime enqueues rather than only the rows it seeded.
func (p mixPlan) TotalJobs() int {
	total := p.Rows * (1 + p.Children)
	if p.Barrier {
		total += p.Rows
	}
	return total
}

// childCount is how many children each parent fans out: the configured override
// when set, otherwise the fan-out default. It sizes both the fan-out and barrier
// mixes.
func childCount(cfg Config) int {
	if cfg.Children > 0 {
		return cfg.Children
	}
	return fanOutChildren
}

// runtimeFanOutCeiling mirrors the runtime's default FollowUpLimit /
// BarrierMaxChildren (defaultFollowUpLimit, defaultBarrierMaxChildren). It is
// duplicated here because those defaults are unexported; workDriverOpts only ever
// raises a bound above it, never lowers one, so a drift in the runtime's default
// cannot make the harness stricter than the runtime.
const runtimeFanOutCeiling = 10_000

// workDriverOpts sizes the runner driver's fan-out and barrier bounds to the run's
// child count, so a -children beyond the runtime's default ceiling does not fail
// every parent's finalize with ErrFollowUpLimit or ErrBarrierTooWide. A run at or
// below the ceiling gets the zero value, which is exactly the runtime default.
func workDriverOpts(cfg Config) flywheel.DriverOpts {
	var opts flywheel.DriverOpts
	c := childCount(cfg)
	if (cfg.Mix == WorkloadFanOut || cfg.Mix == WorkloadBarrier) && c > runtimeFanOutCeiling {
		opts.FollowUpLimit = c
	}
	if cfg.Mix == WorkloadBarrier && c > runtimeFanOutCeiling {
		opts.BarrierMaxChildren = c
	}
	return opts
}

// plan decides the run's shape from its mix.
//
// The choice of seed path is part of the shape, not an implementation detail:
// the enqueue and steady mixes exist to measure the producer API, so they must
// go through it one row at a time. The rest use the API only as setup, where
// 100k single-row inserts would be 30–100 seconds of untimed overhead per
// benchmark iteration.
func plan(cfg Config) mixPlan {
	p := mixPlan{Rows: cfg.Jobs, BaseWork: cfg.WorkDuration, Bulk: true}

	switch cfg.Mix {
	case WorkloadEnqueueOnly, WorkloadSteady:
		p.Bulk = false
	case WorkloadFanOut:
		if cfg.FailFraction > 0 {
			// Replay shape: seed the children directly under one parent, as leaves
			// (no runtime fan-out), so each carries a small MaxAttempts and a lineage
			// for ReplayByParent. -children is the cohort size here.
			p.Rows = childCount(cfg)
			p.ParentID = replayParentID
		} else {
			// One parent per (1 + children) jobs, so the run's total job count is
			// still Config.Jobs and a fan-out report is comparable with a drain one.
			p.Children = childCount(cfg)
			p.Rows = max(1, cfg.Jobs/(1+p.Children))
		}
	case WorkloadBarrier:
		// The same fan-out shape, plus a barrier: each parent declares a
		// continuation over its children. The continuation is one extra job per
		// parent, so the parent budget leaves room for it — one parent per
		// (2 + children) jobs — keeping the run's total near Config.Jobs.
		p.Children = childCount(cfg)
		p.Barrier = true
		p.Rows = max(1, cfg.Jobs/(2+p.Children))
	case WorkloadMixedSpeed:
		base := cfg.WorkDuration
		if base <= 0 {
			base = mixedSpeedBaseWork
		}
		p.BaseWork = base
		p.SlowWork = slowFactor * base
		p.SlowShare = slowFraction
	case WorkloadFairness:
		// Parents synthetic parents, each with Children ready leaf children, direct-
		// seeded and all available. The seed count is the product, not Config.Jobs —
		// the fairness mix's size is its own Parents×Children, so the drain target
		// counts exactly the children that compete for claims.
		p.Rows = cfg.Parents * childCount(cfg)
	case WorkloadDrainOnly:
	}
	return p
}

// generate produces the run's workload.
//
// It is single-threaded and completes before any runner starts, which is what
// makes "concurrency does not change the workload" a directly assertable
// property rather than a hope: generate at Runners: 1 and generate at
// Runners: 64 return identical slices, and a test asserts exactly that. A
// generator that drew from per-goroutine streams at execution time could not
// make that claim, because the goroutine that drew a value would depend on
// scheduling.
//
// Nothing here reads the wall clock. Every value comes from the run seed.
func generate(cfg Config) []jobSpec {
	p := plan(cfg)

	work := rand.New(rand.NewPCG(uint64(cfg.Seed), streamWork))         //nolint:gosec // reproducibility, not security
	payload := rand.New(rand.NewPCG(uint64(cfg.Seed), streamPayload))   //nolint:gosec // reproducibility, not security
	priority := rand.New(rand.NewPCG(uint64(cfg.Seed), streamPriority)) //nolint:gosec // reproducibility, not security

	specs := make([]jobSpec, p.Rows)
	for i := range specs {
		specs[i] = jobSpec{
			N:         i,
			WorkNanos: drawWork(work, p, cfg.WorkJitter),
			Priority:  100 + priority.IntN(priorityBands),
			Children:  p.Children,
			Payload:   drawPayload(payload),
			Fail:      shouldFail(cfg.Seed, i, cfg.FailFraction),
		}
	}
	return specs
}

// shouldFail decides whether job ordinal n fails, deterministically and
// independently of draw order — so the seed path and a runtime fan-out agree on a
// given child by ordinal alone. It mixes the run seed, the fail stream, and the
// ordinal into a uniform value in [0,1) with a splitmix64 finalizer and compares it
// to the fraction. A fraction of zero never fails; a fraction at or above one is
// caught by Config.validate.
func shouldFail(seed int64, n int, fraction float64) bool {
	if fraction <= 0 {
		return false
	}
	//nolint:gosec // reproducible hashing, not security; wraparound is intended
	x := uint64(seed)*0x9E3779B97F4A7C15 + uint64(n)*0xD6E8FEB86659FD93 + streamFail
	x ^= x >> 30
	x *= 0xBF58476D1CE4E5B9
	x ^= x >> 27
	x *= 0x94D049BB133111EB
	x ^= x >> 31
	return float64(x>>11)/float64(uint64(1)<<53) < fraction
}

// drawWork picks one job's simulated work duration: the slow mode with
// probability SlowShare, otherwise the base mode, spread by the configured
// jitter. A negative result is clamped to zero, so a jitter wider than the base
// duration yields a no-op job rather than a negative sleep.
func drawWork(rng *rand.Rand, p mixPlan, jitter time.Duration) int64 {
	d := p.BaseWork
	if p.SlowShare > 0 && rng.Float64() < p.SlowShare {
		d = p.SlowWork
	}
	if jitter > 0 {
		// Symmetric spread: [-jitter/2, +jitter/2).
		d += time.Duration(rng.Int64N(int64(jitter))) - jitter/2
	}
	if d < 0 {
		return 0
	}
	return d.Nanoseconds()
}

// drawPayload renders one job's filler args.
func drawPayload(rng *rand.Rand) string {
	buf := make([]byte, payloadBytes/2)
	for i := range buf {
		buf[i] = byte(rng.UintN(256))
	}
	return hex.EncodeToString(buf)
}

// workloadDigest hashes a generated workload.
//
// It covers exactly the fields the generator decides, and deliberately not the
// row ids: on the measured enqueue path the ids come from the runtime's own
// models.NewID, a random-bit UUIDv7 with no injection seam, so two runs with
// identical configs produce identical workloads and different ids. Hashing the
// ids would make the digest report "not reproducible" about the one thing that
// genuinely cannot be — and hide whether the part that can be, was.
//
// The encoding is fixed-width and length-prefixed so no two distinct workloads
// can hash the same by field-boundary ambiguity.
func workloadDigest(specs []jobSpec) string {
	h := sha256.New()
	var scratch [8]byte

	binary.BigEndian.PutUint64(scratch[:], uint64(len(specs)))
	_, _ = h.Write(scratch[:])

	for _, s := range specs {
		binary.BigEndian.PutUint64(scratch[:], uint64(s.N))
		_, _ = h.Write(scratch[:])
		binary.BigEndian.PutUint64(scratch[:], uint64(s.WorkNanos))
		_, _ = h.Write(scratch[:])
		binary.BigEndian.PutUint64(scratch[:], uint64(s.Priority))
		_, _ = h.Write(scratch[:])
		binary.BigEndian.PutUint64(scratch[:], uint64(s.Children))
		_, _ = h.Write(scratch[:])
		binary.BigEndian.PutUint64(scratch[:], uint64(len(s.Payload)))
		_, _ = h.Write(scratch[:])
		_, _ = h.Write([]byte(s.Payload))
		var fail byte
		if s.Fail {
			fail = 1
		}
		_, _ = h.Write([]byte{fail})
	}
	return hex.EncodeToString(h.Sum(nil))
}
