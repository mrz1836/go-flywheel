//go:build loadtest

package loadtest

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"math/rand/v2"
	"time"
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
)

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
}

// TotalJobs reports how many jobs the run will process, parents plus children.
// It is what the drain is measured against, so a fan-out run's denominator
// counts the children it will create rather than only the rows it inserted.
func (p mixPlan) TotalJobs() int {
	return p.Rows * (1 + p.Children)
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
		// One parent per (1 + children) jobs, so the run's total job count is
		// still Config.Jobs and a fan-out report is comparable with a drain one.
		p.Children = fanOutChildren
		p.Rows = max(1, cfg.Jobs/(1+fanOutChildren))
	case WorkloadMixedSpeed:
		base := cfg.WorkDuration
		if base <= 0 {
			base = mixedSpeedBaseWork
		}
		p.BaseWork = base
		p.SlowWork = slowFactor * base
		p.SlowShare = slowFraction
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
		}
	}
	return specs
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
	}
	return hex.EncodeToString(h.Sum(nil))
}
