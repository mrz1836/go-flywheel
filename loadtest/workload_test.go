//go:build loadtest

package loadtest

import (
	"reflect"
	"testing"
	"time"
)

// baseWorkloadConfig is a fully specified config for the generator tests. Every
// case below derives from it and mutates one field, so a difference in output is
// attributable to that field.
func baseWorkloadConfig() Config {
	return Config{
		DSN: testDSN, Jobs: 500, Seed: 42,
		Runners: 1, Workers: 1,
		Mix: WorkloadDrainOnly, Indexes: IndexesFull,
		WorkDuration: 2 * time.Millisecond, WorkJitter: time.Millisecond,
	}
}

// TestGenerateIsDeterministic is the requirement, stated directly: two runs with
// equal Config produce byte-identical workloads.
func TestGenerateIsDeterministic(t *testing.T) {
	t.Parallel()

	cfg := baseWorkloadConfig()
	first, second := generate(cfg), generate(cfg)

	if !reflect.DeepEqual(first, second) {
		t.Fatal("generate is not deterministic for a fixed Config")
	}
	if workloadDigest(first) != workloadDigest(second) {
		t.Fatal("workloadDigest disagrees with the workload it hashes")
	}
}

// TestConcurrencyDoesNotChangeTheWorkload is the property the design exists to
// make assertable. Generation is single-threaded and completes before any runner
// starts, so the runner and worker counts cannot reach it.
//
// A generator that drew from per-goroutine streams at execution time would fail
// here, because which goroutine drew which value would depend on scheduling —
// and then no two runs at different concurrencies would be comparable.
func TestConcurrencyDoesNotChangeTheWorkload(t *testing.T) {
	t.Parallel()

	one := baseWorkloadConfig()
	one.Runners, one.Workers = 1, 1

	many := baseWorkloadConfig()
	many.Runners, many.Workers = 8, 8

	if !reflect.DeepEqual(generate(one), generate(many)) {
		t.Fatal("the workload changed with the concurrency: the two are not comparable")
	}
	if workloadDigest(generate(one)) != workloadDigest(generate(many)) {
		t.Fatal("the digest changed with the concurrency")
	}
}

// TestSeedChangesTheWorkload is the negative half. Without it, a generator that
// ignored the seed entirely would pass every determinism assertion above.
func TestSeedChangesTheWorkload(t *testing.T) {
	t.Parallel()

	a := baseWorkloadConfig()
	b := baseWorkloadConfig()
	b.Seed = a.Seed + 1

	if workloadDigest(generate(a)) == workloadDigest(generate(b)) {
		t.Fatal("two different seeds produced the same workload: the seed is not being used")
	}
}

// TestDigestDistinguishesWorkloads proves the hash is sensitive to each field it
// covers. A digest that collided across genuinely different workloads would make
// reproducibility unfalsifiable, which is worse than not claiming it.
func TestDigestDistinguishesWorkloads(t *testing.T) {
	t.Parallel()

	base := []jobSpec{{N: 1, WorkNanos: 10, Priority: 100, Children: 0, Payload: "ab"}}
	digest := workloadDigest(base)

	mutations := map[string][]jobSpec{
		"n":         {{N: 2, WorkNanos: 10, Priority: 100, Payload: "ab"}},
		"work":      {{N: 1, WorkNanos: 11, Priority: 100, Payload: "ab"}},
		"priority":  {{N: 1, WorkNanos: 10, Priority: 101, Payload: "ab"}},
		"children":  {{N: 1, WorkNanos: 10, Priority: 100, Children: 1, Payload: "ab"}},
		"payload":   {{N: 1, WorkNanos: 10, Priority: 100, Payload: "ac"}},
		"row count": {{N: 1, WorkNanos: 10, Priority: 100, Payload: "ab"}, {N: 2}},
	}
	for name, mutated := range mutations {
		if workloadDigest(mutated) == digest {
			t.Errorf("the digest ignores %s", name)
		}
	}

	// Field boundaries must not be ambiguous: two workloads whose payloads
	// concatenate the same way have to hash differently.
	ab := []jobSpec{{Payload: "a"}, {Payload: "b"}}
	a := []jobSpec{{Payload: "ab"}}
	if workloadDigest(ab) == workloadDigest(a) {
		t.Error("the digest is ambiguous across field boundaries")
	}
}

// TestPlanShapesEachMix pins the arithmetic behind each shape, so a later change
// to one mix cannot silently change what another one measures.
func TestPlanShapesEachMix(t *testing.T) {
	t.Parallel()

	cfg := baseWorkloadConfig()
	cfg.Jobs = 1000

	t.Run("drain bulk-seeds every row", func(t *testing.T) {
		t.Parallel()
		cfg := cfg
		cfg.Mix = WorkloadDrainOnly
		p := plan(cfg)
		if !p.Bulk || p.Rows != 1000 || p.TotalJobs() != 1000 || p.Children != 0 {
			t.Fatalf("drain plan = %+v", p)
		}
	})

	t.Run("enqueue goes through the producer API", func(t *testing.T) {
		t.Parallel()
		cfg := cfg
		cfg.Mix = WorkloadEnqueueOnly
		if p := plan(cfg); p.Bulk {
			t.Fatal("the enqueue mix measures the producer API, so it must not bulk-insert")
		}
	})

	t.Run("steady goes through the producer API", func(t *testing.T) {
		t.Parallel()
		cfg := cfg
		cfg.Mix = WorkloadSteady
		if p := plan(cfg); p.Bulk {
			t.Fatal("the steady mix measures enqueue against drain, so it must not bulk-insert")
		}
	})

	t.Run("fan-out totals the same job count", func(t *testing.T) {
		t.Parallel()
		cfg := cfg
		cfg.Mix = WorkloadFanOut
		p := plan(cfg)
		if p.Children != fanOutChildren {
			t.Errorf("Children = %d, want %d", p.Children, fanOutChildren)
		}
		if p.Rows != 100 {
			t.Errorf("Rows = %d, want 100 parents for 1000 jobs", p.Rows)
		}
		if p.TotalJobs() != 1000 {
			t.Errorf("TotalJobs = %d, want 1000 — a fan-out report must be comparable with a drain one",
				p.TotalJobs())
		}
	})

	t.Run("mixed-speed is bimodal", func(t *testing.T) {
		t.Parallel()
		cfg := cfg
		cfg.Mix = WorkloadMixedSpeed
		p := plan(cfg)
		if p.SlowShare != slowFraction {
			t.Errorf("SlowShare = %v, want %v", p.SlowShare, slowFraction)
		}
		if p.SlowWork != slowFactor*p.BaseWork {
			t.Errorf("SlowWork = %v, want %d× BaseWork (%v)", p.SlowWork, slowFactor, p.BaseWork)
		}
	})

	t.Run("barrier is fan-out plus one continuation per parent", func(t *testing.T) {
		t.Parallel()
		cfg := cfg
		cfg.Mix = WorkloadBarrier
		p := plan(cfg)
		if !p.Barrier {
			t.Error("the barrier mix must declare a barrier")
		}
		if p.Children != fanOutChildren {
			t.Errorf("Children = %d, want %d", p.Children, fanOutChildren)
		}
		// One parent per (2 + children) jobs, and TotalJobs counts the continuation.
		if p.Rows != 90 {
			t.Errorf("Rows = %d, want 90 parents for 1000 jobs at 2+9 each", p.Rows)
		}
		if p.TotalJobs() != p.Rows*(2+fanOutChildren) {
			t.Errorf("TotalJobs = %d, want the continuation counted per parent", p.TotalJobs())
		}
	})

	t.Run("children overrides the fan-out width", func(t *testing.T) {
		t.Parallel()
		for _, mix := range []Workload{WorkloadFanOut, WorkloadBarrier} {
			cfg := cfg
			cfg.Mix = mix
			cfg.Children = 100
			if p := plan(cfg); p.Children != 100 {
				t.Errorf("%s: Children = %d, want the -children override of 100", mix, p.Children)
			}
		}
	})
}

// TestWorkDriverOptsRaisesBoundsForWideFanOut proves the harness lifts the
// runtime's follow-up and barrier ceilings when -children exceeds them, so a
// wide-generation run does not fail every parent's finalize, and leaves the zero
// value (the runtime default) for a run within them.
func TestWorkDriverOptsRaisesBoundsForWideFanOut(t *testing.T) {
	t.Parallel()

	within := workDriverOpts(Config{Mix: WorkloadBarrier, Children: 1000})
	if within.FollowUpLimit != 0 || within.BarrierMaxChildren != 0 {
		t.Errorf("within the ceiling the opts must be the runtime default, got %+v", within)
	}

	wide := workDriverOpts(Config{Mix: WorkloadBarrier, Children: 100_000})
	if wide.FollowUpLimit != 100_000 || wide.BarrierMaxChildren != 100_000 {
		t.Errorf("a wide barrier must raise both bounds, got %+v", wide)
	}

	fanOut := workDriverOpts(Config{Mix: WorkloadFanOut, Children: 100_000})
	if fanOut.FollowUpLimit != 100_000 || fanOut.BarrierMaxChildren != 0 {
		t.Errorf("a wide fan-out raises only the follow-up bound, got %+v", fanOut)
	}
}

// TestMixedSpeedProducesBothModes proves the bimodal claim is real in the
// generated data, not only in the plan. It is the mix's whole value: at
// Workers: 8 with a tenth of jobs slow, most claimed batches contain one, and a
// batch runs at the speed of its slowest member.
func TestMixedSpeedProducesBothModes(t *testing.T) {
	t.Parallel()

	cfg := baseWorkloadConfig()
	cfg.Mix = WorkloadMixedSpeed
	cfg.Jobs = 10_000
	cfg.WorkJitter = 0 // isolate the two modes

	p := plan(cfg)
	var slow, fast int
	for _, s := range generate(cfg) {
		switch time.Duration(s.WorkNanos) {
		case p.SlowWork:
			slow++
		case p.BaseWork:
			fast++
		default:
			t.Fatalf("job %d has duration %v, which is neither mode (%v or %v)",
				s.N, time.Duration(s.WorkNanos), p.BaseWork, p.SlowWork)
		}
	}
	if slow+fast != cfg.Jobs {
		t.Fatalf("accounted for %d of %d jobs", slow+fast, cfg.Jobs)
	}
	// 10% of 10,000 with a fixed seed: assert the order of magnitude, not the
	// exact draw, so the test does not break on an unrelated generator change.
	if slow < 800 || slow > 1200 {
		t.Errorf("slow jobs = %d, want roughly %.0f%% of %d", slow, 100*slowFraction, cfg.Jobs)
	}
}

// TestGenerateProducesRealisticRows guards the two properties that make the
// storage numbers mean anything: rows are not empty, and priorities are not all
// the same.
func TestGenerateProducesRealisticRows(t *testing.T) {
	t.Parallel()

	specs := generate(baseWorkloadConfig())
	priorities := make(map[int]bool)
	for _, s := range specs {
		if len(s.Payload) != payloadBytes {
			t.Fatalf("job %d payload is %d bytes, want %d: row width drives every storage number",
				s.N, len(s.Payload), payloadBytes)
		}
		if s.WorkNanos < 0 {
			t.Fatalf("job %d has a negative work duration", s.N)
		}
		priorities[s.Priority] = true
	}
	if len(priorities) < 2 {
		t.Error("every job got the same priority: the claim's ORDER BY would do less work than in production")
	}
}

// TestJitterCannotProduceNegativeWork covers the boundary where a jitter wider
// than the base duration would otherwise yield a negative sleep.
func TestJitterCannotProduceNegativeWork(t *testing.T) {
	t.Parallel()

	cfg := baseWorkloadConfig()
	cfg.WorkDuration = time.Millisecond
	cfg.WorkJitter = 10 * time.Millisecond

	for _, s := range generate(cfg) {
		if s.WorkNanos < 0 {
			t.Fatalf("job %d work = %d ns, want it clamped at zero", s.N, s.WorkNanos)
		}
	}
}
