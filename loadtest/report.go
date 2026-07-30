//go:build loadtest

package loadtest

import (
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
	"time"
)

// Latency is a duration distribution measured by the harness itself.
//
// It is deliberately independent of the observer and metrics packages: the
// harness must be able to produce percentiles before the runtime can, and the
// two answer different questions — this one is about this project's numbers, the
// operator-facing one is about a production deployment.
//
// Count, Min, Max, and Mean are exact. The percentiles come from a bucketed
// histogram and carry the relative error HistogramSpec publishes; see
// histogram.go for why the bucketing is what it is.
type Latency struct {
	Count                   int64
	Min, P50, P95, P99, Max time.Duration
	Mean                    time.Duration
}

// StorageSample is one sampling of the target's storage and contention state.
// The maps are keyed by table name — jobs, job_runs — because every one of these
// numbers is only meaningful per relation.
type StorageSample struct {
	At time.Time
	// LiveTuples, DeadTuples, and the vacuum counters come from
	// pg_stat_user_tables.
	LiveTuples, DeadTuples            map[string]int64
	AutovacuumCount, AutoanalyzeCount map[string]int64
	// SeqScans and IdxScans come from the same view. They are what turns an
	// index-condition comparison from an inference about timing into direct
	// evidence about the plan.
	SeqScans, IdxScans map[string]int64
	// TableBytes and IndexBytes come from pg_total_relation_size and
	// pg_indexes_size; FreePercent and DeadTuplePercent from pgstattuple_approx
	// when the extension is installed. Absent, the two percent maps are nil and
	// the report says so in Notes rather than reporting zero.
	TableBytes, IndexBytes        map[string]int64
	FreePercent, DeadTuplePercent map[string]float64
	// WALBytes is the delta in pg_stat_wal since the previous sample. It is
	// cluster-wide: PostgreSQL offers no per-database breakdown.
	WALBytes int64
	// LockWaits counts ungranted lock requests against this database at the
	// instant of the sample. It is a sample of a transient condition and reads
	// zero through most real contention; see Notes.
	LockWaits int64
	// LongestLockWait is how long, in seconds, the longest-blocked backend has
	// been waiting at the instant of the sample. Where LockWaits counts a
	// condition, this measures its duration — which is what survives being
	// sampled on an interval.
	LongestLockWait float64
	// MaxXactAge is the age in seconds of the longest transaction open against
	// this database at the instant of the sample, excluding the sampler's own.
	//
	// Its guarantee is one-sided: a sampler at interval I always observes a
	// transaction living longer than I, and may miss one shorter than I entirely.
	// That makes it the right instrument for a long transaction and the wrong one
	// for a short one; see the sampleXactAge doc comment.
	MaxXactAge float64
	// RSS is the harness client process's resident set, not the server's.
	RSS uint64
}

// HistogramSpec publishes the bucketing behind the percentiles, so a reported
// p99 carries its error bar. A percentile with no error bar is a claim.
type HistogramSpec struct {
	// SubBucketsPerOctave is how many buckets divide each power-of-two range.
	SubBucketsPerOctave int
	// MinExponent and MaxExponent bound the covered range as powers of two,
	// in nanoseconds.
	MinExponent, MaxExponent int
	// Buckets is the total bucket count, including underflow and overflow.
	Buckets int
	// MaxRelativeError is the worst-case relative error on a reported quantile,
	// derived from the widest bucket. Count, Min, Max, and Mean are exact and are
	// not subject to it.
	MaxRelativeError float64
}

// Report is one run's result. Every field is a measured number; nothing is
// derived from a claim.
type Report struct {
	Config Config

	// StartedAt and Duration bound the measured window: seeding on a drain run
	// happens before StartedAt and is not in it.
	StartedAt time.Time
	Duration  time.Duration

	EnqueueThroughput float64 // jobs/second
	DrainThroughput   float64 // jobs/second

	// SlotUtilization is the fraction of the run's worker capacity that was
	// occupied by an attempt: the summed duration of every finished and superseded
	// attempt, over Runners × Workers × Duration.
	//
	// It is the number the worker pool exists to move. A dispatch loop that claims
	// a fixed batch and waits for the slowest of it leaves the rest of its slots
	// idle for the difference, and with a mixed-speed workload that difference is
	// most of the run.
	//
	// It understates true occupancy, and by a knowable amount: an attempt's
	// duration is measured from the run stub to the finalize, so the claim, stub,
	// and finalize round trips around it are capacity the pool was using and this
	// does not count. Treat it as a floor, and treat a before/after comparison at
	// the same configuration as what carries the claim.
	SlotUtilization float64

	Claim    Latency
	Finalize Latency
	Sweep    Latency

	Storage []StorageSample
	PeakRSS uint64

	// PeakXactAge is the longest server-side transaction age any sample observed,
	// in seconds. It is the run-level answer to "did any single transaction hold
	// a snapshot open", which is what an unbounded maintenance pass looks like
	// from outside the process running it.
	//
	// It under-reports a transaction shorter than the sample interval, by
	// construction. For bounded maintenance the exact figure is Sweep.Max, which
	// is client-side and measures one transaction per call — at the cost of
	// including pool acquisition, which the server-side age excludes.
	PeakXactAge float64
	// PeakLockWaits is the highest ungranted lock count any sample observed, and
	// LongestLockWait the longest single wait. Both are scoped to this run's
	// database.
	PeakLockWaits   int64
	LongestLockWait float64

	// Enqueued, Retried and Superseded are event counts accumulated during the
	// run. Drained and Discarded are *residual state counts*, read from the table
	// after the run: how many rows were sitting in a terminal state at the end.
	//
	// The distinction is invisible on a drain run, where nothing removes rows, and
	// decisive on a run with retention enabled, where terminal rows are being
	// deleted continuously — there, Drained counts what retention had not yet
	// pruned, which can be an order of magnitude below the number of jobs the run
	// actually drained. The event-based figure is DrainThroughput × Duration, and
	// a run with retention on says so in its Notes.
	Enqueued, Drained, Retried, Discarded, Superseded int64

	// Reclaimed counts jobs the harness's sweeper returned to available after
	// their lease expired. A non-zero value in a run with no injected fault is a
	// finding, not noise.
	Reclaimed int64

	// RunRows is the total job_runs rows the run wrote — one per attempt. It is the
	// pre-claim gate's headline against the claim-then-snooze baseline: a gated run
	// writes about one row per drained job, where a snoozing run writes one per
	// deferral too, so its audit table grows at the snooze rate rather than the
	// completion rate.
	RunRows int64

	// BlockedClaims counts claim attempts a fault's gate refused before they
	// reached the database. It is how "the runner backed off during the outage"
	// becomes a number in the report rather than a narration: a gated call
	// deliberately records no latency observation, so without this a pause-database
	// run has no evidence of how hard the runners hammered the gate.
	BlockedClaims int64

	// ConcurrentExecutions counts times the same job was observed running in two
	// workers at once. It must be zero: it is the exactly-once guarantee stated
	// as a number.
	ConcurrentExecutions int64

	// Errors are the distinct errors the run collected, each carrying its
	// occurrence count. See errset for why they are deduplicated.
	Errors []error

	// Notes carries every caveat the run discovered: which RSS mechanism ran,
	// whether pgstattuple was available, what LockWaits does and does not see. A
	// number published without its caveat is a claim, and this file is where the
	// caveats travel with the numbers.
	Notes []string

	// WorkloadDigest is a hash of the generated workload. Two runs with equal
	// Config produce equal digests, which makes reproducibility a diff of two
	// strings rather than an argument.
	WorkloadDigest string

	// Histogram publishes the bucketing the three Latency values came from.
	Histogram HistogramSpec

	// StorageParams is pg_class.reloptions per table as actually installed,
	// keyed by table name. An empty string means the table carries PostgreSQL's
	// defaults. It records what the run got rather than what it asked for.
	StorageParams map[string]string

	// Schema is the isolated schema the run provisioned, retained so a failed run
	// can be inspected before the next one drops it.
	Schema string

	// Replay carries the replay phase's re-convergence account on a -replay run,
	// and is nil otherwise.
	Replay *ReplayReport
}

// ReplayReport is the account of a replay phase: what it recovered and whether the
// cohort re-converged. It is present only on a run with -replay.
//
// The re-convergence guarantee is three numbers. SucceededBeforeReplay and
// SucceededAfterReplay must be equal — a replay of the discarded cohort never
// re-runs succeeded work. MaxRunsOverBudget must be non-positive — no job ran more
// times than its restored budget allowed. And the run reaching this report at all
// means the second drain completed, so every replayed job re-converged to terminal.
type ReplayReport struct {
	// Parents is how many parents the replay scoped to (ReplayByParent per parent).
	Parents int `json:"parents"`
	// Replayed is the total children returned to available across those parents —
	// the sum of ScopeResult.Changed.
	Replayed int64 `json:"replayed"`
	// SkippedTerminal is the sum of ScopeResult.SkippedTerminal: the terminal
	// children the replay left alone, which are the succeeded ones.
	SkippedTerminal int64 `json:"skipped_terminal"`
	// SucceededBeforeReplay and SucceededAfterReplay bracket the replay. They must
	// be equal: a replay of the discarded cohort must not re-run succeeded work.
	SucceededBeforeReplay int64 `json:"succeeded_before_replay"`
	SucceededAfterReplay  int64 `json:"succeeded_after_replay"`
	// DiscardedAfterReplay is the terminal discarded count once the replayed cohort
	// re-converged — the second-drain terminal count.
	DiscardedAfterReplay int64 `json:"discarded_after_replay"`
	// MaxRunsOverBudget is the largest amount by which any job's recorded run count
	// exceeded its restored max_attempts. It must be non-positive.
	MaxRunsOverBudget int64 `json:"max_runs_over_budget"`
}

// --- JSON wire form ---------------------------------------------------------
//
// Report does not round-trip through encoding/json as declared, and the reasons
// are not stylistic:
//
//   - Errors is []error. An interface marshals to {} and cannot be unmarshaled
//     at all, so a committed report would carry an array of empty objects where
//     its failures should be.
//   - Config.DSN carries credentials. These reports are committed under docs/,
//     so writing it verbatim would publish a password and trip the repository's
//     secret scanner.
//   - Config.Faults is an interface, with the same problem as Errors.
//   - A time.Duration marshals as an integer count of nanoseconds, which is
//     unreadable at both ends of the range this harness spans.
//   - A zero-Count Latency would print p50: 0, p99: 0 — which reads as "fast"
//     rather than "never measured".
//
// So the wire form is its own type. Report owns the conversion in both
// directions, and the round-trip is asserted by a test rather than assumed.

// latencyJSON is a Latency in readable units. It is a pointer field on
// reportJSON so a never-measured distribution is absent rather than zero.
type latencyJSON struct {
	Count int64  `json:"count"`
	Min   string `json:"min"`
	P50   string `json:"p50"`
	P95   string `json:"p95"`
	P99   string `json:"p99"`
	Max   string `json:"max"`
	Mean  string `json:"mean"`
	// Nanos repeats the same distribution in raw nanoseconds, for tooling that
	// needs to compute rather than read.
	Nanos latencyNanosJSON `json:"nanos"`
}

// latencyNanosJSON is the machine-readable half of latencyJSON.
type latencyNanosJSON struct {
	Min  int64 `json:"min"`
	P50  int64 `json:"p50"`
	P95  int64 `json:"p95"`
	P99  int64 `json:"p99"`
	Max  int64 `json:"max"`
	Mean int64 `json:"mean"`
}

// configJSON is Config's wire form: the DSN redacted, durations readable, and
// any configured fault reduced to its description.
type configJSON struct {
	Target       string `json:"target"`
	Jobs         int    `json:"jobs"`
	Seed         int64  `json:"seed"`
	Runners      int    `json:"runners"`
	Workers      int    `json:"workers"`
	Mix          string `json:"mix"`
	Indexes      string `json:"indexes"`
	WorkDuration string `json:"work_duration"`
	WorkJitter   string `json:"work_jitter"`
	// Lease is the resolved lease the runners used, not the configured zero. A
	// report whose lease was derived and one whose lease was set must be
	// distinguishable, because the whole point of setting it is to make the lease
	// shorter than the work.
	Lease string `json:"lease"`
	// Heartbeat is the configured renewal interval verbatim: "0s" means derived
	// from the lease, a negative value means renewal was off. A report of a run
	// with the heartbeat disabled must be identifiable as one.
	Heartbeat      string `json:"heartbeat"`
	SampleInterval string `json:"sample_interval"`
	Timeout        string `json:"timeout"`
	Queue          string `json:"queue"`
	ExecutorClass  string `json:"executor_class"`
	// Faults is the fault's description, never the value: a Fault is an
	// interface, so it marshals to an empty object and cannot be read back.
	Faults string `json:"faults,omitempty"`
	// Limiter and its parameters record the admission gate the run installed, and
	// WorkerSnooze the claim-then-snooze baseline it was measured against. All
	// omit-empty: an ungated run's config section is unchanged.
	Limiter       string `json:"limiter,omitempty"`
	Rate          int    `json:"rate,omitempty"`
	Burst         int    `json:"burst,omitempty"`
	MaxConcurrent int    `json:"max_concurrent,omitempty"`
	WorkerSnooze  int    `json:"worker_snooze,omitempty"`
}

// reportJSON is Report's wire form.
type reportJSON struct {
	Config         configJSON        `json:"config"`
	StartedAt      time.Time         `json:"started_at"`
	Duration       string            `json:"duration"`
	DurationNanos  int64             `json:"duration_nanos"`
	Enqueue        float64           `json:"enqueue_throughput_per_sec"`
	Drain          float64           `json:"drain_throughput_per_sec"`
	SlotUtil       float64           `json:"slot_utilization"`
	Claim          *latencyJSON      `json:"claim,omitempty"`
	Finalize       *latencyJSON      `json:"finalize,omitempty"`
	Sweep          *latencyJSON      `json:"sweep,omitempty"`
	Storage        []StorageSample   `json:"storage,omitempty"`
	PeakRSS        uint64            `json:"peak_rss_bytes"`
	PeakXactAge    float64           `json:"peak_xact_age_seconds"`
	PeakLockWaits  int64             `json:"peak_lock_waits"`
	LongestLockWt  float64           `json:"longest_lock_wait_seconds"`
	Reclaimed      int64             `json:"reclaimed"`
	RunRows        int64             `json:"run_rows,omitempty"`
	BlockedClaims  int64             `json:"blocked_claims,omitempty"`
	Concurrent     int64             `json:"concurrent_executions"`
	Enqueued       int64             `json:"enqueued"`
	Drained        int64             `json:"drained"`
	Retried        int64             `json:"retried"`
	Discarded      int64             `json:"discarded"`
	Superseded     int64             `json:"superseded"`
	Errors         []errEntry        `json:"errors,omitempty"`
	Notes          []string          `json:"notes,omitempty"`
	WorkloadDigest string            `json:"workload_digest"`
	Histogram      HistogramSpec     `json:"histogram"`
	Schema         string            `json:"schema,omitempty"`
	StorageParams  map[string]string `json:"storage_params,omitempty"`
	Replay         *ReplayReport     `json:"replay,omitempty"`
}

// MarshalJSON renders the report in its wire form.
func (r Report) MarshalJSON() ([]byte, error) {
	out := reportJSON{
		Config:         configToJSON(r.Config),
		StartedAt:      r.StartedAt,
		Duration:       r.Duration.String(),
		DurationNanos:  r.Duration.Nanoseconds(),
		Enqueue:        r.EnqueueThroughput,
		Drain:          r.DrainThroughput,
		SlotUtil:       r.SlotUtilization,
		Claim:          latencyToJSON(r.Claim),
		Finalize:       latencyToJSON(r.Finalize),
		Sweep:          latencyToJSON(r.Sweep),
		Storage:        r.Storage,
		PeakRSS:        r.PeakRSS,
		PeakXactAge:    r.PeakXactAge,
		PeakLockWaits:  r.PeakLockWaits,
		LongestLockWt:  r.LongestLockWait,
		Reclaimed:      r.Reclaimed,
		RunRows:        r.RunRows,
		BlockedClaims:  r.BlockedClaims,
		Concurrent:     r.ConcurrentExecutions,
		Enqueued:       r.Enqueued,
		Drained:        r.Drained,
		Retried:        r.Retried,
		Discarded:      r.Discarded,
		Superseded:     r.Superseded,
		Errors:         errorsToJSON(r.Errors),
		Notes:          r.Notes,
		WorkloadDigest: r.WorkloadDigest,
		Histogram:      r.Histogram,
		Schema:         r.Schema,
		StorageParams:  r.StorageParams,
		Replay:         r.Replay,
	}
	data, err := json.Marshal(out)
	if err != nil {
		return nil, fmt.Errorf("loadtest: marshal report: %w", err)
	}
	return data, nil
}

// UnmarshalJSON reads a report back from its wire form.
//
// Two fields do not survive as written, both deliberately, and in opposite
// directions. Config.DSN comes back redacted, because the credentials were never
// written — a reader that wants to re-run a report supplies its own DSN, which
// is the only safe way for a committed artifact to work. Config.Lease comes back
// *resolved*: the report records the lease the runners actually used, so a run
// that left it derived reads back with the derived value spelled out. The
// reconstructed Config is behaviorally identical either way, since leaseFor
// returns an explicit lease unchanged.
func (r *Report) UnmarshalJSON(data []byte) error {
	var in reportJSON
	if err := json.Unmarshal(data, &in); err != nil {
		return fmt.Errorf("loadtest: unmarshal report: %w", err)
	}
	*r = Report{
		Config:               configFromJSON(in.Config),
		StartedAt:            in.StartedAt,
		Duration:             time.Duration(in.DurationNanos),
		EnqueueThroughput:    in.Enqueue,
		DrainThroughput:      in.Drain,
		SlotUtilization:      in.SlotUtil,
		Claim:                latencyFromJSON(in.Claim),
		Finalize:             latencyFromJSON(in.Finalize),
		Sweep:                latencyFromJSON(in.Sweep),
		Storage:              in.Storage,
		PeakRSS:              in.PeakRSS,
		PeakXactAge:          in.PeakXactAge,
		PeakLockWaits:        in.PeakLockWaits,
		LongestLockWait:      in.LongestLockWt,
		Reclaimed:            in.Reclaimed,
		RunRows:              in.RunRows,
		BlockedClaims:        in.BlockedClaims,
		ConcurrentExecutions: in.Concurrent,
		Enqueued:             in.Enqueued,
		Drained:              in.Drained,
		Retried:              in.Retried,
		Discarded:            in.Discarded,
		Superseded:           in.Superseded,
		Errors:               errorsFromJSON(in.Errors),
		Notes:                in.Notes,
		WorkloadDigest:       in.WorkloadDigest,
		Histogram:            in.Histogram,
		Schema:               in.Schema,
		StorageParams:        in.StorageParams,
		Replay:               in.Replay,
	}
	return nil
}

// configToJSON renders a Config for the wire, redacting the DSN.
func configToJSON(c Config) configJSON {
	return configJSON{
		Target:         redactDSN(c.DSN),
		Jobs:           c.Jobs,
		Seed:           c.Seed,
		Runners:        c.Runners,
		Workers:        c.Workers,
		Mix:            string(c.Mix),
		Indexes:        string(c.Indexes),
		WorkDuration:   c.WorkDuration.String(),
		WorkJitter:     c.WorkJitter.String(),
		Lease:          leaseFor(c).String(),
		Heartbeat:      c.Heartbeat.String(),
		SampleInterval: c.SampleInterval.String(),
		Timeout:        c.Timeout.String(),
		Queue:          c.Queue,
		ExecutorClass:  c.ExecutorClass,
		Faults:         describeFault(c.Faults),
		Limiter:        gatedName(c.Limiter),
		Rate:           c.Rate,
		Burst:          c.Burst,
		MaxConcurrent:  c.MaxConcurrent,
		WorkerSnooze:   c.WorkerSnooze,
	}
}

// gatedName renders a limiter kind for the wire, dropping the "none" default to
// the empty string so an ungated run's config section carries no limiter key.
func gatedName(k LimiterKind) string {
	if !k.gated() {
		return ""
	}
	return string(k)
}

// describeFault renders a configured fault, or "" when there is none.
func describeFault(f Fault) string {
	if f == nil {
		return ""
	}
	return f.Describe()
}

// configFromJSON reads a Config back. DSN stays empty: the credentials were
// never written, so there is nothing to restore and a placeholder would be a
// target that does not exist.
func configFromJSON(c configJSON) Config {
	return Config{
		Jobs:           c.Jobs,
		Seed:           c.Seed,
		Runners:        c.Runners,
		Workers:        c.Workers,
		Mix:            Workload(c.Mix),
		Indexes:        IndexCondition(c.Indexes),
		WorkDuration:   mustParseDuration(c.WorkDuration),
		WorkJitter:     mustParseDuration(c.WorkJitter),
		Lease:          mustParseDuration(c.Lease),
		Heartbeat:      mustParseDuration(c.Heartbeat),
		SampleInterval: mustParseDuration(c.SampleInterval),
		Timeout:        mustParseDuration(c.Timeout),
		Queue:          c.Queue,
		ExecutorClass:  c.ExecutorClass,
		Limiter:        LimiterKind(c.Limiter),
		Rate:           c.Rate,
		Burst:          c.Burst,
		MaxConcurrent:  c.MaxConcurrent,
		WorkerSnooze:   c.WorkerSnooze,
	}
}

// mustParseDuration parses a duration written by configToJSON, yielding zero for
// anything it cannot read. The readable string is a convenience for a human; the
// nanosecond fields are what tooling reads, so a malformed value here costs
// nothing that matters.
func mustParseDuration(s string) time.Duration {
	d, err := time.ParseDuration(s)
	if err != nil {
		return 0
	}
	return d
}

// latencyToJSON renders a distribution, or nil when nothing was measured. A
// never-measured distribution must be absent from the report: printed as zeros
// it reads as "instant" instead of "not observed".
func latencyToJSON(l Latency) *latencyJSON {
	if l.Count == 0 {
		return nil
	}
	return &latencyJSON{
		Count: l.Count,
		Min:   l.Min.String(), P50: l.P50.String(), P95: l.P95.String(),
		P99: l.P99.String(), Max: l.Max.String(), Mean: l.Mean.String(),
		Nanos: latencyNanosJSON{
			Min: l.Min.Nanoseconds(), P50: l.P50.Nanoseconds(), P95: l.P95.Nanoseconds(),
			P99: l.P99.Nanoseconds(), Max: l.Max.Nanoseconds(), Mean: l.Mean.Nanoseconds(),
		},
	}
}

// latencyFromJSON reads a distribution back from its nanosecond fields.
func latencyFromJSON(l *latencyJSON) Latency {
	if l == nil {
		return Latency{}
	}
	return Latency{
		Count: l.Count,
		Min:   time.Duration(l.Nanos.Min), P50: time.Duration(l.Nanos.P50),
		P95: time.Duration(l.Nanos.P95), P99: time.Duration(l.Nanos.P99),
		Max: time.Duration(l.Nanos.Max), Mean: time.Duration(l.Nanos.Mean),
	}
}

// errorsToJSON renders collected errors as message/count pairs. errset already
// carries the count inside the message for the in-process form, so this parses
// it back out rather than losing it.
func errorsToJSON(errs []error) []errEntry {
	if len(errs) == 0 {
		return nil
	}
	out := make([]errEntry, len(errs))
	for i, err := range errs {
		msg, count := splitErrorCount(err.Error())
		out[i] = errEntry{Message: msg, Count: count}
	}
	return out
}

// errorsFromJSON reads message/count pairs back into errors, in the same form
// errset produces.
func errorsFromJSON(entries []errEntry) []error {
	if len(entries) == 0 {
		return nil
	}
	out := make([]error, len(entries))
	for i, entry := range entries {
		if entry.Count > 1 {
			out[i] = fmt.Errorf("%s (×%d)", entry.Message, entry.Count) //nolint:err113 // reconstructing a reported error
			continue
		}
		out[i] = fmt.Errorf("%s", entry.Message) //nolint:err113 // reconstructing a reported error
	}
	return out
}

// splitErrorCount separates errset's "message (×N)" suffix from the message.
func splitErrorCount(s string) (string, int64) {
	const open = " (×"
	if !strings.HasSuffix(s, ")") {
		return s, 1
	}
	i := strings.LastIndex(s, open)
	if i < 0 {
		return s, 1
	}
	var n int64
	if _, err := fmt.Sscanf(s[i+len(open):len(s)-1], "%d", &n); err != nil || n <= 0 {
		return s, 1
	}
	return s[:i], n
}

// redactDSN reduces a PostgreSQL DSN to what a reader of a committed report
// needs — host, port, and database — and drops everything else.
//
// These reports are committed under docs/, so this is the difference between an
// artifact and a credential leak. It is written to fail closed: any DSN it
// cannot confidently parse yields a fixed placeholder rather than a best effort,
// because a best effort on an unparsed string is how a password reaches a public
// repository.
func redactDSN(dsn string) string {
	if dsn == "" {
		return ""
	}
	if strings.HasPrefix(dsn, "postgres://") || strings.HasPrefix(dsn, "postgresql://") {
		u, err := url.Parse(dsn)
		if err != nil {
			return "(unparsed dsn, redacted)"
		}
		host := u.Host
		if host == "" {
			host = "(unknown host)"
		}
		return "postgres://" + host + u.EscapedPath()
	}
	// Keyword/value form: host=... port=... dbname=... user=... password=...
	// Only the three non-secret keywords survive, so a keyword this function has
	// never heard of cannot smuggle a secret through.
	var kept []string
	for _, field := range strings.Fields(dsn) {
		key, _, found := strings.Cut(field, "=")
		if !found {
			continue
		}
		switch strings.ToLower(strings.TrimSpace(key)) {
		case "host", "port", "dbname":
			kept = append(kept, field)
		}
	}
	if len(kept) == 0 {
		return "(unparsed dsn, redacted)"
	}
	return strings.Join(kept, " ")
}
