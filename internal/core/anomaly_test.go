package core

import (
	"context"
	"math"
	"math/rand/v2"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// noiseKind is one synthetic kind's normal behavior in the noise suite.
type noiseKind struct {
	name     string
	perHour  float64 // mean runs per hour
	medianMs float64
	sigma    float64 // lognormal shape of a single run's duration
	discardP float64 // chance a run discards its job
	fromHour int     // first hour the kind exists — a brand-new kind starts late
	spiky    bool    // occasional hours at 6× volume
}

// noiseKinds is the population the suite's quiet hours are drawn from: a high-
// and a medium-volume kind, a low-volume kind that needs pooling, a bursty one,
// and one that is brand new two days before the end.
//
//nolint:gochecknoglobals // shared fixture
var noiseKinds = []noiseKind{
	{name: "high", perHour: 220, medianMs: 180, sigma: 0.6, discardP: 0.01},
	{name: "medium", perHour: 40, medianMs: 450, sigma: 0.5, discardP: 0.01},
	{name: "low", perHour: 4, medianMs: 2500, sigma: 0.5, discardP: 0.02},
	{name: "bursty", perHour: 30, medianMs: 900, sigma: 0.7, discardP: 0.01, spiky: true},
	{name: "fresh", perHour: 50, medianMs: 120, sigma: 0.5, discardP: 0.01, fromHour: 12 * 24},
}

// noiseHours is how much history the suite synthesizes: a full baseline week
// before the evaluated week.
const noiseHours = 14 * 24

// injection overrides one kind's behavior for a stretch of hours.
type injection struct {
	kind         string
	from, hours  int
	durationMult float64
	discardP     float64
}

// synthesizeRollups writes noiseHours of hourly rollup rows for noiseKinds,
// starting at base, with the injections applied. Each hour's runs are drawn one
// by one: a lognormal duration around a median that carries a ±30% hourly jitter
// and a 1.5× diurnal swing, a Poisson-ish volume that carries the same diurnal
// swing and, for a spiky kind, occasional 6× bursts, and a binomial discard
// count.
func synthesizeRollups(t testing.TB, db *gorm.DB, rng *rand.Rand, base time.Time, inject ...injection) {
	t.Helper()
	var rows []jobStatsHourlyRow
	for h := range noiseHours {
		hour := base.Add(time.Duration(h) * time.Hour)
		diurnal := 1 + 0.2*math.Sin(2*math.Pi*float64(h%24)/24) // 0.8–1.2: a 1.5× swing
		aggs := map[statsGroup]*runAgg{}
		for _, k := range noiseKinds {
			if h < k.fromHour {
				continue
			}
			durMult, discardP := 1.0, k.discardP
			for _, in := range inject {
				if in.kind == k.name && h >= in.from && h < in.from+in.hours {
					if in.durationMult > 0 {
						durMult = in.durationMult
					}
					if in.discardP > 0 {
						discardP = in.discardP
					}
				}
			}
			volume := k.perHour * diurnal
			if k.spiky && rng.IntN(12) == 0 {
				volume *= 6
			}
			n := max(int(math.Round(volume+math.Sqrt(volume)*rng.NormFloat64())), 0)
			median := k.medianMs * diurnal * (0.7 + 0.6*rng.Float64()) * durMult // ±30% jitter
			a := newRunAgg(k.name, defaultQueue)
			for range n {
				if rng.Float64() < discardP {
					a.Attempts++
					a.Error++
					a.Discarded++
					continue
				}
				ms := int64(math.Round(math.Exp(math.Log(median) + k.sigma*rng.NormFloat64())))
				a.Attempts++
				a.Success++
				a.DurCount++
				a.DurSumMs += ms
				a.DurMaxMs = max(a.DurMaxMs, ms)
				a.DurHist.observe(ms)
			}
			if a.Attempts > 0 {
				aggs[statsGroup{Kind: k.name, Queue: defaultQueue}] = a
			}
		}
		rows = append(rows, hourRows(hour, aggs, hour)...)
	}
	require.NoError(t, db.CreateInBatches(rows, 500).Error)
}

// evaluateWeek runs Anomalies for every hour of the evaluated (second) week and
// returns every anomaly reported, onsets and continuations alike.
func evaluateWeek(t testing.TB, db *gorm.DB, base time.Time) []Anomaly {
	t.Helper()
	var all []Anomaly
	for h := 7 * 24; h < noiseHours; h++ {
		got, err := Anomalies(context.Background(), db, AnomalyParams{Hour: base.Add(time.Duration(h) * time.Hour)})
		require.NoError(t, err)
		all = append(all, got...)
	}
	return all
}

// onsets filters anomalies to their onsets.
func onsets(all []Anomaly) []Anomaly {
	var out []Anomaly
	for _, a := range all {
		if a.Onset {
			out = append(out, a)
		}
	}
	return out
}

// noiseSeeds is the seeds the noise suite runs. FLYWHEEL_NOISE_SEEDS widens it
// for a Monte Carlo run of the false-positive rate, which is how the default
// thresholds were chosen; CI runs the fixed set.
func noiseSeeds(t *testing.T) []uint64 {
	t.Helper()
	if v := os.Getenv("FLYWHEEL_NOISE_SEEDS"); v != "" {
		n, err := strconv.Atoi(v)
		require.NoError(t, err)
		seeds := make([]uint64, n)
		for i := range seeds {
			seeds[i] = uint64(1000 + i) //nolint:gosec // small positive loop index
		}
		return seeds
	}
	return []uint64{1, 2, 3}
}

// TestAnomalyNoiseSuiteRaisesNothingOnNormalHistory is the gate on the
// detector's defaults: a week of evaluations over realistic, noisy, normal
// history raises not a single anomaly.
func TestAnomalyNoiseSuiteRaisesNothingOnNormalHistory(t *testing.T) {
	t.Parallel()
	for _, seed := range noiseSeeds(t) {
		db := newDB(t)
		rng := rand.New(rand.NewPCG(seed, 77)) //nolint:gosec // deterministic test data
		synthesizeRollups(t, db, rng, rollupBase)
		all := evaluateWeek(t, db, rollupBase)
		assert.Empty(t, all, "seed %d: normal history must raise nothing", seed)
	}
}

// TestAnomalyNoiseSuiteFlagsRealRegressionsOnce proves the detector is not
// quiet by being deaf: a real 3× slowdown and a real failure spike, each
// sustained, are each reported as exactly one onset — and nothing else is.
func TestAnomalyNoiseSuiteFlagsRealRegressionsOnce(t *testing.T) {
	t.Parallel()
	for _, seed := range noiseSeeds(t) {
		db := newDB(t)
		rng := rand.New(rand.NewPCG(seed, 78)) //nolint:gosec // deterministic test data
		regressAt, spikeAt := 9*24+5, 11*24+14
		synthesizeRollups(t, db, rng, rollupBase,
			injection{kind: "medium", from: regressAt, hours: 8, durationMult: 3},
			injection{kind: "high", from: spikeAt, hours: 5, discardP: 0.15},
		)
		got := onsets(evaluateWeek(t, db, rollupBase))
		require.Len(t, got, 2, "seed %d: exactly the two injected anomalies, once each: %+v", seed, got)
		byKind := map[string]Anomaly{}
		for _, a := range got {
			byKind[a.Kind] = a
		}
		reg := byKind["medium"]
		assert.Equal(t, SignalDurationRegression, reg.Signal)
		// Two of three: the onset is the second bad hour — or the third, when a
		// quiet hour had to pool with the hour before it to reach its sample size.
		assert.False(t, reg.Hour.Before(rollupBase.Add(time.Duration(regressAt+1)*time.Hour)), "seed %d", seed)
		assert.False(t, reg.Hour.After(rollupBase.Add(time.Duration(regressAt+2)*time.Hour)), "seed %d", seed)
		assert.GreaterOrEqual(t, reg.Ratio, 2.0, "seed %d: the 3x injection (on top of jitter) clears the 2x threshold", seed)
		spike := byKind["high"]
		assert.Equal(t, SignalFailureSpike, spike.Signal)
		assert.Equal(t, rollupBase.Add(time.Duration(spikeAt+1)*time.Hour), spike.Hour)
		assert.Equal(t, "discard_rate", spike.Metric)
	}
}

// TestAnomaliesReportContinuationsAndDefaults covers the API surface beyond the
// gate: a sustained anomaly is reported every hour it lasts with Onset only on
// the first, the zero Hour evaluates the latest rolled hour, and an empty
// database reports nothing.
func TestAnomaliesReportContinuationsAndDefaults(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	empty, err := Anomalies(ctx, newDB(t), AnomalyParams{})
	require.NoError(t, err)
	assert.Empty(t, empty)

	db := newDB(t)
	rng := rand.New(rand.NewPCG(5, 5)) //nolint:gosec // deterministic test data
	regressAt := noiseHours - 4
	synthesizeRollups(t, db, rng, rollupBase, injection{kind: "medium", from: regressAt, hours: 4, durationMult: 4})

	latest, err := Anomalies(ctx, db, AnomalyParams{})
	require.NoError(t, err)
	require.Len(t, latest, 1, "the zero Hour evaluates the last rolled hour")
	assert.False(t, latest[0].Onset, "four hours in, it is a continuation")
	assert.Equal(t, rollupBase.Add(time.Duration(noiseHours-1)*time.Hour), latest[0].Hour)

	var onsetHours []time.Time
	for h := regressAt - 1; h < noiseHours; h++ {
		got, err := Anomalies(ctx, db, AnomalyParams{Hour: rollupBase.Add(time.Duration(h) * time.Hour)})
		require.NoError(t, err)
		for _, a := range got {
			if a.Onset {
				onsetHours = append(onsetHours, a.Hour)
			}
		}
	}
	assert.Len(t, onsetHours, 1, "a sustained anomaly has exactly one onset")

	strict, err := Anomalies(ctx, db, AnomalyParams{
		Thresholds: AnomalyThresholds{DurationRatio: 10},
	})
	require.NoError(t, err)
	assert.Empty(t, strict, "a threshold is honored")
}
