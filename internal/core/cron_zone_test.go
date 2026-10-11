package core

import (
	"testing"
	"time"
	_ "time/tzdata" // the zone database, so the test runs on a host without one

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCronZoneFollowsTheClockUnlessCronTZPinsIt pins what PeriodicSpec.Cron
// documents: an expression that names no zone fires in the zone of the time it is
// evaluated against, and a CRON_TZ= prefix pins the zone whatever that time's is.
func TestCronZoneFollowsTheClockUnlessCronTZPinsIt(t *testing.T) {
	t.Parallel()
	newYork, err := time.LoadLocation("America/New_York")
	require.NoError(t, err)
	// Noon in New York on 15 January is 17:00 UTC.
	now := time.Date(2026, 1, 15, 12, 0, 0, 0, newYork)

	for _, tc := range []struct {
		name string
		cron string
		at   time.Time
		want time.Time
	}{
		{
			name: "no zone, a New York clock: 09:00 New York",
			cron: "0 9 * * *", at: now,
			want: time.Date(2026, 1, 16, 9, 0, 0, 0, newYork),
		},
		{
			name: "no zone, the same instant on a UTC clock: 09:00 UTC",
			cron: "0 9 * * *", at: now.UTC(),
			want: time.Date(2026, 1, 16, 9, 0, 0, 0, time.UTC),
		},
		{
			name: "CRON_TZ=UTC on a New York clock: 09:00 UTC",
			cron: "CRON_TZ=UTC 0 9 * * *", at: now,
			want: time.Date(2026, 1, 16, 9, 0, 0, 0, time.UTC),
		},
		{
			name: "CRON_TZ=America/New_York on a UTC clock: 09:00 New York",
			cron: "CRON_TZ=America/New_York 0 9 * * *", at: now.UTC(),
			want: time.Date(2026, 1, 16, 9, 0, 0, 0, newYork),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			spec := PeriodicSpec{Slug: "s", Kind: "k", Cron: tc.cron}
			require.NoError(t, spec.validate())
			next, err := nextFireAfter(spec, tc.at)
			require.NoError(t, err)
			assert.True(t, next.Equal(tc.want), "next fire %s, want %s", next, tc.want)
		})
	}

	// A zone the database does not know is refused like a malformed expression.
	err = PeriodicSpec{Slug: "s", Kind: "k", Cron: "CRON_TZ=Nowhere/Atlantis 0 9 * * *"}.validate()
	require.ErrorIs(t, err, ErrValidation)
}
