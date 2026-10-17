//go:build integration

package core

import (
	"testing"
)

// TestBulkNeverMatchesAJobAboveMaxIDPostgres runs the id-ceiling suite on
// PostgreSQL, where a batch locks its candidates with FOR UPDATE.
func TestBulkNeverMatchesAJobAboveMaxIDPostgres(t *testing.T) {
	t.Parallel()
	bulkMaxIDSuite(t, postgresOpener)
}

// TestBulkResumesWithoutRepeatingAJobPostgres runs the resume suite on
// PostgreSQL.
func TestBulkResumesWithoutRepeatingAJobPostgres(t *testing.T) {
	t.Parallel()
	bulkResumeSuite(t, postgresOpener)
}

// TestBulkAppliesTheErrorTextFilterPostgres proves a bulk selection applies
// ErrorContains, which only PostgreSQL has, exactly as QueryJobs does: the
// latest attempt's message holds the text in any case, within the created
// window, with %, _, and \ each matching only themselves, alone and beside a
// class or a state.
func TestBulkAppliesTheErrorTextFilterPostgres(t *testing.T) {
	t.Parallel()
	contains := func(text string) JobQuery {
		return JobQuery{ErrorContains: text, CreatedFrom: queryDay(0), CreatedTo: queryDay(1)}
	}
	withClass := contains("refused")
	withClass.ErrorClasses = []ErrorClass{ErrorTransient}
	withState := contains("exceeded")
	withState.States = []JobState{StateDiscarded}
	cases := map[string]JobQuery{
		"text_as_written":                   contains("Connection Refused"),
		"text_in_another_case":              contains("CONNECTION refused"),
		"text_only_an_earlier_attempt_held": contains("refused by upstream"),
		"a_percent_matches_only_itself":     contains("100%"),
		"an_underscore_matches_only_itself": contains("a_b"),
		"a_backslash_matches_only_itself":   contains(`C:\temp`),
		"text_and_a_class":                  withClass,
		"text_and_a_state":                  withState,
		"text_outside_the_window":           {ErrorContains: "refused", CreatedFrom: queryDay(1), CreatedTo: queryDay(2)},
	}
	for name, q := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			db := NewPostgresIsolatedDB(t)
			seedLatestRunFixture(t, db)
			assertBulkActsOnWhatQueryJobsLists(t, db, q)
		})
	}
}
