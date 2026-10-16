//go:build integration

package core

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestQueryJobsAppliesEveryFilterPostgres runs the filter suite on PostgreSQL,
// then the filters only PostgreSQL has: args and metadata containment within a
// created window, and the tag filter served by jobs_explore_tags.
func TestQueryJobsAppliesEveryFilterPostgres(t *testing.T) {
	t.Parallel()
	queryFilterSuite(t, postgresOpener)

	db := NewPostgresIsolatedDB(t)
	ids := seedQueryFixture(t, db)
	object := func(s string) json.RawMessage { return json.RawMessage(s) }
	runQueryFilterCases(t, db, ids, []queryFilterCase{
		{"args_contains_within_a_day", func(queryIDs) JobQuery {
			return JobQuery{ArgsContains: object(`{"person_id":"p1"}`), CreatedFrom: queryDay(0), CreatedTo: queryDay(1)}
		}, []string{"a1"}},
		{"args_contains_within_two_days", func(queryIDs) JobQuery {
			return JobQuery{ArgsContains: object(`{"person_id":"p1"}`), CreatedFrom: queryDay(0), CreatedTo: queryDay(2)}
		}, []string{"a3", "a1"}},
		{"args_contains_and_a_kind", func(queryIDs) JobQuery {
			return JobQuery{
				ArgsContains: object(`{"person_id":"p2"}`), Kinds: []string{"a"},
				CreatedFrom: queryDay(0), CreatedTo: queryDay(3),
			}
		}, []string{"a2"}},
		{"metadata_contains", func(queryIDs) JobQuery {
			return JobQuery{MetadataContains: object(`{"source":"fixture"}`), CreatedFrom: queryDay(0), CreatedTo: queryDay(3)}
		}, []string{"a1"}},
		{"both_containments", func(queryIDs) JobQuery {
			return JobQuery{
				ArgsContains: object(`{"n":1}`), MetadataContains: object(`{"source":"fixture"}`),
				CreatedFrom: queryDay(0), CreatedTo: queryDay(3),
			}
		}, []string{"a1"}},
	})

	t.Run("a_null_metadata_never_matches", func(t *testing.T) {
		require.NoError(t, db.Exec(`ALTER TABLE jobs ALTER COLUMN metadata DROP NOT NULL`).Error)
		require.NoError(t, db.Exec(`UPDATE jobs SET metadata = NULL WHERE id <> ?`, ids.id["a1"]).Error)
		page, err := QueryJobs(context.Background(), db, JobQuery{
			MetadataContains: object(`{}`), CreatedFrom: queryDay(0), CreatedTo: queryDay(3),
		})
		require.NoError(t, err)
		require.Equal(t, []string{"a1"}, ids.keys(t, page))
	})

	tagCases := func() []queryFilterCase {
		var out []queryFilterCase
		for _, c := range queryFilterCases() {
			switch c.name {
			case "one_tag", "any_of_the_tags", "several_fields_and_a_tag":
				out = append(out, c)
			}
		}
		require.Len(t, out, 3)
		return out
	}
	t.Run("tags_without_their_index", func(t *testing.T) {
		db := NewPostgresIsolatedDB(t)
		runQueryFilterCases(t, db, seedQueryFixture(t, db), tagCases())
	})
	t.Run("tags_with_their_index", func(t *testing.T) {
		db := NewPostgresIsolatedDB(t)
		set, err := ExplorerIndexSet("postgres")
		require.NoError(t, err)
		require.NoError(t, InstallIndexesWithOptions(context.Background(), db, IndexOpts{ExtraIndexes: set}))
		runQueryFilterCases(t, db, seedQueryFixture(t, db), tagCases())
	})
}

// TestQueryJobsPagesNeverSkipOrRepeatTiedRowsPostgres runs the paging suite on
// PostgreSQL, where the time sorts compare instants.
func TestQueryJobsPagesNeverSkipOrRepeatTiedRowsPostgres(t *testing.T) {
	t.Parallel()
	queryPagingSuite(t, postgresOpener)
}

// TestLatestRunFiltersMatchOnlyTheLatestAttemptPostgres runs the latest-run
// filters on PostgreSQL, then ErrorContains, which only PostgreSQL has: the
// latest attempt's message holds the text in any case, within the created
// window, with %, _, and \ each matching only itself; and CountJobs counts what
// each filter lists.
func TestLatestRunFiltersMatchOnlyTheLatestAttemptPostgres(t *testing.T) {
	t.Parallel()
	db := NewPostgresIsolatedDB(t)
	ids := seedLatestRunFixture(t, db)
	runQueryFilterCases(t, db, ids, latestRunFilterCases())

	contains := func(text string) JobQuery {
		return JobQuery{ErrorContains: text, CreatedFrom: queryDay(0), CreatedTo: queryDay(1)}
	}
	cases := []queryFilterCase{
		{"text_as_written", func(queryIDs) JobQuery { return contains("Connection Refused") }, []string{"legacy", "a"}},
		{"text_in_another_case", func(queryIDs) JobQuery { return contains("CONNECTION refused") }, []string{"legacy", "a"}},
		{"text_only_an_earlier_attempt_held", func(queryIDs) JobQuery { return contains("refused by upstream") }, []string{}},
		{"a_percent_matches_only_itself", func(queryIDs) JobQuery { return contains("100%") }, []string{"pct"}},
		{"an_underscore_matches_only_itself", func(queryIDs) JobQuery { return contains("a_b") }, []string{"under"}},
		{"a_backslash_matches_only_itself", func(queryIDs) JobQuery { return contains(`C:\temp`) }, []string{"slash"}},
		{"text_and_a_class", func(queryIDs) JobQuery {
			q := contains("refused")
			q.ErrorClasses = []ErrorClass{ErrorTransient}
			return q
		}, []string{"a"}},
		{"text_and_a_state", func(queryIDs) JobQuery {
			q := contains("exceeded")
			q.States = []JobState{StateDiscarded}
			return q
		}, []string{"b"}},
		{"text_outside_the_window", func(queryIDs) JobQuery {
			return JobQuery{ErrorContains: "refused", CreatedFrom: queryDay(1), CreatedTo: queryDay(2)}
		}, []string{}},
	}
	runQueryFilterCases(t, db, ids, cases)
	t.Run("counts", func(t *testing.T) {
		requireCountsWhatItLists(t, db, ids, append(latestRunFilterCases(), cases...))
	})
}
