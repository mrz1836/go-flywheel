package main

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"testing"
	"time"

	flywheel "github.com/mrz1836/go-flywheel"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// The error fixture's two fingerprints.
const (
	errorsFetchFP = "1111111111111111"
	errorsParseFP = "2222222222222222"
)

// seedErrorRun writes one failed attempt of kind finishing a second after at,
// with its error class, message, and fingerprint, and its job left in jobState.
func seedErrorRun(t *testing.T, db *gorm.DB, id, kind, class, message, fingerprint, jobState string, at time.Time) {
	t.Helper()
	insertStatsJob(t, db, id, kind, "default", jobState, at, nil)
	fin := at.Add(time.Second)
	insertStatsRun(t, db, id+"-r", id, kind, "default", "error", jobState, at, &fin, 1000)
	require.NoError(t, db.Exec(`UPDATE job_runs SET error_class = ?, error_message = ?, error_fingerprint = ? WHERE id = ?`,
		class, message, fingerprint, id+"-r").Error)
}

// TestCLIErrorsListsErrorGroups proves `flywheel errors` lists a window's error
// groups, the most failures first, one row each with its counts, when it was
// first and last seen, and its normalized message cut to 60 characters, then
// the coverage line `flywheel stats` prints; that --kind, --fingerprint,
// --limit, --hourly, and --json shape the read; and that a bad request is
// refused.
func TestCLIErrorsListsErrorGroups(t *testing.T) {
	t.Parallel()
	cfg := migratedCLIConfig(t)
	db, _ := openCLIConfigDB(t, cfg)
	hour := statusAnchor.Add(-2 * time.Hour)
	long := "Upstream returned 503 for item 42 after a wait that runs well past sixty characters"
	for i := range 3 {
		seedErrorRun(t, db, fmt.Sprintf("fetch-%d", i), "fetch", "transient", long, errorsFetchFP, "retryable",
			hour.Add(time.Duration(i)*time.Minute))
	}
	seedErrorRun(t, db, "fetch-3", "fetch", "transient", long, errorsFetchFP, "discarded", hour.Add(10*time.Minute))
	seedErrorRun(t, db, "parse-0", "parse", "permanent", "bad input", errorsParseFP, "discarded", hour.Add(20*time.Minute))
	ctx := statusClockCtx()

	out, err := runRoot(ctx, "--config", cfg, "errors")
	require.NoError(t, err)
	assert.Contains(t, out, "flywheel errors:")
	assert.Regexp(t, `FINGERPRINT\s+KIND\s+CLASS\s+FAILURES\s+RETRIED\s+DISCARDED\s+FIRST SEEN\s+LAST SEEN\s+MESSAGE`, out)
	normalized := flywheel.NormalizeErrorMessage(long)
	require.Greater(t, len(normalized), 60, "the fixture's message is long enough to be cut")
	assert.Regexp(t, regexp.MustCompile(errorsFetchFP+`\s+fetch\s+transient\s+4\s+3\s+1\s+2026-06-22T10:00:01Z\s+2026-06-22T10:10:01Z\s+`+
		regexp.QuoteMeta(normalized[:60])+`\n`), out, "the row carries the counts, both sightings, and the cut message")
	assert.NotContains(t, out, normalized, "the message is cut at 60 characters")
	assert.Regexp(t, errorsParseFP+`\s+parse\s+permanent\s+1\s+0\s+1\s+`, out)
	assert.Less(t, strings.Index(out, errorsFetchFP), strings.Index(out, errorsParseFP), "the most failures first")
	assert.Contains(t, out, "coverage: no hourly rollups yet")
	assert.NotContains(t, out, "HOURLY")

	out, err = runRoot(ctx, "--config", cfg, "errors", "--kind", "parse")
	require.NoError(t, err)
	assert.Contains(t, out, errorsParseFP)
	assert.NotContains(t, out, errorsFetchFP)
	out, err = runRoot(ctx, "--config", cfg, "errors", "--fingerprint", errorsFetchFP)
	require.NoError(t, err)
	assert.Contains(t, out, errorsFetchFP)
	assert.NotContains(t, out, errorsParseFP)
	out, err = runRoot(ctx, "--config", cfg, "errors", "--limit", "1")
	require.NoError(t, err)
	assert.Contains(t, out, errorsFetchFP)
	assert.NotContains(t, out, errorsParseFP)
	out, err = runRoot(ctx, "--config", cfg, "errors", "--since", "1h")
	require.NoError(t, err)
	assert.NotContains(t, out, errorsFetchFP, "--since bounds the window")

	out, err = runRoot(ctx, "--config", cfg, "errors", "--hourly")
	require.NoError(t, err)
	assert.Regexp(t, `MESSAGE\s+HOURLY`, out)
	assert.Regexp(t, `(?m)^`+errorsFetchFP+`\s.*\s(0,){22}4,0$`, out, "one count for each hour of the window, oldest first")

	out, err = runRoot(ctx, "--config", cfg, "errors", "--hourly", "--json")
	require.NoError(t, err)
	var res flywheel.ErrorGroupsResult
	require.NoError(t, json.Unmarshal([]byte(out), &res))
	require.Len(t, res.Groups, 2)
	assert.Equal(t, errorsFetchFP, res.Groups[0].Fingerprint)
	assert.Equal(t, int64(4), res.Groups[0].Failures)
	assert.Len(t, res.Groups[0].Hourly, 24)

	_, err = runRoot(ctx, "--config", cfg, "errors", "--since", "soon")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "--since")
	_, err = runRoot(ctx, "--config", cfg, "errors", "--fingerprint", "ABC")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "fingerprint", "the library's refusal prints as it comes")
}
