package core

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/mrz1836/go-foundation/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// integratingDoc is the guide whose upgrade DDL these tests execute.
const integratingDoc = "../../docs/INTEGRATING.md"

// documentedUpgradeDDL extracts the statements of the SQL block that follows
// the guide's `<!-- upgrade-ddl:<block> -->` marker, split on the semicolons
// that end its lines. A block is named by its dialect — this release's upgrade
// — or by its dialect and the older release it upgrades from: ":nodes", a
// release before the node registry, ":errors", a release before the error
// groups, ":kinds", a release before the kind catalog, ":audit", a release
// before the audit history, ":pause", a release before the schedule pause, and
// ":stats", a release before the stats tables.
func documentedUpgradeDDL(t *testing.T, block string) []string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Clean(integratingDoc))
	require.NoError(t, err)
	doc := string(raw)
	marker := "<!-- upgrade-ddl:" + block + " -->"
	at := strings.Index(doc, marker)
	require.GreaterOrEqual(t, at, 0, "the guide must carry the %s upgrade block", block)
	rest := doc[at+len(marker):]
	open := strings.Index(rest, "```sql\n")
	require.GreaterOrEqual(t, open, 0)
	rest = rest[open+len("```sql\n"):]
	end := strings.Index(rest, "```")
	require.GreaterOrEqual(t, end, 0)

	var stmts []string
	for _, stmt := range strings.Split(rest[:end], ";\n") {
		if stmt = strings.TrimSpace(stmt); stmt != "" {
			stmts = append(stmts, strings.TrimSuffix(stmt, ";"))
		}
	}
	require.NotEmpty(t, stmts)
	return stmts
}

// applyDocumentedUpgrade runs the guide's named blocks against db, in order,
// exactly as written.
func applyDocumentedUpgrade(t *testing.T, db *gorm.DB, blocks ...string) {
	t.Helper()
	for _, block := range blocks {
		for _, stmt := range documentedUpgradeDDL(t, block) {
			require.NoError(t, db.Exec(stmt).Error, "documented statement:\n%s", stmt)
		}
	}
}

// documentedUpgradeSuite applies the guide's upgrade, exactly as written, to a
// database the previous release installed and wrote a schedule into, then the
// deploy step the guide names, and asserts the result has no drift and works:
// the schedule is not paused, fires, and remembers the fire, and a control on
// the job it fired is recorded in the history. The documented upgrade is the
// upgrade, not a description of one.
func documentedUpgradeSuite(t *testing.T, db *gorm.DB, dialect string) {
	t.Helper()
	installPreviousReleaseSchema(t, db)
	due := time.Now().UTC().Truncate(time.Second).Add(-30 * time.Second)
	writeLegacyPeriodic(t, db, "doc-legacy", "doc.periodic", due)

	applyDocumentedUpgrade(t, db, dialect)
	ctx := context.Background()
	require.NoError(t, InstallIndexesWithOptions(ctx, db, IndexOpts{Concurrently: true}))
	assertNoSchemaOrIndexDrift(t, db)

	views, err := ListPeriodics(ctx, db)
	require.NoError(t, err)
	require.Len(t, views, 1)
	assert.False(t, views[0].Paused, "a schedule written before the upgrade is not paused")
	assert.Empty(t, views[0].Source, "and has no source until its next write")
	s := newSchedulerCfg(t, SchedulerConfig{DB: db, Client: NewClient(db), Driver: driverForTest(t, db)})
	require.NoError(t, s.checkSchema(ctx))
	n, err := s.Tick(clockCtx(ctx, models.NewFixedClock(due.Add(time.Second))))
	require.NoError(t, err)
	assert.Equal(t, 1, n, "and it fires on its cadence")
	assertFireRemembered(t, db, "doc-legacy", "doc.periodic")

	// A control on the upgraded schema records its entry, and the history reads
	// it back.
	var fired []string
	require.NoError(t, db.Model(&jobRow{}).Where("kind = ?", "doc.periodic").Pluck("id", &fired).Error)
	require.Len(t, fired, 1)
	operator := WithActor(ctx, Actor{Type: ActorUser, ID: "operator", Reason: "after the upgrade"})
	require.NoError(t, RetryJobWithOptions(operator, db, fired[0], RetryOpts{Force: true}))
	page, err := ListAudit(ctx, db, ListAuditParams{TargetType: AuditTargetJob, TargetID: fired[0]})
	require.NoError(t, err)
	require.Len(t, page.Entries, 1)
	assert.Equal(t, AuditRetried, page.Entries[0].Action)
	assert.Equal(t, "operator", page.Entries[0].Actor.ID)
}

// documentedStatsUpgradeSuite applies the guide's upgrade from a release
// before the stats tables — that upgrade's block, then the schedule pause's,
// then the audit history's, then the kind catalog's, then the error groups',
// then the node registry's, then this release's, oldest first — to a database
// such a release installed and wrote history into, then the deploy step, and
// asserts the result has no drift, runs, and reaches the old history.
func documentedStatsUpgradeSuite(t *testing.T, db *gorm.DB, dialect string) {
	t.Helper()
	installLegacySchema(t, db)
	writeLegacyHistory(t, db, "doc-legacy", "doc.kind", StateSucceeded, rollupBase)

	applyDocumentedUpgrade(t, db,
		dialect+":stats", dialect+":pause", dialect+":audit", dialect+":kinds", dialect+":errors", dialect+":nodes",
		dialect)
	ctx := context.Background()
	require.NoError(t, InstallIndexesWithOptions(ctx, db, IndexOpts{Concurrently: true}))

	gaps, err := InspectSchema(ctx, db)
	require.NoError(t, err)
	assert.Empty(t, gaps, "the documented DDL adds every column and table the runtime declares")
	drift, err := InspectIndexes(ctx, db)
	require.NoError(t, err)
	assert.Empty(t, drift)

	// The upgraded schema runs this release, and the old history reaches the stats.
	n, err := BackfillRunFinishes(ctx, db)
	require.NoError(t, err)
	assert.EqualValues(t, 1, n)
	reg := NewRegistry()
	Register[statsWriteArgs](reg, statsWriteWorker{})
	_, err = Insert(ctx, NewClient(db), statsWriteArgs{}, InsertOpts{})
	require.NoError(t, err)
	r, err := NewRunner(RunnerConfig{
		DB: db, Driver: driverForTest(t, db), Registry: reg, Queues: []string{defaultQueue}, ClaimAnyClass: true,
	})
	require.NoError(t, err)
	require.NoError(t, r.RunUntilIdle(ctx))
}

// TestDocumentedUpgradeDDLSQLite runs the guide's SQLite upgrade block.
func TestDocumentedUpgradeDDLSQLite(t *testing.T) {
	t.Parallel()
	documentedUpgradeSuite(t, newBareSQLite(t), "sqlite")
}

// TestDocumentedStatsUpgradeDDLSQLite runs the guide's SQLite upgrade from a
// release before the stats tables.
func TestDocumentedStatsUpgradeDDLSQLite(t *testing.T) {
	t.Parallel()
	documentedStatsUpgradeSuite(t, newBareSQLite(t), "sqlite")
}
