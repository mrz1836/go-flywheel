package core

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

// integratingDoc is the guide whose upgrade DDL these tests execute.
const integratingDoc = "../../docs/INTEGRATING.md"

// documentedUpgradeDDL extracts the statements of the SQL block that follows
// the guide's `<!-- upgrade-ddl:<dialect> -->` marker, split on the semicolons
// that end its lines.
func documentedUpgradeDDL(t *testing.T, dialect string) []string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Clean(integratingDoc))
	require.NoError(t, err)
	doc := string(raw)
	marker := "<!-- upgrade-ddl:" + dialect + " -->"
	at := strings.Index(doc, marker)
	require.GreaterOrEqual(t, at, 0, "the guide must carry the %s upgrade block", dialect)
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

// documentedUpgradeSuite applies the guide's upgrade, exactly as written, to a
// database the previous release installed, then the deploy step the guide names,
// and asserts the result has no drift and works: the documented upgrade is the
// upgrade, not a description of one.
func documentedUpgradeSuite(t *testing.T, db *gorm.DB, dialect string) {
	t.Helper()
	installLegacySchema(t, db)
	writeLegacyHistory(t, db, "doc-legacy", "doc.kind", StateSucceeded, rollupBase)

	for _, stmt := range documentedUpgradeDDL(t, dialect) {
		require.NoError(t, db.Exec(stmt).Error, "documented statement:\n%s", stmt)
	}
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
