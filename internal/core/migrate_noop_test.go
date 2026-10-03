package core

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// ddlRecorder is a GORM logger that keeps every schema-changing statement a
// session runs, so a test can assert what DDL a Migrate issued.
type ddlRecorder struct {
	logger.Interface

	mu  sync.Mutex
	ddl []string
}

// Trace records the statement when it is DDL.
func (r *ddlRecorder) Trace(_ context.Context, _ time.Time, fc func() (string, int64), _ error) {
	sql, _ := fc()
	verb := strings.ToUpper(strings.Fields(strings.TrimSpace(sql) + " x")[0])
	switch verb {
	case "ALTER", "CREATE", "DROP":
		r.mu.Lock()
		r.ddl = append(r.ddl, sql)
		r.mu.Unlock()
	}
}

// statements returns the DDL recorded so far.
func (r *ddlRecorder) statements() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.ddl...)
}

// assertMigrateOfACurrentSchemaRunsNoDDL migrates db, already current, once more
// through a recording session and asserts it changed nothing. Migrate runs on
// every `flywheel serve` start, so any DDL here is a lock taken on a live table
// for no effect: GORM's PostgreSQL migrator, for one, re-sets every quoted jsonb
// default on each run (an ACCESS EXCLUSIVE lock on jobs), and an unconditional
// storage ALTER waits on autovacuum.
func assertMigrateOfACurrentSchemaRunsNoDDL(t *testing.T, db *gorm.DB) {
	t.Helper()
	rec := &ddlRecorder{Interface: logger.Default.LogMode(logger.Silent)}
	session := db.Session(&gorm.Session{Logger: rec})
	require.NoError(t, MigrateWithOptions(session, MigrateOpts{Concurrently: true}))
	assert.Empty(t, rec.statements(), "a Migrate of an up-to-date schema issues no DDL")
}

// TestMigrateOfACurrentSchemaRunsNoDDL is the SQLite half; the PostgreSQL half,
// where the spurious ALTERs were, is in the integration suite.
func TestMigrateOfACurrentSchemaRunsNoDDL(t *testing.T) {
	t.Parallel()
	db := newBareSQLite(t)
	require.NoError(t, Migrate(db))
	assertMigrateOfACurrentSchemaRunsNoDDL(t, db)
}
