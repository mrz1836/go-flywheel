//go:build loadtest

package loadtest

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	flywheel "github.com/mrz1836/go-flywheel"
	"gorm.io/gorm"
)

// schemaSeq disambiguates schema names minted within one process.
//
//nolint:gochecknoglobals // per-process sequence counter for schema uniqueness
var schemaSeq atomic.Uint64

// maxSchemaNameLen is PostgreSQL's identifier limit. A longer name is silently
// truncated by the server, which turns two distinct runs into one schema.
const maxSchemaNameLen = 63

// newSchemaName mints a fresh schema name for one run.
//
// The name is concatenated into DDL — PostgreSQL has no bind parameter for an
// identifier — so it is built to be injection-safe by construction rather than
// escaped after the fact: every component is a base-36 rendering of an integer,
// which can only ever produce [0-9a-z]. There is no code path by which caller
// input reaches it. validSchemaName then re-checks the result, so the guarantee
// is enforced rather than argued.
func newSchemaName() string {
	return "lt_" +
		strconv.FormatInt(time.Now().UnixNano(), 36) + "_" +
		strconv.FormatInt(int64(os.Getpid()), 36) + "_" +
		strconv.FormatUint(schemaSeq.Add(1), 36)
}

// validSchemaName reports whether name is safe to concatenate into DDL: a
// lowercase-leading identifier of [a-z0-9_] within PostgreSQL's length limit.
func validSchemaName(name string) bool {
	if name == "" || len(name) > maxSchemaNameLen {
		return false
	}
	if name[0] < 'a' || name[0] > 'z' {
		return false
	}
	for i := range len(name) {
		c := name[i]
		switch {
		case c >= 'a' && c <= 'z', c >= '0' && c <= '9', c == '_':
		default:
			return false
		}
	}
	return true
}

// withSearchPath appends a search_path runtime parameter to a Postgres DSN so
// every connection on the resulting pool resolves unqualified names to the given
// schema first. It is how a run gets its own copy of the runtime's three tables
// inside a shared database without qualifying a single query.
func withSearchPath(dsn, schema string) string {
	sep := "?"
	if strings.Contains(dsn, "?") {
		sep = "&"
	}
	return dsn + sep + "search_path=" + schema
}

// createSchema creates the run's isolated schema on the admin connection.
func createSchema(ctx context.Context, admin *gorm.DB, schema string) error {
	if !validSchemaName(schema) {
		return fmt.Errorf("loadtest: refusing to create schema %q: %w", schema, ErrInvalidConfig)
	}
	if err := admin.WithContext(ctx).Exec(`CREATE SCHEMA ` + schema).Error; err != nil {
		return fmt.Errorf("loadtest: create schema %s: %w", schema, err)
	}
	return nil
}

// dropSchema removes the run's schema and everything in it.
func dropSchema(ctx context.Context, admin *gorm.DB, schema string) error {
	if !validSchemaName(schema) {
		return fmt.Errorf("loadtest: refusing to drop schema %q: %w", schema, ErrInvalidConfig)
	}
	if err := admin.WithContext(ctx).Exec(`DROP SCHEMA IF EXISTS ` + schema + ` CASCADE`).Error; err != nil {
		return fmt.Errorf("loadtest: drop schema %s: %w", schema, err)
	}
	return nil
}

// installSchema brings up the runtime's tables and the index set the condition
// selects.
//
// Both conditions run the same two steps in the same order — AutoMigrate over
// the runtime's own Models, then the runtime's own IndexSet — and differ only in
// which entries of that set are applied. That is what makes a delta between the
// two conditions attributable to the indexes: there is exactly one variable, and
// neither arm hand-writes a line of DDL.
//
// It deliberately does not call Migrate. Migrate would install the full set
// unconditionally, so the correctness-only condition would be unbuildable
// through it, and its pre-1.0 column reconciliation is a no-op on a schema
// created seconds earlier.
func installSchema(ctx context.Context, db *gorm.DB, cond IndexCondition, storage StorageCondition) error {
	if db.Name() != "postgres" {
		return fmt.Errorf("loadtest: target dialect is %q: %w", db.Name(), ErrUnsupportedDialect)
	}
	if !cond.Valid() {
		return fmt.Errorf("loadtest: unknown index condition %q: %w", cond, ErrInvalidConfig)
	}
	if !storage.Valid() {
		return fmt.Errorf("loadtest: unknown storage condition %q: %w", storage, ErrInvalidConfig)
	}

	if err := db.WithContext(ctx).AutoMigrate(flywheel.Models()...); err != nil {
		return fmt.Errorf("loadtest: automigrate: %w", err)
	}

	// Storage parameters go on between the tables and the indexes, and the order
	// is load-bearing rather than tidy: fillfactor only governs pages written
	// after it is set, so applying it after the seed would leave every seeded
	// page at the old setting and the condition would differ from its label.
	if err := applyStorage(ctx, db, storage); err != nil {
		return err
	}

	set, err := flywheel.IndexSet(db.Name())
	if err != nil {
		return fmt.Errorf("loadtest: index set: %w", err)
	}
	for _, idx := range set {
		if cond == IndexesCorrectness && idx.Kind != flywheel.IndexCorrectness {
			continue
		}
		if err := db.WithContext(ctx).Exec(idx.DDL).Error; err != nil {
			return fmt.Errorf("loadtest: create index %s: %w", idx.Name, err)
		}
	}
	return nil
}

// tunedStorageParameters are the per-table settings the tuned condition applies
// to jobs.
//
// fillfactor leaves free space on each page for an updated tuple version to land
// beside its predecessor. What it does *not* do here is enable HOT updates, and
// the tuning guide must not claim otherwise: a HOT update requires that no
// indexed column change, and `state` appears in the predicate of jobs_ready,
// jobs_unique_active_key, jobs_running_leased, and jobs_state. A predicate
// column is index-relevant, so every state transition is already non-HOT --
// before any of these settings, and before jobs_state was added.
//
// The autovacuum scale factor is the setting with a mechanism that plainly
// applies. At 1M rows the default 0.2 means 200,050 dead tuples accumulate
// before a vacuum triggers; 0.02 makes it 20,050. A table that churns every row
// at least twice reaches either threshold quickly, so the difference is the
// *period* of the saw-tooth, which is most of what the tuning does.
//
//nolint:gochecknoglobals // fixed DDL for the tuned condition
var tunedStorageParameters = []string{
	`ALTER TABLE jobs SET (fillfactor = 80)`,
	`ALTER TABLE jobs SET (autovacuum_vacuum_scale_factor = 0.02, autovacuum_vacuum_cost_limit = 1000)`,
}

// applyStorage applies the storage condition's parameters to the run's tables.
//
// The default condition applies nothing at all rather than explicitly setting
// PostgreSQL's defaults: an ALTER TABLE that writes reloptions produces a
// non-empty pg_class.reloptions, and the report reads that back as evidence of
// what was actually installed. A "default" arm that left a fingerprint would be
// indistinguishable from a tuned one in the artifact.
func applyStorage(ctx context.Context, db *gorm.DB, storage StorageCondition) error {
	if storage != StorageTuned {
		return nil
	}
	for _, ddl := range tunedStorageParameters {
		if err := db.WithContext(ctx).Exec(ddl).Error; err != nil {
			return fmt.Errorf("loadtest: apply storage parameters: %w", err)
		}
	}
	return nil
}

// installedStorage reads back the storage parameters actually present on the
// run's tables, so a report records what was installed rather than what was
// requested.
//
// It follows the precedent the index condition set: the report names the
// installed indexes, not the requested condition, because the two can differ and
// the artifact is what someone reads a year later.
func installedStorage(ctx context.Context, db *gorm.DB) (map[string]string, error) {
	var rows []struct {
		Relname    string
		Reloptions string
	}
	if err := db.WithContext(ctx).Raw(`
		SELECT c.relname, coalesce(array_to_string(c.reloptions, ','), '') AS reloptions
		FROM pg_class c
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = current_schema() AND c.relkind = 'r'
		ORDER BY c.relname`).Scan(&rows).Error; err != nil {
		return nil, fmt.Errorf("loadtest: read storage parameters: %w", err)
	}
	out := make(map[string]string, len(rows))
	for _, r := range rows {
		out[r.Relname] = r.Reloptions
	}
	return out, nil
}

// installedIndexes lists the indexes present in the current schema, so a run can
// report the condition it actually got rather than the one it asked for.
func installedIndexes(ctx context.Context, db *gorm.DB) ([]string, error) {
	var names []string
	if err := db.WithContext(ctx).Raw(
		`SELECT indexname FROM pg_indexes WHERE schemaname = current_schema() ORDER BY indexname`,
	).Scan(&names).Error; err != nil {
		return nil, fmt.Errorf("loadtest: list installed indexes: %w", err)
	}
	return names, nil
}
