package core

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"gorm.io/gorm"
)

// ErrSchemaOutdated is returned when the database lacks a table or column this
// binary writes — the schema is older than the code. A Runner returns it from
// Run (and so a Node from Run) before claiming anything, and a Scheduler with
// stats rollups enabled returns it from Run before its first pass. The wrapping
// error names every missing table and column.
//
// The fix is to migrate before deploying: run Migrate (the `flywheel migrate`
// command) on a library-owned install, or apply the upgrade DDL from
// docs/INTEGRATING.md on a host-owned one. The order is the contract — a schema
// newer than the binary is always safe, because every column the runtime adds
// is defaulted or nullable and an older binary simply never writes it.
var ErrSchemaOutdated = errors.New("flywheel: database schema is older than this binary")

// SchemaDrift is one table or column the runtime's models declare that a
// database lacks.
type SchemaDrift struct {
	// Table is the runtime table the gap is in.
	Table string `json:"table"`
	// Column is the missing column, or empty when the whole table is missing.
	Column string `json:"column,omitempty"`
}

// String renders the gap as table or table.column.
func (d SchemaDrift) String() string {
	if d.Column == "" {
		return d.Table + " (table)"
	}
	return d.Table + "." + d.Column
}

// InspectSchema reports every table and column the runtime's models declare that
// db lacks. It reads and writes nothing else: it is the column-level counterpart
// of InspectIndexes, the parity check a host-owned install runs in CI to assert
// its migrations keep up with the runtime's Models.
//
// An empty slice means every table in Models is present with every column the
// runtime reads or writes. It reports gaps only — a host's own extra columns on
// the runtime's tables, or a column whose type differs, are not drift: the
// runtime never reads the first, and a type difference is the host's schema to
// own.
func InspectSchema(ctx context.Context, db *gorm.DB) ([]SchemaDrift, error) {
	if db == nil {
		return nil, fmt.Errorf("flywheel: InspectSchema: db is nil")
	}
	drift, err := inspectSchema(ctx, db, Models()...)
	if err != nil {
		return nil, fmt.Errorf("flywheel: InspectSchema: %w", err)
	}
	return drift, nil
}

// inspectSchema compares the given models' declared columns against what db's
// tables hold, returning one SchemaDrift per missing table or column, in model
// and then field order. It is the shared core of InspectSchema and the runtime's
// startup probe, which checks only the tables the runtime itself writes.
func inspectSchema(ctx context.Context, db *gorm.DB, models ...any) ([]SchemaDrift, error) {
	var drift []SchemaDrift
	for _, model := range models {
		stmt := &gorm.Statement{DB: db}
		if err := stmt.Parse(model); err != nil {
			return nil, fmt.Errorf("parse model %T: %w", model, err)
		}
		table := stmt.Schema.Table
		have, exists, err := tableColumns(ctx, db, table)
		if err != nil {
			return nil, err
		}
		if !exists {
			drift = append(drift, SchemaDrift{Table: table})
			continue
		}
		for _, column := range stmt.Schema.DBNames {
			if !have[column] {
				drift = append(drift, SchemaDrift{Table: table, Column: column})
			}
		}
	}
	return drift, nil
}

// tableColumns reads table's column names from a zero-row SELECT, which both
// dialects answer with the full column list at the cost of one round trip and no
// catalog query. exists is false when the table is absent; any other failure is
// returned as an error.
func tableColumns(ctx context.Context, db *gorm.DB, table string) (map[string]bool, bool, error) {
	rows, err := db.WithContext(ctx).Raw(`SELECT * FROM ` + quoteIndexIdent(table) + ` WHERE 1 = 0`).Rows()
	if err != nil {
		// The SELECT fails for a missing table and for a dead connection alike;
		// the catalog tells the two apart. GORM's Migrator.HasTable is no use
		// here: it swallows its own error and answers false, which would turn an
		// outage into a "table missing" verdict.
		exists, catalogErr := tableExists(ctx, db, table)
		if catalogErr != nil {
			return nil, false, catalogErr
		}
		if !exists {
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("read columns of %s: %w", table, err)
	}
	defer func() { _ = rows.Close() }()
	columns, err := rows.Columns()
	if err != nil {
		return nil, false, fmt.Errorf("read columns of %s: %w", table, err)
	}
	if err := rows.Err(); err != nil {
		return nil, false, fmt.Errorf("read columns of %s: %w", table, err)
	}
	have := make(map[string]bool, len(columns))
	for _, c := range columns {
		have[strings.ToLower(c)] = true
	}
	return have, true, nil
}

// tableExists asks the catalog whether table exists, returning the catalog
// read's own error rather than a guess.
func tableExists(ctx context.Context, db *gorm.DB, table string) (bool, error) {
	var query string
	switch db.Name() {
	case "postgres":
		query = `SELECT count(*) FROM information_schema.tables WHERE table_schema = current_schema() AND table_name = ?`
	case "sqlite":
		query = `SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name = ?`
	default:
		return false, fmt.Errorf("flywheel: %w: %q: schema inspection requires postgres or sqlite",
			ErrUnsupportedDialect, db.Name())
	}
	var n int64
	if err := db.WithContext(ctx).Raw(query, table).Scan(&n).Error; err != nil {
		return false, fmt.Errorf("look up table %s: %w", table, err)
	}
	return n > 0, nil
}

// schemaOutdatedError renders a non-empty drift list as ErrSchemaOutdated,
// naming every gap and the fix, so the operator who reads it in a crash loop
// needs nothing else to act.
func schemaOutdatedError(drift []SchemaDrift) error {
	names := make([]string, len(drift))
	for i, d := range drift {
		names[i] = d.String()
	}
	return fmt.Errorf(
		"%w: missing %s; migrate before deploying this binary: run Migrate (`flywheel migrate`) "+
			"on a library-owned install, or apply the upgrade DDL in docs/INTEGRATING.md on a host-owned one",
		ErrSchemaOutdated, strings.Join(names, ", "),
	)
}

// probeSchema is the runtime's startup check that db carries every column the
// given models declare. It distinguishes the two ways the check can fail, because
// they want opposite treatment:
//
//   - A definite gap — a missing table or column — returns ErrSchemaOutdated.
//     Proceeding would fail every write the runtime makes; the lease sweep would
//     then reclaim the same jobs forever. Failing fast is the only useful answer.
//   - A probe that could not run (the database is unreachable at startup) returns
//     checked == false and no error. A Runner has always survived a database that
//     is down when it starts — its poll backoff is built for exactly that — and a
//     schema check must not turn an outage into a crash. The caller probes again
//     on its next start.
func probeSchema(ctx context.Context, db *gorm.DB, models ...any) (bool, error) {
	drift, err := inspectSchema(ctx, db, models...)
	if err != nil {
		return false, nil //nolint:nilerr // an unreachable database is not a schema verdict; see the doc comment
	}
	if len(drift) > 0 {
		return true, schemaOutdatedError(drift)
	}
	return true, nil
}
