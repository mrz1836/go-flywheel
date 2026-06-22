package flywheel

import (
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"
	"gorm.io/gorm"
)

// SQLite extended result codes for the constraint violations the runtime
// classifies. These are stable SQLite ABI values (from sqlite3.h), matched
// structurally so the library depends on no specific SQLite driver: the CLI
// uses the pure-Go modernc driver, while an embedding host (e.g. bedrock) may
// use another — importing a concrete driver here would register a database/sql
// driver and could collide with the host's.
const (
	sqliteConstraintUnique     = 2067 // SQLITE_CONSTRAINT_UNIQUE
	sqliteConstraintPrimaryKey = 1555 // SQLITE_CONSTRAINT_PRIMARYKEY
	sqliteConstraintForeignKey = 787  // SQLITE_CONSTRAINT_FOREIGNKEY
)

// sqliteCoder is implemented by a SQLite driver error that exposes its result
// code (modernc/glebarez errors do). Matching this interface keeps the runtime
// driver-agnostic; an error that does not implement it (e.g. the CGO mattn type)
// degrades to the message-based fallback in wrapByMessage.
type sqliteCoder interface{ Code() int }

// Sentinel errors for database operations. Use errors.Is() to check for these
// in callers. These are self-contained: the runtime classifies driver errors
// through WrapDBError without importing any external foundation package.
var (
	// ErrNotFound indicates the requested record was not found.
	ErrNotFound = errors.New("record not found")

	// ErrDuplicateKey indicates a unique constraint violation occurred. The
	// runtime relies on this to make unique_key enqueue idempotent.
	ErrDuplicateKey = errors.New("duplicate key violation")

	// ErrForeignKey indicates a foreign key constraint violation occurred.
	ErrForeignKey = errors.New("foreign key constraint violated")

	// ErrDatabaseError indicates a general database operation failure.
	ErrDatabaseError = errors.New("database operation failed")
)

// PostgreSQL SQLSTATE codes, stable across pgx driver versions.
const (
	pgCodeUniqueViolation     = "23505"
	pgCodeForeignKeyViolation = "23503"
)

// WrapDBError normalizes a raw driver error into one of the package sentinels so
// callers can branch on errors.Is(err, ErrDuplicateKey) regardless of dialect.
// It returns nil for a nil error.
func WrapDBError(err error) error {
	if err == nil {
		return nil
	}

	if errors.Is(err, gorm.ErrRecordNotFound) {
		return ErrNotFound
	}

	if ok, wrapped := wrapPgError(err); ok {
		return wrapped
	}

	if ok, wrapped := wrapSqliteError(err); ok {
		return wrapped
	}

	return wrapByMessage(err)
}

// wrapPgError matches PostgreSQL (pgx) errors via SQLSTATE codes, which are
// stable across driver versions.
func wrapPgError(err error) (bool, error) {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		return false, nil
	}

	switch pgErr.Code {
	case pgCodeUniqueViolation:
		return true, fmt.Errorf("%w: %s", ErrDuplicateKey, pgErr.Message)
	case pgCodeForeignKeyViolation:
		return true, fmt.Errorf("%w: %s", ErrForeignKey, pgErr.Message)
	}

	return true, fmt.Errorf("%w: %s", ErrDatabaseError, pgErr.Message)
}

// wrapSqliteError matches a SQLite driver error by its extended result code.
// modernc enables extended result codes on every connection, so Code() returns
// the specific constraint variant (UNIQUE/PRIMARYKEY/FOREIGNKEY) rather than the
// generic SQLITE_CONSTRAINT. An error that exposes no Code() falls through to the
// message-based fallback.
func wrapSqliteError(err error) (bool, error) {
	var coder sqliteCoder
	if !errors.As(err, &coder) {
		return false, nil
	}

	switch coder.Code() {
	case sqliteConstraintUnique, sqliteConstraintPrimaryKey:
		return true, fmt.Errorf("%w: %s", ErrDuplicateKey, err.Error())
	case sqliteConstraintForeignKey:
		return true, fmt.Errorf("%w: %s", ErrForeignKey, err.Error())
	}

	return true, fmt.Errorf("%w: %s", ErrDatabaseError, err.Error())
}

// wrapByMessage is a defensive fallback for unwrapped driver errors. The
// slog.Warn highlights any driver-format change so we can promote it to a
// typed match.
func wrapByMessage(err error) error {
	errStr := err.Error()
	if strings.Contains(errStr, "UNIQUE constraint failed") ||
		strings.Contains(errStr, "duplicate key value violates unique constraint") {
		slog.Warn("flywheel.WrapDBError: matched unique violation by string; driver error not unwrapped", "err", errStr)
		return fmt.Errorf("%w: %s", ErrDuplicateKey, errStr)
	}

	if strings.Contains(errStr, "FOREIGN KEY constraint failed") ||
		strings.Contains(errStr, "violates foreign key constraint") {
		slog.Warn("flywheel.WrapDBError: matched FK violation by string; driver error not unwrapped", "err", errStr)
		return fmt.Errorf("%w: %s", ErrForeignKey, errStr)
	}

	return fmt.Errorf("%w: %s", ErrDatabaseError, errStr)
}
