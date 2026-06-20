package flywheel

import (
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"
	sqlite3 "github.com/mattn/go-sqlite3"
	"gorm.io/gorm"
)

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

// wrapSqliteError matches SQLite errors via mattn/go-sqlite3 extended codes.
func wrapSqliteError(err error) (bool, error) {
	var sqliteErr sqlite3.Error
	if !errors.As(err, &sqliteErr) {
		return false, nil
	}

	switch sqliteErr.ExtendedCode {
	case sqlite3.ErrConstraintUnique, sqlite3.ErrConstraintPrimaryKey:
		return true, fmt.Errorf("%w: %s", ErrDuplicateKey, sqliteErr.Error())
	case sqlite3.ErrConstraintForeignKey:
		return true, fmt.Errorf("%w: %s", ErrForeignKey, sqliteErr.Error())
	}

	return true, fmt.Errorf("%w: %s", ErrDatabaseError, sqliteErr.Error())
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
