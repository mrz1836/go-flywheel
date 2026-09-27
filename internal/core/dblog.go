package core

import (
	"context"
	"errors"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// quietMissing binds db to a session whose logger does not report
// gorm.ErrRecordNotFound, for a single lookup whose caller treats a missing row as
// an expected outcome rather than a failure.
//
// Several internal reads branch on ErrRecordNotFound as ordinary control flow: the
// check-before-write in UpsertPeriodic, FindJob's mapping of a miss to ErrJobNotFound,
// Progress's best-effort read of a parent that may have been pruned, and the driver's
// read-backs of a job that may have been superseded or whose parent may be gone. GORM
// reports a First that finds nothing as an error by default — Config's
// IgnoreRecordNotFoundError is false — so each of those lookups would otherwise emit a
// spurious "record not found" line through the host's logger. The host cannot silence
// that without also disabling the signal for its own queries, because flywheel runs on
// the host-provided *gorm.DB and never owns its logger.
//
// The override is scoped to the returned session, so it applies only to the query it
// is chained onto and leaves the host's connection logger untouched. Real errors and
// slow queries still log, and at Info level the statement still echoes as an ordinary
// query — only the not-found signal these call sites already handle is muted. It never
// changes the error returned to the caller: a First that misses still yields
// ErrRecordNotFound, so every existing branch keeps working.
func quietMissing(db *gorm.DB) *gorm.DB {
	return db.Session(&gorm.Session{Logger: ignoreNotFoundLogger{db.Logger}})
}

// ignoreNotFoundLogger wraps a gorm logger.Interface and drops the
// gorm.ErrRecordNotFound signal before tracing, delegating every other call — Info,
// Warn, Error, and the slow-query and successful-statement traces — to the wrapped
// logger unchanged. It backs quietMissing and carries no state, so the zero value is
// unusable: the embedded Interface must be set.
type ignoreNotFoundLogger struct {
	logger.Interface
}

// LogMode returns a logger at the requested level that still ignores the not-found
// signal, so the wrapper survives GORM cloning the logger for a sub-scope rather than
// unwrapping back to the bare host logger.
func (l ignoreNotFoundLogger) LogMode(level logger.LogLevel) logger.Interface {
	return ignoreNotFoundLogger{l.Interface.LogMode(level)}
}

// Trace forwards to the wrapped logger, first clearing an ErrRecordNotFound — checked
// with errors.Is so a wrapped one is caught too — so the statement is traced as an
// ordinary (rows:0) query instead of an error. This is the same test GORM's own logger
// applies when Config.IgnoreRecordNotFoundError is set; every other error, and every
// slow-query or successful trace, passes through unchanged.
func (l ignoreNotFoundLogger) Trace(
	ctx context.Context, begin time.Time, fc func() (sql string, rowsAffected int64), err error,
) {
	if errors.Is(err, gorm.ErrRecordNotFound) {
		err = nil
	}
	l.Interface.Trace(ctx, begin, fc, err)
}
