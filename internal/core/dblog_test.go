package core

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// captureLogger is a logger.Interface that records the error passed to every Trace
// call, so a test can assert what the query logger was asked to report. LogMode
// records the level and returns the same recorder, so the wrapper's LogMode can be
// checked end to end. Everything else delegates to the embedded no-op logger.
type captureLogger struct {
	logger.Interface
	mu           sync.Mutex
	traced       []error
	logModeLevel logger.LogLevel
	logModeCalls int
}

var _ logger.Interface = (*captureLogger)(nil)

func newCaptureLogger() *captureLogger {
	return &captureLogger{Interface: logger.Discard}
}

func (c *captureLogger) LogMode(level logger.LogLevel) logger.Interface {
	c.mu.Lock()
	c.logModeLevel = level
	c.logModeCalls++
	c.mu.Unlock()
	return c
}

func (c *captureLogger) Trace(
	ctx context.Context, begin time.Time, fc func() (string, int64), err error,
) {
	c.mu.Lock()
	c.traced = append(c.traced, err)
	c.mu.Unlock()
	c.Interface.Trace(ctx, begin, fc, err)
}

func (c *captureLogger) reset() {
	c.mu.Lock()
	c.traced = nil
	c.mu.Unlock()
}

// lastTraced returns the error from the most recent Trace call, and false when Trace
// has not been called since the last reset.
func (c *captureLogger) lastTraced() (error, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.traced) == 0 {
		return nil, false
	}
	return c.traced[len(c.traced)-1], true
}

// sawNotFound reports whether any Trace call since the last reset carried
// gorm.ErrRecordNotFound.
func (c *captureLogger) sawNotFound() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, e := range c.traced {
		if errors.Is(e, gorm.ErrRecordNotFound) {
			return true
		}
	}
	return false
}

func noopFC() (string, int64) { return "SELECT 1", 0 }

// TestIgnoreNotFoundLoggerTrace verifies Trace clears an ErrRecordNotFound — bare or
// wrapped — before delegating, and forwards every other error, and a nil, unchanged.
func TestIgnoreNotFoundLoggerTrace(t *testing.T) {
	t.Parallel()

	realErr := errors.New("boom")
	cases := []struct {
		name string
		in   error
		want error // the error the wrapped logger should receive
	}{
		{name: "nil passes through", in: nil, want: nil},
		{name: "bare not-found cleared", in: gorm.ErrRecordNotFound, want: nil},
		{name: "wrapped not-found cleared", in: fmt.Errorf("load: %w", gorm.ErrRecordNotFound), want: nil},
		{name: "real error passes through", in: realErr, want: realErr},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rec := newCaptureLogger()
			ignoreNotFoundLogger{rec}.Trace(context.Background(), time.Now(), noopFC, tc.in)

			got, ok := rec.lastTraced()
			require.True(t, ok, "Trace must always delegate to the wrapped logger")
			assert.Equal(t, tc.want, got)
		})
	}
}

// TestIgnoreNotFoundLoggerLogMode verifies LogMode applies the level to the wrapped
// logger and stays wrapped, so the not-found suppression survives GORM cloning the
// logger for a sub-scope.
func TestIgnoreNotFoundLoggerLogMode(t *testing.T) {
	t.Parallel()
	rec := newCaptureLogger()

	got := ignoreNotFoundLogger{rec}.LogMode(logger.Info)

	wrapped, ok := got.(ignoreNotFoundLogger)
	require.True(t, ok, "LogMode must return an ignoreNotFoundLogger, not unwrap to the bare logger")
	assert.Equal(t, 1, rec.logModeCalls, "LogMode must be applied to the wrapped logger")
	assert.Equal(t, logger.Info, rec.logModeLevel)

	// The re-leveled logger still suppresses the not-found signal.
	wrapped.Trace(context.Background(), time.Now(), noopFC, gorm.ErrRecordNotFound)
	got2, ok := rec.lastTraced()
	require.True(t, ok)
	assert.NoError(t, got2)
}

// TestQuietMissingReturnsScopedWrapper verifies quietMissing wraps the session logger
// and leaves the host connection's own logger untouched.
func TestQuietMissingReturnsScopedWrapper(t *testing.T) {
	t.Parallel()
	rec := newCaptureLogger()
	db := newDB(t)
	db.Logger = rec

	scoped := quietMissing(db)

	_, ok := scoped.Logger.(ignoreNotFoundLogger)
	assert.True(t, ok, "the scoped session must log through the wrapper")
	assert.Same(t, rec, db.Logger, "the host connection's logger must not be mutated")
}

// TestQuietMissingSuppressesNotFound verifies a First that misses through quietMissing
// still returns ErrRecordNotFound to the caller but emits no not-found log line, while
// the identical query without quietMissing does log it — so the wrapper is exactly what
// changes the logging.
func TestQuietMissingSuppressesNotFound(t *testing.T) {
	t.Parallel()
	rec := newCaptureLogger()
	db := newDB(t)
	db.Logger = rec
	ctx := context.Background()

	// Baseline: the bare connection logs the miss.
	rec.reset()
	var row jobRow
	err := db.WithContext(ctx).Where("id = ?", "missing").First(&row).Error
	require.ErrorIs(t, err, gorm.ErrRecordNotFound)
	assert.True(t, rec.sawNotFound(), "baseline: GORM reports the miss without quietMissing")

	// Through quietMissing: same returned error, but nothing logged as not-found.
	rec.reset()
	err = quietMissing(db).WithContext(ctx).Where("id = ?", "missing").First(&row).Error
	require.ErrorIs(t, err, gorm.ErrRecordNotFound, "quietMissing must not change the error the caller sees")
	assert.False(t, rec.sawNotFound(), "quietMissing must suppress the not-found log line")

	last, ok := rec.lastTraced()
	require.True(t, ok, "the query is still traced")
	assert.NoError(t, last, "the miss reaches the logger as an ordinary statement, not an error")
}

// TestQuietMissingStillLogsRealError verifies quietMissing suppresses only the
// not-found signal: a genuine database error on the scoped session still reaches the
// logger unchanged.
func TestQuietMissingStillLogsRealError(t *testing.T) {
	t.Parallel()
	rec := newCaptureLogger()
	db := newDB(t)
	db.Logger = rec
	ctx := context.Background()

	rec.reset()
	var dest int
	err := quietMissing(db).WithContext(ctx).Raw("SELECT 1 FROM definitely_missing_table").Scan(&dest).Error
	require.Error(t, err, "a query against a missing table must fail")

	last, ok := rec.lastTraced()
	require.True(t, ok)
	require.Error(t, last, "a real error must still be logged")
	assert.False(t, errors.Is(last, gorm.ErrRecordNotFound))
}

// TestIgnoreNotFoundLoggerParamsFilter verifies the wrapper forwards to the wrapped
// logger's parameter filter when it has one, and returns the params unchanged when it
// does not, matching what GORM does for a logger without a filter.
func TestIgnoreNotFoundLoggerParamsFilter(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	const sql = "SELECT * FROM jobs WHERE id = ?"

	t.Run("parameterized host logger withholds params", func(t *testing.T) {
		t.Parallel()
		host := logger.NewSlogLogger(slog.New(slog.DiscardHandler), logger.Config{ParameterizedQueries: true})

		gotSQL, gotVars := ignoreNotFoundLogger{host}.ParamsFilter(ctx, sql, "secret")

		assert.Equal(t, sql, gotSQL)
		assert.Nil(t, gotVars, "the host's filter must be honored through the wrapper")
	})

	t.Run("host logger without a filter keeps params", func(t *testing.T) {
		t.Parallel()

		gotSQL, gotVars := ignoreNotFoundLogger{newCaptureLogger()}.ParamsFilter(ctx, sql, "kept")

		assert.Equal(t, sql, gotSQL)
		assert.Equal(t, []any{"kept"}, gotVars)
	})
}

// TestQuietMissingHonorsParameterizedQueries verifies a lookup traced through
// quietMissing keeps its placeholder and never writes the bound value, when the host
// logger is configured with ParameterizedQueries. GORM finds the filter by a type
// assertion on the session logger, so this fails if the wrapper hides it.
func TestQuietMissingHonorsParameterizedQueries(t *testing.T) {
	t.Parallel()
	const sentinel = "quiet-missing-sentinel-6f1d"
	var buf bytes.Buffer
	db := newDB(t)
	db.Logger = logger.NewSlogLogger(
		slog.New(slog.NewJSONHandler(&buf, nil)),
		logger.Config{LogLevel: logger.Info, ParameterizedQueries: true},
	)

	var row jobRow
	err := quietMissing(db).WithContext(context.Background()).Where("id = ?", sentinel).First(&row).Error
	require.ErrorIs(t, err, gorm.ErrRecordNotFound)

	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	require.Len(t, lines, 1, "the lookup must be traced exactly once: %q", buf.String())
	var record struct {
		Msg   string `json:"msg"`
		Trace struct {
			SQL string `json:"sql"`
		} `json:"trace"`
	}
	require.NoError(t, json.Unmarshal([]byte(lines[0]), &record))
	assert.Equal(t, "SQL executed", record.Msg)
	assert.Contains(t, record.Trace.SQL, "id = ?", "the traced statement must keep its placeholder")
	assert.NotContains(t, buf.String(), sentinel, "the bound value must never reach the log")
}
