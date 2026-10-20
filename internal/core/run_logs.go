package core

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/mrz1836/go-foundation/models"
	"gorm.io/datatypes"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// The bounds of one attempt's captured run log (RunLogCapture).
const (
	// DefaultRunLogLines is how many lines an attempt keeps when
	// RunLogCapture.MaxLines is zero.
	DefaultRunLogLines = 1000
	// MaxRunLogLines is the largest RunLogCapture.MaxLines: a larger value keeps
	// this many.
	MaxRunLogLines = 10000
	// DefaultRunLogBytes is how many bytes of lines an attempt keeps when
	// RunLogCapture.MaxBytes is zero.
	DefaultRunLogBytes = 262144
	// MaxRunLogBytes is the largest RunLogCapture.MaxBytes: a larger value keeps
	// this many.
	MaxRunLogBytes = 1048576
)

// runLogWriteBatch is the most lines one insert writes: seven binds a line,
// 3,500 a statement, far below PostgreSQL's 65,535 and SQLite's 32,766.
const runLogWriteBatch = 500

// runLogTruncated is the message of the line an attempt's last write adds when
// the capture dropped lines.
const runLogTruncated = "jobs: run log truncated"

// RunLogCapture configures what a Runner keeps of the lines its workers log
// through Job.Logger (RunnerConfig.CaptureLogs). The zero value captures info
// and above, up to DefaultRunLogLines lines and DefaultRunLogBytes bytes an
// attempt.
type RunLogCapture struct {
	// MinLevel is the lowest level captured; zero is info. It filters only what
	// is captured: the host's handler receives every record its own level
	// allows, captured or not.
	MinLevel slog.Level
	// MaxLines is how many lines one attempt keeps. Zero or negative selects
	// DefaultRunLogLines; above MaxRunLogLines reads MaxRunLogLines.
	MaxLines int
	// MaxBytes is how many bytes of lines one attempt keeps, a line counting its
	// message plus its attributes as JSON. Zero or negative selects
	// DefaultRunLogBytes; above MaxRunLogBytes reads MaxRunLogBytes.
	MaxBytes int
}

// lines resolves MaxLines against its default and its maximum.
func (c RunLogCapture) lines() int {
	switch {
	case c.MaxLines <= 0:
		return DefaultRunLogLines
	case c.MaxLines > MaxRunLogLines:
		return MaxRunLogLines
	}
	return c.MaxLines
}

// bytes resolves MaxBytes against its default and its maximum.
func (c RunLogCapture) bytes() int {
	switch {
	case c.MaxBytes <= 0:
		return DefaultRunLogBytes
	case c.MaxBytes > MaxRunLogBytes:
		return MaxRunLogBytes
	}
	return c.MaxBytes
}

// runLogBuffer is one attempt's captured lines between its worker and the
// database: the resolved bounds, how many lines and bytes it kept and how many
// it dropped, the next sequence number, and the lines not yet written. It is
// safe for concurrent use, so a worker may log from several goroutines.
//
// It keeps a prefix of what the worker logged: once a line would pass either
// bound, that line and every later one are dropped and counted, so the lines
// kept read without a gap, and the truncation line the last write adds stands
// for everything after them.
type runLogBuffer struct {
	runID, jobID       string
	minLevel           slog.Level
	maxLines, maxBytes int
	// clock stamps a record that carries no time of its own.
	clock models.Clock

	// encMu serializes the attribute encoders, which all write into scratch.
	encMu   sync.Mutex
	scratch bytes.Buffer

	mu      sync.Mutex
	nextSeq int64
	lines   int
	bytes   int
	dropped int
	pending []jobRunLogRow
}

// newRunLogBuffer returns an empty buffer for one attempt, bounded by c, with
// ctx's clock.
func newRunLogBuffer(ctx context.Context, c RunLogCapture, jobID, runID string) *runLogBuffer {
	return &runLogBuffer{
		runID: runID, jobID: jobID, minLevel: c.MinLevel,
		maxLines: c.lines(), maxBytes: c.bytes(),
		clock: models.ClockFrom(ctx),
	}
}

// encoder returns a fresh attribute encoder: slog's JSON handler, writing into
// the buffer's scratch, with the record's own level and message left out, so
// what it writes for a record is that record's attributes, nested by group, as
// a JSON object.
func (b *runLogBuffer) encoder() slog.Handler {
	return slog.NewJSONHandler(&b.scratch, &slog.HandlerOptions{ReplaceAttr: dropRecordFields})
}

// dropRecordFields leaves the level and the message a record carries out of
// what the JSON handler writes. encode hands it records with no time, the info
// level, and an empty message, so the check matches those built-in values and
// leaves a worker's own attributes of the same names alone.
func dropRecordFields(groups []string, a slog.Attr) slog.Attr {
	if len(groups) > 0 {
		return a
	}
	switch a.Key {
	case slog.LevelKey:
		if _, builtIn := a.Value.Any().(slog.Level); builtIn {
			return slog.Attr{}
		}
	case slog.MessageKey:
		if a.Value.Kind() == slog.KindString && a.Value.String() == "" {
			return slog.Attr{}
		}
	}
	return a
}

// capture keeps one record the worker logged at or above MinLevel, encoding
// its attributes with enc, which carries those the worker added with With and
// WithGroup.
func (b *runLogBuffer) capture(ctx context.Context, enc slog.Handler, r slog.Record) {
	if b.truncated() {
		b.drop()
		return
	}
	at := r.Time
	if at.IsZero() {
		at = b.clock.Now(ctx)
	}
	b.add(r.Level, r.Message, b.encode(ctx, enc, r), at.UTC())
}

// encode returns r's attributes as a JSON object, "{}" for none.
func (b *runLogBuffer) encode(ctx context.Context, enc slog.Handler, r slog.Record) []byte {
	attrsOnly := slog.NewRecord(time.Time{}, slog.LevelInfo, "", 0)
	r.Attrs(func(a slog.Attr) bool {
		attrsOnly.AddAttrs(a)
		return true
	})
	b.encMu.Lock()
	defer b.encMu.Unlock()
	b.scratch.Reset()
	if err := enc.Handle(ctx, attrsOnly); err != nil {
		return []byte("{}")
	}
	return bytes.Clone(bytes.TrimSuffix(b.scratch.Bytes(), []byte("\n")))
}

// truncated reports whether the buffer has dropped a line, after which it
// keeps none.
func (b *runLogBuffer) truncated() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.dropped > 0
}

// drop counts one line the buffer did not keep.
func (b *runLogBuffer) drop() {
	b.mu.Lock()
	b.dropped++
	b.mu.Unlock()
}

// add keeps one line, numbered next, while both bounds hold and nothing has
// been dropped; otherwise it counts it dropped. A line's size is its message
// plus its attributes as JSON.
func (b *runLogBuffer) add(level slog.Level, msg string, attrs []byte, at time.Time) {
	size := len(msg) + len(attrs)
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.dropped > 0 || b.lines+1 > b.maxLines || b.bytes+size > b.maxBytes {
		b.dropped++
		return
	}
	b.lines++
	b.bytes += size
	b.nextSeq++
	b.pending = append(b.pending, jobRunLogRow{
		RunID: b.runID, Seq: b.nextSeq, JobID: b.jobID, LoggedAt: at,
		Level: int64(level), Message: msg, Attrs: datatypes.JSON(attrs),
	})
}

// flush writes the lines not yet written, in statements of at most
// runLogWriteBatch lines that skip a line already written, so a retry never
// fails on its own earlier half. Each statement commits on its own, with no
// transaction around it: that skip is what makes a half-written flush safe to
// repeat, and leaving out the transaction's BEGIN and COMMIT keeps an attempt's
// last write, which holds its slot, to one round trip. final marks the
// attempt's last write, after
// the finalize and the heartbeat's last write, which adds the truncation line
// when any line was dropped. A failed write keeps its lines for the next one
// and is logged to logger, the Runner's own, never the worker's. A nil buffer
// writes nothing.
func (b *runLogBuffer) flush(ctx context.Context, db *gorm.DB, logger *slog.Logger, final bool) {
	if b == nil {
		return
	}
	b.mu.Lock()
	rows := b.pending
	b.pending = nil
	if final && b.dropped > 0 {
		b.nextSeq++
		rows = append(rows, jobRunLogRow{
			RunID: b.runID, Seq: b.nextSeq, JobID: b.jobID, LoggedAt: b.clock.Now(ctx).UTC(),
			Level: int64(slog.LevelWarn), Message: runLogTruncated,
			Attrs: datatypes.JSON(fmt.Sprintf(`{"dropped_lines":%d}`, b.dropped)),
		})
	}
	b.mu.Unlock()
	if len(rows) == 0 {
		return
	}
	err := db.WithContext(ctx).Session(&gorm.Session{SkipDefaultTransaction: true}).
		Clauses(clause.OnConflict{DoNothing: true}).CreateInBatches(rows, runLogWriteBatch).Error
	if err == nil {
		return
	}
	b.mu.Lock()
	b.pending = append(rows, b.pending...)
	b.mu.Unlock()
	logger.WarnContext(ctx, "jobs: run log write failed",
		slog.String("job_id", b.jobID), slog.String("run_id", b.runID), slog.String("error", err.Error()))
}

// runLogHandler sits beneath the logger a Runner binds onto Job.Logger: it
// passes every record to the host's handler, next, exactly as next would have
// received it, and copies each one at or above MinLevel into the attempt's
// buffer. Its encoder receives the same With and WithGroup calls next does, but
// none made before it was installed, so it carries only what the worker added.
type runLogHandler struct {
	next slog.Handler
	enc  slog.Handler
	buf  *runLogBuffer
}

// newRunLogHandler wraps next, capturing into buf.
func newRunLogHandler(next slog.Handler, buf *runLogBuffer) *runLogHandler {
	return &runLogHandler{next: next, enc: buf.encoder(), buf: buf}
}

// Enabled reports whether a record at level is wanted by either the capture or
// the host's handler.
func (h *runLogHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return level >= h.buf.minLevel || h.next.Enabled(ctx, level)
}

// Handle passes r to the host's handler when that handler is enabled for its
// level, returning its error, and captures r when its level is at or above
// MinLevel.
func (h *runLogHandler) Handle(ctx context.Context, r slog.Record) error {
	var err error
	if h.next.Enabled(ctx, r.Level) {
		err = h.next.Handle(ctx, r)
	}
	if r.Level >= h.buf.minLevel {
		h.buf.capture(ctx, h.enc, r)
	}
	return err
}

// WithAttrs returns a handler whose host handler and encoder both carry attrs.
func (h *runLogHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &runLogHandler{next: h.next.WithAttrs(attrs), enc: h.enc.WithAttrs(attrs), buf: h.buf}
}

// WithGroup returns a handler whose host handler and encoder both open name.
func (h *runLogHandler) WithGroup(name string) slog.Handler {
	return &runLogHandler{next: h.next.WithGroup(name), enc: h.enc.WithGroup(name), buf: h.buf}
}

// RunLogLine is one line a worker logged during an attempt, as
// RunnerConfig.CaptureLogs kept it.
type RunLogLine struct {
	// Seq numbers the attempt's lines from 1, in the order they were captured.
	Seq int `json:"seq"`
	// LoggedAt is when the line was logged, in UTC.
	LoggedAt time.Time `json:"logged_at"`
	// Level is slog's name for the line's level: DEBUG, INFO, WARN, ERROR, or an
	// offset from one of them, such as INFO+2.
	Level string `json:"level"`
	// Message is the line's message.
	Message string `json:"message"`
	// Attrs is the line's attributes as a JSON object, nested by group, as
	// slog's JSON handler encodes them; nil when it has none.
	Attrs json.RawMessage `json:"attrs,omitempty"`
}

// ListRunLogsParams configures ListRunLogs. The zero value reads an attempt's
// first 200 lines at every captured level.
type ListRunLogsParams struct {
	// AfterSeq reads the lines after this sequence number; zero reads from the
	// first. Negative is refused.
	AfterSeq int
	// MinLevel, when set, reads only the lines at or above it; nil reads every
	// captured level.
	MinLevel *slog.Level
	// Limit caps the page. Zero or negative selects 200; above MaxReadLimit
	// reads MaxReadLimit.
	Limit int
}

// ListRunLogs reads the lines one attempt's worker logged, as
// RunnerConfig.CaptureLogs kept them: in sequence order, after p.AfterSeq, at
// or above p.MinLevel when it is set, a page of at most p.Limit at a time, by
// the table's primary key. A run with no captured lines, or one that does not
// exist, reads as an empty slice. An empty runID or a negative AfterSeq is a
// *ValidationError naming run_id or after_seq, refused before any read.
//
// Lines reach the table on the attempt's heartbeat and after its finalize, so a
// running attempt's page grows; page on with AfterSeq set to the last Seq read.
func ListRunLogs(ctx context.Context, db *gorm.DB, runID string, p ListRunLogsParams) ([]RunLogLine, error) {
	if runID == "" {
		return nil, newValidationError("run_id", "is required")
	}
	if p.AfterSeq < 0 {
		return nil, newValidationError("after_seq", "must not be negative")
	}
	q := db.WithContext(ctx).Model(&jobRunLogRow{}).Where("run_id = ? AND seq > ?", runID, p.AfterSeq)
	if p.MinLevel != nil {
		q = q.Where("level >= ?", int64(*p.MinLevel))
	}
	var rows []jobRunLogRow
	if err := q.Order("seq").Limit(readLimit(p.Limit, MaxReadLimit)).Find(&rows).Error; err != nil {
		return nil, fmt.Errorf("flywheel: list run logs: %w", err)
	}
	out := make([]RunLogLine, len(rows))
	for i, r := range rows {
		out[i] = RunLogLine{
			Seq: int(r.Seq), LoggedAt: r.LoggedAt.UTC(), Level: slog.Level(r.Level).String(), Message: r.Message,
			Attrs: runLogAttrs(r.Attrs),
		}
	}
	return out, nil
}

// runLogAttrs returns a stored line's attributes, compacted, or nil when it has
// none.
func runLogAttrs(stored datatypes.JSON) json.RawMessage {
	var compact bytes.Buffer
	if err := json.Compact(&compact, stored); err != nil || compact.String() == "{}" || compact.Len() == 0 {
		return nil
	}
	return compact.Bytes()
}
