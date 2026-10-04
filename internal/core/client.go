package core

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"time"

	"github.com/mrz1836/go-foundation/ctxutil"
	"github.com/mrz1836/go-foundation/models"
	"gorm.io/datatypes"
	"gorm.io/gorm"
)

// Default values applied to an Insert without an explicit choice.
const (
	defaultQueue       = "default"
	defaultPriority    = 100
	defaultMaxAttempts = 25
)

// Client is the producer-side handle for enqueuing jobs. It is built lazily by
// the service container from the write database connection.
type Client struct {
	writeDB *gorm.DB
}

// NewClient returns a Client that enqueues onto writeDB.
func NewClient(writeDB *gorm.DB) *Client {
	return &Client{writeDB: writeDB}
}

// WriteDB returns the client's write connection. The Scheduler reuses it.
func (c *Client) WriteDB() *gorm.DB {
	return c.writeDB
}

// kindNamer is implemented by an args value that names its job kind.
type kindNamer interface {
	Kind() string
}

// Insert enqueues one job with typed args. The job kind is read from the args
// value, which must implement Kind() string.
//
// When opts.Tx is set the row is written on that transaction, so enqueuing is
// atomic with the caller's own writes — the outbox pattern, without an outbox
// table. A unique-key collision returns an *AlreadyEnqueuedError naming the job
// that holds the key (it unwraps to ErrAlreadyEnqueued) rather than a raw driver
// error, so a caller can treat "already submitted" as a normal outcome — or join
// the holder — without matching on driver-specific constraint-violation text. A
// collision on opts.Tx leaves the transaction usable.
func Insert[A Args](ctx context.Context, c *Client, args A, opts InsertOpts) (string, error) {
	namer, ok := any(args).(kindNamer)
	if !ok {
		return "", ErrMissingKind
	}
	payload, err := json.Marshal(args)
	if err != nil {
		return "", fmt.Errorf("jobs: marshal args: %w", err)
	}
	return c.enqueue(ctx, namer.Kind(), payload, opts)
}

// Enqueue writes one available job of an arbitrary kind from a pre-marshaled
// JSON payload, with no registered worker required. It is the host seed seam:
// fixtures and inspection hosts create real jobs through the same insert core as
// Insert — honoring opts (UniqueKey/Queue/Priority/…) and the row's lifecycle
// defaults — without touching flywheel's unexported row structs. A unique-key
// collision returns an *AlreadyEnqueuedError naming the job that holds the key,
// exactly as Insert does.
func Enqueue(ctx context.Context, c *Client, kind string, args []byte, opts InsertOpts) (string, error) {
	return c.enqueue(ctx, kind, args, opts)
}

// handle returns the handle one insert runs on: the caller's transaction when
// opts.Tx is set, otherwise the client's write connection.
func (c *Client) handle(opts InsertOpts) *gorm.DB {
	if opts.Tx != nil {
		return opts.Tx
	}
	return c.writeDB
}

// enqueue is insert plus what Insert and Enqueue owe a caller on a collision: an
// *AlreadyEnqueuedError naming the holder, read on the handle the insert ran on.
// On a caller's transaction that is required twice over: a holder written earlier
// in it is visible only there, and on SQLite the write handle would wait on the
// transaction's own lock.
func (c *Client) enqueue(ctx context.Context, kind string, payload []byte, opts InsertOpts) (string, error) {
	id, collided, err := c.insert(ctx, kind, payload, opts)
	if err != nil {
		return "", err
	}
	if collided {
		return "", alreadyEnqueued(ctx, c.handle(opts), opts.UniqueKey, opts.UniqueActiveKey)
	}
	return id, nil
}

// insert is the non-generic enqueue core shared by Insert, Enqueue, and the
// Scheduler. It writes one jobs row, honoring opts, and reports a unique-key
// collision as collided — never as an error — so each caller decides what a
// collision is worth: Insert and Enqueue name the job holding the key, while the
// Scheduler's bucketed tick treats it as the no-op it is and reads nothing more.
//
// On opts.Tx a keyed row goes through the conflict-insert primitive InsertMany
// uses — ON CONFLICT DO NOTHING, then the same-handle read-back of its own row —
// because a failed INSERT aborts a PostgreSQL transaction, and a collision must
// leave the caller's transaction usable. It takes no savepoint: one would cost
// two more statements on every insert, and GORM opens none on a transaction
// handle. Every other insert is a plain INSERT whose duplicate-key error is the
// collision signal, so its success path costs the insert alone.
func (c *Client) insert(
	ctx context.Context, kind string, payload []byte, opts InsertOpts,
) (id string, collided bool, err error) {
	db := c.handle(opts)
	row := buildRow(ctx, kind, payload, opts)

	if opts.Tx != nil && (row.UniqueKey != nil || row.UniqueActiveKey != nil) {
		rows := []jobRow{row}
		if insertErr := insertSkippingConflicts(ctx, db, rows); insertErr != nil {
			return "", false, fmt.Errorf("jobs: insert: %w", models.WrapDBError(insertErr))
		}
		landed, readErr := landedRows(ctx, db, rows)
		if readErr != nil {
			return "", false, fmt.Errorf("jobs: insert: read back the row: %w", readErr)
		}
		if _, ok := landed[row.ID]; !ok {
			return "", true, nil
		}
		return row.ID, false, nil
	}

	if createErr := db.WithContext(ctx).Create(&row).Error; createErr != nil {
		if isDuplicateKey(createErr) {
			return "", true, nil
		}
		return "", false, fmt.Errorf("jobs: insert: %w", models.WrapDBError(createErr))
	}
	return row.ID, false, nil
}

// isDuplicateKey reports whether err is a unique violation: the driver's own
// error, which models.WrapDBError classifies, or gorm.ErrDuplicatedKey, which
// replaces it when the host opened its database with gorm.Config.TranslateError.
// Every collision mapping in the runtime goes through it, so a host's GORM
// configuration cannot turn a collision into a database failure.
func isDuplicateKey(err error) bool {
	return errors.Is(err, gorm.ErrDuplicatedKey) || errors.Is(models.WrapDBError(err), models.ErrDuplicateKey)
}

// keyHolders maps each held key to its holder's id, one map per unique index.
type keyHolders struct {
	byKey       map[string]string
	byActiveKey map[string]string
}

// readKeyHolders reads, on db, the jobs holding any of keys or activeKeys. Its
// predicate mirrors the two partial unique indexes exactly — unique_key with no
// state or deleted_at condition, unique_active_key only in a live state — and it
// reads Unscoped, because neither index excludes a soft-deleted row. An empty
// list drops its half of the predicate; with both empty it reads nothing.
//
// The read must run on the handle the colliding write ran on. On a caller's
// transaction that is the only handle that sees a holder written earlier in it,
// and on SQLite a read on the write connection would wait for the transaction's
// own lock.
func readKeyHolders(ctx context.Context, db *gorm.DB, keys, activeKeys []string) (keyHolders, error) {
	h := keyHolders{byKey: map[string]string{}, byActiveKey: map[string]string{}}
	keySet, activeSet := keySetOf(keys), keySetOf(activeKeys)
	q := keyHoldersQuery(db.WithContext(ctx),
		slices.Sorted(maps.Keys(keySet)), slices.Sorted(maps.Keys(activeSet)))
	if q == nil {
		return h, nil
	}
	live := nonTerminalStateStrings()

	var rows []jobRow
	if err := q.Find(&rows).Error; err != nil {
		return keyHolders{}, err
	}
	for _, r := range rows {
		if r.UniqueKey != nil && keySet[*r.UniqueKey] {
			h.byKey[*r.UniqueKey] = r.ID
		}
		// A row the unique_key half matched may also carry a UniqueActiveKey it no
		// longer holds; it counts only in a live state, as the index's predicate
		// says — membership in the live states, not "not terminal", so a state the
		// index does not name never counts.
		if r.UniqueActiveKey != nil && activeSet[*r.UniqueActiveKey] && slices.Contains(live, r.State) {
			h.byActiveKey[*r.UniqueActiveKey] = r.ID
		}
	}
	return h, nil
}

// keyHoldersQuery builds readKeyHolders' read on db for the given (deduplicated,
// non-empty) keys, or returns nil when both lists are empty. The live states are
// SQL literals (liveStatesSQL), not a bound list, so the active-key half can be
// served by jobs_unique_active_key: each half is an index lookup, OR-ed when
// both are present, rather than a scan of jobs.
func keyHoldersQuery(db *gorm.DB, keys, activeKeys []string) *gorm.DB {
	activeHeld := "unique_active_key IN ? AND state IN " + liveStatesSQL()
	q := db.Unscoped().Model(&jobRow{}).Select("id", "unique_key", "unique_active_key", "state")
	switch {
	case len(keys) > 0 && len(activeKeys) > 0:
		return q.Where("unique_key IN ? OR ("+activeHeld+")", keys, activeKeys)
	case len(keys) > 0:
		return q.Where("unique_key IN ?", keys)
	case len(activeKeys) > 0:
		return q.Where(activeHeld, activeKeys)
	default:
		return nil
	}
}

// keySetOf returns the non-empty keys as a set.
func keySetOf(keys []string) map[string]bool {
	set := make(map[string]bool, len(keys))
	for _, k := range keys {
		if k != "" {
			set[k] = true
		}
	}
	return set
}

// holderOf returns the job holding uniqueKey, else the live job holding
// activeKey, and the key it holds. When the read found neither held, it returns
// an empty id and activeKey if one was set, else uniqueKey: a key the collision
// saw held but the read did not has freed since, and that is almost always a
// live holder reaching a terminal state — a UniqueKey frees only when retention
// hard-deletes its holder, so a read that finds no unique_key holder is
// authoritative about it.
func (h keyHolders) holderOf(uniqueKey, activeKey string) (id, key string) {
	if uniqueKey != "" {
		if id = h.byKey[uniqueKey]; id != "" {
			return id, uniqueKey
		}
	}
	if activeKey != "" {
		if id = h.byActiveKey[activeKey]; id != "" {
			return id, activeKey
		}
		return "", activeKey
	}
	return "", uniqueKey
}

// alreadyEnqueued builds the collision error for one insert, naming the holder
// when the read on db finds one. A failed read yields the error with ExistingID
// empty and the first key set, UniqueKey before UniqueActiveKey, since nothing
// says which one collided: the collision already happened, and a lookup failure
// must not turn it into a different error.
func alreadyEnqueued(ctx context.Context, db *gorm.DB, uniqueKey, activeKey string) *AlreadyEnqueuedError {
	var keys, activeKeys []string
	if uniqueKey != "" {
		keys = []string{uniqueKey}
	}
	if activeKey != "" {
		activeKeys = []string{activeKey}
	}
	h, err := readKeyHolders(ctx, db, keys, activeKeys)
	if err != nil {
		return &AlreadyEnqueuedError{Key: orString(uniqueKey, activeKey)}
	}
	id, key := h.holderOf(uniqueKey, activeKey)
	return &AlreadyEnqueuedError{ExistingID: id, Key: key}
}

// buildRow constructs the jobs row for one enqueue from kind, payload, and opts:
// it reads the context clock and request id, applies the producer defaults, and
// sets the four optional columns (schedule, unique key, unique-active key,
// timeout).
//
// It is a free function and never reads opts.Tx — picking the handle is the
// caller's job — so the single insert and the bulk InsertMany path share one row
// builder and land byte-identical rows. BeforeCreate re-defaults any field left
// zero here, so the two paths cannot drift on a producer default either.
func buildRow(ctx context.Context, kind string, payload []byte, opts InsertOpts) jobRow {
	now := models.ClockFrom(ctx).Now(ctx)

	requestID := opts.RequestID
	if requestID == "" {
		requestID = ctxutil.RequestIDFrom(ctx)
	}

	row := jobRow{
		ID:            models.NewID(),
		CreatedAt:     now,
		UpdatedAt:     now,
		Metadata:      datatypes.JSON(ctxutil.RequestIDToMetadata(nil, requestID)),
		Kind:          kind,
		Queue:         orString(opts.Queue, defaultQueue),
		Args:          datatypes.JSON(payload),
		Priority:      orInt(opts.Priority, defaultPriority),
		State:         string(StateAvailable),
		MaxAttempts:   orInt(opts.MaxAttempts, defaultMaxAttempts),
		ScheduledAt:   now,
		ParentJobID:   opts.Parent,
		ExecutorClass: string(opts.ExecutorClass),
		Tags:          datatypes.JSON("[]"),
	}
	if opts.ScheduleAt != nil {
		row.ScheduledAt = *opts.ScheduleAt
	}
	if opts.UniqueKey != "" {
		uk := opts.UniqueKey
		row.UniqueKey = &uk
	}
	if opts.UniqueActiveKey != "" {
		uak := opts.UniqueActiveKey
		row.UniqueActiveKey = &uak
	}
	if opts.Timeout > 0 {
		ms := int(opts.Timeout.Milliseconds())
		row.TimeoutMs = &ms
	}
	return row
}

// orString returns value when non-empty, otherwise fallback.
func orString(value, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}

// orInt returns value when non-zero, otherwise fallback.
func orInt(value, fallback int) int {
	if value == 0 {
		return fallback
	}
	return value
}

// orDuration returns value when positive, otherwise fallback. Unlike orInt it
// treats a negative value as unset too: the durations it guards — a limiter's
// RetryAfter hint, a starvation interval — have no meaningful negative reading, so
// a non-positive one falls through to the caller's default.
func orDuration(value, fallback time.Duration) time.Duration {
	if value <= 0 {
		return fallback
	}
	return value
}
