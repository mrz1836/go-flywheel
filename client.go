package flywheel

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

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
// value, which must implement Kind() string. When opts.Tx is set the row is
// written on that transaction (outbox, FR-003). A unique_key collision returns
// ErrAlreadyEnqueued, never a raw driver error (FR-004).
func Insert[A Args](ctx context.Context, c *Client, args A, opts InsertOpts) (string, error) {
	namer, ok := any(args).(kindNamer)
	if !ok {
		return "", ErrMissingKind
	}
	payload, err := json.Marshal(args)
	if err != nil {
		return "", fmt.Errorf("jobs: marshal args: %w", err)
	}
	return c.insert(ctx, namer.Kind(), payload, opts)
}

// Enqueue writes one available job of an arbitrary kind from a pre-marshaled
// JSON payload, with no registered worker required. It is the host seed seam:
// fixtures and inspection hosts create real jobs through the same insert core as
// Insert — honoring opts (UniqueKey/Queue/Priority/…) and the row's lifecycle
// defaults — without touching flywheel's unexported row structs. A unique_key
// collision returns ErrAlreadyEnqueued.
func Enqueue(ctx context.Context, c *Client, kind string, args []byte, opts InsertOpts) (string, error) {
	return c.insert(ctx, kind, args, opts)
}

// insert is the non-generic enqueue core shared by Insert and the Scheduler. It
// writes one jobs row, honoring opts, and maps a unique_key collision to
// ErrAlreadyEnqueued.
func (c *Client) insert(ctx context.Context, kind string, payload []byte, opts InsertOpts) (string, error) {
	db := c.writeDB
	if opts.Tx != nil {
		db = opts.Tx
	}
	now := ClockFrom(ctx).Now(ctx)

	requestID := opts.RequestID
	if requestID == "" {
		requestID = RequestIDFrom(ctx)
	}

	row := jobRow{
		ID:            NewID(),
		CreatedAt:     now,
		UpdatedAt:     now,
		Metadata:      datatypes.JSON(metadataWithRequestID(nil, requestID)),
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

	if createErr := db.WithContext(ctx).Create(&row).Error; createErr != nil {
		wrapped := WrapDBError(createErr)
		if errors.Is(wrapped, ErrDuplicateKey) {
			return "", ErrAlreadyEnqueued
		}
		return "", fmt.Errorf("jobs: insert: %w", wrapped)
	}
	return row.ID, nil
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
