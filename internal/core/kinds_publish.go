package core

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/mrz1836/go-foundation/models"
	"gorm.io/datatypes"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// KindRecord is one kind of the published catalog in the job_kinds table: the
// KindInfo the publishing registry reported for it, the publisher of its last
// publish, and when it was first and last published.
type KindRecord struct {
	KindInfo
	PublishedBy      string    `json:"published_by"`
	FirstPublishedAt time.Time `json:"first_published_at"`
	PublishedAt      time.Time `json:"published_at"`
}

// PublishKindsOpts configures PublishKindsWithOptions. Its zero value records
// the process's executor identity (host:pid) as the publisher.
type PublishKindsOpts struct {
	Publisher string // recorded as published_by; empty selects the process's executor identity (host:pid)
}

// kindRecordColumns are the job_kinds columns a publish of a kind already listed
// overwrites: every one but the key and first_published_at.
//
//nolint:gochecknoglobals // a fixed column list
var kindRecordColumns = []string{
	"description", "args_schema", "example_args", "queue", "priority", "max_attempts", "timeout_ms",
	"executor_class", "validates_args", "custom_backoff", "classifies_errors", "published_by", "published_at",
}

// PublishKinds writes reg's catalog to the job_kinds table, so a process
// without the registry (an API, the CLI) reads it with ListKinds. It is
// PublishKindsWithOptions with the zero PublishKindsOpts.
func PublishKinds(ctx context.Context, db *gorm.DB, reg *Registry) error {
	return PublishKindsWithOptions(ctx, db, reg, PublishKindsOpts{})
}

// PublishKindsWithOptions upserts one job_kinds row per kind reg registers, in
// one statement: a new kind is inserted, and a listed one takes the registry's
// KindInfo, the publisher, and the publish time, keeping its first publish
// time. The publish time is the context clock's. It never deletes a row, so a
// kind no release registers any more stays listed with an old published_at, and
// an empty registry publishes nothing.
//
// Two releases publishing at once during a rolling deploy each upsert their own
// kinds; for a kind both register, the last writer's KindInfo wins. A database
// without the job_kinds table fails the call, naming the table; nothing at
// startup reads it.
func PublishKindsWithOptions(ctx context.Context, db *gorm.DB, reg *Registry, opts PublishKindsOpts) error {
	if db == nil {
		return fmt.Errorf("flywheel: PublishKinds: db is nil")
	}
	if reg == nil {
		return fmt.Errorf("flywheel: PublishKinds: registry is nil")
	}
	kinds := reg.Kinds()
	if len(kinds) == 0 {
		return nil
	}
	now := models.ClockFrom(ctx).Now(ctx).UTC()
	publisher := opts.Publisher
	if publisher == "" {
		publisher = executorIdentity()
	}
	rows := make([]jobKindRow, len(kinds))
	for i, k := range kinds {
		rows[i] = jobKindRow{
			Kind:             k.Kind,
			Description:      k.Description,
			ArgsSchema:       datatypes.JSON(k.ArgsSchema),
			ExampleArgs:      datatypes.JSON(k.ExampleArgs),
			Queue:            k.Queue,
			Priority:         k.Priority,
			MaxAttempts:      k.MaxAttempts,
			ExecutorClass:    string(k.ExecutorClass),
			ValidatesArgs:    k.ValidatesArgs,
			CustomBackoff:    k.CustomBackoff,
			ClassifiesErrors: k.ClassifiesErrors,
			PublishedBy:      publisher,
			FirstPublishedAt: now,
			PublishedAt:      now,
		}
		if k.TimeoutMs > 0 {
			ms := k.TimeoutMs
			rows[i].TimeoutMs = &ms
		}
	}
	if err := db.WithContext(ctx).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "kind"}},
		DoUpdates: clause.AssignmentColumns(kindRecordColumns),
	}).Create(&rows).Error; err != nil {
		return fmt.Errorf("flywheel: PublishKinds: %w", models.WrapDBError(err))
	}
	return nil
}

// ListKinds returns every kind of the published catalog, sorted by kind. It
// returns every row, not MaxReadLimit of them: the catalog lists a host's
// kinds, which its code bounds, and a cut catalog would hide a kind. A database
// without the job_kinds table fails the call.
func ListKinds(ctx context.Context, db *gorm.DB) ([]KindRecord, error) {
	if db == nil {
		return nil, fmt.Errorf("flywheel: ListKinds: db is nil")
	}
	var rows []jobKindRow
	if err := db.WithContext(ctx).Order("kind").Find(&rows).Error; err != nil {
		return nil, fmt.Errorf("flywheel: ListKinds: %w", models.WrapDBError(err))
	}
	out := make([]KindRecord, len(rows))
	for i, row := range rows {
		out[i] = KindRecord{
			KindInfo: KindInfo{
				Kind:             row.Kind,
				Description:      row.Description,
				ArgsSchema:       storedJSON(row.ArgsSchema),
				ExampleArgs:      storedJSON(row.ExampleArgs),
				Queue:            row.Queue,
				Priority:         row.Priority,
				MaxAttempts:      row.MaxAttempts,
				ExecutorClass:    ExecutorClass(row.ExecutorClass),
				ValidatesArgs:    row.ValidatesArgs,
				CustomBackoff:    row.CustomBackoff,
				ClassifiesErrors: row.ClassifiesErrors,
			},
			PublishedBy:      row.PublishedBy,
			FirstPublishedAt: row.FirstPublishedAt,
			PublishedAt:      row.PublishedAt,
		}
		if row.TimeoutMs != nil {
			out[i].TimeoutMs = *row.TimeoutMs
		}
	}
	return out, nil
}

// storedJSON returns a nullable JSON column as the raw message it holds, nil for
// SQL NULL, which the column scans as the text null.
func storedJSON(j datatypes.JSON) json.RawMessage {
	if len(j) == 0 || bytes.Equal(j, []byte("null")) {
		return nil
	}
	return json.RawMessage(j)
}
