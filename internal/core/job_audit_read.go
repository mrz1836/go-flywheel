package core

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"gorm.io/gorm"
)

// AuditOrder is the order ListAudit reads entries in.
type AuditOrder string

// The two orders, by (created_at, id).
const (
	// AuditNewest reads newest first: a target's history. It is the zero Order.
	AuditNewest AuditOrder = "newest"
	// AuditOldest reads oldest first: a feed a reader follows forward.
	AuditOldest AuditOrder = "oldest"
)

// ListAuditParams filters and pages a ListAudit read. Every filter that is set
// must match; the zero value reads the whole history, newest first, 50 entries a
// page.
type ListAuditParams struct {
	// TargetType, with TargetID, reads one target's history; alone, every entry
	// about that kind of target.
	TargetType AuditTarget
	// TargetID is the target's stable name. It needs TargetType.
	TargetID string
	// Actions keeps the entries with any of these actions.
	Actions []AuditAction
	// ActorType keeps one kind of actor's entries.
	ActorType ActorType
	// ActorID keeps one actor's entries.
	ActorID string
	// Since and Until bound created_at to [Since, Until), compared in UTC
	// whatever zone they carry. Either may be zero; set together, Until must be
	// after Since.
	Since time.Time
	Until time.Time
	// Order is AuditNewest (the zero value) or AuditOldest.
	Order AuditOrder
	// Cursor is the NextCursor of the previous page, read in the same Order.
	Cursor string
	// Limit is the most entries a page holds. Zero or negative selects 50, and a
	// value above MaxReadLimit reads MaxReadLimit.
	Limit int
}

// AuditPage is one page of a ListAudit read.
type AuditPage struct {
	// Entries is the page's entries, in the read's order. It is never nil.
	Entries []AuditEntry `json:"entries"`
	// NextCursor reads the next page when passed as Cursor, with the same
	// filters and Order. It is empty on the last page.
	NextCursor string `json:"next_cursor,omitempty"`
}

// defaultAuditLimit is the page a zero or negative ListAuditParams.Limit reads.
const defaultAuditLimit = 50

// ListAudit reads the control history: one target's entries (TargetType and
// TargetID) or the whole feed, filtered by action, actor, and time, newest first
// or, with AuditOldest, oldest first. Entries are ordered by (created_at, id), so
// entries one transaction wrote, which share created_at, come in id order.
//
// It pages through an opaque cursor: pass a page's NextCursor as the next call's
// Cursor, with the same filters and Order. No page skips or repeats an entry, and
// an entry written after a page was read appears on a later page when it sorts
// after that page's last entry. A cursor is bound to the order it was issued for.
//
// It refuses, before any query and as a *ValidationError naming the field, an
// unknown Order (order), TargetType (target_type, also when TargetID is set
// without it), or ActorType (actor_type), an Until not after Since (until), and a
// Cursor that is malformed or was issued for the other order (cursor). It reads
// with one statement on db: a target's history is served by the job_audit_target
// index, and the feed by job_audit_created.
func ListAudit(ctx context.Context, db *gorm.DB, p ListAuditParams) (AuditPage, error) {
	if db == nil {
		return AuditPage{}, errors.New("flywheel: ListAudit: db is nil")
	}
	order, cursor, err := validateListAudit(p)
	if err != nil {
		return AuditPage{}, err
	}
	limit := p.Limit
	switch {
	case limit <= 0:
		limit = defaultAuditLimit
	case limit > MaxReadLimit:
		limit = MaxReadLimit
	}

	q := db.WithContext(ctx).Model(&jobAuditRow{})
	if p.TargetType != "" {
		q = q.Where("target_type = ?", string(p.TargetType))
	}
	if p.TargetID != "" {
		q = q.Where("target_id = ?", p.TargetID)
	}
	if len(p.Actions) > 0 {
		actions := make([]string, len(p.Actions))
		for i, a := range p.Actions {
			actions[i] = string(a)
		}
		q = q.Where("action IN ?", actions)
	}
	if p.ActorType != "" {
		q = q.Where("actor_type = ?", string(p.ActorType))
	}
	if p.ActorID != "" {
		q = q.Where("actor_id = ?", p.ActorID)
	}
	// Entries are stamped in UTC, and on SQLite created_at compares as text, so a
	// bound is compared in UTC whatever zone it carries.
	if !p.Since.IsZero() {
		q = q.Where("created_at >= ?", p.Since.UTC())
	}
	if !p.Until.IsZero() {
		q = q.Where("created_at < ?", p.Until.UTC())
	}
	dir := "DESC"
	if order == AuditOldest {
		dir = "ASC"
	}
	if cursor != nil {
		after := "(created_at < ? OR (created_at = ? AND id < ?))"
		if order == AuditOldest {
			after = "(created_at > ? OR (created_at = ? AND id > ?))"
		}
		q = q.Where(after, cursor.At, cursor.At, cursor.ID)
	}
	// One row past the page says whether another page follows.
	var rows []jobAuditRow
	if err := q.Order("created_at " + dir + ", id " + dir).Limit(limit + 1).Find(&rows).Error; err != nil {
		return AuditPage{}, fmt.Errorf("flywheel: list audit: %w", err)
	}

	page := AuditPage{Entries: make([]AuditEntry, 0, min(len(rows), limit))}
	for i := range rows[:min(len(rows), limit)] {
		page.Entries = append(page.Entries, auditEntryFromRow(rows[i]))
	}
	if len(rows) > limit {
		last := rows[limit-1]
		next, err := encodeAuditCursor(auditCursor{Order: order, At: last.CreatedAt.UTC(), ID: last.ID})
		if err != nil {
			return AuditPage{}, fmt.Errorf("flywheel: list audit: %w", err)
		}
		page.NextCursor = next
	}
	return page, nil
}

// validateListAudit refuses p's malformed fields, returning the order it reads
// in and the decoded cursor, if any.
func validateListAudit(p ListAuditParams) (AuditOrder, *auditCursor, error) {
	order := p.Order
	if order == "" {
		order = AuditNewest
	}
	switch {
	case order != AuditNewest && order != AuditOldest:
		return "", nil, newValidationError("order", "is not newest or oldest")
	case p.TargetType != "" && !p.TargetType.valid():
		return "", nil, newValidationError("target_type", "is not one of job, queue, periodic, node, or limiter")
	case p.TargetType == "" && p.TargetID != "":
		return "", nil, newValidationError("target_type", "is required with a target_id")
	case p.ActorType != "" && !p.ActorType.Valid():
		return "", nil, newValidationError("actor_type", "is not one of user, agent, or system")
	case !p.Since.IsZero() && !p.Until.IsZero() && !p.Until.After(p.Since):
		return "", nil, newValidationError("until", "must be after since")
	}
	if p.Cursor == "" {
		return order, nil, nil
	}
	c, err := decodeAuditCursor(p.Cursor, order)
	if err != nil {
		return "", nil, err
	}
	return order, &c, nil
}

// valid reports whether t is one of the AuditTarget constants.
func (t AuditTarget) valid() bool {
	switch t {
	case AuditTargetJob, AuditTargetQueue, AuditTargetPeriodic, AuditTargetNode, AuditTargetLimiter:
		return true
	default:
		return false
	}
}

// auditEntryFromRow projects a job_audit row into the entry ListAudit returns.
func auditEntryFromRow(r jobAuditRow) AuditEntry {
	details := json.RawMessage(r.Details)
	if len(details) == 0 {
		details = json.RawMessage("{}")
	}
	return AuditEntry{
		ID:         r.ID,
		CreatedAt:  r.CreatedAt.UTC(),
		TargetType: AuditTarget(r.TargetType),
		TargetID:   r.TargetID,
		Action:     AuditAction(r.Action),
		Actor: Actor{
			Type: ActorType(r.ActorType), ID: r.ActorID, Reason: r.Reason, Source: r.Source, RequestID: r.RequestID,
		},
		Details: details,
	}
}

// auditCursorPrefix versions ListAudit's cursor, so a later shape can tell its
// own cursors from this one's.
const auditCursorPrefix = "au1."

// auditCursor is the key of the last entry on a page: the order that read it,
// its created_at as read back (in UTC, so PostgreSQL's microseconds round-trip
// exactly), and its id, which breaks ties. It carries no filter.
type auditCursor struct {
	Order AuditOrder `json:"o"`
	At    time.Time  `json:"t"`
	ID    string     `json:"i"`
}

// encodeAuditCursor renders c as an opaque cursor: the prefix, then the key as
// base64url JSON without padding.
func encodeAuditCursor(c auditCursor) (string, error) {
	body, err := json.Marshal(c)
	if err != nil {
		return "", fmt.Errorf("encode cursor: %w", err)
	}
	return auditCursorPrefix + base64.RawURLEncoding.EncodeToString(body), nil
}

// decodeAuditCursor reads a cursor ListAudit issued for order, refusing another
// prefix, a body that does not decode, a zero time or an empty id, and a cursor
// issued for the other order.
func decodeAuditCursor(raw string, order AuditOrder) (auditCursor, error) {
	malformed := newValidationError("cursor", "is malformed: pass the NextCursor of a previous page")
	body, ok := strings.CutPrefix(raw, auditCursorPrefix)
	if !ok {
		return auditCursor{}, malformed
	}
	data, err := base64.RawURLEncoding.DecodeString(body)
	if err != nil {
		return auditCursor{}, malformed
	}
	var c auditCursor
	if err := json.Unmarshal(data, &c); err != nil || c.ID == "" || c.At.IsZero() {
		return auditCursor{}, malformed
	}
	if c.Order != order {
		return auditCursor{}, newValidationError("cursor", fmt.Sprintf("was issued for another order than %s", order))
	}
	return c, nil
}
