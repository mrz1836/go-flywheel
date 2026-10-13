package core

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/mrz1836/go-foundation/constants"
	"github.com/mrz1836/go-foundation/ctxutil"
	"gorm.io/datatypes"
)

// The limits on what an insert stores beside a job's args.
const (
	// MaxTags is the most tags one job carries, counted after duplicates are
	// dropped.
	MaxTags = 16
	// MaxTagBytes is the most bytes in one tag.
	MaxTagBytes = 64
	// MaxMetadataBytes is the most bytes a job's metadata object marshals to,
	// once the request id, the caller's keys, and an insert hook's are merged.
	MaxMetadataBytes = 8192
)

// InsertSource says where an insert came from. An InsertHook reads it in
// InsertInfo.
type InsertSource string

// The insert paths an InsertHook is called on.
const (
	// InsertSourceClient is an insert through a Client: Insert, Enqueue,
	// InsertMany, or InsertManyTyped.
	InsertSourceClient InsertSource = "client"
	// InsertSourcePeriodic is a Scheduler's periodic fire.
	InsertSourcePeriodic InsertSource = "periodic"
	// InsertSourceFollowUp is one of a worker's Result.FollowUps, inserted by
	// its finalize.
	InsertSourceFollowUp InsertSource = "follow_up"
	// InsertSourceBarrier is a barrier's continuation, fired by the finalize or
	// the lease sweep that made the generation's last pending child terminal.
	InsertSourceBarrier InsertSource = "barrier"
)

// InsertInfo describes the job an InsertHook is called for.
type InsertInfo struct {
	// Kind is the job's kind.
	Kind string
	// Queue is the queue the job lands on, its default resolved.
	Queue string
	// Source is the insert path.
	Source InsertSource
	// SpawnedBy is the job that caused the insert: the finishing job, for a
	// follow-up; for a barrier continuation, the child whose finalize fired it,
	// or the last child whose discard by the lease sweep fired it. It is empty
	// for any other source.
	SpawnedBy string
	// ParentJobID is the parent the new job records; empty when it has none.
	ParentJobID string
	// PeriodicSlug is the schedule a periodic fire is for; empty otherwise.
	PeriodicSlug string
}

// InsertHook stamps a job's metadata as the job is inserted, so a host records
// who or what enqueued every job in one place: the hook's keys are merged into
// the job's metadata over the caller's (InsertOpts.Metadata, FollowUp.Metadata),
// under the request id, which neither can replace.
//
// It is called once per row, before the row is built, on every insert through
// the Client whose ClientOpts carries it; through RunnerConfig.Client, on the
// follow-ups and barrier continuations that Runner's finalizes insert; and
// through SchedulerConfig.Client, on periodic fires and the barrier
// continuations the lease sweep fires. A continuation fired by a control
// (CancelJob, CancelByParent) runs no hook, even one a worker calls inside
// Work: the Runner's Client rides only on the context it finalizes on, never on
// a worker's.
//
// It returns no error, so it cannot refuse an insert, but its result counts
// toward MaxMetadataBytes: a result that pushes the merged metadata past the
// limit, or that JSON cannot encode, fails the insert, and inside a finalize or
// a sweep batch it fails that whole transaction. It may run inside the
// transaction of a finalize or a sweep batch, so it must not block, and it may
// be called from several finalizes at once. It may read ctx: the caller's on a
// Client insert, and in a finalize the context the Runner finalizes on, which
// carries the finishing job's request id.
type InsertHook func(ctx context.Context, in InsertInfo) map[string]any

// stamp is what an insert writes beside a job's args: its tags and its
// metadata, both validated.
type stamp struct{ tags, metadata datatypes.JSON }

// stampRow validates tags, calls hook once (when there is one) with in, and
// merges the job's metadata over requestID, in that order. It writes nothing;
// a refusal is a *ValidationError naming its field.
//
// Every insert path builds its row's tags and metadata here, so they cannot
// drift on a limit or on the order the metadata merges in.
func stampRow(
	ctx context.Context, hook InsertHook, in InsertInfo, tags []string, meta map[string]any, requestID string,
) (stamp, error) {
	tagJSON, err := tagsJSON(tags)
	if err != nil {
		return stamp{}, err
	}
	var hooked map[string]any
	if hook != nil {
		hooked = hook(ctx, in)
	}
	metaJSON, err := metadataJSON(requestID, meta, hooked)
	if err != nil {
		return stamp{}, err
	}
	return stamp{tags: tagJSON, metadata: metaJSON}, nil
}

// apply writes the stamp's tags and metadata onto row.
func (st stamp) apply(row *jobRow) {
	row.Tags = st.tags
	row.Metadata = st.metadata
}

// tagsJSON returns tags as the JSON array jobs.tags stores: in order, the first
// of each duplicate kept, "[]" when there are none. The limits apply after the
// duplicates are dropped. A refusal names the rule a tag broke, never the tag,
// which may be data the caller did not mean to log.
func tagsJSON(tags []string) (datatypes.JSON, error) {
	if len(tags) == 0 {
		return datatypes.JSON("[]"), nil
	}
	kept := make([]string, 0, len(tags))
	seen := make(map[string]struct{}, len(tags))
	for _, tag := range tags {
		if _, dup := seen[tag]; dup {
			continue
		}
		seen[tag] = struct{}{}
		kept = append(kept, tag)
	}
	if len(kept) > MaxTags {
		return nil, newValidationError("tags", fmt.Sprintf("holds more than %d tags", MaxTags))
	}
	for _, tag := range kept {
		switch {
		case tag == "":
			return nil, newValidationError("tags", "holds an empty tag")
		case len(tag) > MaxTagBytes:
			return nil, newValidationError("tags", fmt.Sprintf("holds a tag longer than %d bytes", MaxTagBytes))
		case !utf8.ValidString(tag):
			return nil, newValidationError("tags", "holds a tag that is not valid UTF-8")
		case strings.ContainsFunc(tag, unicode.IsControl):
			return nil, newValidationError("tags", "holds a tag with a control character")
		}
	}
	out, err := json.Marshal(kept)
	if err != nil {
		return nil, newValidationError("tags", "cannot be encoded as JSON")
	}
	return datatypes.JSON(out), nil
}

// metadataJSON merges a job's metadata object: the caller's keys, then the
// hook's over them, then the request id over both, so neither can replace it
// (with no request id, the object has no request_id key at all). With no keys
// from either it returns exactly what an insert wrote before metadata could be
// passed: the request id alone, or {}.
func metadataJSON(requestID string, caller, hooked map[string]any) (datatypes.JSON, error) {
	if len(caller) == 0 && len(hooked) == 0 {
		return datatypes.JSON(ctxutil.RequestIDToMetadata(nil, requestID)), nil
	}
	merged := make(map[string]any, len(caller)+len(hooked)+1)
	for k, v := range caller {
		if k != constants.FieldRequestID {
			merged[k] = v
		}
	}
	for k, v := range hooked {
		if k != constants.FieldRequestID {
			merged[k] = v
		}
	}
	if requestID != "" {
		merged[constants.FieldRequestID] = requestID
	}
	out, err := json.Marshal(merged)
	if err != nil {
		return nil, newValidationError("metadata", "cannot be encoded as JSON")
	}
	if len(out) > MaxMetadataBytes {
		return nil, newValidationError("metadata", fmt.Sprintf("is larger than %d bytes once merged", MaxMetadataBytes))
	}
	return datatypes.JSON(out), nil
}

// insertOrigin is where a Client insert came from, for its hook.
type insertOrigin struct {
	source       InsertSource
	periodicSlug string
}

// insertClientKey carries a Client on the context a Runner finalizes on, or a
// Scheduler sweeps on.
type insertClientKey struct{}

// withInsertClient returns ctx carrying c, so the follow-ups and barrier
// continuations the finalize or sweep it reaches inserts are stamped by c's
// hook. A nil c returns ctx unchanged.
func withInsertClient(ctx context.Context, c *Client) context.Context {
	if c == nil {
		return ctx
	}
	return context.WithValue(ctx, insertClientKey{}, c)
}

// insertHookFrom returns the InsertHook of the Client ctx carries, or nil when
// it carries none or that Client has no hook.
func insertHookFrom(ctx context.Context) InsertHook {
	if c, _ := ctx.Value(insertClientKey{}).(*Client); c != nil {
		return c.hook
	}
	return nil
}
