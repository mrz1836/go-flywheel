package flywheel

import (
	"context"
	"encoding/json"
)

// requestIDKey types the context key used to thread an HTTP request_id through
// the job runtime. The HTTP layer stamps the id on ctx after its middleware
// extracts X-Request-ID; the client copies it into the job's metadata at
// enqueue time; the runner reads it back on dequeue, stamps the worker's ctx,
// and decorates the worker logger so every log line and every side-effect row
// downstream carries the same id. The chain stays walkable across the queue
// boundary without callers having to thread the value explicitly.
type requestIDKey struct{}

// WithRequestID returns ctx tagged with id. An empty id is a no-op so callers
// can pass through without branching.
func WithRequestID(ctx context.Context, id string) context.Context {
	if id == "" {
		return ctx
	}
	return context.WithValue(ctx, requestIDKey{}, id)
}

// RequestIDFrom returns the request_id stamped on ctx, or "" when none is
// present. Workers and services use this to read the id without taking a
// dependency on the HTTP layer.
func RequestIDFrom(ctx context.Context) string {
	v, _ := ctx.Value(requestIDKey{}).(string)
	return v
}

// metadataWithRequestID returns a Metadata JSON blob (always non-empty)
// containing the request_id when it is non-empty, or an empty object when it
// is not. base, when non-empty, is merged in first so a caller-supplied
// metadata blob can carry additional tags alongside the request_id.
func metadataWithRequestID(base []byte, requestID string) []byte {
	m := map[string]any{}
	if len(base) > 0 {
		_ = json.Unmarshal(base, &m)
	}
	if requestID != "" {
		m["request_id"] = requestID
	}
	out, err := json.Marshal(m)
	if err != nil || len(out) == 0 {
		return []byte("{}")
	}
	return out
}

// requestIDFromMetadata extracts request_id from a jobs.metadata blob, or
// returns "" when none is present. It tolerates malformed blobs so a partial
// upgrade across an old DB row does not crash the runner.
func requestIDFromMetadata(raw []byte) string {
	if len(raw) == 0 {
		return ""
	}
	var m struct {
		RequestID string `json:"request_id"`
	}
	if err := json.Unmarshal(raw, &m); err != nil {
		return ""
	}
	return m.RequestID
}
