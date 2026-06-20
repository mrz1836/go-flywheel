package flywheel

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

// dispatchInput carries the non-typed fields the Runner has assembled for one
// attempt. The dispatch closure decodes RawArgs into the worker's argument type
// and builds the typed Job[A].
type dispatchInput struct {
	ID          string
	Kind        string
	Queue       string
	RawArgs     []byte
	Attempt     int
	MaxAttempts int
	ParentJobID *string
	EnqueuedAt  time.Time
	Tags        []string
	Logger      *slog.Logger
	RunID       string
}

// dispatchFunc decodes args and runs a worker; the worker's type parameter is
// erased so the Runner's hot path does no reflection.
type dispatchFunc func(ctx context.Context, in dispatchInput) (Result, error)

// registryEntry is one registered worker: its dispatch closure plus any
// optional interfaces it implements, captured once at registration so the
// Runner need not re-assert.
type registryEntry struct {
	dispatch   dispatchFunc
	classifier Classifier
	retryable  Retryable
	defaults   Defaults
}

// Registry maps a job kind to its registered worker. It is safe for concurrent
// reads after registration.
type Registry struct {
	mu      sync.RWMutex
	entries map[string]registryEntry
}

// NewRegistry returns an empty Registry.
func NewRegistry() *Registry {
	return &Registry{entries: map[string]registryEntry{}}
}

// Register builds the typed dispatch closure for w and stores it keyed by
// w.Kind(). It panics if the kind is already registered — a duplicate
// registration is a programming error that must fail at startup (FR-037).
func Register[A Args](reg *Registry, w Worker[A]) {
	kind := w.Kind()

	reg.mu.Lock()
	defer reg.mu.Unlock()
	if _, exists := reg.entries[kind]; exists {
		panic(fmt.Sprintf("jobs: duplicate worker registration for kind %q", kind))
	}

	entry := registryEntry{
		dispatch: func(ctx context.Context, in dispatchInput) (Result, error) {
			var args A
			if len(in.RawArgs) > 0 {
				if err := json.Unmarshal(in.RawArgs, &args); err != nil {
					return Result{}, fmt.Errorf("jobs: decode args for kind %q: %w", kind, err)
				}
			}
			job := &Job[A]{
				ID:          in.ID,
				Kind:        in.Kind,
				Queue:       in.Queue,
				Args:        args,
				Attempt:     in.Attempt,
				MaxAttempts: in.MaxAttempts,
				ParentJobID: in.ParentJobID,
				EnqueuedAt:  in.EnqueuedAt,
				Tags:        in.Tags,
				Logger:      in.Logger,
				RunID:       in.RunID,
			}
			return w.Work(ctx, job)
		},
	}
	if c, ok := any(w).(Classifier); ok {
		entry.classifier = c
	}
	if r, ok := any(w).(Retryable); ok {
		entry.retryable = r
	}
	if d, ok := any(w).(Defaults); ok {
		entry.defaults = d
	}
	reg.entries[kind] = entry
}

// lookup returns the registered entry for kind.
func (reg *Registry) lookup(kind string) (registryEntry, bool) {
	reg.mu.RLock()
	defer reg.mu.RUnlock()
	e, ok := reg.entries[kind]
	return e, ok
}
