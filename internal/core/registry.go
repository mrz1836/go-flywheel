package core

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
// Runner need not re-assert. defaults is nil when the worker implements no
// Defaults.
type registryEntry struct {
	dispatch   dispatchFunc
	classifier Classifier
	retryable  Retryable
	defaults   *kindDefaults
	timeouter  Timeouter
}

// kindDefaults is the part of a worker's Defaults() an insert applies: read
// once, at registration, so no insert path calls worker code.
type kindDefaults struct {
	queue         string
	priority      int
	maxAttempts   int
	timeout       time.Duration
	executorClass ExecutorClass
}

// kindDefaultsOf keeps the fields of opts an insert reads from Defaults.
func kindDefaultsOf(opts InsertOpts) *kindDefaults {
	return &kindDefaults{
		queue:         opts.Queue,
		priority:      opts.Priority,
		maxAttempts:   opts.MaxAttempts,
		timeout:       opts.Timeout,
		executorClass: opts.ExecutorClass,
	}
}

// applyTo fills each field opts leaves zero from d, and returns opts with
// every other field as it was. A nil d returns opts unchanged.
func (d *kindDefaults) applyTo(opts InsertOpts) InsertOpts {
	if d == nil {
		return opts
	}
	if opts.Queue == "" {
		opts.Queue = d.queue
	}
	if opts.Priority == 0 {
		opts.Priority = d.priority
	}
	if opts.MaxAttempts == 0 {
		opts.MaxAttempts = d.maxAttempts
	}
	if opts.Timeout == 0 {
		opts.Timeout = d.timeout
	}
	if opts.ExecutorClass == AnyClass {
		opts.ExecutorClass = d.executorClass
	}
	return opts
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
// w.Kind(). It panics if the kind is already registered: two workers claiming
// one kind is a programming error with no correct resolution — silently keeping
// either one would route live jobs to a worker the author did not intend — so it
// fails at startup rather than at dispatch.
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
					// Args that do not decode never will: the error is a validation
					// failure the Runner discards on, not one it retries.
					return Result{}, &decodeArgsError{kind: kind, err: err}
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
		entry.defaults = kindDefaultsOf(d.Defaults())
	}
	if to, ok := any(w).(Timeouter); ok {
		entry.timeouter = to
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

// defaultsFor returns the defaults kind's worker declares, or nil when reg is
// nil, the kind is not registered, or its worker declares none.
func (reg *Registry) defaultsFor(kind string) *kindDefaults {
	if reg == nil {
		return nil
	}
	e, _ := reg.lookup(kind)
	return e.defaults
}

// kindRegistryKey carries a Runner's Registry on the context it finalizes on.
type kindRegistryKey struct{}

// withKindRegistry returns ctx carrying reg, so the finalize it reaches applies
// reg's worker defaults to the follow-ups and barrier continuations it inserts.
// A nil reg returns ctx unchanged.
func withKindRegistry(ctx context.Context, reg *Registry) context.Context {
	if reg == nil {
		return ctx
	}
	return context.WithValue(ctx, kindRegistryKey{}, reg)
}
