package core

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
)

// Describer is an optional worker interface: a one-line, human-readable
// description of what the kind does, reported in its KindInfo.
type Describer interface {
	Description() string
}

// ArgsSchemer is an optional worker interface: a JSON Schema document for the
// kind's args, from any generator the host uses, reported in its KindInfo.
// Register calls ArgsSchema once and panics, naming the kind, when it returns an
// error or text that is not valid JSON. An empty result declares no schema.
type ArgsSchemer interface {
	ArgsSchema() (json.RawMessage, error)
}

// ArgsExampler is an optional worker interface: an example of the kind's args,
// reported in its KindInfo. Register calls ExampleArgs once and panics, naming
// the kind, when its result is not valid JSON; an empty result declares no
// example. Register does not check the example against the schema or the
// ArgsValidator: a host that wants that proves it in its own test.
type ArgsExampler interface {
	ExampleArgs() json.RawMessage
}

// ArgsValidator is an optional worker interface: it checks a kind's decoded
// args before Registry.Validate and Registry.Enqueue accept them. Its error is
// returned as a *ValidationError on the field "args". The Runner does not call
// it: a job inserted without Registry.Enqueue reaches the worker unchecked.
type ArgsValidator[A Args] interface {
	ValidateArgs(args A) error
}

// KindInfo is what a registry reports about one registered kind: what its
// worker declares, the routing an insert of it defaults to, and which optional
// interfaces its worker implements.
type KindInfo struct {
	Kind             string          `json:"kind"`
	Description      string          `json:"description,omitempty"`
	ArgsSchema       json.RawMessage `json:"args_schema,omitempty"`
	ExampleArgs      json.RawMessage `json:"example_args,omitempty"`
	Queue            string          `json:"queue"`                // its Defaults' queue, else "default"
	Priority         int             `json:"priority"`             // its Defaults' priority, else 100
	MaxAttempts      int             `json:"max_attempts"`         // its Defaults' max attempts, else 25
	TimeoutMs        int             `json:"timeout_ms,omitempty"` // its Defaults' timeout, else its Timeouter's; 0 leaves the runner's
	ExecutorClass    ExecutorClass   `json:"executor_class,omitempty"`
	ValidatesArgs    bool            `json:"validates_args"`    // implements ArgsValidator
	CustomBackoff    bool            `json:"custom_backoff"`    // implements Retryable
	ClassifiesErrors bool            `json:"classifies_errors"` // implements Classifier
}

// Kinds returns a KindInfo for every registered kind, sorted by kind. Each is
// read from its worker once, when it was registered, so Kinds calls no worker
// code, and the result is a copy the caller may change.
func (reg *Registry) Kinds() []KindInfo {
	reg.mu.RLock()
	out := make([]KindInfo, 0, len(reg.entries))
	for _, e := range reg.entries {
		info := e.info
		info.ArgsSchema = bytes.Clone(info.ArgsSchema)
		info.ExampleArgs = bytes.Clone(info.ExampleArgs)
		out = append(out, info)
	}
	reg.mu.RUnlock()
	slices.SortFunc(out, func(a, b KindInfo) int { return strings.Compare(a.Kind, b.Kind) })
	return out
}

// Validate checks args for kind as a worker would receive them, writing
// nothing. It returns an error wrapping ErrUnknownKind when the registry does
// not register kind, and a *ValidationError on the field "args", carrying the
// reason, when the args do not decode into the kind's args type (decoded as the
// Runner's dispatch decodes them; empty args are the zero value) or the
// worker's ArgsValidator refuses them. Otherwise it returns nil.
func (reg *Registry) Validate(kind string, args []byte) error {
	entry, ok := reg.lookup(kind)
	if !ok {
		return fmt.Errorf("jobs: kind %q: %w", kind, ErrUnknownKind)
	}
	return entry.validate(args)
}

// Enqueue is a validated Enqueue: it checks kind and args with Validate, fills
// each field opts leaves zero from the kind's Defaults (see Defaults), and then
// enqueues through c exactly as Enqueue does, so c's own ClientOpts apply and a
// unique-key collision is an *AlreadyEnqueuedError. A kind the registry does
// not register, or args Validate refuses, is refused at once and writes no row,
// rather than waiting out a Runner's RunnerConfig.UnknownKindGrace or failing
// its first attempt. Enqueue itself stays the unvalidated seed seam.
func (reg *Registry) Enqueue(ctx context.Context, c *Client, kind string, args []byte, opts InsertOpts) (string, error) {
	if err := reg.Validate(kind, args); err != nil {
		return "", err
	}
	return Enqueue(ctx, c, kind, args, reg.defaultsFor(kind).applyTo(opts))
}

// decodeJobArgs decodes raw into A as a worker receives it: an empty payload is the
// zero value. Dispatch and Validate both decode through it, so an args shape
// Validate accepts is one the worker receives. Its error is the decoder's own.
func decodeJobArgs[A Args](raw []byte) (A, error) {
	var args A
	if len(raw) == 0 {
		return args, nil
	}
	err := json.Unmarshal(raw, &args)
	return args, err
}

// describeKind reads what w declares about its kind from the optional
// interfaces it implements, once, at registration. It panics, naming the kind
// and what failed, when the args schema fails to build or is not valid JSON, or
// when the example args are not valid JSON. No panic echoes either text, which
// may be data the host did not mean to log.
func describeKind[A Args](kind string, w Worker[A], d *kindDefaults) KindInfo {
	info := KindInfo{
		Kind:        kind,
		Queue:       defaultQueue,
		Priority:    defaultPriority,
		MaxAttempts: defaultMaxAttempts,
	}
	if desc, ok := any(w).(Describer); ok {
		info.Description = desc.Description()
	}
	if s, ok := any(w).(ArgsSchemer); ok {
		schema, err := s.ArgsSchema()
		if err != nil {
			panic(fmt.Sprintf("jobs: args schema for kind %q: %v", kind, err))
		}
		if len(schema) > 0 && !json.Valid(schema) {
			panic(fmt.Sprintf("jobs: args schema for kind %q is not valid JSON", kind))
		}
		info.ArgsSchema = bytes.Clone(schema)
	}
	if ex, ok := any(w).(ArgsExampler); ok {
		example := ex.ExampleArgs()
		if len(example) > 0 && !json.Valid(example) {
			panic(fmt.Sprintf("jobs: example args for kind %q are not valid JSON", kind))
		}
		info.ExampleArgs = bytes.Clone(example)
	}
	if d != nil {
		info.Queue = orString(d.queue, defaultQueue)
		info.Priority = orInt(d.priority, defaultPriority)
		info.MaxAttempts = orInt(d.maxAttempts, defaultMaxAttempts)
		info.ExecutorClass = d.executorClass
		if d.timeout > 0 {
			info.TimeoutMs = int(d.timeout.Milliseconds())
		}
	}
	if to, ok := any(w).(Timeouter); ok && info.TimeoutMs == 0 {
		if timeout := to.Timeout(); timeout > 0 {
			info.TimeoutMs = int(timeout.Milliseconds())
		}
	}
	_, info.ValidatesArgs = any(w).(ArgsValidator[A])
	_, info.CustomBackoff = any(w).(Retryable)
	_, info.ClassifiesErrors = any(w).(Classifier)
	return info
}

// argsValidation returns the check Registry.Validate runs for w's kind: the
// args decode as dispatch decodes them, then meet w's ArgsValidator when it
// implements one. Either refusal is a *ValidationError on the field "args".
func argsValidation[A Args](w Worker[A]) func(raw []byte) error {
	validator, _ := any(w).(ArgsValidator[A])
	return func(raw []byte) error {
		args, err := decodeJobArgs[A](raw)
		if err != nil {
			return newValidationError("args", "do not decode: "+err.Error())
		}
		if validator == nil {
			return nil
		}
		if err := validator.ValidateArgs(args); err != nil {
			return newValidationError("args", err.Error())
		}
		return nil
	}
}
