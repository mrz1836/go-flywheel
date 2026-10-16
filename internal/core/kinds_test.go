package core

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// signupArgs is the args type of the catalog tests' "mail.welcome" kind.
type signupArgs struct {
	UserID string `json:"user_id"`
}

// signupSchema is the JSON Schema "mail.welcome" declares for its args.
const signupSchema = `{"type":"object","properties":{"user_id":{"type":"string"}},"required":["user_id"]}`

// describedWorker implements every optional interface a kind can declare but
// Classifier: a description, an args schema and example, defaults, a retry
// backoff, and an args validator.
type describedWorker struct{}

func (describedWorker) Kind() string { return "mail.welcome" }

func (describedWorker) Work(context.Context, *Job[signupArgs]) (Result, error) { return Result{}, nil }

func (describedWorker) Description() string { return "Send the welcome email" }

func (describedWorker) ArgsSchema() (json.RawMessage, error) {
	return json.RawMessage(signupSchema), nil
}

func (describedWorker) ExampleArgs() json.RawMessage { return json.RawMessage(`{"user_id":"u1"}`) }

func (describedWorker) Defaults() InsertOpts {
	return InsertOpts{Queue: "mail", MaxAttempts: 4, Timeout: 30 * time.Second}
}

func (describedWorker) NextRetry(error, int) time.Duration { return time.Second }

func (describedWorker) ValidateArgs(args signupArgs) error {
	if args.UserID == "" {
		return errors.New("user_id is required")
	}
	return nil
}

// plainWorker implements no optional interface.
type plainWorker struct{}

func (plainWorker) Kind() string { return "report.daily" }

func (plainWorker) Work(context.Context, *Job[signupArgs]) (Result, error) { return Result{}, nil }

// timedWorker declares no Defaults and times out through its Timeouter.
type timedWorker struct{}

func (timedWorker) Kind() string { return "report.timed" }

func (timedWorker) Work(context.Context, *Job[signupArgs]) (Result, error) { return Result{}, nil }

func (timedWorker) Timeout() time.Duration { return 45 * time.Second }

// timedDefaultsWorker declares both a Defaults timeout and a Timeouter.
type timedDefaultsWorker struct{ timedWorker }

func (timedDefaultsWorker) Kind() string { return "report.timed_defaults" }

func (timedDefaultsWorker) Defaults() InsertOpts { return InsertOpts{Timeout: 30 * time.Second} }

// catalogClassifyingWorker classifies its errors.
type catalogClassifyingWorker struct{ plainWorker }

func (catalogClassifyingWorker) Kind() string { return "report.classified" }

func (catalogClassifyingWorker) Classify(error) ErrorClass { return ErrorPermanent }

// catalogRegistry registers describedWorker and plainWorker.
func catalogRegistry() *Registry {
	reg := NewRegistry()
	Register(reg, plainWorker{})
	Register(reg, describedWorker{})
	return reg
}

// TestKindsDescribeEveryRegisteredWorker proves Kinds reports one KindInfo per
// registered kind, sorted by kind: what its worker declares, the runtime's
// defaults where it declares nothing, and which optional interfaces it
// implements. A Defaults timeout wins over a Timeouter's, and a Timeouter's
// stands in when there is none.
func TestKindsDescribeEveryRegisteredWorker(t *testing.T) {
	t.Parallel()

	t.Run("a described kind and a plain one", func(t *testing.T) {
		t.Parallel()
		kinds := catalogRegistry().Kinds()
		require.Len(t, kinds, 2)
		assert.Equal(t, KindInfo{
			Kind:          "mail.welcome",
			Description:   "Send the welcome email",
			ArgsSchema:    json.RawMessage(signupSchema),
			ExampleArgs:   json.RawMessage(`{"user_id":"u1"}`),
			Queue:         "mail",
			Priority:      defaultPriority,
			MaxAttempts:   4,
			TimeoutMs:     30_000,
			ValidatesArgs: true,
			CustomBackoff: true,
		}, kinds[0])
		assert.Equal(t, KindInfo{
			Kind: "report.daily", Queue: defaultQueue, Priority: defaultPriority, MaxAttempts: defaultMaxAttempts,
		}, kinds[1])
	})

	t.Run("the timeout and the classifier", func(t *testing.T) {
		t.Parallel()
		reg := NewRegistry()
		Register(reg, timedDefaultsWorker{})
		Register(reg, timedWorker{})
		Register(reg, catalogClassifyingWorker{})
		kinds := reg.Kinds()
		require.Len(t, kinds, 3)
		assert.Equal(t, "report.classified", kinds[0].Kind)
		assert.True(t, kinds[0].ClassifiesErrors, "a Classifier classifies its errors")
		assert.Zero(t, kinds[0].TimeoutMs)
		assert.Equal(t, "report.timed", kinds[1].Kind)
		assert.Equal(t, 45_000, kinds[1].TimeoutMs, "a Timeouter stands in for a missing Defaults timeout")
		assert.Equal(t, "report.timed_defaults", kinds[2].Kind)
		assert.Equal(t, 30_000, kinds[2].TimeoutMs, "a Defaults timeout wins over the Timeouter's")
	})

	t.Run("a caller cannot change the catalog", func(t *testing.T) {
		t.Parallel()
		reg := catalogRegistry()
		kinds := reg.Kinds()
		kinds[0].ArgsSchema[0] = '['
		kinds[0].Description = "changed"
		assert.Equal(t, json.RawMessage(signupSchema), reg.Kinds()[0].ArgsSchema)
		assert.Equal(t, "Send the welcome email", reg.Kinds()[0].Description)
	})

	t.Run("an empty registry", func(t *testing.T) {
		t.Parallel()
		assert.Empty(t, NewRegistry().Kinds())
	})
}

// schemaTextWorker returns an args schema that is not JSON.
type schemaTextWorker struct{ plainWorker }

func (schemaTextWorker) Kind() string { return "catalog.schema_text" }

func (schemaTextWorker) ArgsSchema() (json.RawMessage, error) {
	return json.RawMessage(`{"type": "object"`), nil
}

// schemaErrorWorker fails to build its args schema.
type schemaErrorWorker struct{ plainWorker }

func (schemaErrorWorker) Kind() string { return "catalog.schema_error" }

func (schemaErrorWorker) ArgsSchema() (json.RawMessage, error) {
	return nil, errors.New("schema generator failed")
}

// exampleTextWorker returns example args that are not JSON.
type exampleTextWorker struct{ plainWorker }

func (exampleTextWorker) Kind() string { return "catalog.example_text" }

func (exampleTextWorker) ExampleArgs() json.RawMessage { return json.RawMessage(`{"user_id":`) }

// TestAnInvalidSchemaPanicsAtRegistration proves Register refuses a kind whose
// args schema is not JSON or fails to build, or whose example args are not JSON:
// it panics naming the kind and what failed, never echoing the text, and the
// registry does not hold the kind.
func TestAnInvalidSchemaPanicsAtRegistration(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name     string
		register func(reg *Registry)
		kind     string
		message  string
	}{
		{
			name:     "a schema that is not JSON",
			register: func(reg *Registry) { Register(reg, schemaTextWorker{}) },
			kind:     "catalog.schema_text",
			message:  `jobs: args schema for kind "catalog.schema_text" is not valid JSON`,
		},
		{
			name:     "a schema that fails to build",
			register: func(reg *Registry) { Register(reg, schemaErrorWorker{}) },
			kind:     "catalog.schema_error",
			message:  `jobs: args schema for kind "catalog.schema_error": schema generator failed`,
		},
		{
			name:     "example args that are not JSON",
			register: func(reg *Registry) { Register(reg, exampleTextWorker{}) },
			kind:     "catalog.example_text",
			message:  `jobs: example args for kind "catalog.example_text" are not valid JSON`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			reg := NewRegistry()
			require.PanicsWithValue(t, tc.message, func() { tc.register(reg) })
			_, ok := reg.lookup(tc.kind)
			assert.False(t, ok, "the registry does not hold the refused kind")
			assert.Empty(t, reg.Kinds())
		})
	}
}

// TestValidateRefusesArgsThatDoNotDecode proves Validate refuses a kind the
// registry does not register with ErrUnknownKind, and args that do not decode
// into the kind's args type with a *ValidationError on "args" that carries the
// decoder's reason.
func TestValidateRefusesArgsThatDoNotDecode(t *testing.T) {
	t.Parallel()
	reg := catalogRegistry()

	err := reg.Validate("mail.welcom", []byte(`{"user_id":"u1"}`))
	require.ErrorIs(t, err, ErrUnknownKind)
	assert.Contains(t, err.Error(), `"mail.welcom"`, "the error names the kind")

	err = reg.Validate("mail.welcome", []byte(`{"user_id": 7}`))
	require.ErrorIs(t, err, ErrValidation)
	var verr *ValidationError
	require.ErrorAs(t, err, &verr)
	assert.Equal(t, "args", verr.Field)
	assert.Contains(t, verr.Message, "cannot unmarshal number", "the message carries the decode failure")

	err = reg.Validate("report.daily", []byte(`[1, 2]`))
	require.ErrorAs(t, err, &verr, "a kind without a validator still decodes its args")
	assert.Equal(t, "args", verr.Field)
	assert.NoError(t, reg.Validate("report.daily", []byte(`{"user_id":"u1"}`)))
	assert.NoError(t, reg.Validate("report.daily", nil), "empty args decode to the zero value")
}

// TestValidateRunsTheWorkersArgsValidator proves Validate passes the decoded
// args to the worker's ArgsValidator and returns its refusal as a
// *ValidationError on "args"; empty args decode to the zero value and meet it
// too.
func TestValidateRunsTheWorkersArgsValidator(t *testing.T) {
	t.Parallel()
	reg := catalogRegistry()

	for _, raw := range [][]byte{[]byte(`{"user_id": ""}`), nil} {
		err := reg.Validate("mail.welcome", raw)
		var verr *ValidationError
		require.ErrorAs(t, err, &verr, "args %q", raw)
		assert.Equal(t, "args", verr.Field)
		assert.Equal(t, "user_id is required", verr.Message, "the validator's message")
	}
	assert.NoError(t, reg.Validate("mail.welcome", []byte(`{"user_id": "u1"}`)))
}

// TestRegistryEnqueueRefusesAnUnknownKind proves Registry.Enqueue refuses an
// unknown kind and args Validate refuses before anything is written, while
// Enqueue, the unvalidated seed seam, still writes the unknown kind; and that a
// valid enqueue lands with the kind's defaults under the caller's options,
// stamped by the Client's insert hook.
func TestRegistryEnqueueRefusesAnUnknownKind(t *testing.T) {
	t.Parallel()
	db := newDB(t)
	ctx := context.Background()
	reg := catalogRegistry()
	c := NewClientWithOptions(db, ClientOpts{InsertHook: func(context.Context, InsertInfo) map[string]any {
		return map[string]any{"enqueued_by": "operator"}
	}})

	_, err := reg.Enqueue(ctx, c, "mail.welcom", []byte(`{"user_id":"u1"}`), InsertOpts{})
	require.ErrorIs(t, err, ErrUnknownKind)
	assert.EqualValues(t, 0, countJobsOfKind(t, db, "mail.welcom"), "a refused kind writes no row")

	_, err = reg.Enqueue(ctx, c, "mail.welcome", []byte(`{"user_id": 7}`), InsertOpts{})
	require.ErrorIs(t, err, ErrValidation)
	_, err = reg.Enqueue(ctx, c, "mail.welcome", []byte(`{"user_id": ""}`), InsertOpts{})
	require.ErrorIs(t, err, ErrValidation)
	assert.EqualValues(t, 0, countJobsOfKind(t, db, "mail.welcome"), "refused args write no row")

	seeded, err := Enqueue(ctx, c, "mail.welcom", []byte(`{"user_id":"u1"}`), InsertOpts{})
	require.NoError(t, err, "Enqueue stays the unvalidated seed seam")
	assert.NotEmpty(t, seeded)
	assert.EqualValues(t, 1, countJobsOfKind(t, db, "mail.welcom"))

	id, err := reg.Enqueue(ctx, c, "mail.welcome", []byte(`{"user_id":"u1"}`), InsertOpts{Priority: 5})
	require.NoError(t, err)
	assert.Equal(t, routing{Queue: "mail", Priority: 5, MaxAttempts: 4, TimeoutMs: 30_000}, routingOf(t, db, id),
		"the kind's defaults fill what the caller leaves zero, and the caller's priority wins")
	var row jobRow
	require.NoError(t, db.Where("id = ?", id).First(&row).Error)
	assert.JSONEq(t, `{"enqueued_by":"operator"}`, string(row.Metadata), "the Client's hook stamps the job")

	_, err = reg.Enqueue(ctx, c, "mail.welcome", []byte(`{"user_id":"u1"}`), InsertOpts{UniqueKey: "welcome-u1"})
	require.NoError(t, err)
	_, err = reg.Enqueue(ctx, c, "mail.welcome", []byte(`{"user_id":"u1"}`), InsertOpts{UniqueKey: "welcome-u1"})
	var dup *AlreadyEnqueuedError
	require.ErrorAs(t, err, &dup, "a unique-key collision is an *AlreadyEnqueuedError, as Enqueue's is")
}
