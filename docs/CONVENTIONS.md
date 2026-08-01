# Conventions

The rules a contributor inherits rather than re-derives. Each is a pattern the codebase already holds
across its whole surface; this file states it once so the next change extends it instead of inventing a
sibling. None of these is a matter of taste — each has a reason, and the reason is what tells you when a
new case belongs under the rule and when it is genuinely different.

<br>

## Error and log prefixes: `jobs:` and `flywheel:`

Every error string and log message carries one of two prefixes, and which one is not a coin flip — it
names *what kind of operation is speaking*.

- **`jobs:`** is the hot runtime path a job travels: the driver's claim / enqueue / finalize, the
  runner's dispatch and its poll-backoff, limiter-gate, and heartbeat log lines, the scheduler's lease
  sweep, and the sentinels a job's own lifecycle raises (`ErrAlreadyEnqueued`, `ErrUnknownKind`,
  `ErrMissingKind`, `ErrSQLiteConcurrency`, the runner/scheduler config-validation errors, the
  worker-panic error).
- **`flywheel:`** is everything out of band of that path — the library and schema operations a host runs
  around the queue rather than through it: `Migrate` / `InstallIndexes` / `InstallStorageParameters`,
  the audit and health reads, `SeedRun`, `Replay*`, retention, the limiter's own construction and
  database sweep, driver and pragma construction, and the library-level sentinels (`ErrValidation`,
  `ErrUnsupportedDialect`, `ErrIndexDrift`, `ErrSQLitePragma`, `ErrFollowUpLimit`, `ErrBarrierTooWide`).

The dividing line is the operation, not the file: a file that does both (`internal/core/errors.go`,
`internal/core/scheduler.go`, `internal/core/driver_sqlite.go`) carries both prefixes, one per operation.
When you add a message, ask which of the
two an operator would `grep` for — a job stuck in the runtime, or a schema/admin task — and match it.

**These strings are an observable contract, not decoration.** Tests match on the literal prefix and
operators `grep` production logs by it. Change one deliberately, as you would any observable behavior —
never as a tidy-up. A rename that touches the strings and their matchers is a feature change with its
own release note, not polish.

<br>

## Sentinel errors: `Err` + PascalCase

Every exported error value is a package-level `var` named `Err` followed by PascalCase (`ErrJobTerminal`,
`ErrReplayUnbounded`, `ErrSQLitePragma`), constructed with `errors.New` or a wrapping `fmt.Errorf`, and
documented with a comment that opens with its name and says what condition it reports. Callers match with
`errors.Is`; the message prefix follows the rule above. An unexported sentinel used only within the
package is the same shape with a lowercase `err` lead (`errRunnerNeedsDB`).

<br>

## Optional configuration: `…WithOptions` plus a `…Opts` struct

A call that takes options ships as a pair: a plain constructor with the common signature and a
`…WithOptions` variant that takes a `…Opts` struct — `RetryJob` / `RetryJobWithOptions` + `RetryOpts`,
`Migrate` / `MigrateWithOptions` + `MigrateOpts`, `InstallIndexes` / `InstallIndexesWithOptions` +
`IndexOpts`, `NewSQLiteDriver` / `NewSQLiteDriverWithOptions` + `SQLiteOpts`,
`DeleteFinishedJobs` / `DeleteFinishedJobsWithOptions` + `RetentionOpts`.

**The `…Opts` struct documents its zero value in a sentence.** A reader must be able to tell what
`Opts{}` does without reading the constructor — every field is optional and the zero value is the safe,
common default, stated in the struct's doc comment (`RetentionOpts`, `IndexOpts`, `DriverOpts`,
`MigrateOpts`, `RetryOpts`, `SQLiteOpts` all carry the line). The plain constructor is exactly the
`…WithOptions` call with the zero struct.

<br>

## The Postgres test mirror: `*_pg_test.go`, `Test…Postgres`

A behavior that must be proven against real PostgreSQL — anything depending on `FOR UPDATE SKIP LOCKED`,
a partial index's plan, a real concurrent claim, or a server-side count — lives in a `*_pg_test.go` file
in a function suffixed `Postgres` (`TestParentScopedControlsPostgres`,
`TestInstallStorageParametersReachesAHostOwnedSchemaPostgres`). These run under `-tags=integration`
against `FLYWHEEL_TEST_DATABASE_URL`; the SQLite-backed unit test of the same behavior keeps the plain
name and runs everywhere. The suffix is what lets the two suites be selected apart.

<br>

## No local index DDL in tests — source it from `IndexSet`

A test that needs the runtime's indexes reads them from `flywheel.IndexSet(dialect)`, never a
hand-copied `CREATE INDEX` string. A literal copy is a second definition that drifts silently from the
one the runtime installs — exactly the failure mode the definition-level reconciliation exists to catch,
reintroduced in the test that is meant to guard against it. Source the DDL from the one place that owns
it, and a re-keyed index updates every test that asserts on it for free.

<br>

## Retained public seams

An exported symbol the runtime no longer routes through in production is **kept and justified, not
silently left to rot**. Removing it from a public interface is a breaking change; keeping it without a
note leaves the next reader to guess whether it is load-bearing or dead. `InsertChild` on the `Driver`
interface is the archetype: production's finalize fan-out uses the shared chunk primitive, but the
single-row method stays on the interface with a comment saying so and why. When you supersede a public
seam, leave the same: a comment stating that production routes around it and what replaced it, so the
decision to keep it reads as a decision.

The same discipline covers the rest of the seams a change leaves behind. A `//nolint` directive carries
a site-specific reason after the `//` — never a bare suppression — and is removed when the reason no
longer applies. Dead unexported code is deleted, not commented out; the `unused` linter is the gate that
keeps this honest, so nothing dead survives a green lint.

<br>

## Verifying cohesion

Cohesion is a property you prove, not eyeball. It has two layers.

**The fast, doc-focused gate — `scripts/cohesion-check.sh`.** Run it from anywhere; it resolves every
relative link and `#anchor` in `README.md` and `docs/*.md` (there is no one-liner for the anchor half —
it computes GitHub heading slugs), re-runs the README voice grep and the hygiene grep, renders godoc per
package, and builds the examples. It encodes two `go doc` traps, because both look right and neither
works:

- `go doc ./...` is invalid — `go doc` rejects the wildcard (*too many periods*). Use a per-package
  loop: `go doc .` (the facade), then `go doc ./internal/core` and `./internal/node` (the implementation
  behind it), then `./config`, `./observers`, `./workers`, `./cmd/flywheel`.
- `go doc` has no build-tag support (no `-tags` flag, and `GOFLAGS=-tags` is ignored), so it cannot
  reach a package that is entirely behind a build tag. The `loadtest` package is proven with
  `go build -tags=loadtest ./loadtest/...` instead.

```bash
./scripts/cohesion-check.sh                        # links + voice/hygiene greps + godoc + examples + lint
COHESION_SKIP_LINT=1 ./scripts/cohesion-check.sh   # skip the slow golangci-lint passes
```

**The full gate — `magex` is the authority.** The doc gate does not replace the build/test gate CI runs:

```bash
magex format:fix && magex lint && magex vet && magex test    # then: magex test:race
# Lint under every build tag (what magex lint covers):
golangci-lint run ./...
golangci-lint run --build-tags=integration ./...
golangci-lint run --build-tags=loadtest ./loadtest/...
# Integration suite against a local PostgreSQL:
export FLYWHEEL_TEST_DATABASE_URL="postgres://$USER@localhost:5432/flywheel_test?sslmode=disable"
export FLYWHEEL_REQUIRE_POSTGRES=1
go test -tags=integration -race -count=1 ./...
# The secret scan CI runs — gitleaks specifically, not the full `go-pre-commit run lint`,
# which fails on this repo by a known pre-existing rss_*.go GOOS collision, unrelated to any change:
go-pre-commit run gitleaks --all-files
```

After `magex format:fix`, check `git status` and revert any reformatted committed JSON or snapshot churn
that is not part of your change — it reformats benchmark reports it should not touch.
