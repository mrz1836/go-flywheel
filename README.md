# go-flywheel

Durable, Postgres- and SQLite-backed job runtime for Go: typed workers, scheduler, retries with backoff, lease-based recovery, and per-run audit.

## Schema setup

`go-flywheel` owns its three tables (`jobs`, `job_runs`, `job_periodics`) and ships them via a single exported entry point:

```go
import "github.com/mrz1836/go-flywheel"

if err := flywheel.Migrate(db); err != nil { // db is a *gorm.DB
    return err
}
```

`Migrate` runs `AutoMigrate` over the row structs and then reconciles the partial/unique indexes GORM cannot express from struct tags — including the correctness-bearing `jobs_unique_key` partial unique index that enforces enqueue idempotency. It is idempotent (`AutoMigrate` no-op + `CREATE INDEX IF NOT EXISTS`), so repeated calls are safe.

It supports two consumption modes:

- **Standalone** — call `Migrate(db)` against a bare SQLite or PostgreSQL database and the runtime stands up its own schema with no external migration tooling.
- **Embedded** — call `Migrate(db)` as one step of a host project's install/migration process.

### No hard Atlas dependency

The module takes **no** dependency on Atlas (or any external migration tool). A host that prefers versioned SQL — e.g. an Atlas / `atlas-provider-gorm` flow — can point its loader at `flywheel.Models()`, which exposes the runtime's row structs as a single source of truth, and generate migrations from there instead of calling `Migrate`.

Only PostgreSQL and SQLite are supported, because both express the partial indexes the runtime relies on; `Migrate` returns an error for any other dialect rather than silently dropping idempotency.
