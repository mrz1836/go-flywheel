// Package flywheeltest provides ready-made test fixtures for code that embeds the
// flywheel job runtime.
//
// Testing a background-job integration usually means standing up a database with
// the runtime schema applied, enqueueing work, and waiting for a job to reach a
// terminal state. This package packages those steps so a consumer's tests can do
// them in one line each, against a real (in-memory or file-backed) SQLite database
// or an isolated PostgreSQL schema, with no bespoke harness of their own.
//
// # Databases
//
//   - [NewBareSQLite] opens an empty in-memory SQLite database with no schema.
//   - [NewDB] opens an in-memory SQLite database with the full runtime schema
//     migrated in — the usual starting point for a unit test.
//   - [NewWALFileDB] opens a file-backed SQLite database in WAL mode, the shape a
//     runner needs so it can write while a test polls for progress.
//   - [NewPostgresIsolatedDB] mints a fresh, uniquely named PostgreSQL schema with
//     the runtime schema migrated in, dropped on cleanup, so parallel tests do not
//     collide. It skips (or, under FLYWHEEL_REQUIRE_POSTGRES, fails) when no test
//     database is configured; see [RequirePostgresDSN].
//
// # Assertions
//
//   - [JobState] reads a job's current state column.
//   - [WaitForJobState] polls until a job reaches an expected state or a timeout
//     elapses, failing the test with the last observed state on timeout.
//
// # Helpers
//
//   - [FreeAddr] reserves an ephemeral loopback address for a server under test.
//   - [InstallPeriodic] seeds a due periodic definition directly, so a scheduler
//     test has something to fire.
//   - [SuccessArgs] and [SuccessWorker] are a trivial always-succeeding job type and
//     worker, handy for exercising a runner end to end.
//
// Every database helper registers its own cleanup, so a test never has to close a
// connection or drop a schema by hand. The fixtures take a [testing.TB], so they
// work from tests, benchmarks, and subtests alike.
//
// See the package examples for the common enqueue-and-wait flow through the public
// flywheel API.
package flywheeltest
