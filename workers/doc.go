// Package workers provides ready-made flywheel workers for the cases that need no
// custom Go:
//
//   - ExecWorker runs any command or binary.
//   - ShellWorker runs a shell script — a .sh file or an inline snippet.
//   - PythonWorker runs a Python script, module, or snippet.
//   - MageWorker runs magex/mage build targets.
//   - HTTPWorker calls a URL.
//
// Every worker implements flywheel.Worker[A], records its result and timing into
// the job_runs audit trail, and drives flywheel's retry ladder on failure — so a
// crontab entry becomes a durable, retried, observable job with full history. The
// command-running workers (Exec, Shell, Python, Mage) share one execution engine,
// so they capture output, honor timeouts, inject an env allowlist, and classify
// failures identically; they differ only in how they build the command line and in
// the kind recorded on each run.
//
// The package depends only on the standard library and the flywheel root package,
// so importing it adds no new dependency to a consumer. The command workers run the
// exact command they assemble and HTTPWorker calls the exact URL it is handed:
// validating those inputs (command allowlists, SSRF protection) is the operator's
// responsibility, the same contract as flywheel.DefaultHTTPDoer.
package workers

// defaultMaxOutputBytes caps captured stdout/stderr (the command workers) and
// response bodies (HTTPWorker) stored in the audit trail.
const defaultMaxOutputBytes = 64 * 1024
