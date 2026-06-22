// Package workers provides ready-made flywheel workers for the cases that need
// no custom Go: ExecWorker runs a shell command or binary, and HTTPWorker calls a
// URL. Both implement flywheel.Worker[A], record their result and timing into the
// job_runs audit trail, and drive flywheel's retry ladder on failure — so a
// crontab entry becomes a durable, retried, observable job with full history.
//
// The package depends only on the standard library and the flywheel root package,
// so importing it adds no new dependency to a consumer. ExecWorker runs the exact
// command it is handed and HTTPWorker calls the exact URL it is handed: validating
// those inputs (command allowlists, SSRF protection) is the operator's
// responsibility, the same contract as flywheel.DefaultHTTPDoer.
package workers

// defaultMaxOutputBytes caps captured stdout/stderr (ExecWorker) and response
// bodies (HTTPWorker) stored in the audit trail.
const defaultMaxOutputBytes = 64 * 1024
