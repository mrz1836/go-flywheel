// Package observers provides ready-made, dependency-free implementations of
// flywheel.Observer — the telemetry seam the Runner invokes around each attempt —
// so a consumer gets metrics, structured logs, and a Prometheus endpoint without
// hand-rolling and wiring an adapter first.
//
// The package depends only on the standard library and the flywheel root package,
// so importing it adds no metrics or tracing dependency to a consumer. The
// boundary that does carry a third-party stack is MetricsRecorder: a one-method
// sink a consumer implements against Prometheus, OpenTelemetry, statsd, or
// CloudWatch. flywheel itself imports none of them — MemRecorder, the in-memory
// reference recorder, backs the local `/metrics` endpoint with no external system.
//
// The three building blocks compose:
//
//   - MetricsObserver translates lifecycle events into MetricsRecorder calls (the
//     metric taxonomy lives on MetricsObserver).
//   - SlogObserver logs each event at debug level for `--log debug` diagnosis.
//   - Multi fans one event out to several observers, so a Node can run metrics and
//     logging side by side: NewMulti(NewSlog(logger), NewMetrics(rec)).
//
// WritePrometheus and MetricsHandler render a MemRecorder's counters plus a
// freshly sampled flywheel.QueueHealth into the Prometheus text-exposition format.
//
// Every method runs synchronously on the dispatch path and must not block, the
// same contract as flywheel.Observer: MemRecorder takes a short mutex-guarded map
// write and SlogObserver defaults to debug, so neither stalls a worker.
package observers
