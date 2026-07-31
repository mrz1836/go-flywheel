# Example dashboards

A starting-point Grafana dashboard for the runtime's Prometheus exposition. It is an example, not a
product surface — copy it, rename the `uid`, and adapt the panels to your alerting.

## Importing

1. Serve the runtime's metrics: wire `observers.NewMetrics(rec)` into your runner (and scheduler) and
   expose `observers.MetricsHandler(rec, sample)` on your metrics address. See the reading-the-metrics
   section of [`../RUNBOOK.md`](../RUNBOOK.md).
2. Point Prometheus at that `/metrics` endpoint.
3. In Grafana, **Dashboards → New → Import**, upload [`flywheel-overview.json`](flywheel-overview.json),
   and select your Prometheus data source when prompted for `DS_PROMETHEUS`.

## What it shows

| Panel | Series | Question it answers |
|---|---|---|
| Claim latency | `flywheel_claim_duration_seconds` | Are executors contending for rows? |
| Finalize latency | `flywheel_finalize_duration_seconds` | Is outcome persistence slow (a database problem)? |
| Worker duration by kind | `flywheel_job_duration_seconds` | Which kind's work is slow (a downstream problem)? |
| Sweep latency | `flywheel_sweep_duration_seconds` | Is stuck-lease recovery keeping up? |
| Queue lag | `flywheel_queue_oldest_ready_seconds` | Are the runners falling behind (under-capacity)? |
| Ready vs in-flight | `flywheel_queue_ready`, `flywheel_queue_inflight` | Is work arriving faster than it drains? |
| Jobs by state | `flywheel_queue_jobs` | Is failure permanent (discarded) or transient (retryable)? |
| Supersede / retry / error rates | `flywheel_jobs_superseded_total`, `_retried_total`, `_errored_total` | Is work being executed twice? |
| Dropped series | `flywheel_metrics_dropped_series_total` | Did an unbounded tag exhaust the series budget? |

The latency panels compute percentiles with `histogram_quantile` over the `_bucket` series, so their
accuracy is bounded by the recorder's bucket boundaries (`observers.DefaultLatencyBuckets` by default).
Widen or refine the buckets with `observers.NewMemRecorderWithConfig` when your latencies sit in a
different range.
