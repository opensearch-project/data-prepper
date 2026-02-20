# OTLP Sink Plugin

The OTLP sink plugin sends telemetry data (traces, metrics, and logs) using the OpenTelemetry Protocol (OTLP) format.
The plugin supports exporting to AWS X-Ray and any OTLP Protobuf-compatible endpoint.

---

## Known Limitations

- No support for DQL-based loss-less delivery in this release.
- Only AWS X-Ray-compatible OTLP endpoints are currently supported for traces (`https://xray.<region>.amazonaws.com/v1/traces`).
- For metrics and logs, use standard OTLP endpoints (e.g., `https://otlp-endpoint.example.com/v1/metrics` or `/v1/logs`).
- Only OTLP over HTTP is supported; gRPC is not yet supported.

---

## Batching Behavior with Mixed Signal Types

When a single OTLP sink receives mixed signal types (logs, metrics, and traces), the sink automatically flushes the current batch whenever the signal type changes. This ensures that each batch contains only one signal type, which is required by the OTLP protocol.

### Batching Limitation

If your pipeline produces events in mixed order (e.g., log → trace → log → metric), batching efficiency may be reduced because:
- Each signal type change triggers an immediate flush
- Batches may not reach the configured `max_events` or `max_batch_size` thresholds
- More frequent HTTP requests result in higher overhead

### Recommendation: Use Conditional Routing for Optimal Batching

For pipelines with mixed signal types, we recommend using Data Prepper's conditional routing feature to separate signals into dedicated sink instances. This allows each sink to batch events of the same type efficiently without forced flushes.

**Example Configuration:**

```yaml
mixed_signal_pipeline:
  source:
    # Source that produces logs, metrics, and traces
    http:
      path: "/v1/data"

  routes:
    - logs: 'getEventType() == "LOG"'
    - traces: 'getEventType() == "TRACE"'
    - metrics: 'getEventType() == "METRIC"'

  sink:
    - otlp:
        endpoint: "https://otlp-collector.example.com/v1/logs"
        routes:
          - logs
        threshold:
          max_events: 512
          max_batch_size: 1mb
          flush_timeout: 200ms
        aws: { }

    - otlp:
        endpoint: "https://otlp-collector.example.com/v1/traces"
        routes:
          - traces
        threshold:
          max_events: 512
          max_batch_size: 1mb
          flush_timeout: 200ms
        aws: { }

    - otlp:
        endpoint: "https://otlp-collector.example.com/v1/metrics"
        routes:
          - metrics
        threshold:
          max_events: 512
          max_batch_size: 1mb
          flush_timeout: 200ms
        aws: { }
```

**Benefits of this approach:**
- Each sink receives only one signal type, maximizing batch efficiency
- No forced flushes due to signal type changes
- Better throughput and reduced HTTP request overhead
- Ability to configure different batching thresholds per signal type if needed

For more information on conditional routing, see the [Data Prepper Expression Syntax](https://github.com/opensearch-project/data-prepper/blob/main/docs/expression_syntax.md) documentation.

---

## Sample Pipeline Configuration

### Traces to AWS X-Ray (Minimal Configuration)

Use this when Data Prepper has permission to write to AWS X-Ray directly.

```yaml
trace_pipeline:
  source:
    otel_trace_source:

  sink:
    - otlp:
        endpoint: "https://xray.us-west-2.amazonaws.com/v1/traces"
        aws: { }
```

### Metrics to OTLP Endpoint

```yaml
metrics_pipeline:
  source:
    otel_metrics_source:

  sink:
    - otlp:
        endpoint: "https://otlp-collector.example.com/v1/metrics"
        aws: { }
```

### Logs to OTLP Endpoint

```yaml
logs_pipeline:
  source:
    otel_logs_source:

  sink:
    - otlp:
        endpoint: "https://otlp-collector.example.com/v1/logs"
        aws: { }

### Full Configuration with STS (Traces)

Use this when assuming a cross-account role is required.

```yaml
trace_pipeline:
  workers: 2

  source:
    otel_trace_source:
      ssl: false
      port: 21890

  buffer:
    bounded_blocking:
      buffer_size: 1000000
      batch_size: 125000

  sink:
    - otlp:
        endpoint: "https://xray.us-west-2.amazonaws.com/v1/traces"
        max_retries: 5
        threshold:
          max_events: 512
          max_batch_size: 1mb
          flush_timeout: 200ms
        aws:
          sts_role_arn: arn:aws:iam::123456789012:role/MyRole
          sts_external_id: external-id-value
```

### Multi-Signal Pipeline

You can use the same sink configuration for different signal types by adjusting the endpoint:

```yaml
trace_pipeline:
  source:
    otel_trace_source:
  sink:
    - otlp:
        endpoint: "https://xray.us-west-2.amazonaws.com/v1/traces"
        aws: { }

metrics_pipeline:
  source:
    otel_metrics_source:
  sink:
    - otlp:
        endpoint: "https://otlp-collector.example.com/v1/metrics"
        aws: { }

logs_pipeline:
  source:
    otel_logs_source:
  sink:
    - otlp:
        endpoint: "https://otlp-collector.example.com/v1/logs"
        aws: { }
```

---

## Configuration Options

| Property                   | Type     | Required | Default               | Description                                                                                                    |
|----------------------------|----------|----------|-----------------------|----------------------------------------------------------------------------------------------------------------|
| `endpoint`                 | `String` | Yes      | —                     | OTLP endpoint where telemetry data will be sent. Must include signal type path (e.g., `/v1/traces`, `/v1/metrics`, `/v1/logs`). |
| `max_retries`              | `int`    | No       | `5`                   | Maximum number of retry attempts on HTTP send failures.                                                        |
| **threshold**              | `Object` | No       | —                     | Controls batching behavior. See below for sub-properties.                                                      |
| `threshold.max_events`     | `int`    | No       | `512` (recommended)   | Maximum number of events (spans/metrics/logs) per batch. Use `0` to disable count-based flushing. Must be ≥ 0. |
| `threshold.max_batch_size` | `String` | No       | `1mb` (recommended)   | Maximum total payload bytes per batch. Supports human-readable suffixes (`kb`, `mb`).                          |   
| `threshold.flush_timeout`  | `String` | No       | `200ms` (recommended) | Maximum time to wait before flushing a non-empty batch. Minimum: 1ms (e.g., `200ms`, `1s`)                     |
| **aws**                    | `Object` | Yes      | —                     | AWS authentication settings. Use `{}` if no STS role is needed. See below.                                     |
| `aws.sts_role_arn`         | `String` | No       | —                     | IAM Role ARN that Data Prepper (or OSI) assumes to send data on behalf of a customer account.                  |
| `aws.sts_external_id`      | `String` | No       | —                     | External ID to use when assuming the role. Required only if the target IAM role enforces sts:ExternalId.       |

**Additional Notes:**

- `aws.region` is automatically derived from the endpoint.

---

## Performance Benchmark

### Summary (Traces)

* Sustains ~3.5K TPS with ≤150ms p99 latency on t4g.large.
* Uses only ~8% CPU, ~100MB heap.
* 0 errors, retries, or drops during a 3-hour soak test.

**Note:** Performance characteristics for metrics and logs may vary based on payload size and structure. The tuning recommendations below apply to all signal types.

### Tuning Recommendations

| Setting          | Recommended | Reason                                                                                          |
|------------------|-------------|-------------------------------------------------------------------------------------------------|
| `max_retries`    | `5`         | Matches AWS SDK default. Gives ~8s of exponential backoff to tolerate transient 503/5xx errors. |
| `max_events`     | `512`       | Supports up to 3.5K TPS with 2 workers. Keeps p99 latency around 130ms.                         |
| `max_batch_size` | `1mb`       | Aligns with OTEL + AWS X-Ray guidance. Larger batches get split, increasing latency/load.       |
| `flush_timeout`  | `200ms`     | Short enough to avoid delay, long enough to fill batches and keep CPU/GCs low.                  |

**Note:** These recommendations are based on trace data. Adjust based on your specific signal type and payload characteristics.

### Additional Tuning tips

* Lower `max_events` to **200–400** to reduce latency below 100 ms
* Decrease `flush_timeout` to **100 ms** for faster flushes (with higher CPU/network cost)
* Increase `max_batch_size` to **≥ 8 MB** only if p99 span > 9 KB
* Add pipeline workers if queue saturates at >4K TPS

### Queue Sizing Rule

> Queue capacity = max_events * 10 (minimum 2000)
>
> To keep memory usage under ~50MB:  
> max_events ≤ 50_000_000 ÷ (10 × p99_span_size_bytes)
>
> Example: With p99 span size of 1 KB, max_events should be ≤ 5000


---

## Protocol Details

* Protocol: OTLP over HTTP
* Content-Type: `application/x-protobuf`
* Compression: `gzip` (enabled by default)  
  All outgoing HTTP requests use gzip compression to reduce payload size and bandwidth usage.

---

## Delivery Semantics

Currently, the sink provides at-most-once delivery. Once retries are exhausted, span batches are dropped.
Future releases will support durable queueing via DQL for loss-less guarantees.

---

## Retry Behavior

- The sink uses an exponential backoff with jitter strategy for retryable HTTP status codes (e.g., 429, 502, 503, 504).
- Maximum number of attempts is controlled by `max_retries`. Once exceeded:
     - The span batch is dropped.
     - The plugin logs the exception and increments the error metric.
- Non-retryable errors (e.g., 400, 403) are logged and counted immediately without retry.
- Retry logic follows
  the [OTLP/HTTP response specification](https://opentelemetry.io/docs/specs/otlp/#otlphttp-response).
- `Retry-After` header is not used for dynamic backoff because:
  - Armeria’s retry rule API only supports boolean conditions or fixed `Backoff` strategies.
  - Supporting `Retry-After` would require a custom `Backoff` implementation, adding unnecessary complexity.
  - The exponential backoff already handles common retry intervals effectively.
---

## Logging & Metrics

* Exceptions are logged with full stack traces. No customer data is logged.
* Metrics are emitted via Micrometer and include:
     * recordsIn, recordsOut (applies to all signal types: traces, metrics, logs)
     * httpLatency, HTTP codes
     * errorCount, rejectedRecordsCount, failedRecordsCount, retriesCount (applies to all signal types)
     * queueSize, queueCapacity
     * payloadSize, payloadGzipSize
     * JVM stats if configured (e.g., heap usage, GC pauses)

---

## Developer Guide

See the [CONTRIBUTING](https://github.com/opensearch-project/data-prepper/blob/main/CONTRIBUTING.md) guide for general information on contributions.

### Run unit tests locally

```bash
./gradlew :data-prepper-plugins:otlp-sink:test
```
