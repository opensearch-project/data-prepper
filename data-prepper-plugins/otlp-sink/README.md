# OTLP Sink Plugin

The OTLP sink plugin sends span data using the OpenTelemetry Protocol (OTLP) format.
The initial release supports exporting spans to AWS X-Ray. Future releases will support sending spans, metrics, and logs
to any OTLP Protobuf-compatible endpoint.

---

## Known Limitations

- Currently, supports trace and log data. Support for metrics will be added in future releases.
- No support for DQL-based loss-less delivery in this release.
- Only OTLP over HTTP is supported; gRPC is not yet supported.

---

## Sample Pipeline Configuration

### Minimal Configuration (No STS)

Use this when Data Prepper has permission to write to AWS X-Ray directly.

```yaml
otlp_pipeline:
  source:
    otel_trace_source:

  sink:
    - otlp:
        endpoint: "https://xray.us-west-2.amazonaws.com/v1/traces"
        aws: { }
```

### Full Configuration with STS

Use this when assuming a cross-account role is required.

```yaml
otlp_pipeline:
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

### Log Pipeline Configuration

Use this to send log events to an OTLP-compatible log ingestion endpoint.

```yaml
log_pipeline:
  source:
    http:
      port: 2021
      path: "/logs"

  sink:
    - otlp:
        endpoint: "https://my-otlp-endpoint.example.com:443/v1/logs"
        signal_type: logs
        max_retries: 3
        threshold:
          max_events: 100
          flush_timeout: 5s
        aws:
          service_name: my-service
          region: us-east-1
          sts_role_arn: arn:aws:iam::123456789012:role/MyRole
        additional_headers:
          x-custom-header: my-value
```

---

## Configuration Options

| Property                   | Type              | Required | Default               | Description                                                                                              |
|----------------------------|-------------------|----------|-----------------------|----------------------------------------------------------------------------------------------------------|
| `endpoint`                 | `String`          | Yes      | —                     | OTLP endpoint where data will be sent.                                                                   |
| `signal_type`              | `String`          | No       | `traces`              | Signal type to send: `traces` or `logs`.                                                                 |
| `max_retries`              | `int`             | No       | `5`                   | Maximum number of retry attempts on HTTP send failures.                                                  |
| **threshold**              | `Object`          | No       | —                     | Controls batching behavior. See below for sub-properties.                                                |
| `threshold.max_events`     | `int`             | No       | `512` (recommended)   | Maximum number of events per batch. Use `0` to disable count-based flushing. Must be ≥ 0.                |
| `threshold.max_batch_size` | `String`          | No       | `1mb` (recommended)   | Maximum total payload bytes per batch. Supports human-readable suffixes (`kb`, `mb`).                    |   
| `threshold.flush_timeout`  | `String`          | No       | `200ms` (recommended) | Maximum time to wait before flushing a non-empty batch. Minimum: 1ms (e.g., `200ms`, `1s`)               |
| **aws**                    | `Object`          | Yes      | —                     | AWS authentication settings. Use `{}` if no STS role is needed. See below.                               |
| `aws.sts_role_arn`         | `String`          | No       | —                     | IAM Role ARN that Data Prepper assumes to send data to the endpoint on behalf of a customer account.     |
| `aws.sts_external_id`      | `String`          | No       | —                     | External ID to use when assuming the role. Required only if the target IAM role enforces sts:ExternalId. |
| `aws.region`               | `String`          | No       | parsed from endpoint  | AWS region for SigV4 signing. Falls back to parsing from the endpoint URL if not set.                    |
| `aws.service_name`         | `String`          | No       | `xray`                | SigV4 signing service name. Override for non-X-Ray OTLP endpoints.                                      |
| `additional_headers`       | `Map<String,String>` | No    | `{}`                  | Custom HTTP headers added to every outgoing request after SigV4 signing.                                 |

**Additional Notes:**

- `aws.region` can be set explicitly or is automatically derived from the endpoint. Explicit `aws.region` takes precedence.
- `aws.service_name` defaults to `xray` for backward compatibility with existing X-Ray trace pipelines.
- `additional_headers` are injected after SigV4 signing, so they are not included in the signature.

---

## Performance Benchmark

### Summary

* Sustains ~3.5K TPS with ≤150ms p99 latency on t4g.large.
* Uses only ~8% CPU, ~100MB heap.
* 0 errors, retries, or drops during a 3-hour soak test.

### Tuning Recommendations

| Setting          | Recommended | Reason                                                                                          |
|------------------|-------------|-------------------------------------------------------------------------------------------------|
| `max_retries`    | `5`         | Matches AWS SDK default. Gives ~8s of exponential backoff to tolerate transient 503/5xx errors. |
| `max_events`     | `512`       | Supports up to 3.5K TPS with 2 workers. Keeps p99 latency around 130ms.                         |
| `max_batch_size` | `1mb`       | Aligns with OTEL + AWS X-Ray guidance. Larger batches get split, increasing latency/load.       |
| `flush_timeout`  | `200ms`     | Short enough to avoid delay, long enough to fill batches and keep CPU/GCs low.                  |

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
     * recordsIn, recordsOut
     * httpLatency, HTTP codes
  * errorCount, rejectedSpansCount, failedSpansCount, retriesCount
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
