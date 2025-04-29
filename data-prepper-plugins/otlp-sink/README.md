# OTLP Sink Plugin

In the first release, the otlp sink plugin sends span data to AWS X-Ray using the OTLP (OpenTelemetry Protocol) format.
Future releases will enhance the sink to send spans, metrics, and traces to any OTLP Protobuf endpoints.

---

## Usage

### Sample Pipeline Configuration

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
        endpoint: "https://performance.us-west-2.xray.cloudwatch.aws.dev/v1/traces"
        max_retries: 5          # Optional, default: 5
        threshold:
          max_events: 512       # Optional, default: 512
          max_batch_size: 1mb   # Optional, default: 1mb
          flush_timeout: 200ms  # Optional, default: 200ms
        aws:
          sts_role_arn: arn:aws:iam::123456789012:role/MyRole     # Optional STS Role ARN
          sts_external_id: external-id-value                      # Optional external ID for STS
```

---

## Configuration Options

| Property                   | Type     | Required | Default | Description                                                                           |
|----------------------------|----------|----------|---------|---------------------------------------------------------------------------------------|
| `endpoint`                 | `String` | Yes      | —       | OTLP gRPC or HTTP endpoint where spans will be sent.                                  |
| `max_retries`              | `int`    | No       | `5`     | Maximum number of retry attempts on HTTP send failures.                               |
| **threshold**              | `Object` | No       | —       | Controls batching behavior. See below for sub-properties.                             |
| `threshold.max_events`     | `int`    | No       | `512`   | Maximum number of spans per batch.                                                    |
| `threshold.max_batch_size` | `String` | No       | `1mb`   | Maximum total payload bytes per batch. Supports human-readable suffixes (`kb`, `mb`). |
| `threshold.flush_timeout`  | `String` | No       | `200ms` | Time to wait (in milliseconds) before flushing a non-empty batch.                     |
| **aws**                    | `Object` | No       | —       | AWS authentication settings. See below.                                               |
| `aws.sts_role_arn`         | `String` | No       | —       | Amazon Resource Name of the IAM role to assume.                                       |
| `aws.sts_external_id`      | `String` | No       | —       | Optional external ID for assuming IAM roles with STS.                                 |

**Note:** `aws.region` will be derived from the provided AWS endpoint.

---

## Developer Guide

See the [CONTRIBUTING](https://github.com/opensearch-project/data-prepper/blob/main/CONTRIBUTING.md) guide for general information on contributions.

The integration tests for this plugin do **not** run as part of the main Data Prepper build and will be added in future
releases.

### Run unit tests locally

```bash
./gradlew :data-prepper-plugins:otlp-sink:test
```

### Run integration tests locally

```bash
./gradlew :data-prepper-plugins:otlp-sink:integrationTest
```
