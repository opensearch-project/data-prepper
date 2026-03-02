# Prometheus Sink

This is the Data Prepper Prometheus sink plugin that sends metric data to Prometheus Remote Write endpoints. It supports both open source Prometheus and AWS Managed Prometheus (AMP).

## Usages

The Prometheus sink should be configured as part of a Data Prepper pipeline YAML file.

### Open Source Prometheus (No Auth)

To use with a vanilla Prometheus instance, provide an `http://` or `https://` URL. No `aws` block is needed.

Prometheus must be started with the `--web.enable-remote-write-receiver` flag.

```yaml
pipeline:
  ...
  sink:
    - prometheus:
        url: "http://localhost:9090/api/v1/write"
        threshold:
          max_events: 500
          flush_interval: 5s
```

### Open Source Prometheus with Basic Auth

To authenticate with HTTP Basic credentials (e.g., when Prometheus is behind a reverse proxy with basic auth):

```yaml
pipeline:
  ...
  sink:
    - prometheus:
        url: "http://localhost:9090/api/v1/write"
        authentication:
          http_basic:
            username: "promuser"
            password: "prompass"
```

### AWS Managed Prometheus (AMP)

To use with AMP, provide the `aws` configuration block. An `https://` URL is required when using AWS authentication.

```yaml
pipeline:
  ...
  sink:
    - prometheus:
        url: "https://aps-workspaces.us-east-2.amazonaws.com/workspaces/ws-xxxxxxxx-xxxx/api/v1/remote_write"
        aws:
          region: "us-east-2"
          sts_role_arn: "arn:aws:iam::123456789012:role/data-prepper-prometheus-role"
        threshold:
          max_events: 500
          flush_interval: 5s
```

### Response Status

* `200`: The request data has been successfully pushed to the endpoint.
* `400`: Bad request error.
* `404`: The endpoint is not reachable.
* `429`: Too many requests (retried automatically).
* `500`: Internal server error.
* `502`, `503`, `504`: Server errors (retried automatically).

## Configuration

### Required

| Option | Description |
|--------|-------------|
| `url` | The Prometheus Remote Write endpoint URL. Supports `http://` and `https://` schemes. When `aws` is configured, `https://` is required. |

### Optional

| Option | Default | Description |
|--------|---------|-------------|
| `aws` | `null` | AWS configuration for SigV4 signing. When present, requests are signed with AWS credentials. See [AWS Configuration](#aws-configuration). |
| `authentication` | `null` | HTTP Basic authentication credentials. See [Authentication](#authentication). Cannot be used with `aws`. |
| `encoding` | `snappy` | Compression encoding. Currently only `snappy` is supported. |
| `content_type` | `application/x-protobuf` | Content type of the request body. Currently only `application/x-protobuf` is supported. |
| `remote_write_version` | `0.1.0` | Prometheus Remote Write protocol version. Currently only `0.1.0` is supported. |
| `max_retries` | `5` | Maximum number of retry attempts for failed requests. Uses exponential backoff with jitter on retryable status codes (429, 502, 503, 504). |
| `request_timeout` | `60s` | HTTP request timeout. Must be between 1s and 600s. |
| `connection_timeout` | `60s` | TCP connection timeout. Must be between 1s and 600s. |
| `idle_timeout` | `60s` | Connection idle timeout. Must be between 1s and 600s. |
| `out_of_order_time_window` | `10s` | Time window for handling out-of-order metric samples. |
| `sanitize_names` | `true` | Whether to sanitize metric names to be Prometheus-compliant. |
| `threshold` | See below | Buffer threshold configuration. See [Threshold Configuration](#threshold-configuration). |

### <a name="threshold-configuration">Threshold Configuration</a>

| Option | Default | Description |
|--------|---------|-------------|
| `max_events` | `500` | Maximum number of events to buffer before flushing. |
| `max_request_size` | `1048576` (1 MB) | Maximum request size in bytes before flushing. |
| `flush_interval` | `10000` (ms) | Maximum time in milliseconds to wait before flushing the buffer. |

### <a name="aws-configuration">AWS Configuration</a>

When the `aws` block is present, requests are automatically signed with AWS SigV4. An `https://` URL is required.

| Option | Required | Description |
|--------|----------|-------------|
| `region` | No | The AWS region to use for credentials. Defaults to [standard SDK behavior to determine the region](https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/region-selection.html). |
| `sts_role_arn` | No | The STS role to assume for requests to AWS. Defaults to null, which uses [standard SDK credential behavior](https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/credentials.html). |
| `sts_header_overrides` | No | A map of header overrides to make when assuming the IAM role. |
| `sts_external_id` | No | An optional external ID to use when assuming the IAM role. |

### <a name="authentication">Authentication</a>

The `authentication` block supports HTTP Basic authentication. It cannot be used together with `aws` (SigV4 signing).

| Option | Description |
|--------|-------------|
| `http_basic.username` | The username for HTTP Basic authentication. |
| `http_basic.password` | The password for HTTP Basic authentication. |

### End-to-End Acknowledgements

If the events received by the Prometheus sink have end-to-end acknowledgements enabled (tracked via EventHandle), then upon successful posting a positive acknowledgement is sent, otherwise a negative acknowledgement is sent.

## Not Yet Implemented

The following features have configuration classes defined but are **not currently wired up**:

- Bearer token / OAuth authentication
- Proxy support
- SSL/TLS certificate configuration
- Custom headers
- DLQ (Dead Letter Queue) file/plugin support

These are planned for future development.

## Developer Guide

This plugin is compatible with Java 11. See:

- [CONTRIBUTING](https://github.com/opensearch-project/data-prepper/blob/main/CONTRIBUTING.md)
- [Monitoring](https://github.com/opensearch-project/data-prepper/blob/main/docs/monitoring.md)

### Build and Test

```bash
# Run unit tests
./gradlew :data-prepper-plugins:prometheus-sink:test

# Run integration tests
./gradlew :data-prepper-plugins:prometheus-sink:integrationTest -Dtests.prometheus.sink.http.endpoint=<http-endpoint>

# Code formatting
./gradlew :data-prepper-plugins:prometheus-sink:spotlessApply

# Checkstyle
./gradlew :data-prepper-plugins:prometheus-sink:checkstyleMain :data-prepper-plugins:prometheus-sink:checkstyleTest
```
