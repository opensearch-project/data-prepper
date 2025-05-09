# OTLP Source

This is a source which follows the [OTLP Protocol](https://opentelemetry.io/docs/specs/otlp/) and supports three endpoints for logs, metrics, and traces. It supports both `OTLP/grpc` and `OTLP/HTTP`.

## Usages

### Routing telemetry signal based on event type

Each of the telemetry signals may be sent to different processors or sinks based on specific needs. The routing to downstream pipelines is based on meta-data routing.

```yaml
otel-telemetry-pipeline:
  source:
    otlp:
      ssl: false
  route:
    - logs: 'getEventType() == "LOG"'
    - traces: 'getEventType() == "TRACE"'
    - metrics: 'getEventType() == "METRIC"'
  sink:
    - pipeline:
        name: "logs-pipeline"
        routes:
          - "logs"
    - pipeline:
        name: "traces-pipeline"
        routes:
          - "traces"
    - pipeline:
        name: "metrics-pipeline"
        routes:
          - "metrics"
```

## Configurations

- port(Optional) => An `int` represents the port OTLP source is running on. Default is `21893`.
- logs_path(Optional) => A `String` which represents the path for sending unframed HTTP requests for logs. It should start with `/` and length should be at least 1. Default is `/opentelemetry.proto.collector.logs.v1.LogsService/Export`.
- metrics_path(Optional) => A `String` which represents the path for sending unframed HTTP requests for metrics. It should start with `/` and length should be at least 1. Default is `/opentelemetry.proto.collector.metrics.v1.MetricsService/Export`.
- traces_path(Optional) => A `String` which represents the path for sending unframed HTTP requests for traces. It should start with `/` and length should be at least 1. Default is `/opentelemetry.proto.collector.trace.v1.TraceService/Export`.
- request_timeout(Optional) => An `int` represents request timeout in millis. Default is `10000`.
- health_check_service(Optional) => A boolean enables a gRPC health check service under `grpc.health.v1.Health/Check`. Default is `false`.
- proto_reflection_service(Optional) => A boolean enables a reflection service for Protobuf services (see [ProtoReflectionService](https://grpc.github.io/grpc-java/javadoc/io/grpc/protobuf/services/ProtoReflectionService.html) and [gRPC reflection](https://github.com/grpc/grpc-java/blob/master/documentation/server-reflection-tutorial.md) docs). Default is `false`.
- unframed_requests(Optional) => A boolean to enable requests not framed using the gRPC wire protocol. When `health_check_service` is true and `unframed_requests` is true, enables HTTP health check service under `/health`.
- thread_count(Optional) => the number of threads to keep in the ScheduledThreadPool. Default is `200`.
- max_connection_count(Optional) => the maximum allowed number of open connections. Default is `500`.
- compression(Optional) => The compression type applied on the client request payload. Defaults to `none`. Supported values are:
  - `none`: no compression
  - `gzip`: apply GZip de-compression on the incoming request.
- logs_output_format(Optional) => Specifies the decoded output format for logs. Supported values are:
  - `otel`: OpenTelemetry format (default).
  - `opensearch`: OpenSearch format.
- metrics_output_format(Optional) => Specifies the decoded output format for metrics. Supported values are:
  - `otel`: OpenTelemetry format (default).
  - `opensearch`: OpenSearch format.
- traces_output_format(Optional) => Specifies the decoded output format for traces. Supported values are:
  - `otel`: OpenTelemetry format (default).
  - `opensearch`: OpenSearch format.

### Retry Information

Data Prepper replies with a `RetryInfo` specifying how long to wait for the next request in case backpressure builds up. The retry information is implemented as exponential backoff, with a max delay of `retry_info.max_delay`.

```yaml
source:
  otlp:
    retry_info:
      min_delay: 1000ms # defaults to 100ms
      max_delay: 5s # defaults to 2s
```

### Authentication Configurations

By default, the otlp input is unauthenticated.

The following is an example of how to run the server with HTTP Basic authentication:

```yaml
source:
  otlp:
    authentication:
      http_basic:
        username: my-user
        password: my_s3cr3t
```

You can also explicitly disable authentication with:

```yaml
source:
  otlp:
    authentication:
      unauthenticated:
```

This plugin uses pluggable authentication for GRPC servers. To provide custom authentication,
create a plugin which implements [`GrpcAuthenticationProvider`](../armeria-common/src/main/java/org/opensearch/dataprepper/armeria/authentication/GrpcAuthenticationProvider.java)


### SSL

- ssl(Optional) => A boolean enables TLS/SSL. Default is `true`.
- sslKeyCertChainFile(Optional) => A `String` represents the SSL certificate chain file path or AWS S3 path. S3 path example `s3://<bucketName>/<path>`. Required if `ssl` is set to `true`.
- sslKeyFile(Optional) => A `String` represents the SSL key file path or AWS S3 path. S3 path example `s3://<bucketName>/<path>`. Required if `ssl` is set to `true`.
- useAcmCertForSSL(Optional) => A boolean enables TLS/SSL using certificate and private key from AWS Certificate Manager (ACM). Default is `false`.
- acmCertificateArn(Optional) => A `String` represents the ACM certificate ARN. ACM certificate takes preference over S3 or local file system certificate. Required if `useAcmCertForSSL` is set to `true`.
- awsRegion(Optional) => A `String` represents the AWS region to use ACM or S3. Required if `useAcmCertForSSL` is set to `true` or `sslKeyCertChainFile` and `sslKeyFile` is `AWS S3 path`.

## Metrics

### Counter

- `requestTimeouts`: measures total number of requests that time out.
- `requestsReceived`: measures total number of requests received by OTLP source.
- `successRequests`: measures total number of requests successfully processed by OTLP source plugin.
- `badRequests`: measures total number of requests with invalid format processed by OTLP source plugin.
- `requestsTooLarge`: measures total number of requests that exceed the maximum allowed size.
- `internalServerError`: measures total number of requests processed by OTLP source with custom exception type.

### Timer

- `requestProcessDuration`: measures latency of requests processed by OTLP source plugin in seconds.

### Distribution Summary

- `payloadSize`: measures the distribution of incoming requests payload sizes in bytes.

## Developer Guide

This plugin is compatible with Java 8. See

- [CONTRIBUTING](https://github.com/opensearch-project/data-prepper/blob/main/CONTRIBUTING.md)
- [monitoring](https://github.com/opensearch-project/data-prepper/blob/main/docs/monitoring.md)
