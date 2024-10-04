# OTel Metrics Source

This is a source which follows the [OTLP Protocol](https://github.com/open-telemetry/oteps/blob/master/text/0035-opentelemetry-protocol.md) and exports `ExportMetricsServiceRequest` records.

This source supports ```OTLP/grpc```.


## Usages
Example `.yaml` configuration:
```
source:
    - otel_metrics_source:
```

## Configurations

* port(Optional) => An `int` represents the port Otel metrics source is running on. Default is ```21891```.
* path(Optional) => A `String` which represents the path for sending unframed HTTP requests. This can be used for supporting unframed gRPC with HTTP idiomatic path to a configurable path. It should start with `/` and length should be at least 1. `/opentelemetry.proto.collector.metrics.v1.MetricsService/Export` endpoint will be disabled for both gRPC and HTTP requests if path is configured. Path can contain `${pipelineName}` placeholder which will be replaced with pipeline name. 
* request_timeout(Optional) => An `int` represents request timeout in millis. Default is ```10_000```.
* health_check_service(Optional) => A boolean enables health check service. When ```true``` enables a gRPC health check service under ```grpc.health.v1.Health/Check```. Default is ```false```. In order to use the health check service, you must also enable ```proto_reflection_service```.
* proto_reflection_service(Optional) => A boolean enables a reflection service for Protobuf services (see [ProtoReflectionService](https://grpc.github.io/grpc-java/javadoc/io/grpc/protobuf/services/ProtoReflectionService.html) and [gRPC reflection](https://github.com/grpc/grpc-java/blob/master/documentation/server-reflection-tutorial.md) docs). Default is ```false```.
* unframed_requests(Optional) => A boolean to enable requests not framed using the gRPC wire protocol. When ```health_check_service``` is true and ```unframed_requests``` is true, enables HTTP health check service under ```/health```.
* thread_count(Optional) => the number of threads to keep in the ScheduledThreadPool. Default is `200`.
* max_connection_count(Optional) => the maximum allowed number of open connections. Default is `500`.
* compression (Optional) : The compression type applied on the client request payload. Defaults to `none`. Supported values are:
    * `none`: no compression
    * `gzip`: apply GZip de-compression on the incoming request.

### Retry Information

Data Prepper replies with a `RetryInfo` specifying how long to wait for the next request in case backpressure builds up. The retry information is implemented as exponential backoff, with a max delay of `retry_info.max_delay`.

```yaml
source:
  otel_trace_source:
    retry_info:
      min_delay: 1000ms # defaults to 100ms
      max_delay: 5s     # defaults to 2s
```

### SSL

* ssl(Optional) => A boolean enables TLS/SSL. Default is ```true```.
* sslKeyCertChainFile(Optional) => A `String` represents the SSL certificate chain file path or AWS S3 path. S3 path example ```s3://<bucketName>/<path>```. Required if ```ssl``` is set to ```true```.
* sslKeyFile(Optional) => A `String` represents the SSL key file path or AWS S3 path. S3 path example ```s3://<bucketName>/<path>```. Required if ```ssl``` is set to ```true```.
* useAcmCertForSSL(Optional) => A boolean enables TLS/SSL using certificate and private key from AWS Certificate Manager (ACM). Default is ```false```.
* acmCertificateArn(Optional) => A `String` represents the ACM certificate ARN. ACM certificate take preference over S3 or local file system certificate. Required if ```useAcmCertForSSL``` is set to ```true```.
* awsRegion(Optional) => A `String` represents the AWS region to use ACM or S3. Required if ```useAcmCertForSSL``` is set to ```true``` or ```sslKeyCertChainFile``` and ```sslKeyFile``` is ```AWS S3 path```.

## Metrics

### Counter
- `requestTimeouts`: measures total number of requests that time out.
- `requestsReceived`: measures total number of requests received by otel metrics source.

## Developer Guide
This plugin is compatible with Java 8. See 
- [CONTRIBUTING](https://github.com/opensearch-project/data-prepper/blob/main/CONTRIBUTING.md) 
- [monitoring](https://github.com/opensearch-project/data-prepper/blob/main/docs/monitoring.md)
