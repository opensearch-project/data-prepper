# OTel Trace Source

This is a source which follows the [OTLP Protocol](https://github.com/open-telemetry/oteps/blob/master/text/0035-opentelemetry-protocol.md). This source supports ```OTLP/grpc``` and ```OTLP/HTTP```. Support for ```OTLP/HTTP+JSON``` is not complete due as the traceId and spanId will be ```base64``` and not ```HexString```.


## Usages
Example `.yaml` configuration for Data Prepper 2.0:

```yaml
source:
  otel_trace_source:
```


Example `.yaml` configuration for Data Prepper 1.4 and above:

```yaml
source:
  otel_trace_source:
    record_type: event
```

### Migrating to Data Prepper 2.0

For more information on migrating from Data Prepper 1.x to Data Prepper 2.x, see _Migrating to Data Prepper 2.0_ in the 
[Trace Analytics documentation](https://github.com/opensearch-project/data-prepper/blob/main/docs/trace_analytics.md).

## Configurations

* port(Optional) => An `int` represents the port Otel trace source is running on. Default is ```21890```.
* path(Optional) => A `String` which represents the path for sending unframed HTTP requests. This can be used for supporting unframed gRPC with HTTP idiomatic path to a configurable path. It should start with `/` and length should be at least 1. `/opentelemetry.proto.collector.trace.v1.TraceService/Export` endpoint will be disabled for both gRPC and HTTP requests if path is configured. Path can contain `${pipelineName}` placeholder which will be replaced with pipeline name.
* request_timeout(Optional) => An `int` represents request timeout in millis. Default is ```10_000```.
* health_check_service(Optional) => A boolean enables health check service. When ```true``` enables a gRPC health check service under ```grpc.health.v1.Health/Check```. Default is ```false```. In order to use the health check service, you must also enable ```proto_reflection_service```.
* unauthenticated_health_check (Optional) => A `boolean` that determines if the health endpoint will require authentication. This option is ignored if no authentication is defined. Default is `false`
* proto_reflection_service(Optional) => A boolean enables a reflection service for Protobuf services (see [ProtoReflectionService](https://grpc.github.io/grpc-java/javadoc/io/grpc/protobuf/services/ProtoReflectionService.html) and [gRPC reflection](https://github.com/grpc/grpc-java/blob/master/documentation/server-reflection-tutorial.md) docs). Default is ```false```.
* unframed_requests(Optional) => A boolean to enable requests not framed using the gRPC wire protocol. When ```health_check_service``` is true and ```unframed_requests``` is true, enables HTTP health check service under ```/health```.
* thread_count(Optional) => the number of threads to keep in the ScheduledThreadPool. Default is `200`.
* max_connection_count(Optional) => the maximum allowed number of open connections. Default is `500`. 
* authentication(Optional) => An authentication configuration. By default, this runs an unauthenticated server. See below for more information.
* record_type(Optional) => A string represents the supported record data type that is written into the buffer plugin. Value options are `otlp` or `event`. Default is `otlp`.
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

### Authentication Configurations

By default, the otel-trace-source input is unauthenticated.

The following is an example of how to run the server with HTTP Basic authentication:

```yaml
source:
  otel_trace_source:
    record_type: event
    authentication:
      http_basic:
        username: my-user
        password: my_s3cr3t
```

You can also explicitly disable authentication with:

```yaml
source:
  otel_trace_source:
    record_type: event
    authentication:
      unauthenticated:
```

This plugin uses pluggable authentication for GRPC servers. To provide custom authentication,
create a plugin which implements [`GrpcAuthenticationProvider`](../armeria-common/src/main/java/org/opensearch/dataprepper/armeria/authentication/GrpcAuthenticationProvider.java)

### SSL

* ssl(Optional) => A boolean enables TLS/SSL. Default is ```true```.
* sslKeyCertChainFile(Optional) => A `String` represents the SSL certificate chain file path or AWS S3 path. S3 path example ```s3://<bucketName>/<path>```. Required if ```ssl``` is set to ```true```.
* sslKeyFile(Optional) => A `String` represents the SSL key file path or AWS S3 path. S3 path example ```s3://<bucketName>/<path>```. Required if ```ssl``` is set to ```true```.
* useAcmCertForSSL(Optional) => A boolean enables TLS/SSL using certificate and private key from AWS Certificate Manager (ACM). Default is ```false```.
* acmCertificateArn(Optional) => A `String` represents the ACM certificate ARN. ACM certificate take preference over S3 or local file system certificate. Required if ```useAcmCertForSSL``` is set to ```true```.
* awsRegion(Optional) => A `String` represents the AWS region to use ACM or S3. Required if ```useAcmCertForSSL``` is set to ```true``` or ```sslKeyCertChainFile``` and ```sslKeyFile``` is ```AWS S3 path```.


### Example to enable SSL using OpenSSL

Create the following otel-trace-source configuration in your `pipeline.yaml`.

```yaml
source:
  otel_trace_source:
    record_type: event
    ssl: true
    sslKeyCertChainFile: "/full/path/to/certfile.crt"
    sslKeyFile: "/full/path/to/keyfile.key"
    unframed_requests: true
```

Generate a private key named `keyfile.key`, along with a self-signed certificate named `certfile.crt`.

```
openssl req  -nodes -new -x509  -keyout keyfile.key -out certfile.crt -subj "/L=test/O=Example Com Inc./OU=Example Com Inc. Root CA/CN=Example Com Inc. Root CA"
```

Make sure to replace the paths for the `sslKeyCertChainFile` and `sslKeyFile` for the otel-trace-source configuration with the actual paths of the files.

Send a sample span with the following https curl command:

```
curl -k -H 'Content-Type: application/json; charset=utf-8'  -d '{"resourceSpans":[{"scopeSpans":[{"spans":[{"spanId":"AAAAAAAAAAM=","name":"test-span"}]}]}]}' https://localhost:21890/opentelemetry.proto.collector.trace.v1.TraceService/Export
```

If `path` option is configured, you can send a sample span to the custom path with the following https curl command:
```
curl -k -H 'Content-Type: application/json; charset=utf-8'  -d '{"resourceSpans":[{"scopeSpans":[{"spans":[{"spanId":"AAAAAAAAAAM=","name":"test-span"}]}]}]}' https://localhost:21890/<path>
```

## Metrics

### Counter
- `requestTimeouts`: measures total number of requests that time out.
- `requestsReceived`: measures total number of requests received by otel trace source.
- `successRequests`: measures total number of requests successfully processed by otel trace source plugin.
- `badRequests`: measures total number of requests with invalid format processed by otel trace source plugin.
- `requestsTooLarge`: measures total number of requests of which the number of spans in the content is larger than the buffer capacity.
- `internalServerError`: measures total number of requests processed by otel trace source with custom exception type.

### Timer
- `requestProcessDuration`: measures latency of requests processed by otel trace source plugin in seconds.

### Distribution Summary
- `payloadSize`: measures the distribution of incoming requests payload sizes in bytes.

## Developer Guide
This plugin is compatible with Java 8. See 
- [CONTRIBUTING](https://github.com/opensearch-project/data-prepper/blob/main/CONTRIBUTING.md) 
- [monitoring](https://github.com/opensearch-project/data-prepper/blob/main/docs/monitoring.md)
