# OTel Trace Source

This is a source which follows the [OTLP Protocol](https://github.com/open-telemetry/oteps/blob/master/text/0035-opentelemetry-protocol.md). This source supports ```OTLP/grpc``` and ```OTLP/HTTP```. Support for ```OTLP/HTTP+JSON``` is not complete due as the traceId and spanId will be ```base64``` and not ```HexString```.


## Usages
Example `.yaml` configuration:
```
source:
    otel_trace_source:
```

## Configurations

* port(Optional) => An `int` represents the port Otel trace source is running on. Default is ```21890```.
* request_timeout(Optional) => An `int` represents request timeout in millis. Default is ```10_000```.
* health_check_service(Optional) => A boolean enables a gRPC health check service under ```grpc.health.v1 / Health / Check```. Default is ```false```.
* proto_reflection_service(Optional) => A boolean enables a reflection service for Protobuf services (see [ProtoReflectionService](https://grpc.github.io/grpc-java/javadoc/io/grpc/protobuf/services/ProtoReflectionService.html) and [gRPC reflection](https://github.com/grpc/grpc-java/blob/master/documentation/server-reflection-tutorial.md) docs). Default is ```false```.
* unframed_requests(Optional) => A boolean to enable requests not framed using the gRPC wire protocol. 
* thread_count(Optional) => the number of threads to keep in the ScheduledThreadPool. Default is `200`.
* max_connection_count(Optional) => the maximum allowed number of open connections. Default is `500`.

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

Send a sample span with the following https curl command

```
curl -k -H 'Content-Type: application/json; charset=utf-8'  -d '{"resourceSpans":[{"instrumentationLibrarySpans":[{"spans":[{"spanId":"AAAAAAAAAAM=","name":"test-span"}]}]}]}' https://localhost:21890/opentelemetry.proto.collector.trace.v1.TraceService/Export
```

## Metrics

### Counter
- `requestTimeouts`: measures total number of requests that time out.
- `requestsReceived`: measures total number of requests received by otel trace source.

## Developer Guide
This plugin is compatible with Java 8. See 
- [CONTRIBUTING](https://github.com/opensearch-project/data-prepper/blob/main/CONTRIBUTING.md) 
- [monitoring](https://github.com/opensearch-project/data-prepper/blob/main/docs/monitoring.md)
