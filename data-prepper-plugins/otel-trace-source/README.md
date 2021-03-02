# OTel Trace Source 

This is a source which follows the [OTLP Protocol](https://github.com/open-telemetry/oteps/blob/master/text/0035-opentelemetry-protocol.md). This source supports ```OTLP/grpc``` and ```OTLP/HTTP```. Support for ```OTLP/HTTP+JSON``` is not complete due as the traceId and spanId will be ```base64``` and not ```HexString```.


## Usages
Example `.yaml` configuration:
```
source:
    - otel_trace_source:
```

## Configurations

* port(Optional) => An `int` represents the port Otel trace source is running on. Default is ```21890```. 
* request_timeout(Optional) => An `int` represents request timeout in millis. Default is ```10_000```.
* health_check_service(Optional) => A boolean enables a gRPC health check service under ```grpc.health.v1 / Health / Check```. Default is ```false```.
* proto_reflection_service(Optional) => A boolean enables a reflection service for Protobuf services (see [ProtoReflectionService](https://grpc.github.io/grpc-java/javadoc/io/grpc/protobuf/services/ProtoReflectionService.html) and [gRPC reflection](https://github.com/grpc/grpc-java/blob/master/documentation/server-reflection-tutorial.md) docs). Default is ```false```.
* ssl(Optional) => A boolean enables TLS/SSL. Default is ```true```.
* sslKeyCertChainFile(Optional) => A `String` represents the SSL certificate chain file path. Required if ```ssl``` is set to ```true```
* sslKeyFile(Optional) => A `String` represents the SSL key file path. Required if ```ssl``` is set to ```true```
* thread_count(Optional) => the number of threads to keep in the ScheduledThreadPool. Default is `200`
* max_connection_count(Optional) => the maximum allowed number of open connections. Default is `500`

## Metrics

### Counter
- `requestTimeouts`: measures total number of requests that time out.
- `requestsReceived`: measures total number of requests received by otel trace source. 

## Developer Guide
This plugin is compatible with Java 8. See 
- [CONTRIBUTING](https://github.com/opendistro-for-elasticsearch/data-prepper/blob/main/CONTRIBUTING.md) 
- [monitoring](https://github.com/opendistro-for-elasticsearch/data-prepper/blob/main/docs/readme/monitoring.md)
