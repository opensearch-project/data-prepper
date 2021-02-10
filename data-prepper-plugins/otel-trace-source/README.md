# OTel Trace Source 

This is a source which follows the [OTLP Protocol](https://github.com/open-telemetry/oteps/blob/master/text/0035-opentelemetry-protocol.md). This source supports ```OTLP/grpc``` and ```OTLP/HTTP```. Support for ```OTLP/HTTP+JSON``` is not complete due as the traceId and spanId will be ```base64``` and not ```HexString```.

```
source:
    - otel_trace_source:
```

## Configurations

* port => Default is ```21890```. 
* request_timeout => Default is ```10_000``` millis.
* health_check_service => This will enable a gRPC health check service under ```grpc.health.v1 / Health / Check```. Default is ```false```.
* proto_reflection_service => This will enable a reflection service for Protobuf services (see [ProtoReflectionService](https://grpc.github.io/grpc-java/javadoc/io/grpc/protobuf/services/ProtoReflectionService.html) and [gRPC reflection](https://github.com/grpc/grpc-java/blob/master/documentation/server-reflection-tutorial.md) docs). Default is ```false```.
* ssl => Default is ```true```.
* sslKeyCertChainFile => Should be provided if ```ssl``` is set to ```true```
* sslKeyFile => Should be provided if ```ssl``` is set to ```true```
* thread_count => the number of threads to keep in the ScheduledThreadPool. Default is `200`
* max_connection_count => the maximum allowed number of open connections. Default is `500`
