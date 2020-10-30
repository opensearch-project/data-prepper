# OTel Trace Source 

This is a source which follows the [OTLP Protocol](https://github.com/open-telemetry/oteps/blob/master/text/0035-opentelemetry-protocol.md). This source supports ```OTLP/grpc``` and ```OTLP/HTTP```. Support for ```OTLP/HTTP+JSON``` is not complete due as the traceId and spanId will be ```base64``` and not ```HexString```.

```
source:
    otel_trace_source:
```

## Configurations

* port => Default is ```21890```. 
* request_timeout => Default is ```10_000``` millis.
* health_check => This will add health check at <your_host>:port/health_check.
* ssl => Default is ```false```.
* sslKeyCertChainFile => Should be provided if ```ssl``` is set to ```true```
* sslKeyFile => Should be provided if ```ssl``` is set to ```true```