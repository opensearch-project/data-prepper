# OTel Trace Raw Prepper

This is a prepper that serializes collection of `ExportTraceServiceRequest` sent from [otel-trace-source](../dataPrepper-plugins/otel-trace-source) into collection of string records. 

## Usages
Example `.yaml` configuration
```
prepper:
    - otel_trace_raw_prepper:
```

## Configuration

* `parent_span_flush_delay`: An `int` represents the time interval in seconds to flush all the root spans in the prepper together with their descendants. Default to 5.
* `gc_interval`: An `int` represents the time interval in seconds to flush all the descendant spans without any root span. Default to 30.

## Metrics
Apart from common metrics in [AbstractPrepper](https://github.com/opendistro-for-elasticsearch/data-prepper/blob/main/data-prepper-api/src/main/java/com/amazon/dataprepper/model/prepper/AbstractPrepper.java), otel-trace-raw-prepper introduces the following custom metrics.

### Counter
- `spanProcessingErrors`: records the number of processing exceptions for invalid spans.
- `resourceSpansProcessingErrors`: records the number of processing exceptions for invalid resource spans.
- `totalProcessingErrors`: records the total number of processing errors for spans and resource spans.

## Developer Guide
This plugin is compatible with Java 8. See 
- [CONTRIBUTING](https://github.com/opendistro-for-elasticsearch/data-prepper/blob/main/CONTRIBUTING.md) 
- [monitoring](https://github.com/opendistro-for-elasticsearch/data-prepper/blob/main/docs/readme/monitoring.md)