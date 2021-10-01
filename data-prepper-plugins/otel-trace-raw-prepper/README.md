# OTel Trace Raw Prepper

This is a prepper that serializes collection of `ExportTraceServiceRequest` sent from [otel-trace-source](../dataPrepper-plugins/otel-trace-source) into collection of string records.

## Usages
Example `.yaml` configuration
```
prepper:
    - otel_trace_raw_prepper:
```

## Configuration

* `trace_flush_interval`: An `int` represents the time interval in seconds to flush all the descendant spans without any root span. Default to 180.

## Metrics
Apart from common metrics in [AbstractPrepper](https://github.com/opensearch-project/data-prepper/blob/main/data-prepper-api/src/main/java/com/amazon/dataprepper/model/prepper/AbstractPrepper.java), otel-trace-raw-prepper introduces the following custom metrics.

### Counter
- `spanProcessingErrors`: records the number of processing exceptions for invalid spans.
- `resourceSpansProcessingErrors`: records the number of processing exceptions for invalid resource spans.
- `totalProcessingErrors`: records the total number of processing errors for spans and resource spans.

## Developer Guide
This plugin is compatible with Java 8. See 
- [CONTRIBUTING](https://github.com/opensearch-project/data-prepper/blob/main/CONTRIBUTING.md) 
- [monitoring](https://github.com/opensearch-project/data-prepper/blob/main/docs/monitoring.md)
