# OTel Trace Raw Processor

This is a processor that receives collection of [Span](../../data-prepper-api/src/main/java/com/amazon/dataprepper/model/trace/Span.java) records sent from [otel-trace-source](../dataPrepper-plugins/otel-trace-source), does stateful processing on extracting and filling-in trace group related fields.

## Usages
Example `.yaml` configuration
```
processor:
    - otel_trace_raw:
```

## Configuration

* `trace_flush_interval`: An `int` represents the time interval in seconds to flush all the descendant spans without any root span. Default to 180.

## Metrics
Apart from common metrics in [AbstractPrepper](https://github.com/opensearch-project/data-prepper/blob/main/data-prepper-api/src/main/java/com/amazon/dataprepper/model/prepper/AbstractPrepper.java), otel-trace-raw-processor introduces the following custom metrics.

## Developer Guide
This plugin is compatible with Java 8. See 
- [CONTRIBUTING](https://github.com/opensearch-project/data-prepper/blob/main/CONTRIBUTING.md) 
- [monitoring](https://github.com/opensearch-project/data-prepper/blob/main/docs/monitoring.md)
