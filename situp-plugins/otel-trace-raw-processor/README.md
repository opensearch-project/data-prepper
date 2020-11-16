# OTel Trace Raw Processor

This is a processor that serializes collection of `ExportTraceServiceRequest` sent from [otel-trace-source](../situp-plugins/otel-trace-source) into collection of string records. 

```
processor:
    otel_trace_source:
```