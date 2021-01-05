# OTel Trace Raw Prepper

This is a prepper that serializes collection of `ExportTraceServiceRequest` sent from [otel-trace-source](../dataPrepper-plugins/otel-trace-source) into collection of string records. 

```
prepper:
    - otel_trace_raw_prepper:
```