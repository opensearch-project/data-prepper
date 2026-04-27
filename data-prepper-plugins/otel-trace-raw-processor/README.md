# OTel Trace Raw Processor

This is a processor that receives collection of [Span](../../data-prepper-api/src/main/java/com/amazon/dataprepper/model/trace/Span.java) records sent from [otel-trace-source](../dataPrepper-plugins/otel-trace-source), does stateful processing on extracting and filling-in trace group related fields.

> Note: `otel_trace_raw` processor has been renamed to `otel_traces`. You can use either name for now but the support for `otel_trace_raw` will be removed in major version 3.0.

## Usages
Example `.yaml` configuration
```
processor:
    - otel_trace_raw:
```

## Configuration

* `trace_flush_interval`: An `int` represents the time interval in seconds to flush all the descendant spans without any root span. Default to 180.
* `trace_group_cache_ttl`: A `Duration` represents the time-to-live for traces in the trace group cache. Defaults to 15 seconds.
* `trace_group_cache_max_size`: An `int` representing the total number of traces to keep in the trace group cache.

## GenAI Agent Trace Enrichment

This processor automatically enriches GenAI agent traces. The enrichment is always-on and is a no-op for non-GenAI traces.

**What it does:**
1. **Vendor attribute normalization** — Maps vendor-specific attributes (OpenInference, OpenLLMetry) to [OTel GenAI Semantic Conventions](https://opentelemetry.io/docs/specs/semconv/gen-ai/). Original attributes are preserved. See [`genai-attribute-mappings.yaml`](src/main/resources/genai-attribute-mappings.yaml) for the full mapping table.
2. **Root span enrichment** — Propagates `gen_ai.system`, `gen_ai.provider.name`, `gen_ai.agent.name`, `gen_ai.request.model`, and `gen_ai.operation.name` from child spans to root (first-child-wins, skip-if-present). Aggregates `gen_ai.usage.input_tokens` and `gen_ai.usage.output_tokens` across children (sum).
3. **Flattened sub-key stripping** — Removes flattened sub-keys (e.g. `llm.input_messages.0.message.content`) that conflict with parent string values, preventing OpenSearch mapping failures.

**Important:** The enrichment matches attributes by their original OTel key names (e.g. `http.method`). This requires the source to use `output_format: otel` so that attribute keys are not transformed. The default `opensearch` format rewrites keys (e.g. `http.method` → `span.attributes.http@method`), which prevents the enrichment from working. See the [otel-trace-source README](../otel-trace-source/README.md#output-format) for details.

## Metrics
In addition to the metrics from [AbstractProcessor](https://github.com/opensearch-project/data-prepper/blob/main/data-prepper-api/src/main/java/org/opensearch/dataprepper/model/processor/AbstractProcessor.java):

* `traceGroupCacheCount` - (gauge) The count of trace groups in the trace group cache
* `spanSetCount` - (gauge) The count of span sets in the span set collection

## Developer Guide
This plugin is compatible with Java 8. See 
- [CONTRIBUTING](https://github.com/opensearch-project/data-prepper/blob/main/CONTRIBUTING.md) 
- [monitoring](https://github.com/opensearch-project/data-prepper/blob/main/docs/monitoring.md)
