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
* `genai_coverage_cache_max_size`: An `int` representing the total number of span ids to keep in the GenAI coverage cache (see [Late spans and the coverage cache](#late-spans-and-the-coverage-cache)). Default is `500000`.
* `genai_root_propagation`: Which trace root spans get GenAI attributes and token totals copied from their children: `ai_root`, `all_roots` or `none` (see [Root span enrichment](#root-span-enrichment)). Default is `ai_root`.

## GenAI agent trace enrichment

The processor enriches traces from GenAI agents and LLM applications, so that agent runs can be found, listed and measured from the spans themselves. It is always on and does nothing to traces without GenAI data.

It needs the source to use `output_format: otel`, because it matches attributes by their OTel key names (for example `gen_ai.operation.name`). The default `opensearch` format rewrites keys (`http.method` becomes `span.attributes.http@method`) and the enrichment then finds nothing. See the [otel-trace-source README](../otel-trace-source/README.md#output-format).

Each batch is enriched in this order, per trace:

1. [Vendor attribute normalization](#vendor-attribute-normalization)
2. [Flattened sub-key stripping](#flattened-sub-key-stripping)
3. [Root span enrichment](#root-span-enrichment)
4. [Agent root marker](#agent-root-marker)

### Vendor attribute normalization

Instrumentation libraries name the same data differently: input tokens are `llm.token_count.prompt` in OpenInference, `llm.usage.prompt_tokens` in OpenLLMetry and `gen_ai.usage.input_tokens` in the [OTel GenAI semantic conventions](https://opentelemetry.io/docs/specs/semconv/gen-ai/). The processor copies vendor attributes to their `gen_ai.*` names, so queries, dashboards and the rest of the enrichment only need the standard names.

* The mappings are listed in [`genai-attribute-mappings.yaml`](src/main/resources/genai-attribute-mappings.yaml), grouped by source: `openinference`, `openllmetry` (Traceloop) and `claude_code`.
* Original attributes are kept; the `gen_ai.*` copy is added next to them.
* A `gen_ai.*` attribute already on the span is never overwritten.
* When several attributes on one span map to the same `gen_ai.*` name, the one listed first in the file wins. For example OpenInference `session.id` wins over Traceloop's `traceloop.association.properties.session_id`; both map to `gen_ai.conversation.id`.
* Operation kinds are mapped to `gen_ai.operation.name` values, for example OpenInference `LLM` to `chat`, `AGENT` and `CHAIN` to `invoke_agent`, `TOOL` to `execute_tool` (see `operation_name_values` in the file).
* Some mappings wrap a single value in a JSON array (`wrap_as_array`), for example a finish reason into `gen_ai.response.finish_reasons`.

### Flattened sub-key stripping

Some instrumentations emit the same field twice: as a JSON string (`llm.input_messages`) and as flattened sub-keys (`llm.input_messages.0.message.content`). OpenSearch rejects such documents with a mapping conflict. When the parent string is present, the processor removes the indexed sub-keys. When only sub-keys are present, they are kept, so no data is lost.

### Agent root marker

Agents usually run behind a service: the trace's root is an HTTP or RPC span, such as `POST /plan`, and the agent span is below it. Consumers that list traces by their root then miss the agent, or show the HTTP span instead. The processor marks the span that represents each agent run with `derived.gen_ai.is_root: true`.

* **What is marked.** Each outermost GenAI span of a trace: a span with `gen_ai.operation.name` and no GenAI ancestor. That is the root itself when it is a GenAI span, or the agent span below a non-GenAI root.
* **Parallel agents.** Several top-level agents under one root are each marked, one agent run each.
* **Agents calling agents.** In `invoke_agent` > HTTP client > HTTP server > `invoke_agent`, only the calling agent is marked, because the called agent has a GenAI ancestor.
* **Token totals.** A marked span that reports no token usage of its own gets the usage of its descendants, counted as in [Token usage](#token-usage).
* **Markers set at the source.** An SDK or the user can mark a span (for example with `mark_agent_root()` in the GenAI observability SDK for Python, or `markAgentRoot()` in TypeScript):
  * If any span in a trace is marked `true` at the source, the processor marks nothing else in that trace.
  * A span set to `false` is never marked. Set it on a trace's outermost GenAI spans to opt the trace out.
* **Spans without their root.** Spans flushed after `trace_flush_interval` without their root are still marked, because the marker only needs the spans below it.
* **Consumers.** List a trace's agent runs by its marked spans. For data indexed before the marker existed, fall back to root spans that are GenAI spans.

#### Late spans and the coverage cache

Spans of a trace can arrive in several batches. A span that arrives after its agent was processed is enriched without its ancestors, so it would look outermost and be marked as a second agent run. To prevent this, the processor remembers, per trace, which span ids are already covered by a GenAI span or a marker, and checks there when a parent is not in the batch.

* Entries are kept for twice `trace_flush_interval` plus `trace_group_cache_ttl` (375 seconds at defaults), so they outlive the flush of spans that arrive without their root.
* The cache holds at most `genai_coverage_cache_max_size` span ids across all traces. Only traces with GenAI data are stored. The `genAiCoverageCount` metric reports how full it is.
* Size it for your GenAI span rate: it covers about `genai_coverage_cache_max_size` / (2 x `trace_flush_interval` + `trace_group_cache_ttl`) GenAI spans per second, about 1,300 per second at defaults. Above that, traces are evicted early and a late span can be marked again.
* The covered-span lookup, the marking and the update run atomically per trace, so pipeline workers processing spans of one trace at the same time do not both mark one.

Known limits:

* A span that arrives after its trace's entry expired, or was evicted, can be marked.
* If a trace's children are flushed before a GenAI root arrives, both the root and the outermost child can end up marked. Consumers should prefer the marked span with no parent.
* If a called service's spans are flushed after `trace_flush_interval` without the caller's spans, the called agent is marked too, and is listed as its own agent run.

### Root span enrichment

The processor can copy `gen_ai.system`, `gen_ai.provider.name`, `gen_ai.agent.name` and `gen_ai.request.model`, and the trace's token totals, from child spans onto the trace's root span. Attributes the root already has are kept.

`genai_root_propagation` chooses which roots get them:

| Value | Roots that get the copies |
|---|---|
| `ai_root` (default) | Only a root that is its trace's AI root: a GenAI span, or a span marked `derived.gen_ai.is_root: true` at the source. |
| `all_roots` | Every root, including non-GenAI roots such as the HTTP span an agent runs behind. This was the behavior before `ai_root`. |
| `none` | No root. |

With `ai_root`, a non-GenAI root keeps its own attributes. Copying agent data onto it made the HTTP span look like it called a model and used tokens, collapsed parallel agents into one agent name, and counted the trace's tokens again for any consumer that sums usage across spans. The marked agent spans carry that data instead. Use `all_roots` if a dashboard or query reads agent data from root spans and has not moved to the marked spans yet.

Values are taken from the outermost GenAI spans first (earliest start time), then other GenAI spans, then the remaining children.

Marking does not depend on this setting: with `none`, a marked agent span still gets its descendants' token totals.

### Token usage

Wherever the processor sums `gen_ai.usage.input_tokens` and `gen_ai.usage.output_tokens` (onto a root or onto a marked agent span), it counts only spans with no ancestor that also reports usage. Agent spans such as `invoke_agent` often report the total of their own LLM calls, so adding every span with usage would count those calls twice.

* This assumes an ancestor's reported usage includes its subtree. A sub-agent that runs its own LLM calls under a parent that reports only its own usage is not added.
* A span that reports either token count is treated as reporting usage, and keeps its own values.
* Token counts sent as numeric strings are read as numbers; other values are ignored.

## Metrics
In addition to the metrics from [AbstractProcessor](https://github.com/opensearch-project/data-prepper/blob/main/data-prepper-api/src/main/java/org/opensearch/dataprepper/model/processor/AbstractProcessor.java):

* `traceGroupCacheCount` - (gauge) The count of trace groups in the trace group cache
* `spanSetCount` - (gauge) The count of span sets in the span set collection
* `genAiCoverageCount` - (gauge) The count of traces in the GenAI coverage cache

## Developer Guide
This plugin is compatible with Java 8. See 
- [CONTRIBUTING](https://github.com/opensearch-project/data-prepper/blob/main/CONTRIBUTING.md) 
- [monitoring](https://github.com/opensearch-project/data-prepper/blob/main/docs/monitoring.md)
