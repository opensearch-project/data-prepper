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
* `trace_group_cache_ttl`: A `Duration` represents the time-to-live for traces in the trace group cache. Defaults to the value of `trace_flush_interval` (180 seconds unless configured), so that child spans arriving while orphaned spans are still held can be completed with trace group information.
* `trace_group_cache_max_size`: An `int` representing the total number of traces to keep in the trace group cache.
* `disable_genai_enrichment`: A `boolean` that turns off [GenAI agent trace enrichment](#genai-agent-trace-enrichment). Default is `false`. When `true`, `genai_root_propagation`, `genai_root_marking`, `genai_coverage_cache_max_size`, `genai_root_enrichment_cache_max_size`, `genai_root_hold` and `genai_root_hold_max_size` cannot be set.
* `genai_coverage_cache_max_size`: A `long` representing the total number of span ids to keep in the GenAI coverage cache (see [Late spans and the coverage cache](#late-spans-and-the-coverage-cache)). Must be at least `1`. Default is `500000`. Used only with `genai_root_marking: outermost`.
* `genai_root_propagation`: Which trace root spans get GenAI attributes and token totals copied from their children: `ai_root`, `all_roots` or `none`, written exactly so (see [Root span enrichment](#root-span-enrichment)). Default is `ai_root`.
* `genai_root_marking`: Which spans are marked `derived.gen_ai.is_root: true` as agent roots: `outermost`, `agent_spans` or `none`, written exactly so (see [Choosing how agent roots are marked](#choosing-how-agent-roots-are-marked)). Default is `outermost`.
* `genai_root_enrichment_cache_max_size`: A `long` representing the total number of span ids to keep in the cache that trace roots are enriched from when their children arrive in other batches (see [Root span enrichment across batches](#root-span-enrichment-across-batches)). Must be at least `1`. Default is `500000`. Cannot be set when `disable_genai_enrichment` is `true`.
* `genai_root_hold`: A `Duration` for how long to hold a GenAI root span that arrives before the children that carry its model or token usage, so it can be filled from them (see [Root span enrichment across batches](#root-span-enrichment-across-batches)). A held root is written when its hold expires, with what its children reported by then. Default is `0`: no root is held, and a root that arrives before its children is written without their values. For root-first setups such as streaming responses, set it, for example to `30s`. Must not be longer than `trace_flush_interval`. Cannot be set when `disable_genai_enrichment` is `true`.
* `genai_root_hold_max_size`: A `long` representing the maximum number of root spans held at once by `genai_root_hold`. When holding one more root would exceed it, the root held longest is written early. Must be at least `1`. Default is `10000`. Cannot be set when `disable_genai_enrichment` is `true`.

## GenAI agent trace enrichment

The processor enriches traces from GenAI agents and LLM applications, so that agent runs can be found, listed and measured from the spans themselves. It is on by default and does nothing to traces without GenAI data. Set `disable_genai_enrichment: true` to turn it off: span attributes are then left as they arrive, with no normalized attributes, no sub-key stripping, no markers and no root enrichment, and the coverage cache is not created.

It needs the source to use `output_format: otel`, because it matches attributes by their OTel key names (for example `gen_ai.operation.name`). The default `opensearch` format rewrites keys (`http.method` becomes `span.attributes.http@method`) and the enrichment then finds nothing. See the [otel-trace-source README](../otel-trace-source/README.md#output-format).

Example with the GenAI settings at their defaults:

```
processor:
    - otel_traces:
        disable_genai_enrichment: false
        genai_root_propagation: ai_root
        genai_root_marking: outermost
        genai_coverage_cache_max_size: 500000
```

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

#### Choosing how agent roots are marked

`genai_root_marking` chooses which spans the processor marks. It is separate from `genai_root_propagation`, which chooses which roots get model and token totals copied onto them. A marker set at the source (`true`, `false` or any other value) is never changed.

| Value | Spans the processor marks | When to use it |
|---|---|---|
| `outermost` (default) | The outermost GenAI span of each trace, as described above. | Most pipelines, and any consumer that expects one marked span per agent run. A trace whose root is a GenAI span is marked correctly whatever the arrival order. For agents behind a non-GenAI root, the result depends on which spans of a trace are in the same batch, so spans that arrive out of order can mark a second span (see the known limits below). |
| `agent_spans` | Every span whose `gen_ai.operation.name` is `invoke_agent` or `invoke_workflow` (after [vendor attribute normalization](#vendor-attribute-normalization)). No other operation is marked. | Agents instrumented without an SDK that marks roots, when every consumer of the marker handles nested agents. The marker is decided from the span alone, so the result is the same whatever the batch split, arrival order, worker or node, and no coverage cache is kept. |
| `none` | No span. Only markers set at the source are kept. | Applications that mark their own agent roots, for example with `mark_agent_root()` in the GenAI observability SDK for Python or `markAgentRoot()` in TypeScript. This is the most reliable option, because the application knows which span is the agent run. |

With `agent_spans`:

* **Nested agents are marked too.** An agent called by another agent, in process or over HTTP, is marked, so a trace can have several marked spans. Consumers must list a trace by its earliest marked span, or by its marked spans with no marked ancestor; a consumer that lists every marked span shows nested agents as separate agent runs.
* **A trace without an agent or workflow span is not marked.** For example, a `chat` span under an HTTP root. Consumers that also list GenAI root spans still find traces whose root is a GenAI span.
* **Token totals.** A marked span that reports no usage gets the usage of its descendants in the batch, unless it has a marked ancestor in the batch: then only the outer agent gets the total, so summing marked spans does not count a nested agent twice. When the outer agent is in another batch, the nested agent gets its own total. A trace root that `genai_root_propagation` enriches gets the trace's totals instead, from every batch (see [Root span enrichment across batches](#root-span-enrichment-across-batches)). With `genai_root_hold`, a nested agent that arrives while its root is held is written first, with its own total, and the root is written later with totals that include it, so summing marked spans counts that agent's usage twice. `genai_root_propagation: none`, as recommended below for `agent_spans`, avoids this.
* **Markers set at the source** on one span do not stop the other agent spans from being marked.

With `none`, a trace whose root is not a GenAI span and that has no marker set at the source has no marked span.

Recommended settings:

| Setup | `genai_root_marking` | `genai_root_propagation` |
|---|---|---|
| Default | `outermost` | `ai_root` |
| The application marks agent roots, for example with the GenAI observability SDK | `none` | `ai_root` |
| Consumers handle nested agents and sum tokens from model-call spans | `agent_spans` | `none` |

#### Late spans and the coverage cache

This section applies to `genai_root_marking: outermost`.

Spans of a trace can arrive in several batches. A span that arrives after its agent was processed is enriched without its ancestors, so it would look outermost and be marked as a second agent run. To prevent this, the processor remembers, per trace, which span ids are already covered by a GenAI span or a marker, and checks there when a parent is not in the batch.

* Entries are kept for twice `trace_flush_interval` plus `trace_group_cache_ttl` (540 seconds at defaults), so they outlive the flush of spans that arrive without their root.
* The cache holds at most `genai_coverage_cache_max_size` span ids across all traces. Only traces with GenAI data are stored. The `genAiCoverageSpanIdCount` metric reports how full it is, in span ids; `genAiCoverageCount` reports the number of traces.
* Size it for your GenAI span rate: it covers about `genai_coverage_cache_max_size` / (2 x `trace_flush_interval` + `trace_group_cache_ttl`) GenAI spans per second, about 900 per second at defaults. Above that, traces are evicted early and a late span can be marked again.
* The covered-span lookup, the marking and the update run atomically per trace, so pipeline workers processing spans of one trace at the same time do not both mark one.
* **GenAI roots.** When a trace's root is a GenAI span, or is marked `true` at the source, every other span of the trace descends from it, so only the root is marked. The processor records such a root in the cache before any of its children can be processed, so this holds whatever order the trace's spans arrive in and whichever pipeline workers process them: a child, or a nested agent that arrives before its parent, is not marked as a second agent run.
* **Agents behind a non-GenAI root.** When the root is an HTTP or RPC span that an agent runs behind, a span is outermost when no GenAI ancestor is in its batch or the cache. A nested span that arrives before its parent agent, for example a model call before the agent span that made it, finds no GenAI ancestor and is marked as well. `genai_root_marking: agent_spans` or `none` do not depend on arrival order, if that matters for your consumers.

Known limits:

* Behind a non-GenAI root, a nested span that arrives before its parent agent can be marked too, as described above.
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

### Root span enrichment across batches

Root enrichment does not need the children in the root's batch. The processor records, per trace, the values and token usage of the children it processes, and fills each root from every child recorded so far, so a root processed alone, or in a later batch than its children, is still enriched. A root is filled only from children processed before it is written: filling a root that arrives before its children needs `genai_root_hold`, because without a hold such a root is written as it arrives.

* Within a batch, values are preferred as in [Root span enrichment](#root-span-enrichment). Across batches, the first value recorded for each attribute wins.
* Token totals count only spans with no recorded ancestor that also reports usage, as in [Token usage](#token-usage), whichever of an agent span and its own LLM calls arrives first. To tell which spans contain others, the processor records the parent link of each span of a trace with GenAI data.
* With `genai_root_hold` set, a GenAI root that arrives before the children that carry its model or token usage, for example from a streaming response, a background model call or a tool in another process, is held back for that long. Its children are written right away. The root is written when its hold expires, filled with what its children reported by then, and on shutdown. It is written earlier only when holding a newer root would exceed `genai_root_hold_max_size` (the root held longest is written) or when a second root of the same trace arrives (the first is written). Non-GenAI roots, GenAI roots that already carry a model and token usage, and roots with `genai_root_propagation: none` are never held. A root whose children were already waiting for it when it arrived is not held either, even if they do not carry a model or token usage. The held roots count against shutdown readiness; `genAiHeldRootCount` reports how many are held, and the `genAiHeldRoot*Count` counters why each was written.
* A root is written once: children that arrive after it is written are not copied onto it.
* **Agents behind a non-GenAI root.** For `POST /plan` > `invoke_agent` > `chat`, the HTTP root is not a GenAI span, so it is never held, and with `genai_root_propagation: ai_root` it is not filled. The marked agent span gets token totals only from the descendants in its own batch, as in [Agent root marker](#agent-root-marker); descendants in other batches are not counted on it.
* Root enrichment and the hold work with every `genai_root_marking` value. A held root joins the batch it is released in and is marked there as the marking value says. With `outermost`, a GenAI root is recorded as its trace's GenAI root when it arrives, so its children written while it is held are not marked. With `agent_spans`, see the token totals note in [Choosing how agent roots are marked](#choosing-how-agent-roots-are-marked).
* Entries are kept for twice `trace_flush_interval` plus `trace_group_cache_ttl` (540 seconds at defaults). The cache holds at most `genai_root_enrichment_cache_max_size` span ids across all traces, counting each recorded parent link and each span that reports token usage. Only traces with GenAI data are stored. The `genAiRootEnrichmentSpanIdCount` metric reports how full it is; `genAiRootEnrichmentCount` reports the number of traces.
* The recording and the root filling run atomically per trace, so pipeline workers processing spans of one trace at the same time do not lose each other's children.

## Metrics
In addition to the metrics from [AbstractProcessor](https://github.com/opensearch-project/data-prepper/blob/main/data-prepper-api/src/main/java/org/opensearch/dataprepper/model/processor/AbstractProcessor.java):

* `traceGroupCacheCount` - (gauge) The count of trace groups in the trace group cache
* `genAiRootEnrichmentCount` - (gauge) The count of traces in the GenAI root enrichment cache
* `genAiRootEnrichmentSpanIdCount` - (gauge) The count of span ids in the GenAI root enrichment cache, the unit of `genai_root_enrichment_cache_max_size`
* `genAiHeldRootCount` - (gauge) The count of GenAI root spans currently held for late children, up to `genai_root_hold` each
* `genAiHeldRootExpiredCount` - (counter) The count of held GenAI root spans written when their hold expired
* `genAiHeldRootShutdownCount` - (counter) The count of held GenAI root spans written on shutdown
* `genAiHeldRootEvictedCount` - (counter) The count of held GenAI root spans written early because holding a newer root would exceed `genai_root_hold_max_size`
* `genAiHeldRootReplacedCount` - (counter) The count of held GenAI root spans written early because a second root of the same trace arrived
* `spanSetCount` - (gauge) The count of span sets in the span set collection
* `genAiCoverageCount` - (gauge) The count of traces in the GenAI coverage cache
* `genAiCoverageSpanIdCount` - (gauge) The count of span ids in the GenAI coverage cache, the unit of `genai_coverage_cache_max_size`

The GenAI coverage cache is only used with `genai_root_marking: outermost`; with the other values both coverage gauges report `0`.

## Developer Guide
This plugin is compatible with Java 8. See 
- [CONTRIBUTING](https://github.com/opensearch-project/data-prepper/blob/main/CONTRIBUTING.md) 
- [monitoring](https://github.com/opensearch-project/data-prepper/blob/main/docs/monitoring.md)
