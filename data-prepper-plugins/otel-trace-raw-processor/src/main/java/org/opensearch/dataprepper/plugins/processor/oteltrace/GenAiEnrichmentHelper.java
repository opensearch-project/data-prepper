/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.processor.oteltrace;

import org.opensearch.dataprepper.model.trace.Span;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Enriches GenAI agent traces by propagating attributes from child spans to root
 * and stripping conflicting flattened sub-keys.
 *
 * @see <a href="https://opentelemetry.io/docs/specs/semconv/gen-ai/">OTel GenAI Semantic Conventions v1.39.0</a>
 */
final class GenAiEnrichmentHelper {
    private static final Logger LOG = LoggerFactory.getLogger(GenAiEnrichmentHelper.class);

    private static final String GEN_AI_SYSTEM_KEY = "gen_ai.system";
    private static final String GEN_AI_PROVIDER_NAME_KEY = "gen_ai.provider.name";
    private static final String GEN_AI_AGENT_NAME_KEY = "gen_ai.agent.name";
    private static final String GEN_AI_REQUEST_MODEL_KEY = "gen_ai.request.model";
    private static final String GEN_AI_INPUT_TOKENS_KEY = "gen_ai.usage.input_tokens";
    private static final String GEN_AI_OUTPUT_TOKENS_KEY = "gen_ai.usage.output_tokens";
    private static final String GEN_AI_OPERATION_NAME_KEY = "gen_ai.operation.name";
    /**
     * Marks the outermost GenAI span of a trace whose root is not a GenAI span (for example an
     * agent behind an HTTP handler), so the agent span can be listed as the trace's AI root.
     */
    static final String DERIVED_GEN_AI_IS_ROOT_KEY = "derived.gen_ai.is_root";
    private static final String ATTRIBUTES_PREFIX = "attributes/";

    private static final String[] PROPAGATED_STRING_KEYS = {
            GEN_AI_SYSTEM_KEY, GEN_AI_PROVIDER_NAME_KEY, GEN_AI_AGENT_NAME_KEY, GEN_AI_REQUEST_MODEL_KEY
    };

    private static final Comparator<Span> BY_START_TIME = Comparator.comparing(
            Span::getStartTime, Comparator.nullsLast(Comparator.naturalOrder()));

    private static final String[] FLATTENED_PARENT_KEYS = {
            "llm.input_messages", "llm.output_messages",
            "gen_ai.prompt", "gen_ai.completion"
    };

    private GenAiEnrichmentHelper() {}

    /**
     * Enriches a batch of spans: normalizes vendor attributes, strips flattened sub-keys,
     * marks the outermost GenAI spans of traces whose root is not a GenAI span, then
     * propagates select gen_ai attributes from children to root spans grouped by traceId.
     */
    static void enrichBatch(final List<Span> spans) {
        for (final Span span : spans) {
            normalizeAttributes(span);
            stripFlattenedSubkeys(span);
        }

        final Map<String, List<Span>> spansByTrace = new HashMap<>();
        for (final Span span : spans) {
            spansByTrace.computeIfAbsent(span.getTraceId(), k -> new ArrayList<>()).add(span);
        }

        for (final List<Span> traceSpans : spansByTrace.values()) {
            Span rootSpan = null;
            final List<Span> children = new ArrayList<>();
            for (final Span span : traceSpans) {
                if (isRootSpan(span)) {
                    rootSpan = span;
                } else {
                    children.add(span);
                }
            }
            // Root first: it sums the spans' own tokens, before marking adds aggregates to agent roots.
            if (rootSpan != null && !children.isEmpty()) {
                enrichRootSpan(rootSpan, children);
            }
            if (rootSpan == null || !isGenAiSpan(rootSpan)) {
                markAgentRoots(traceSpans);
            }
        }
    }

    private static boolean isGenAiSpan(final Span span) {
        final Map<String, Object> attrs = span.getAttributes();
        return attrs != null && attrs.get(GEN_AI_OPERATION_NAME_KEY) != null;
    }

    /**
     * GenAI spans of one trace with no GenAI ancestor among the given spans, earliest first.
     * A parent that is not in the batch counts as a non-GenAI span.
     */
    static List<Span> outermostGenAiSpans(final Collection<Span> traceSpans) {
        final Map<String, Span> bySpanId = new HashMap<>();
        for (final Span span : traceSpans) {
            if (span.getSpanId() != null) {
                bySpanId.put(span.getSpanId(), span);
            }
        }
        final List<Span> outermost = new ArrayList<>();
        for (final Span span : traceSpans) {
            if (isGenAiSpan(span) && !hasGenAiAncestor(span, bySpanId)) {
                outermost.add(span);
            }
        }
        outermost.sort(BY_START_TIME);
        return outermost;
    }

    private static boolean hasGenAiAncestor(final Span span, final Map<String, Span> bySpanId) {
        final Set<String> seen = new HashSet<>();
        Span parent = bySpanId.get(span.getParentSpanId());
        while (parent != null && seen.add(parent.getSpanId())) {
            if (isGenAiSpan(parent)) {
                return true;
            }
            parent = bySpanId.get(parent.getParentSpanId());
        }
        return false;
    }

    /**
     * Marks the outermost GenAI spans of a trace with {@code derived.gen_ai.is_root} and
     * aggregates their descendants' tokens onto them. Skipped when any span already carries the
     * marker, so a span marked by the SDK or the user is the only AI root.
     */
    static void markAgentRoots(final Collection<Span> traceSpans) {
        for (final Span span : traceSpans) {
            final Map<String, Object> attrs = span.getAttributes();
            if (attrs != null && attrs.containsKey(DERIVED_GEN_AI_IS_ROOT_KEY)) {
                return;
            }
        }
        final Map<String, List<Span>> childrenByParent = new HashMap<>();
        for (final Span span : traceSpans) {
            if (span.getParentSpanId() != null) {
                childrenByParent.computeIfAbsent(span.getParentSpanId(), k -> new ArrayList<>()).add(span);
            }
        }
        for (final Span agentRoot : outermostGenAiSpans(traceSpans)) {
            agentRoot.put(ATTRIBUTES_PREFIX + DERIVED_GEN_AI_IS_ROOT_KEY, true);
            final Map<String, Object> attrs = agentRoot.getAttributes();
            if (attrs != null && attrs.containsKey(GEN_AI_INPUT_TOKENS_KEY)) {
                continue;
            }
            final List<Span> descendants = new ArrayList<>();
            final List<Span> pending = new ArrayList<>(
                    childrenByParent.getOrDefault(agentRoot.getSpanId(), List.of()));
            final Set<String> seen = new HashSet<>();
            while (!pending.isEmpty()) {
                final Span descendant = pending.remove(pending.size() - 1);
                if (!seen.add(descendant.getSpanId())) {
                    continue;
                }
                descendants.add(descendant);
                pending.addAll(childrenByParent.getOrDefault(descendant.getSpanId(), List.of()));
            }
            final TokenTotals tokens = sumTopmostTokens(descendants);
            if (tokens.found) {
                agentRoot.put(ATTRIBUTES_PREFIX + GEN_AI_INPUT_TOKENS_KEY, tokens.input);
                agentRoot.put(ATTRIBUTES_PREFIX + GEN_AI_OUTPUT_TOKENS_KEY, tokens.output);
            }
            LOG.debug("Marked span {} as the GenAI root of trace {}", agentRoot.getSpanId(), agentRoot.getTraceId());
        }
    }

    private static final class TokenTotals {
        private long input;
        private long output;
        private boolean found;
    }

    private static boolean hasTokens(final Span span) {
        final Map<String, Object> attrs = span.getAttributes();
        return attrs != null
                && (attrs.get(GEN_AI_INPUT_TOKENS_KEY) != null || attrs.get(GEN_AI_OUTPUT_TOKENS_KEY) != null);
    }

    /**
     * Sums token usage over the given spans, counting only spans with no ancestor among them that
     * also reports usage. Agent spans such as invoke_agent often report the total of their own
     * LLM calls, so adding every span with usage would count those calls twice.
     */
    static TokenTotals sumTopmostTokens(final Collection<Span> spans) {
        final Map<String, Span> bySpanId = new HashMap<>();
        for (final Span span : spans) {
            if (span.getSpanId() != null) {
                bySpanId.put(span.getSpanId(), span);
            }
        }
        final TokenTotals totals = new TokenTotals();
        for (final Span span : spans) {
            if (!hasTokens(span) || hasTokenAncestor(span, bySpanId)) {
                continue;
            }
            final Map<String, Object> attrs = span.getAttributes();
            final Number input = (Number) attrs.get(GEN_AI_INPUT_TOKENS_KEY);
            final Number output = (Number) attrs.get(GEN_AI_OUTPUT_TOKENS_KEY);
            totals.found = true;
            totals.input += input != null ? input.longValue() : 0;
            totals.output += output != null ? output.longValue() : 0;
        }
        return totals;
    }

    private static boolean hasTokenAncestor(final Span span, final Map<String, Span> bySpanId) {
        final Set<String> seen = new HashSet<>();
        Span parent = bySpanId.get(span.getParentSpanId());
        while (parent != null && seen.add(parent.getSpanId())) {
            if (hasTokens(parent)) {
                return true;
            }
            parent = bySpanId.get(parent.getParentSpanId());
        }
        return false;
    }

    private static boolean isRootSpan(final Span span) {
        final String parentSpanId = span.getParentSpanId();
        return parentSpanId == null || parentSpanId.isEmpty()
                || "0000000000000000".equals(parentSpanId);
    }

    /**
     * Propagates gen_ai.* string attributes and aggregated token counts from children to root.
     * Skips if root already has the attributes.
     */
    static void enrichRootSpan(final Span rootSpan, final Collection<Span> children) {
        final Map<String, Object> rootAttrs = rootSpan.getAttributes();

        final Map<String, String> toPropagate = new HashMap<>();
        for (final String key : PROPAGATED_STRING_KEYS) {
            if (rootAttrs == null || !rootAttrs.containsKey(key)) {
                toPropagate.put(key, null);
            }
        }
        final boolean rootHasTokens = rootAttrs != null && rootAttrs.containsKey(GEN_AI_INPUT_TOKENS_KEY);

        if (toPropagate.isEmpty() && rootHasTokens) {
            return;
        }

        // Propagated values come from the outermost GenAI spans first, so the trace gets the outer
        // agent's name and model whatever order the batch is in.
        final Set<Span> outermost = new HashSet<>(outermostGenAiSpans(children));
        final List<Span> ordered = new ArrayList<>(children);
        ordered.sort(Comparator.<Span>comparingInt(span -> outermost.contains(span) ? 0 : isGenAiSpan(span) ? 1 : 2)
                .thenComparing(BY_START_TIME));

        for (final Span child : ordered) {
            final Map<String, Object> attrs = child.getAttributes();
            if (attrs == null) {
                continue;
            }

            for (final Map.Entry<String, String> entry : toPropagate.entrySet()) {
                if (entry.getValue() == null && attrs.containsKey(entry.getKey())) {
                    entry.setValue((String) attrs.get(entry.getKey()));
                }
            }
        }

        for (final Map.Entry<String, String> entry : toPropagate.entrySet()) {
            if (entry.getValue() != null) {
                rootSpan.put(ATTRIBUTES_PREFIX + entry.getKey(), entry.getValue());
                LOG.debug("Propagated {} = {} to root span {}", entry.getKey(), entry.getValue(), rootSpan.getSpanId());
            }
        }

        final TokenTotals tokens = sumTopmostTokens(children);
        if (!rootHasTokens && tokens.found) {
            rootSpan.put(ATTRIBUTES_PREFIX + GEN_AI_INPUT_TOKENS_KEY, tokens.input);
            rootSpan.put(ATTRIBUTES_PREFIX + GEN_AI_OUTPUT_TOKENS_KEY, tokens.output);
            LOG.debug("Aggregated tokens (input={}, output={}) to root span {}", tokens.input, tokens.output, rootSpan.getSpanId());
        }
    }

    /**
     * Normalizes vendor-specific attributes to OTel GenAI Semantic Conventions.
     * Copies values to the standard gen_ai.* key, keeping originals intact.
     * Skips if the target attribute already exists on the span.
     */
    static void normalizeAttributes(final Span span) {
        final Map<String, Object> attrs = span.getAttributes();
        if (attrs == null) {
            return;
        }

        for (final Map.Entry<String, Object> entry : new ArrayList<>(attrs.entrySet())) {
            final GenAiAttributeMappings.MappingTarget target =
                    GenAiAttributeMappings.getLookupTable().get(entry.getKey());
            if (target == null) {
                continue;
            }
            if (attrs.containsKey(target.getKey())) {
                continue;
            }

            Object value = entry.getValue();
            if (value instanceof String) {
                final String strVal = (String) value;
                if ("gen_ai.operation.name".equals(target.getKey())) {
                    final String mapped = GenAiAttributeMappings.getOperationNameValues()
                            .get(strVal.toLowerCase());
                    if (mapped != null) {
                        value = mapped;
                    }
                }
                if (target.isWrapAsArray()) {
                    value = "[\"" + value + "\"]";
                }
            }

            span.put(ATTRIBUTES_PREFIX + target.getKey(), value);
        }
    }

    /**
     * Removes flattened sub-keys (e.g. "llm.input_messages.0.message.content") that
     * conflict with parent string values (e.g. "llm.input_messages"), preventing OpenSearch mapping failures.
     * Only strips when the parent string value exists. If only sub-keys exist, they are preserved.
     */
    static void stripFlattenedSubkeys(final Span span) {
        final Map<String, Object> attrs = span.getAttributes();
        if (attrs == null) {
            return;
        }

        final List<String> toRemove = new ArrayList<>();
        for (final String parentKey : FLATTENED_PARENT_KEYS) {
            if (!attrs.containsKey(parentKey)) {
                continue;
            }
            for (final String key : attrs.keySet()) {
                if (key.startsWith(parentKey + ".") && key.length() > parentKey.length() + 1
                        && Character.isDigit(key.charAt(parentKey.length() + 1))) {
                    toRemove.add(key);
                }
            }
        }

        for (final String key : toRemove) {
            try {
                span.delete(ATTRIBUTES_PREFIX + key);
            } catch (final Exception e) {
                LOG.warn("Failed to delete flattened sub-key {}: {}", key, e.getMessage());
            }
        }
    }
}
