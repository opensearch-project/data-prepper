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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
    private static final String ATTRIBUTES_PREFIX = "attributes/";

    private static final String[] PROPAGATED_STRING_KEYS = {
            GEN_AI_SYSTEM_KEY, GEN_AI_PROVIDER_NAME_KEY, GEN_AI_AGENT_NAME_KEY, GEN_AI_REQUEST_MODEL_KEY
    };

    private static final String[] FLATTENED_PARENT_KEYS = {
            "llm.input_messages", "llm.output_messages",
            "gen_ai.prompt", "gen_ai.completion"
    };

    private GenAiEnrichmentHelper() {}

    /**
     * Enriches a batch of spans: strips flattened sub-keys, then propagates gen_ai.*
     * attributes from children to root spans grouped by traceId.
     */
    static void enrichBatch(final List<Span> spans) {
        for (final Span span : spans) {
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
            if (rootSpan != null && !children.isEmpty()) {
                enrichRootSpan(rootSpan, children);
            }
        }
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

        long totalInputTokens = 0;
        long totalOutputTokens = 0;
        boolean foundTokens = false;

        for (final Span child : children) {
            final Map<String, Object> attrs = child.getAttributes();
            if (attrs == null) {
                continue;
            }

            for (final Map.Entry<String, String> entry : toPropagate.entrySet()) {
                if (entry.getValue() == null && attrs.containsKey(entry.getKey())) {
                    entry.setValue((String) attrs.get(entry.getKey()));
                }
            }

            final Number inputTokens = (Number) attrs.get(GEN_AI_INPUT_TOKENS_KEY);
            final Number outputTokens = (Number) attrs.get(GEN_AI_OUTPUT_TOKENS_KEY);
            if (inputTokens != null || outputTokens != null) {
                foundTokens = true;
                if (inputTokens != null) {
                    totalInputTokens += inputTokens.longValue();
                }
                if (outputTokens != null) {
                    totalOutputTokens += outputTokens.longValue();
                }
            }
        }

        for (final Map.Entry<String, String> entry : toPropagate.entrySet()) {
            if (entry.getValue() != null) {
                rootSpan.put(ATTRIBUTES_PREFIX + entry.getKey(), entry.getValue());
                LOG.debug("Propagated {} = {} to root span {}", entry.getKey(), entry.getValue(), rootSpan.getSpanId());
            }
        }

        if (!rootHasTokens && foundTokens) {
            rootSpan.put(ATTRIBUTES_PREFIX + GEN_AI_INPUT_TOKENS_KEY, totalInputTokens);
            rootSpan.put(ATTRIBUTES_PREFIX + GEN_AI_OUTPUT_TOKENS_KEY, totalOutputTokens);
            LOG.debug("Aggregated tokens (input={}, output={}) to root span {}", totalInputTokens, totalOutputTokens, rootSpan.getSpanId());
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
