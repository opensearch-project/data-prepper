/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.processor.genai;

import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.processor.AbstractProcessor;
import org.opensearch.dataprepper.model.processor.Processor;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.trace.Span;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import io.micrometer.core.instrument.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Processor that enriches GenAI agent traces for OpenSearch observability.
 * <p>
 * Runs after {@code otel_trace_raw} in the pipeline and performs:
 * <ul>
 *   <li>Propagation of {@code gen_ai.system}, {@code gen_ai.provider.name}, {@code gen_ai.agent.name},
 *       and {@code gen_ai.request.model} from child spans to root span</li>
 *   <li>Aggregation of {@code gen_ai.usage.input_tokens} and {@code output_tokens} across children to root</li>
 *   <li>Removal of flattened sub-keys that conflict with parent string values (e.g. {@code llm.input_messages.0.message.role})</li>
 * </ul>
 *
 * @see <a href="https://opentelemetry.io/docs/specs/semconv/gen-ai/">OTel GenAI Semantic Conventions v1.39.0</a>
 */
@DataPrepperPlugin(name = "otel_trace_genai", pluginType = Processor.class)
public class OTelTraceGenAiProcessor extends AbstractProcessor<Record<Span>, Record<Span>> {
    private static final Logger LOG = LoggerFactory.getLogger(OTelTraceGenAiProcessor.class);

    private static final String GEN_AI_SYSTEM_KEY = "gen_ai.system";
    private static final String GEN_AI_PROVIDER_NAME_KEY = "gen_ai.provider.name";
    private static final String GEN_AI_AGENT_NAME_KEY = "gen_ai.agent.name";
    private static final String GEN_AI_REQUEST_MODEL_KEY = "gen_ai.request.model";
    private static final String GEN_AI_INPUT_TOKENS_KEY = "gen_ai.usage.input_tokens";
    private static final String GEN_AI_OUTPUT_TOKENS_KEY = "gen_ai.usage.output_tokens";
    private static final String ATTRIBUTES_PREFIX = "attributes/";
    private static final String ZERO_PADDED_SPAN_ID = "0000000000000000";

    /** String attributes to propagate from the first child that has them (first-child-wins). */
    private static final String[] PROPAGATED_STRING_KEYS = {
            GEN_AI_SYSTEM_KEY, GEN_AI_PROVIDER_NAME_KEY, GEN_AI_AGENT_NAME_KEY, GEN_AI_REQUEST_MODEL_KEY
    };

    /** Parent keys whose flattened sub-keys (e.g. {@code llm.input_messages.0.message.role}) are stripped when the parent string value exists. */
    private static final String[] FLATTENED_PARENT_KEYS = {
            "llm.input_messages", "llm.output_messages",
            "gen_ai.prompt", "gen_ai.completion"
    };

    @DataPrepperPluginConstructor
    public OTelTraceGenAiProcessor(final PluginMetrics pluginMetrics) {
        super(pluginMetrics);
    }

    @Override
    public Collection<Record<Span>> doExecute(final Collection<Record<Span>> records) {
        final List<Span> spans = records.stream()
                .map(Record::getData)
                .collect(Collectors.toList());

        for (final Span span : spans) {
            stripFlattenedSubkeys(span);
        }

        propagateGenAiAttributes(spans);

        return records;
    }

    /**
     * Removes flattened sub-keys (e.g. "llm.input_messages.0.message.content") that
     * conflict with parent string values (e.g. "llm.input_messages"), preventing OpenSearch mapping failures.
     */
    private void stripFlattenedSubkeys(final Span span) {
        final Map<String, Object> attrs = span.getAttributes();
        if (attrs == null) {
            return;
        }

        // Only strip sub-keys when the parent string value also exists.
        // If only sub-keys exist (no parent), keep them to avoid data loss.
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
                span.delete("attributes/" + key);
            } catch (final Exception e) {
                LOG.warn("Failed to delete flattened sub-key {}: {}", key, e.getMessage());
            }
        }
    }

    /**
     * Propagates gen_ai.* attributes from child spans to root spans within a batch.
     * Groups spans by traceId, then for each trace:
     * <ol>
     *   <li>Copies string attributes ({@code gen_ai.system}, {@code gen_ai.provider.name},
     *       {@code gen_ai.agent.name}, {@code gen_ai.request.model}) from the first child
     *       that has them (if root lacks them). In multi-model traces, the value depends on
     *       child span ordering within the batch — first child wins.</li>
     *   <li>Aggregates {@code gen_ai.usage.input_tokens} and {@code output_tokens} from children (if root lacks them)</li>
     * </ol>
     */
    private void propagateGenAiAttributes(final List<Span> spans) {
        final Map<String, List<Span>> spansByTrace = new HashMap<>();
        for (final Span span : spans) {
            spansByTrace.computeIfAbsent(span.getTraceId(), k -> new ArrayList<>()).add(span);
        }

        for (final List<Span> traceSpans : spansByTrace.values()) {
            Span rootSpan = null;
            for (final Span span : traceSpans) {
                if (isRootSpan(span)) {
                    rootSpan = span;
                    break;
                }
            }
            if (rootSpan == null) {
                continue;
            }

            final Map<String, Object> rootAttrs = rootSpan.getAttributes();

            // Determine which string attrs the root is missing
            final Map<String, String> toPropagate = new HashMap<>();
            for (final String key : PROPAGATED_STRING_KEYS) {
                if (rootAttrs == null || !rootAttrs.containsKey(key)) {
                    toPropagate.put(key, null);
                }
            }
            final boolean rootHasTokens = rootAttrs != null && rootAttrs.containsKey(GEN_AI_INPUT_TOKENS_KEY);

            if (toPropagate.isEmpty() && rootHasTokens) {
                continue;
            }

            long totalInputTokens = 0;
            long totalOutputTokens = 0;
            boolean foundTokens = false;

            for (final Span span : traceSpans) {
                if (span == rootSpan) {
                    continue;
                }
                final Map<String, Object> attrs = span.getAttributes();
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
    }

    /**
     * Returns true if the span is a root span. Handles both empty/null parentSpanId
     * and the zero-padded sentinel ("0000000000000000") used by some instrumentation libraries.
     */
    private static boolean isRootSpan(final Span span) {
        final String parentSpanId = span.getParentSpanId();
        return StringUtils.isBlank(parentSpanId) || ZERO_PADDED_SPAN_ID.equals(parentSpanId);
    }

    @Override
    public void prepareForShutdown() {}

    @Override
    public boolean isReadyForShutdown() {
        return true;
    }

    @Override
    public void shutdown() {}
}
