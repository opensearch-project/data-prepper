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

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Enriches trace root spans with gen_ai.system, gen_ai.provider.name, gen_ai.agent.name,
 * gen_ai.request.model and token totals from their children, including children processed in
 * other batches than the root. Each batch is recorded into a {@link GenAiRootEnrichmentState}
 * first, then the batch's roots are filled from that state, so a root gets the values of every
 * child recorded so far. Agent root marking is not done here.
 */
final class GenAiRootEnrichment {
    private static final Logger LOG = LoggerFactory.getLogger(GenAiRootEnrichment.class);

    private static final String GEN_AI_SYSTEM_KEY = "gen_ai.system";
    private static final String GEN_AI_PROVIDER_NAME_KEY = "gen_ai.provider.name";
    private static final String GEN_AI_AGENT_NAME_KEY = "gen_ai.agent.name";
    private static final String GEN_AI_REQUEST_MODEL_KEY = "gen_ai.request.model";
    private static final String GEN_AI_INPUT_TOKENS_KEY = "gen_ai.usage.input_tokens";
    private static final String GEN_AI_OUTPUT_TOKENS_KEY = "gen_ai.usage.output_tokens";
    private static final String GEN_AI_OPERATION_NAME_KEY = "gen_ai.operation.name";
    private static final String DERIVED_GEN_AI_IS_ROOT_KEY = "derived.gen_ai.is_root";
    private static final String ATTRIBUTES_PREFIX = "attributes/";

    private static final String[] PROPAGATED_STRING_KEYS = {
            GEN_AI_SYSTEM_KEY, GEN_AI_PROVIDER_NAME_KEY, GEN_AI_AGENT_NAME_KEY, GEN_AI_REQUEST_MODEL_KEY
    };

    private GenAiRootEnrichment() {}

    /**
     * Records the spans of the batch into {@code state}, then fills each root of the batch that
     * {@code propagation} selects with the values and token totals it does not have yet. Vendor
     * attributes are normalized first, as {@link GenAiEnrichmentHelper#enrichBatch} does, so the
     * values are read by their gen_ai.* names. Within a batch, values come from the outermost GenAI
     * spans first, then other GenAI spans, then the remaining spans, earliest first; across batches
     * the first value recorded for each attribute wins.
     */
    static void enrichRoots(final List<Span> spans, final GenAiRootEnrichmentState state,
                            final GenAiRootPropagation propagation) {
        if (spans.isEmpty() || propagation == GenAiRootPropagation.NONE) {
            return;
        }
        GenAiEnrichmentHelper.normalizeBatch(spans);
        enrichNormalizedRoots(spans, state, propagation);
    }

    /**
     * Same as {@link #enrichRoots}, for a batch {@link GenAiEnrichmentHelper#normalizeBatch} already
     * normalized.
     */
    static void enrichNormalizedRoots(final List<Span> spans, final GenAiRootEnrichmentState state,
                                      final GenAiRootPropagation propagation) {
        if (spans.isEmpty() || propagation == GenAiRootPropagation.NONE) {
            return;
        }
        final Map<String, List<Span>> spansByTrace = new LinkedHashMap<>();
        for (final Span span : spans) {
            spansByTrace.computeIfAbsent(span.getTraceId(), k -> new ArrayList<>()).add(span);
        }

        for (final Map.Entry<String, List<Span>> trace : spansByTrace.entrySet()) {
            final TraceSpans traceSpans = new TraceSpans(trace.getValue());
            final SpanView root = traceSpans.root();
            if (!traceSpans.hasGenAiData()) {
                // Spans without GenAI data never create state, but once the trace has some they still
                // link later token reporters to earlier ones, and a root selected for propagation (a
                // non-GenAI root with all_roots) is filled from the children recorded so far.
                state.updateIfPresent(trace.getKey(), traceState -> {
                    traceSpans.recordParentLinks(traceState);
                    if (root != null && propagatesTo(root, propagation)) {
                        fillRoot(root, traceState);
                    }
                });
                continue;
            }
            state.update(trace.getKey(), traceState -> {
                traceSpans.record(traceState);
                if (root != null && propagatesTo(root, propagation)) {
                    fillRoot(root, traceState);
                }
            });
        }
    }

    /**
     * Whether a span is itself a GenAI span or is marked {@code true} at the source. The processor
     * normalizes vendor attributes before calling this.
     */
    static boolean isGenAiOrMarkedSpan(final Span span) {
        final Map<String, Object> attributes = span.getAttributes();
        if (attributes == null) {
            return false;
        }
        return attributes.get(GEN_AI_OPERATION_NAME_KEY) != null
                || Boolean.TRUE.equals(GenAiEnrichmentHelper.toBoolean(attributes.get(DERIVED_GEN_AI_IS_ROOT_KEY)));
    }

    /**
     * Whether a trace root is a GenAI root that {@code propagation} enriches and that is still
     * missing a model or token usage {@code state} cannot provide yet, so holding it for late
     * children could complete it. A non-GenAI root is never awaited: holding every root that might
     * turn out to belong to a GenAI trace would delay all traffic. The processor normalizes vendor
     * attributes before calling this, so a vendor root and its vendor model and token keys count.
     */
    static boolean rootAwaitsEnrichment(final Span rootSpan, final GenAiRootEnrichmentState state,
                                        final GenAiRootPropagation propagation) {
        return missingRootData(rootSpan, propagation)
                .map(missing -> !state.query(rootSpan.getTraceId(), missing::isProvidedBy))
                .orElse(false);
    }

    /**
     * What a GenAI root that {@code propagation} enriches still lacks: a model, token usage or
     * both; empty when it is not such a root or lacks neither.
     */
    private static Optional<MissingRootData> missingRootData(final Span rootSpan, final GenAiRootPropagation propagation) {
        if (propagation == GenAiRootPropagation.NONE || !isGenAiOrMarkedSpan(rootSpan)) {
            return Optional.empty();
        }
        final Map<String, Object> attributes = rootSpan.getAttributes();
        final boolean needsModel = !attributes.containsKey(GEN_AI_REQUEST_MODEL_KEY);
        final boolean needsTokens = !attributes.containsKey(GEN_AI_INPUT_TOKENS_KEY)
                && !attributes.containsKey(GEN_AI_OUTPUT_TOKENS_KEY);
        if (!needsModel && !needsTokens) {
            return Optional.empty();
        }
        return Optional.of(new MissingRootData(needsModel, needsTokens));
    }

    /** The model and token usage a GenAI root lacks, and whether the children recorded so far provide them. */
    private static final class MissingRootData {
        private final boolean model;
        private final boolean tokens;

        private MissingRootData(final boolean model, final boolean tokens) {
            this.model = model;
            this.tokens = tokens;
        }

        private boolean isProvidedBy(final GenAiRootEnrichmentState.TraceState traceState) {
            return (!model || traceState.propagatedValue(GEN_AI_REQUEST_MODEL_KEY) != null)
                    && (!tokens || traceState.hasTokenTotals());
        }
    }

    /**
     * Whether the root gets its children's values. With {@code ai_root}, only a root that is its
     * trace's AI root: a GenAI span, or a span marked {@code true} at the source.
     */
    private static boolean propagatesTo(final SpanView root, final GenAiRootPropagation propagation) {
        switch (propagation) {
            case ALL_ROOTS:
                return true;
            case NONE:
                return false;
            case AI_ROOT:
            default:
                return root.genAi || Boolean.TRUE.equals(root.marker);
        }
    }

    private static void fillRoot(final SpanView root, final GenAiRootEnrichmentState.TraceState traceState) {
        for (final String key : PROPAGATED_STRING_KEYS) {
            if (!root.attributes.containsKey(key)) {
                final String value = traceState.propagatedValue(key);
                if (value != null) {
                    root.span.put(ATTRIBUTES_PREFIX + key, value);
                    LOG.debug("Propagated {} = {} to root span {}", key, value, root.spanId());
                }
            }
        }
        if (!root.hasTokens() && traceState.hasTokenTotals()) {
            root.span.put(ATTRIBUTES_PREFIX + GEN_AI_INPUT_TOKENS_KEY, traceState.inputTokenTotal());
            root.span.put(ATTRIBUTES_PREFIX + GEN_AI_OUTPUT_TOKENS_KEY, traceState.outputTokenTotal());
            LOG.debug("Aggregated tokens (input={}, output={}) to root span {}",
                    traceState.inputTokenTotal(), traceState.outputTokenTotal(), root.spanId());
        }
    }

    /** One span with the attributes root enrichment reads, read once. */
    private static final class SpanView {
        private final Span span;
        private final Map<String, Object> attributes;
        private final boolean genAi;
        private final Boolean marker;
        private final Long inputTokens;
        private final Long outputTokens;
        private final Instant startTime;

        private SpanView(final Span span) {
            this.span = span;
            final Map<String, Object> attrs = span.getAttributes();
            this.attributes = attrs == null ? Map.of() : attrs;
            this.genAi = attributes.get(GEN_AI_OPERATION_NAME_KEY) != null;
            this.marker = GenAiEnrichmentHelper.toBoolean(attributes.get(DERIVED_GEN_AI_IS_ROOT_KEY));
            this.inputTokens = GenAiEnrichmentHelper.toLong(attributes.get(GEN_AI_INPUT_TOKENS_KEY));
            this.outputTokens = GenAiEnrichmentHelper.toLong(attributes.get(GEN_AI_OUTPUT_TOKENS_KEY));
            this.startTime = GenAiEnrichmentHelper.toInstant(span.getStartTime());
        }

        private String spanId() {
            return span.getSpanId();
        }

        private String parentSpanId() {
            return span.getParentSpanId();
        }

        private boolean hasTokens() {
            return inputTokens != null || outputTokens != null;
        }

        private boolean isRoot() {
            final String parentSpanId = parentSpanId();
            return parentSpanId == null || parentSpanId.isEmpty() || "0000000000000000".equals(parentSpanId);
        }
    }

    private static final Comparator<SpanView> BY_START_TIME = Comparator.comparing(
            (SpanView view) -> view.startTime, Comparator.nullsLast(Comparator.naturalOrder()));

    /** The spans of one trace in the batch. */
    private static final class TraceSpans {
        private final List<SpanView> spans = new ArrayList<>();
        private final Map<String, SpanView> bySpanId = new HashMap<>();

        private TraceSpans(final Collection<Span> traceSpans) {
            for (final Span span : traceSpans) {
                final SpanView view = new SpanView(span);
                spans.add(view);
                if (view.spanId() != null) {
                    bySpanId.put(view.spanId(), view);
                }
            }
        }

        private boolean hasGenAiData() {
            for (final SpanView view : spans) {
                if (view.genAi || view.marker != null || view.hasTokens()) {
                    return true;
                }
                for (final String key : PROPAGATED_STRING_KEYS) {
                    if (view.attributes.containsKey(key)) {
                        return true;
                    }
                }
            }
            return false;
        }

        private SpanView root() {
            for (final SpanView view : spans) {
                if (view.isRoot()) {
                    return view;
                }
            }
            return null;
        }

        private void recordParentLinks(final GenAiRootEnrichmentState.TraceState traceState) {
            for (final SpanView view : spans) {
                if (!view.isRoot()) {
                    traceState.recordParentLink(view.spanId(), view.parentSpanId());
                }
            }
        }

        /**
         * Records the batch's children of the root (every span but the root): their parent links,
         * the usage each reports itself, and their propagated values in order of preference.
         */
        private void record(final GenAiRootEnrichmentState.TraceState traceState) {
            recordParentLinks(traceState);
            final SpanView root = root();
            final List<SpanView> children = new ArrayList<>();
            for (final SpanView view : spans) {
                if (view != root) {
                    children.add(view);
                }
            }
            for (final SpanView child : children) {
                if (child.hasTokens()) {
                    traceState.recordTokenUsage(child.spanId(),
                            child.inputTokens != null ? child.inputTokens : 0L,
                            child.outputTokens != null ? child.outputTokens : 0L);
                }
            }

            // Outermost among the children: GenAI spans with no GenAI or marked ancestor below the root.
            final Set<SpanView> outer = new HashSet<>();
            for (final SpanView child : children) {
                if (child.genAi && !hasGenAiAncestorBelow(child, root)) {
                    outer.add(child);
                }
            }
            children.sort(Comparator.<SpanView>comparingInt(view -> outer.contains(view) ? 0 : view.genAi ? 1 : 2)
                    .thenComparing(BY_START_TIME));
            for (final SpanView child : children) {
                for (final String key : PROPAGATED_STRING_KEYS) {
                    final Object value = child.attributes.get(key);
                    if (value != null) {
                        traceState.recordPropagatedValue(key, value.toString());
                    }
                }
            }
        }

        private boolean hasGenAiAncestorBelow(final SpanView view, final SpanView root) {
            final Set<String> seen = new HashSet<>();
            SpanView parent = bySpanId.get(view.parentSpanId());
            while (parent != null && seen.add(parent.spanId())) {
                if (parent != root && (parent.genAi || Boolean.TRUE.equals(parent.marker))) {
                    return true;
                }
                parent = bySpanId.get(parent.parentSpanId());
            }
            return false;
        }
    }
}
