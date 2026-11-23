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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Accumulates, per trace, what root span enrichment needs from children that arrive in other
 * batches than their root: the first-seen values of the propagated gen_ai.* attributes, the own
 * token usage of each span that reports it, and the parent links that tell which of those spans
 * contain others, so the trace's totals count only topmost reporters whatever order they arrive
 * in. It keeps nothing about agent root marking.
 */
final class GenAiRootEnrichmentState {

    /** Keeps nothing: every update starts from an empty state that is discarded afterwards. */
    static final GenAiRootEnrichmentState NONE = new GenAiRootEnrichmentState(null);

    private final Cache<String, TraceState> stateByTrace;

    private GenAiRootEnrichmentState(final Cache<String, TraceState> stateByTrace) {
        this.stateByTrace = stateByTrace;
    }

    static GenAiRootEnrichmentState create(final long maxSpanIds, final Duration timeToLive) {
        return new GenAiRootEnrichmentState(Caffeine.newBuilder()
                .maximumWeight(maxSpanIds)
                .<String, TraceState>weigher((traceId, state) -> Math.max(1, state.spanIdCount()))
                .expireAfterWrite(timeToLive)
                .build());
    }

    /**
     * Runs {@code enrich} with the trace's accumulated state and keeps what it records. The read,
     * the change and the write happen atomically for the trace, so pipeline workers processing
     * spans of one trace at the same time do not lose each other's updates. The published state is
     * replaced, never mutated, so {@link #query} needs no lock.
     */
    void update(final String traceId, final Consumer<TraceState> enrich) {
        if (stateByTrace == null || traceId == null) {
            final TraceState state = new TraceState();
            enrich.accept(state);
            state.recomputeTokenTotals();
            return;
        }
        stateByTrace.asMap().compute(traceId, (id, existing) -> {
            final TraceState state = existing == null ? new TraceState() : new TraceState(existing);
            enrich.accept(state);
            state.recomputeTokenTotals();
            return state.isEmpty() ? existing : state;
        });
    }

    /** Same as {@link #update}, but only for a trace that already has state. */
    void updateIfPresent(final String traceId, final Consumer<TraceState> enrich) {
        if (stateByTrace == null || traceId == null) {
            return;
        }
        stateByTrace.asMap().computeIfPresent(traceId, (id, existing) -> {
            final TraceState state = new TraceState(existing);
            enrich.accept(state);
            state.recomputeTokenTotals();
            return state;
        });
    }

    /** Reads the trace's accumulated state; an empty state when the trace is unknown. */
    <T> T query(final String traceId, final Function<TraceState, T> reader) {
        if (stateByTrace == null || traceId == null) {
            return reader.apply(TraceState.EMPTY);
        }
        final TraceState state = stateByTrace.getIfPresent(traceId);
        return reader.apply(state == null ? TraceState.EMPTY : state);
    }

    /** Number of traces in the cache. */
    long estimatedSize() {
        return stateByTrace == null ? 0 : stateByTrace.estimatedSize();
    }

    /**
     * Number of span ids in the cache, parent links plus token-reporting spans, the unit of
     * {@code genai_root_enrichment_cache_max_size}.
     */
    long spanIdCount() {
        if (stateByTrace == null) {
            return 0;
        }
        // The weighted size is updated asynchronously; apply pending writes so the count is current.
        stateByTrace.cleanUp();
        return stateByTrace.policy().eviction()
                .map(eviction -> eviction.weightedSize().orElse(0L))
                .orElse(0L);
    }

    /** The root enrichment state of one trace, accumulated across the batches of its spans. */
    static final class TraceState {
        private static final TraceState EMPTY = new TraceState();

        private final Map<String, String> parentBySpanId;
        private final Map<String, long[]> tokenUsageBySpanId;
        private final Map<String, String> propagatedValues;
        private long inputTokenTotal;
        private long outputTokenTotal;
        private boolean hasTokenTotals;

        private TraceState() {
            parentBySpanId = new HashMap<>();
            tokenUsageBySpanId = new HashMap<>();
            propagatedValues = new HashMap<>();
        }

        private TraceState(final TraceState other) {
            parentBySpanId = new HashMap<>(other.parentBySpanId);
            tokenUsageBySpanId = new HashMap<>(other.tokenUsageBySpanId);
            propagatedValues = new HashMap<>(other.propagatedValues);
            inputTokenTotal = other.inputTokenTotal;
            outputTokenTotal = other.outputTokenTotal;
            hasTokenTotals = other.hasTokenTotals;
        }

        private boolean isEmpty() {
            return parentBySpanId.isEmpty() && tokenUsageBySpanId.isEmpty() && propagatedValues.isEmpty();
        }

        private int spanIdCount() {
            return parentBySpanId.size() + tokenUsageBySpanId.size();
        }

        void recordParentLink(final String spanId, final String parentSpanId) {
            if (spanId != null && parentSpanId != null && !parentSpanId.isEmpty()) {
                parentBySpanId.putIfAbsent(spanId, parentSpanId);
            }
        }

        /** First value wins for each propagated key, within a batch and across batches. */
        void recordPropagatedValue(final String key, final String value) {
            if (value != null) {
                propagatedValues.putIfAbsent(key, value);
            }
        }

        String propagatedValue(final String key) {
            return propagatedValues.get(key);
        }

        /**
         * Records the usage a span reports itself. The trace's totals count only reporters with no
         * recorded ancestor that also reports usage, because an agent span often reports the total
         * of its own LLM calls; they are recomputed from all reporters after each update, so they are
         * the same whichever of an agent and its calls arrives first, and a parent link recorded
         * later still connects reporters recorded earlier.
         */
        void recordTokenUsage(final String spanId, final long inputTokens, final long outputTokens) {
            if (spanId != null) {
                tokenUsageBySpanId.putIfAbsent(spanId, new long[] {inputTokens, outputTokens});
            }
        }

        private void recomputeTokenTotals() {
            long input = 0;
            long output = 0;
            for (final Map.Entry<String, long[]> reporter : tokenUsageBySpanId.entrySet()) {
                if (!hasTokenReportingAncestor(reporter.getKey())) {
                    input += reporter.getValue()[0];
                    output += reporter.getValue()[1];
                }
            }
            inputTokenTotal = input;
            outputTokenTotal = output;
            hasTokenTotals = !tokenUsageBySpanId.isEmpty();
        }

        boolean hasTokenTotals() {
            return hasTokenTotals;
        }

        long inputTokenTotal() {
            return inputTokenTotal;
        }

        long outputTokenTotal() {
            return outputTokenTotal;
        }

        private boolean hasTokenReportingAncestor(final String spanId) {
            final Set<String> seen = new HashSet<>();
            seen.add(spanId);
            String parentSpanId = parentBySpanId.get(spanId);
            while (parentSpanId != null && seen.add(parentSpanId)) {
                if (tokenUsageBySpanId.containsKey(parentSpanId)) {
                    return true;
                }
                parentSpanId = parentBySpanId.get(parentSpanId);
            }
            return false;
        }
    }
}
