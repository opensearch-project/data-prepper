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
 * Accumulates, per trace, the GenAI state seen so far across batches: the span ids already covered
 * by a GenAI span or an agent root marker, the parent link of every span processed once the trace
 * has state, whether the trace's root is a GenAI span or marked at the source, which spans reported
 * token usage with running topmost-only totals, and the first-seen values of the propagated gen_ai.*
 * attributes. Spans of a trace can arrive in many batches; this state lets the agent root marker
 * tell whether a late span is really outermost, and lets a root span be enriched even when its
 * children were processed in earlier batches.
 */
final class GenAiTraceState {

    /** Keeps nothing: every enrichment starts from an empty state that is discarded afterwards. */
    static final GenAiTraceState NONE = new GenAiTraceState(null);

    private final Cache<String, TraceState> stateByTrace;

    private GenAiTraceState(final Cache<String, TraceState> stateByTrace) {
        this.stateByTrace = stateByTrace;
    }

    static GenAiTraceState create(final long maxSpanIds, final Duration timeToLive) {
        return new GenAiTraceState(Caffeine.newBuilder()
                .maximumWeight(maxSpanIds)
                .<String, TraceState>weigher((traceId, state) ->
                        Math.max(1, state.coveredSpanIds.size() + state.parentBySpanId.size()))
                .expireAfterWrite(timeToLive)
                .build());
    }

    /**
     * Runs {@code enrich} with the trace's accumulated state and records what it adds. The read,
     * the enrichment and the write happen atomically for the trace, so pipeline workers processing
     * spans of one trace at the same time cannot both take a span for the outermost. The published
     * state is replaced, never mutated, so {@link #query} needs no lock.
     */
    void update(final String traceId, final Consumer<TraceState> enrich) {
        if (stateByTrace == null || traceId == null) {
            enrich.accept(new TraceState());
            return;
        }
        stateByTrace.asMap().compute(traceId, (id, existing) -> {
            final TraceState state = existing == null ? new TraceState() : new TraceState(existing);
            enrich.accept(state);
            return state.isEmpty() ? existing : state;
        });
    }

    /**
     * Same as {@link #update}, but only for a trace that already has state: a batch without GenAI
     * data must not create state, though once the trace has some its spans' parent links are kept.
     */
    void updateIfPresent(final String traceId, final Consumer<TraceState> enrich) {
        if (stateByTrace == null || traceId == null) {
            return;
        }
        stateByTrace.asMap().computeIfPresent(traceId, (id, existing) -> {
            final TraceState state = new TraceState(existing);
            enrich.accept(state);
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

    /** Span ids of the trace covered so far; for tests. */
    Set<String> covered(final String traceId) {
        return query(traceId, state -> Set.copyOf(state.coveredSpanIds));
    }

    /** Number of traces in the cache. */
    long estimatedSize() {
        return stateByTrace == null ? 0 : stateByTrace.estimatedSize();
    }

    /**
     * Number of span ids in the cache, covered span ids plus recorded parent links, the unit of
     * {@code genai_coverage_cache_max_size}.
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

    /** The GenAI state of one trace, accumulated across the batches of its spans seen so far. */
    static final class TraceState {
        private static final TraceState EMPTY = new TraceState();

        private final Set<String> coveredSpanIds;
        private final Map<String, String> parentBySpanId;
        private final Set<String> tokenReportingSpanIds;
        private final Map<String, String> propagatedValues;
        private long inputTokenTotal;
        private long outputTokenTotal;
        private boolean hasTokenTotals;
        private boolean genAiRootSeen;

        private TraceState() {
            coveredSpanIds = new HashSet<>();
            parentBySpanId = new HashMap<>();
            tokenReportingSpanIds = new HashSet<>();
            propagatedValues = new HashMap<>();
        }

        private TraceState(final TraceState other) {
            coveredSpanIds = new HashSet<>(other.coveredSpanIds);
            parentBySpanId = new HashMap<>(other.parentBySpanId);
            tokenReportingSpanIds = new HashSet<>(other.tokenReportingSpanIds);
            propagatedValues = new HashMap<>(other.propagatedValues);
            inputTokenTotal = other.inputTokenTotal;
            outputTokenTotal = other.outputTokenTotal;
            hasTokenTotals = other.hasTokenTotals;
            genAiRootSeen = other.genAiRootSeen;
        }

        private boolean isEmpty() {
            return coveredSpanIds.isEmpty() && parentBySpanId.isEmpty() && tokenReportingSpanIds.isEmpty()
                    && propagatedValues.isEmpty() && !hasTokenTotals && !genAiRootSeen;
        }

        /**
         * Records that the trace's root is a GenAI span or marked at the source. Every span of a
         * trace descends from its root, so no other span of such a trace can be outermost.
         */
        void recordGenAiRoot() {
            genAiRootSeen = true;
        }

        boolean isGenAiRootSeen() {
            return genAiRootSeen;
        }

        /** The recorded parent of a span processed once the trace had state; null when unknown or a root. */
        String parentSpanId(final String spanId) {
            return parentBySpanId.get(spanId);
        }

        /** Span ids covered by a GenAI span or a marker so far; mutated by recording, so copy to snapshot. */
        Set<String> coveredSpanIds() {
            return coveredSpanIds;
        }

        void cover(final String spanId) {
            if (spanId != null) {
                coveredSpanIds.add(spanId);
            }
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
         * Adds a span's reported usage to the running totals, unless a recorded ancestor or
         * descendant also reported usage: an agent span often reports the total of its own LLM
         * calls, so counting both would count those calls twice.
         */
        void recordTokenUsage(final String spanId, final long inputTokens, final long outputTokens) {
            if (spanId == null || !tokenReportingSpanIds.add(spanId)) {
                return;
            }
            if (hasTokenReportingAncestor(spanId) || isAncestorOfTokenReporter(spanId)) {
                return;
            }
            inputTokenTotal += inputTokens;
            outputTokenTotal += outputTokens;
            hasTokenTotals = true;
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

        /** Recorded ancestors of the span, to order a batch's token reporters topmost first. */
        int ancestorDepth(final String spanId) {
            int depth = 0;
            final Set<String> seen = new HashSet<>();
            String parentSpanId = parentBySpanId.get(spanId);
            while (parentSpanId != null && seen.add(parentSpanId)) {
                depth++;
                parentSpanId = parentBySpanId.get(parentSpanId);
            }
            return depth;
        }

        private boolean hasTokenReportingAncestor(final String spanId) {
            final Set<String> seen = new HashSet<>();
            String parentSpanId = parentBySpanId.get(spanId);
            while (parentSpanId != null && seen.add(parentSpanId)) {
                if (tokenReportingSpanIds.contains(parentSpanId)) {
                    return true;
                }
                parentSpanId = parentBySpanId.get(parentSpanId);
            }
            return false;
        }

        private boolean isAncestorOfTokenReporter(final String spanId) {
            for (final String reporterSpanId : tokenReportingSpanIds) {
                if (reporterSpanId.equals(spanId)) {
                    continue;
                }
                final Set<String> seen = new HashSet<>();
                String parentSpanId = parentBySpanId.get(reporterSpanId);
                while (parentSpanId != null && seen.add(parentSpanId)) {
                    if (parentSpanId.equals(spanId)) {
                        return true;
                    }
                    parentSpanId = parentBySpanId.get(parentSpanId);
                }
            }
            return false;
        }
    }
}
