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
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Remembers, per trace, the span ids already covered by a GenAI span or an agent root marker (the
 * span itself or one of its ancestors). Spans that arrive after their trace's root has been
 * processed are enriched without their ancestors; this lets the agent root marker tell whether
 * such a span is really outermost. It also remembers whether the trace's root is a GenAI span or
 * marked at the source: every span of a trace descends from its root, so then no other span of the
 * trace is outermost, however deep it is and whatever order its ancestors arrive in.
 */
final class GenAiCoverage {

    /** Keeps nothing: every parent outside the batch counts as uncovered. */
    static final GenAiCoverage NONE = new GenAiCoverage(null);

    private final Cache<String, TraceCoverage> coveredByTrace;

    private GenAiCoverage(final Cache<String, TraceCoverage> coveredByTrace) {
        this.coveredByTrace = coveredByTrace;
    }

    static GenAiCoverage create(final long maxSpanIds, final Duration timeToLive) {
        return new GenAiCoverage(Caffeine.newBuilder()
                .maximumWeight(maxSpanIds)
                .<String, TraceCoverage>weigher((traceId, coverage) -> coverage.spanIds.size())
                .expireAfterWrite(timeToLive)
                .build());
    }

    /**
     * Runs {@code enrich} with the trace's span ids covered so far and records the ids it returns.
     * The read, the enrichment and the write happen atomically for the trace, so pipeline workers
     * processing spans of one trace at the same time cannot both take a span for the outermost.
     */
    void update(final String traceId, final Function<Set<String>, Set<String>> enrich) {
        update(traceId, false, (known, genAiRootSeen) -> enrich.apply(known));
    }

    /**
     * Same as {@link #update(String, Function)}, also passing {@code enrich} whether the trace's
     * root is known to be a GenAI span or marked at the source: recorded for an earlier batch, or
     * {@code genAiRootInBatch}, which is then recorded for later batches.
     */
    void update(final String traceId, final boolean genAiRootInBatch,
                final BiFunction<Set<String>, Boolean, Set<String>> enrich) {
        if (coveredByTrace == null || traceId == null) {
            enrich.apply(Collections.emptySet(), genAiRootInBatch);
            return;
        }
        coveredByTrace.asMap().compute(traceId, (id, existing) -> {
            final Set<String> known = existing == null ? Collections.emptySet() : existing.spanIds;
            final boolean genAiRootSeen = genAiRootInBatch || (existing != null && existing.genAiRootSeen);
            final Set<String> added = enrich.apply(known, genAiRootSeen);
            final boolean addsSpanIds = !added.isEmpty() && !known.containsAll(added);
            if (!addsSpanIds && (existing == null ? !genAiRootSeen : existing.genAiRootSeen == genAiRootSeen)) {
                return existing;
            }
            final Set<String> merged = new HashSet<>(known);
            merged.addAll(added);
            return new TraceCoverage(merged, genAiRootSeen);
        });
    }

    /**
     * Records a trace's root, a GenAI span or a span marked at the source, as covered and as the
     * trace's GenAI root, before any of its children is enriched: children processed by other
     * pipeline workers while the root is still being processed then see it.
     */
    void coverGenAiRoot(final String traceId, final String rootSpanId) {
        if (coveredByTrace == null || traceId == null) {
            return;
        }
        coveredByTrace.asMap().compute(traceId, (id, existing) -> {
            final Set<String> spanIds = existing == null ? new HashSet<>() : new HashSet<>(existing.spanIds);
            if (rootSpanId != null) {
                spanIds.add(rootSpanId);
            }
            return new TraceCoverage(spanIds, true);
        });
    }

    /** Span ids of the trace covered so far; for tests. */
    Set<String> covered(final String traceId) {
        if (coveredByTrace == null || traceId == null) {
            return Collections.emptySet();
        }
        final TraceCoverage coverage = coveredByTrace.getIfPresent(traceId);
        return coverage == null ? Collections.emptySet() : coverage.spanIds;
    }

    /** Whether the trace's root is recorded as a GenAI span or marked at the source; for tests. */
    boolean isGenAiRootSeen(final String traceId) {
        if (coveredByTrace == null || traceId == null) {
            return false;
        }
        final TraceCoverage coverage = coveredByTrace.getIfPresent(traceId);
        return coverage != null && coverage.genAiRootSeen;
    }

    /** Number of traces in the cache. */
    long estimatedSize() {
        return coveredByTrace == null ? 0 : coveredByTrace.estimatedSize();
    }

    /** Number of span ids in the cache, the unit of {@code genai_coverage_cache_max_size}. */
    long spanIdCount() {
        if (coveredByTrace == null) {
            return 0;
        }
        // The weighted size is updated asynchronously; apply pending writes so the count is current.
        coveredByTrace.cleanUp();
        return coveredByTrace.policy().eviction()
                .map(eviction -> eviction.weightedSize().orElse(0L))
                .orElse(0L);
    }

    /** The covered span ids of one trace, and whether its root is a GenAI root. Never mutated once stored. */
    private static final class TraceCoverage {
        private final Set<String> spanIds;
        private final boolean genAiRootSeen;

        private TraceCoverage(final Set<String> spanIds, final boolean genAiRootSeen) {
            this.spanIds = spanIds;
            this.genAiRootSeen = genAiRootSeen;
        }
    }
}
