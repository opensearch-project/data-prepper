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
import java.util.function.Function;

/**
 * Remembers, per trace, the span ids already covered by a GenAI span or an agent root marker (the
 * span itself or one of its ancestors). Spans that arrive after their trace's root has been
 * processed are enriched without their ancestors; this lets the agent root marker tell whether
 * such a span is really outermost.
 */
final class GenAiCoverage {

    /** Keeps nothing: every parent outside the batch counts as uncovered. */
    static final GenAiCoverage NONE = new GenAiCoverage(null);

    private final Cache<String, Set<String>> coveredByTrace;

    private GenAiCoverage(final Cache<String, Set<String>> coveredByTrace) {
        this.coveredByTrace = coveredByTrace;
    }

    static GenAiCoverage create(final long maxSpanIds, final Duration timeToLive) {
        return new GenAiCoverage(Caffeine.newBuilder()
                .maximumWeight(maxSpanIds)
                .<String, Set<String>>weigher((traceId, spanIds) -> spanIds.size())
                .expireAfterWrite(timeToLive)
                .build());
    }

    /**
     * Runs {@code enrich} with the trace's span ids covered so far and records the ids it returns.
     * The read, the enrichment and the write happen atomically for the trace, so pipeline workers
     * processing spans of one trace at the same time cannot both take a span for the outermost.
     */
    void update(final String traceId, final Function<Set<String>, Set<String>> enrich) {
        if (coveredByTrace == null || traceId == null) {
            enrich.apply(Collections.emptySet());
            return;
        }
        coveredByTrace.asMap().compute(traceId, (id, existing) -> {
            final Set<String> known = existing == null ? Collections.emptySet() : existing;
            final Set<String> added = enrich.apply(known);
            if (added.isEmpty() || known.containsAll(added)) {
                return existing;
            }
            final Set<String> merged = new HashSet<>(known);
            merged.addAll(added);
            return merged;
        });
    }

    /** Span ids of the trace covered so far; for tests. */
    Set<String> covered(final String traceId) {
        if (coveredByTrace == null || traceId == null) {
            return Collections.emptySet();
        }
        final Set<String> spanIds = coveredByTrace.getIfPresent(traceId);
        return spanIds == null ? Collections.emptySet() : spanIds;
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
        return coveredByTrace.policy().eviction()
                .map(eviction -> eviction.weightedSize().orElse(0L))
                .orElse(0L);
    }
}
