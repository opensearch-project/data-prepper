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
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

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

    Set<String> covered(final String traceId) {
        if (coveredByTrace == null || traceId == null) {
            return Collections.emptySet();
        }
        final Set<String> spanIds = coveredByTrace.getIfPresent(traceId);
        return spanIds == null ? Collections.emptySet() : spanIds;
    }

    void record(final String traceId, final Collection<String> spanIds) {
        if (coveredByTrace == null || traceId == null || spanIds.isEmpty()) {
            return;
        }
        coveredByTrace.asMap().merge(traceId, new HashSet<>(spanIds), (existing, added) -> {
            final Set<String> merged = new HashSet<>(existing);
            merged.addAll(added);
            return merged;
        });
    }

    long estimatedSize() {
        return coveredByTrace == null ? 0 : coveredByTrace.estimatedSize();
    }
}
