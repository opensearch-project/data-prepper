/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.source.opensearch;

import org.opensearch.dataprepper.plugins.source.opensearch.worker.client.model.SearchShardStatistics;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

class ShardFailureAggregator {

    static final int MAX_DISTINCT_REASONS = 20;
    static final String OVERFLOW_REASON_KEY = "__other__";

    private boolean hadFailures;
    private final Map<String, Long> failureReasonCounts;

    ShardFailureAggregator() {
        this.hadFailures = false;
        this.failureReasonCounts = new LinkedHashMap<>();
    }

    ShardFailureAggregator(final boolean hadFailures, final Map<String, Long> counts) {
        this.hadFailures = hadFailures;
        this.failureReasonCounts = counts == null ? new LinkedHashMap<>() : new LinkedHashMap<>(counts);
    }

    void recordShardFailures(final SearchShardStatistics pageStats) {
        if (pageStats == null || !pageStats.hasFailures()) {
            return;
        }
        this.hadFailures = true;
        mergeFailureReasonCounts(pageStats.getFailureReasonCounts());
    }

    void recordRequestFailure(final Throwable throwable) {
        this.hadFailures = true;
        final String key;
        if (throwable == null) {
            key = "unknown";
        } else {
            final String message = throwable.getMessage() == null ? "" : ": " + throwable.getMessage();
            key = SearchShardStatistics.normalizeReason(throwable.getClass().getSimpleName() + message);
        }
        increment(key, 1L);
    }

    boolean hadFailures() {
        return hadFailures;
    }

    Map<String, Long> getFailureReasonCounts() {
        return Collections.unmodifiableMap(failureReasonCounts);
    }

    private void mergeFailureReasonCounts(final Map<String, Long> toMerge) {
        if (toMerge == null || toMerge.isEmpty()) {
            return;
        }
        for (final Map.Entry<String, Long> entry : toMerge.entrySet()) {
            increment(entry.getKey(), entry.getValue() == null ? 0L : entry.getValue());
        }
    }

    private void increment(final String key, final long delta) {
        if (key == null || delta <= 0) {
            return;
        }
        if (failureReasonCounts.containsKey(key)) {
            failureReasonCounts.merge(key, delta, Long::sum);
            return;
        }
        if (failureReasonCounts.size() < MAX_DISTINCT_REASONS) {
            failureReasonCounts.put(key, delta);
            return;
        }
        failureReasonCounts.merge(OVERFLOW_REASON_KEY, delta, Long::sum);
    }
}
