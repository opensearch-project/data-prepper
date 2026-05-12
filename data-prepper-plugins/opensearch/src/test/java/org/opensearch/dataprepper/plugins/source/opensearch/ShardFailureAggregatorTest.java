/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.source.opensearch;

import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.plugins.source.opensearch.worker.client.model.SearchShardStatistics;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

class ShardFailureAggregatorTest {

    @Test
    void new_aggregator_has_no_failures() {
        final ShardFailureAggregator aggregator = new ShardFailureAggregator();

        assertThat(aggregator.hadFailures(), is(false));
        assertThat(aggregator.getFailureReasonCounts().isEmpty(), is(true));
    }

    @Test
    void constructor_with_existing_state_restores_correctly() {
        final Map<String, Long> existing = new LinkedHashMap<>();
        existing.put("reason-a", 5L);
        existing.put("reason-b", 3L);

        final ShardFailureAggregator aggregator = new ShardFailureAggregator(true, existing);

        assertThat(aggregator.hadFailures(), is(true));
        assertThat(aggregator.getFailureReasonCounts().get("reason-a"), equalTo(5L));
        assertThat(aggregator.getFailureReasonCounts().get("reason-b"), equalTo(3L));
    }

    @Test
    void constructor_with_null_counts_initializes_empty_map() {
        final ShardFailureAggregator aggregator = new ShardFailureAggregator(false, null);

        assertThat(aggregator.hadFailures(), is(false));
        assertThat(aggregator.getFailureReasonCounts().isEmpty(), is(true));
    }

    @Test
    void recordShardFailures_with_null_is_a_no_op() {
        final ShardFailureAggregator aggregator = new ShardFailureAggregator();

        aggregator.recordShardFailures(null);

        assertThat(aggregator.hadFailures(), is(false));
        assertThat(aggregator.getFailureReasonCounts().isEmpty(), is(true));
    }

    @Test
    void recordShardFailures_with_no_failures_is_a_no_op() {
        final ShardFailureAggregator aggregator = new ShardFailureAggregator();

        aggregator.recordShardFailures(SearchShardStatistics.empty());

        assertThat(aggregator.hadFailures(), is(false));
    }

    @Test
    void recordShardFailures_sets_flag_and_merges_counts() {
        final ShardFailureAggregator aggregator = new ShardFailureAggregator();

        final Map<String, Long> batch1 = new LinkedHashMap<>();
        batch1.put("timeout", 2L);
        aggregator.recordShardFailures(new SearchShardStatistics(5, 3, 2, 0, batch1));

        final Map<String, Long> batch2 = new LinkedHashMap<>();
        batch2.put("timeout", 1L);
        batch2.put("rejected", 3L);
        aggregator.recordShardFailures(new SearchShardStatistics(5, 2, 3, 0, batch2));

        assertThat(aggregator.hadFailures(), is(true));
        assertThat(aggregator.getFailureReasonCounts().get("timeout"), equalTo(3L));
        assertThat(aggregator.getFailureReasonCounts().get("rejected"), equalTo(3L));
    }

    @Test
    void recordShardFailures_respects_max_distinct_reasons_cap() {
        final ShardFailureAggregator aggregator = new ShardFailureAggregator();

        for (int i = 0; i < ShardFailureAggregator.MAX_DISTINCT_REASONS; i++) {
            final Map<String, Long> batch = new LinkedHashMap<>();
            batch.put("reason-" + i, 1L);
            aggregator.recordShardFailures(new SearchShardStatistics(1, 0, 1, 0, batch));
        }

        final Map<String, Long> overflow = new LinkedHashMap<>();
        overflow.put("new-reason-a", 2L);
        overflow.put("new-reason-b", 3L);
        aggregator.recordShardFailures(new SearchShardStatistics(1, 0, 1, 0, overflow));

        assertThat(aggregator.getFailureReasonCounts().size(),
                equalTo(ShardFailureAggregator.MAX_DISTINCT_REASONS + 1));
        assertThat(aggregator.getFailureReasonCounts().get(ShardFailureAggregator.OVERFLOW_REASON_KEY), equalTo(5L));
        assertThat(aggregator.getFailureReasonCounts().get("new-reason-a"), nullValue());
    }

    @Test
    void recordRequestFailure_normalizes_exception_class_and_message() {
        final ShardFailureAggregator aggregator = new ShardFailureAggregator();

        aggregator.recordRequestFailure(new IOException("connection reset"));
        aggregator.recordRequestFailure(new IOException("connection reset"));

        assertThat(aggregator.hadFailures(), is(true));
        assertThat(aggregator.getFailureReasonCounts().get("IOException: connection reset"), equalTo(2L));
    }

    @Test
    void recordRequestFailure_with_null_throwable_records_unknown() {
        final ShardFailureAggregator aggregator = new ShardFailureAggregator();

        aggregator.recordRequestFailure(null);

        assertThat(aggregator.hadFailures(), is(true));
        assertThat(aggregator.getFailureReasonCounts().get("unknown"), equalTo(1L));
    }

    @Test
    void recordRequestFailure_with_null_message_uses_class_name_only() {
        final ShardFailureAggregator aggregator = new ShardFailureAggregator();

        aggregator.recordRequestFailure(new RuntimeException((String) null));

        assertThat(aggregator.getFailureReasonCounts().get("RuntimeException"), equalTo(1L));
    }

    @Test
    void recordRequestFailure_normalizes_ip_addresses_in_message() {
        final ShardFailureAggregator aggregator = new ShardFailureAggregator();

        aggregator.recordRequestFailure(new IOException("Connection refused to 10.0.1.5:9300"));

        assertThat(aggregator.getFailureReasonCounts().get("IOException: Connection refused to <ip>"), equalTo(1L));
    }

    @Test
    void getFailureReasonCounts_returns_unmodifiable_view() {
        final ShardFailureAggregator aggregator = new ShardFailureAggregator();
        aggregator.recordRequestFailure(new RuntimeException("boom"));

        org.junit.jupiter.api.Assertions.assertThrows(UnsupportedOperationException.class,
                () -> aggregator.getFailureReasonCounts().put("hack", 1L));
    }

    @Test
    void accumulates_both_shard_failures_and_request_failures() {
        final ShardFailureAggregator aggregator = new ShardFailureAggregator();

        final Map<String, Long> shardBatch = new LinkedHashMap<>();
        shardBatch.put("shard_failure: timed out", 2L);
        aggregator.recordShardFailures(new SearchShardStatistics(5, 3, 2, 0, shardBatch));
        aggregator.recordRequestFailure(new IOException("connection refused"));

        assertThat(aggregator.hadFailures(), is(true));
        assertThat(aggregator.getFailureReasonCounts().get("shard_failure: timed out"), equalTo(2L));
        assertThat(aggregator.getFailureReasonCounts().get("IOException: connection refused"), equalTo(1L));
    }
}
