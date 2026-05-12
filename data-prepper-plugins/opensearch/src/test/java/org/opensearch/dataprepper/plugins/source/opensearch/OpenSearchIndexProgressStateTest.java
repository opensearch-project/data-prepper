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
import java.net.SocketTimeoutException;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

class OpenSearchIndexProgressStateTest {

    @Test
    void default_state_has_no_search_failures_and_empty_counts() {
        final OpenSearchIndexProgressState state = new OpenSearchIndexProgressState();

        assertThat(state.isHadSearchFailures(), is(false));
        assertThat(state.getFailureReasonCounts(), notNullValue());
        assertThat(state.getFailureReasonCounts().isEmpty(), is(true));
    }

    @Test
    void jackson_constructor_with_null_failure_counts_initializes_empty_map() {
        final OpenSearchIndexProgressState state = new OpenSearchIndexProgressState(
                "pit-id", 1L, 2L, null, false, null);

        assertThat(state.getFailureReasonCounts(), notNullValue());
        assertThat(state.getFailureReasonCounts().isEmpty(), is(true));
    }

    @Test
    void setFailureReasonCounts_null_resets_to_empty_map() {
        final OpenSearchIndexProgressState state = new OpenSearchIndexProgressState();
        final Map<String, Long> seed = new LinkedHashMap<>();
        seed.put("reason", 2L);
        state.setFailureReasonCounts(seed);
        assertThat(state.getFailureReasonCounts().get("reason"), equalTo(2L));

        state.setFailureReasonCounts(null);

        assertThat(state.getFailureReasonCounts(), notNullValue());
        assertThat(state.getFailureReasonCounts().isEmpty(), is(true));
    }

    @Test
    void recordShardFailures_with_null_is_a_no_op() {
        final OpenSearchIndexProgressState state = new OpenSearchIndexProgressState();

        state.recordShardFailures(null);

        assertThat(state.isHadSearchFailures(), is(false));
        assertThat(state.getFailureReasonCounts().isEmpty(), is(true));
    }

    @Test
    void recordShardFailures_with_no_failures_is_a_no_op() {
        final OpenSearchIndexProgressState state = new OpenSearchIndexProgressState();

        state.recordShardFailures(SearchShardStatistics.empty());

        assertThat(state.isHadSearchFailures(), is(false));
        assertThat(state.getFailureReasonCounts().isEmpty(), is(true));
    }

    @Test
    void recordShardFailures_sets_flag_and_merges_reason_counts() {
        final OpenSearchIndexProgressState state = new OpenSearchIndexProgressState();

        final Map<String, Long> firstBatch = new LinkedHashMap<>();
        firstBatch.put("shard_failure: timed out", 2L);
        firstBatch.put("shard_failure: rejected", 1L);
        state.recordShardFailures(new SearchShardStatistics(5, 2, 3, 0, firstBatch));

        final Map<String, Long> secondBatch = new LinkedHashMap<>();
        secondBatch.put("shard_failure: timed out", 4L);
        secondBatch.put("shard_failure: other", 1L);
        state.recordShardFailures(new SearchShardStatistics(5, 3, 2, 0, secondBatch));

        assertThat(state.isHadSearchFailures(), is(true));
        assertThat(state.getFailureReasonCounts().get("shard_failure: timed out"), equalTo(6L));
        assertThat(state.getFailureReasonCounts().get("shard_failure: rejected"), equalTo(1L));
        assertThat(state.getFailureReasonCounts().get("shard_failure: other"), equalTo(1L));
    }

    @Test
    void recordShardFailures_enforces_the_20_key_cap_across_merges() {
        final OpenSearchIndexProgressState state = new OpenSearchIndexProgressState();

        for (int i = 0; i < SearchShardStatistics.MAX_DISTINCT_REASONS; i++) {
            final Map<String, Long> batch = new LinkedHashMap<>();
            batch.put("reason-" + i, 1L);
            state.recordShardFailures(new SearchShardStatistics(1, 0, 1, 0, batch));
        }

        final Map<String, Long> overflowBatch = new LinkedHashMap<>();
        overflowBatch.put("overflow-a", 1L);
        overflowBatch.put("overflow-b", 2L);
        state.recordShardFailures(new SearchShardStatistics(1, 0, 1, 0, overflowBatch));

        assertThat(state.getFailureReasonCounts().size(), equalTo(SearchShardStatistics.MAX_DISTINCT_REASONS + 1));
        assertThat(state.getFailureReasonCounts().get(SearchShardStatistics.OVERFLOW_REASON_KEY), equalTo(3L));
        assertThat(state.getFailureReasonCounts().get("overflow-a"), nullValue());
    }

    @Test
    void recordRequestFailure_normalizes_exception_into_reason_and_increments_count() {
        final OpenSearchIndexProgressState state = new OpenSearchIndexProgressState();

        state.recordRequestFailure(new SocketTimeoutException("read timed out"));
        state.recordRequestFailure(new SocketTimeoutException("read timed out"));
        state.recordRequestFailure(new IOException("connection reset"));

        assertThat(state.isHadSearchFailures(), is(true));
        assertThat(state.getFailureReasonCounts().get("SocketTimeoutException: read timed out"), equalTo(2L));
        assertThat(state.getFailureReasonCounts().get("IOException: connection reset"), equalTo(1L));
    }

    @Test
    void recordRequestFailure_with_null_throwable_sets_flag_with_unknown_reason() {
        final OpenSearchIndexProgressState state = new OpenSearchIndexProgressState();

        state.recordRequestFailure(null);

        assertThat(state.isHadSearchFailures(), is(true));
        assertThat(state.getFailureReasonCounts().get("unknown"), equalTo(1L));
    }

    @Test
    void recordRequestFailure_with_null_message_uses_class_name_only() {
        final OpenSearchIndexProgressState state = new OpenSearchIndexProgressState();

        state.recordRequestFailure(new RuntimeException((String) null));

        assertThat(state.isHadSearchFailures(), is(true));
        assertThat(state.getFailureReasonCounts().get("RuntimeException"), equalTo(1L));
    }

    @Test
    void setHadSearchFailures_invalidates_aggregator_cache() {
        final OpenSearchIndexProgressState state = new OpenSearchIndexProgressState();
        state.recordRequestFailure(new RuntimeException("boom"));
        assertThat(state.isHadSearchFailures(), is(true));

        state.setHadSearchFailures(false);

        assertThat(state.isHadSearchFailures(), is(false));
    }

    @Test
    void setFailureReasonCounts_invalidates_aggregator_and_uses_new_counts() {
        final OpenSearchIndexProgressState state = new OpenSearchIndexProgressState();
        state.recordRequestFailure(new RuntimeException("initial"));

        final Map<String, Long> replacement = new LinkedHashMap<>();
        replacement.put("replaced", 99L);
        state.setFailureReasonCounts(replacement);

        assertThat(state.getFailureReasonCounts().get("replaced"), equalTo(99L));
        assertThat(state.getFailureReasonCounts().get("RuntimeException: initial"), nullValue());
    }

    @Test
    void recording_after_setter_accumulates_on_top_of_new_state() {
        final OpenSearchIndexProgressState state = new OpenSearchIndexProgressState();

        final Map<String, Long> seed = new LinkedHashMap<>();
        seed.put("existing", 5L);
        state.setFailureReasonCounts(seed);
        state.setHadSearchFailures(true);

        state.recordRequestFailure(new RuntimeException("new error"));

        assertThat(state.getFailureReasonCounts().get("existing"), equalTo(5L));
        assertThat(state.getFailureReasonCounts().get("RuntimeException: new error"), equalTo(1L));
    }

    @Test
    void jackson_constructor_with_pre_populated_data_allows_further_recording() {
        final Map<String, Long> existing = new LinkedHashMap<>();
        existing.put("prior_error", 10L);
        final OpenSearchIndexProgressState state = new OpenSearchIndexProgressState(
                null, null, null, null, true, existing);

        state.recordRequestFailure(new RuntimeException("new"));

        assertThat(state.isHadSearchFailures(), is(true));
        assertThat(state.getFailureReasonCounts().get("prior_error"), equalTo(10L));
        assertThat(state.getFailureReasonCounts().get("RuntimeException: new"), equalTo(1L));
    }
}
