/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.source.opensearch.worker.client.model;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SearchShardStatisticsTest {

    @Test
    void empty_returns_the_same_shared_instance_with_zero_counts() {
        final SearchShardStatistics empty = SearchShardStatistics.empty();

        assertThat(empty, sameInstance(SearchShardStatistics.empty()));
        assertThat(empty.getTotal(), equalTo(0));
        assertThat(empty.getSuccessful(), equalTo(0));
        assertThat(empty.getFailed(), equalTo(0));
        assertThat(empty.getSkipped(), equalTo(0));
        assertThat(empty.getFailureReasonCounts().isEmpty(), is(true));
        assertThat(empty.hasFailures(), is(false));
    }

    @Test
    void constructor_with_null_map_produces_empty_counts() {
        final SearchShardStatistics stats = new SearchShardStatistics(5, 4, 1, 0, null);

        assertThat(stats.getFailureReasonCounts(), equalTo(Collections.emptyMap()));
        assertThat(stats.hasFailures(), is(true));
    }

    @Test
    void hasFailures_is_true_when_failed_is_zero_but_reasons_are_present() {
        final Map<String, Long> counts = new LinkedHashMap<>();
        counts.put("ConnectTimeout: timed out", 1L);
        final SearchShardStatistics stats = new SearchShardStatistics(5, 5, 0, 0, counts);

        assertThat(stats.hasFailures(), is(true));
    }

    @Test
    void getFailureReasonCounts_returns_unmodifiable_view() {
        final Map<String, Long> counts = new LinkedHashMap<>();
        counts.put("reason", 1L);
        final SearchShardStatistics stats = new SearchShardStatistics(1, 0, 1, 0, counts);

        assertThrows(UnsupportedOperationException.class,
                () -> stats.getFailureReasonCounts().put("other", 1L));
    }

    @Test
    void normalizeReason_single_arg_returns_unknown_for_null_or_empty() {
        assertThat(SearchShardStatistics.normalizeReason((String) null), equalTo("unknown"));
        assertThat(SearchShardStatistics.normalizeReason(""), equalTo("unknown"));
        assertThat(SearchShardStatistics.normalizeReason("   "), equalTo("unknown"));
    }

    @Test
    void normalizeReason_strips_shard_ids_node_ids_and_uuids() {
        final String raw = "shard failure on [my-index][3] at node[data-1a2b3c4d] with trace "
                + "123e4567-e89b-12d3-a456-426614174000 failed";

        final String normalized = SearchShardStatistics.normalizeReason(raw);

        assertThat(normalized, containsString("shard failure on [shard] at node[?] with trace <uuid> failed"));
        assertThat(normalized, not(containsString("[my-index][3]")));
        assertThat(normalized, not(containsString("data-1a2b3c4d")));
        assertThat(normalized, not(containsString("123e4567")));
    }

    @Test
    void normalizeReason_two_arg_joins_type_and_message() {
        final String normalized = SearchShardStatistics.normalizeReason("shard_failure", "timed out");

        assertThat(normalized, equalTo("shard_failure: timed out"));
    }

    @Test
    void normalizeReason_two_arg_handles_null_components() {
        assertThat(SearchShardStatistics.normalizeReason(null, null), equalTo("unknown"));
        assertThat(SearchShardStatistics.normalizeReason("type_only", null), equalTo("type_only"));
        assertThat(SearchShardStatistics.normalizeReason(null, "message_only"), equalTo("message_only"));
    }

    @Test
    void incrementFailureReasonCount_adds_new_key_and_sums_existing() {
        final Map<String, Long> counts = new LinkedHashMap<>();

        SearchShardStatistics.incrementFailureReasonCount(counts, "timeout", 1L);
        SearchShardStatistics.incrementFailureReasonCount(counts, "timeout", 2L);
        SearchShardStatistics.incrementFailureReasonCount(counts, "connect_refused", 5L);

        assertThat(counts.get("timeout"), equalTo(3L));
        assertThat(counts.get("connect_refused"), equalTo(5L));
        assertThat(counts.size(), equalTo(2));
    }

    @Test
    void incrementFailureReasonCount_ignores_null_reason_and_non_positive_delta() {
        final Map<String, Long> counts = new LinkedHashMap<>();

        SearchShardStatistics.incrementFailureReasonCount(counts, null, 3L);
        SearchShardStatistics.incrementFailureReasonCount(counts, "reason", 0L);
        SearchShardStatistics.incrementFailureReasonCount(counts, "reason", -1L);

        assertThat(counts.isEmpty(), is(true));
    }

    @Test
    void incrementFailureReasonCount_rejects_null_counts_map() {
        assertThrows(NullPointerException.class,
                () -> SearchShardStatistics.incrementFailureReasonCount(null, "reason", 1L));
    }

    @Test
    void incrementFailureReasonCount_folds_overflow_into_other_bucket_once_cap_is_reached() {
        final Map<String, Long> counts = new LinkedHashMap<>();

        for (int i = 0; i < SearchShardStatistics.MAX_DISTINCT_REASONS; i++) {
            SearchShardStatistics.incrementFailureReasonCount(counts, "reason-" + i, 1L);
        }
        // these all exceed the cap and should fold into the overflow bucket
        SearchShardStatistics.incrementFailureReasonCount(counts, "overflow-a", 2L);
        SearchShardStatistics.incrementFailureReasonCount(counts, "overflow-b", 3L);
        SearchShardStatistics.incrementFailureReasonCount(counts, "overflow-c", 4L);

        assertThat(counts.size(), equalTo(SearchShardStatistics.MAX_DISTINCT_REASONS + 1));
        assertThat(counts.get(SearchShardStatistics.OVERFLOW_REASON_KEY), equalTo(9L));
        assertThat(counts.get("overflow-a"), nullValue());
    }

    @Test
    void incrementFailureReasonCount_still_increments_existing_keys_after_cap_is_reached() {
        final Map<String, Long> counts = new LinkedHashMap<>();
        for (int i = 0; i < SearchShardStatistics.MAX_DISTINCT_REASONS; i++) {
            SearchShardStatistics.incrementFailureReasonCount(counts, "reason-" + i, 1L);
        }

        // already-known key still increments even after cap is hit
        SearchShardStatistics.incrementFailureReasonCount(counts, "reason-0", 10L);
        SearchShardStatistics.incrementFailureReasonCount(counts, "new-key", 1L);

        assertThat(counts.get("reason-0"), equalTo(11L));
        assertThat(counts.get(SearchShardStatistics.OVERFLOW_REASON_KEY), equalTo(1L));
    }

    @Test
    void mergeFailureReasonCounts_merges_maps_and_honors_cap() {
        final Map<String, Long> destination = new LinkedHashMap<>();
        SearchShardStatistics.incrementFailureReasonCount(destination, "shared", 2L);

        final Map<String, Long> source = new LinkedHashMap<>();
        source.put("shared", 3L);
        source.put("new", 5L);

        SearchShardStatistics.mergeFailureReasonCounts(destination, source);

        assertThat(destination.get("shared"), equalTo(5L));
        assertThat(destination.get("new"), equalTo(5L));
    }

    @Test
    void mergeFailureReasonCounts_is_a_no_op_for_null_or_empty_source() {
        final Map<String, Long> counts = new LinkedHashMap<>();
        counts.put("reason", 1L);

        SearchShardStatistics.mergeFailureReasonCounts(counts, null);
        SearchShardStatistics.mergeFailureReasonCounts(counts, Collections.emptyMap());

        assertThat(counts.size(), equalTo(1));
        assertThat(counts.get("reason"), equalTo(1L));
    }

    @Test
    void equals_and_hashCode_compare_all_fields() {
        final Map<String, Long> counts = new LinkedHashMap<>();
        counts.put("a", 1L);

        final SearchShardStatistics a = new SearchShardStatistics(5, 4, 1, 0, counts);
        final SearchShardStatistics b = new SearchShardStatistics(5, 4, 1, 0, counts);
        final SearchShardStatistics c = new SearchShardStatistics(5, 4, 1, 0, Collections.emptyMap());

        assertThat(a, equalTo(b));
        assertThat(a.hashCode(), equalTo(b.hashCode()));
        assertThat(a.equals(c), is(false));
    }

    @Test
    void numberOrZero_returns_zero_for_null() {
        assertThat(SearchShardStatistics.numberOrZero(null), equalTo(0));
    }

    @Test
    void numberOrZero_returns_int_value_for_non_null() {
        assertThat(SearchShardStatistics.numberOrZero(5), equalTo(5));
        assertThat(SearchShardStatistics.numberOrZero(3L), equalTo(3));
        assertThat(SearchShardStatistics.numberOrZero(7.9), equalTo(7));
    }

    @Test
    void fromShardCounts_with_null_failures_produces_empty_reason_map() {
        final SearchShardStatistics stats = SearchShardStatistics.fromShardCounts(10, 8, 2, 0, null);

        assertThat(stats.getTotal(), equalTo(10));
        assertThat(stats.getSuccessful(), equalTo(8));
        assertThat(stats.getFailed(), equalTo(2));
        assertThat(stats.getSkipped(), equalTo(0));
        assertThat(stats.getFailureReasonCounts().isEmpty(), is(true));
    }

    @Test
    void fromShardCounts_with_empty_failures_list_produces_empty_reason_map() {
        final SearchShardStatistics stats = SearchShardStatistics.fromShardCounts(5, 5, 0, 0, Collections.emptyList());

        assertThat(stats.getFailureReasonCounts().isEmpty(), is(true));
        assertThat(stats.hasFailures(), is(false));
    }

    @Test
    void fromShardCounts_with_null_shard_counts_defaults_to_zero() {
        final SearchShardStatistics stats = SearchShardStatistics.fromShardCounts(null, null, null, null, null);

        assertThat(stats.getTotal(), equalTo(0));
        assertThat(stats.getSuccessful(), equalTo(0));
        assertThat(stats.getFailed(), equalTo(0));
        assertThat(stats.getSkipped(), equalTo(0));
    }

    @Test
    void fromShardCounts_aggregates_failure_reasons_from_type_message_pairs() {
        final List<String[]> failures = Arrays.asList(
                new String[]{"shard_failure", "timed out"},
                new String[]{"shard_failure", "timed out"},
                new String[]{"connect_exception", "connection refused"}
        );

        final SearchShardStatistics stats = SearchShardStatistics.fromShardCounts(5, 2, 3, 0, failures);

        assertThat(stats.getFailed(), equalTo(3));
        assertThat(stats.getFailureReasonCounts().get("shard_failure: timed out"), equalTo(2L));
        assertThat(stats.getFailureReasonCounts().get("connect_exception: connection refused"), equalTo(1L));
        assertThat(stats.getFailureReasonCounts().size(), equalTo(2));
    }

    @Test
    void fromShardCounts_handles_null_entries_in_failures_list() {
        final List<String[]> failures = Arrays.asList(
                null,
                new String[]{},
                new String[]{null},
                new String[]{"type_only"}
        );

        final SearchShardStatistics stats = SearchShardStatistics.fromShardCounts(4, 0, 4, 0, failures);

        // null entry, empty array, and array with only null type all normalize to "unknown"
        assertThat(stats.getFailureReasonCounts().containsKey("unknown"), is(true));
        assertThat(stats.getFailureReasonCounts().get("unknown"), equalTo(3L));
        assertThat(stats.getFailureReasonCounts().get("type_only"), equalTo(1L));
    }

    @Test
    void fromShardCounts_respects_cap_when_many_distinct_failures() {
        final List<String[]> failures = new java.util.ArrayList<>();
        for (int i = 0; i < SearchShardStatistics.MAX_DISTINCT_REASONS + 5; i++) {
            failures.add(new String[]{"type_" + i, "message_" + i});
        }

        final SearchShardStatistics stats = SearchShardStatistics.fromShardCounts(30, 5, 25, 0, failures);

        // 20 distinct keys + 1 overflow bucket
        assertThat(stats.getFailureReasonCounts().size(), equalTo(SearchShardStatistics.MAX_DISTINCT_REASONS + 1));
        assertThat(stats.getFailureReasonCounts().get(SearchShardStatistics.OVERFLOW_REASON_KEY), equalTo(5L));
    }

}
