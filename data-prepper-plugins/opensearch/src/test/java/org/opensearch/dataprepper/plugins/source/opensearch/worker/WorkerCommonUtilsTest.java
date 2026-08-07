/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.opensearch.worker;

import io.micrometer.core.instrument.Counter;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.plugins.source.opensearch.OpenSearchIndexProgressState;
import org.opensearch.dataprepper.plugins.source.opensearch.metrics.OpenSearchSourcePluginMetrics;
import org.opensearch.dataprepper.plugins.source.opensearch.worker.client.model.SearchShardStatistics;
import org.opensearch.dataprepper.plugins.source.opensearch.worker.client.model.SearchWithSearchAfterResults;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.plugins.source.opensearch.worker.WorkerCommonUtils.MAX_BACKOFF;

public class WorkerCommonUtilsTest {

    @ParameterizedTest
    @MethodSource("retryCountToExpectedBackoffRange")
    void calculateLinearBackoffAndJitter_returns_expected_backoff_range_for_retryCount(
            final int retryCount, final long minExpectedBackoff, final long maxExpectedBackoff) {

        final long backOffForRetryCount = WorkerCommonUtils.calculateExponentialBackoffAndJitter(retryCount);

        assertThat(backOffForRetryCount, Matchers.greaterThanOrEqualTo(minExpectedBackoff));
        assertThat(backOffForRetryCount, Matchers.lessThanOrEqualTo(maxExpectedBackoff));
    }

    private static Stream<Arguments> retryCountToExpectedBackoffRange() {
        return Stream.of(
                Arguments.of(1, 1, 2_500),
                Arguments.of(2, 1, 3_000),
                Arguments.of(3, 1, 4_000),
                Arguments.of(4, 2_000, 6_000),
                Arguments.of(7, 30_000, 34_000),
                Arguments.of(8, MAX_BACKOFF.toMillis(), MAX_BACKOFF.toMillis())
        );
    }

    @Test
    void hasMorePages_returns_false_when_results_is_null() {
        assertThat(WorkerCommonUtils.hasMorePages(null), equalTo(false));
    }

    @Test
    void hasMorePages_returns_false_when_nextSearchAfter_is_null() {
        final SearchWithSearchAfterResults results = SearchWithSearchAfterResults.builder()
                .withDocuments(List.of(mock(Event.class)))
                .withNextSearchAfter(null)
                .build();

        assertThat(WorkerCommonUtils.hasMorePages(results), equalTo(false));
    }

    @Test
    void hasMorePages_returns_false_when_documents_list_is_empty() {
        final SearchWithSearchAfterResults results = SearchWithSearchAfterResults.builder()
                .withDocuments(Collections.emptyList())
                .withNextSearchAfter(List.of("cursor"))
                .build();

        assertThat(WorkerCommonUtils.hasMorePages(results), equalTo(false));
    }

    @Test
    void hasMorePages_returns_false_when_documents_is_null() {
        final SearchWithSearchAfterResults results = SearchWithSearchAfterResults.builder()
                .withDocuments(null)
                .withNextSearchAfter(List.of("cursor"))
                .build();

        assertThat(WorkerCommonUtils.hasMorePages(results), equalTo(false));
    }

    @Test
    void hasMorePages_returns_true_when_documents_present_and_nextSearchAfter_present() {
        final SearchWithSearchAfterResults results = SearchWithSearchAfterResults.builder()
                .withDocuments(List.of(mock(Event.class)))
                .withNextSearchAfter(List.of("cursor"))
                .build();

        assertThat(WorkerCommonUtils.hasMorePages(results), equalTo(true));
    }

    @Test
    void hasMorePages_returns_true_for_short_page_when_nextSearchAfter_present() {
        final SearchWithSearchAfterResults results = SearchWithSearchAfterResults.builder()
                .withDocuments(List.of(mock(Event.class)))
                .withNextSearchAfter(List.of("cursor"))
                .build();

        assertThat(WorkerCommonUtils.hasMorePages(results), equalTo(true));
    }

    @Test
    void recordShardFailuresIfAny_is_no_op_when_stats_is_null() {
        final OpenSearchIndexProgressState progressState = new OpenSearchIndexProgressState();
        final OpenSearchSourcePluginMetrics metrics = mock(OpenSearchSourcePluginMetrics.class);

        WorkerCommonUtils.recordShardFailuresIfAny("test-index", null, progressState, metrics);

        assertThat(progressState.isHadSearchFailures(), is(false));
    }

    @Test
    void recordShardFailuresIfAny_is_no_op_when_stats_has_no_failures() {
        final OpenSearchIndexProgressState progressState = new OpenSearchIndexProgressState();
        final OpenSearchSourcePluginMetrics metrics = mock(OpenSearchSourcePluginMetrics.class);

        WorkerCommonUtils.recordShardFailuresIfAny("test-index", SearchShardStatistics.empty(), progressState, metrics);

        assertThat(progressState.isHadSearchFailures(), is(false));
    }

    @Test
    void recordShardFailuresIfAny_increments_counter_and_records_to_progress_state() {
        final OpenSearchIndexProgressState progressState = new OpenSearchIndexProgressState();
        final OpenSearchSourcePluginMetrics metrics = mock(OpenSearchSourcePluginMetrics.class);
        final Counter shardsFailedCounter = mock(Counter.class);
        when(metrics.getSearchShardsFailedCounter()).thenReturn(shardsFailedCounter);

        final Map<String, Long> reasons = new LinkedHashMap<>();
        reasons.put("shard_failure: timeout", 3L);
        final SearchShardStatistics stats = new SearchShardStatistics(5, 2, 3, 0, reasons);

        WorkerCommonUtils.recordShardFailuresIfAny("test-index", stats, progressState, metrics);

        verify(shardsFailedCounter).increment(3);
        assertThat(progressState.isHadSearchFailures(), is(true));
        assertThat(progressState.getFailureReasonCounts().get("shard_failure: timeout"), equalTo(3L));
    }

    @Test
    void recordShardFailuresIfAny_handles_null_metrics_gracefully() {
        final OpenSearchIndexProgressState progressState = new OpenSearchIndexProgressState();

        final Map<String, Long> reasons = new LinkedHashMap<>();
        reasons.put("reason", 1L);
        final SearchShardStatistics stats = new SearchShardStatistics(5, 4, 1, 0, reasons);

        WorkerCommonUtils.recordShardFailuresIfAny("test-index", stats, progressState, null);

        assertThat(progressState.isHadSearchFailures(), is(true));
    }

    @Test
    void recordShardFailuresIfAny_handles_null_progress_state_gracefully() {
        final OpenSearchSourcePluginMetrics metrics = mock(OpenSearchSourcePluginMetrics.class);
        final Counter shardsFailedCounter = mock(Counter.class);
        when(metrics.getSearchShardsFailedCounter()).thenReturn(shardsFailedCounter);

        final Map<String, Long> reasons = new LinkedHashMap<>();
        reasons.put("reason", 1L);
        final SearchShardStatistics stats = new SearchShardStatistics(5, 4, 1, 0, reasons);

        WorkerCommonUtils.recordShardFailuresIfAny("test-index", stats, null, metrics);

        verify(shardsFailedCounter).increment(1);
    }

    @Test
    void recordShardFailuresIfAny_does_not_increment_counter_when_failed_is_zero_but_reasons_present() {
        final OpenSearchSourcePluginMetrics metrics = mock(OpenSearchSourcePluginMetrics.class);
        final Counter shardsFailedCounter = mock(Counter.class);
        when(metrics.getSearchShardsFailedCounter()).thenReturn(shardsFailedCounter);

        final Map<String, Long> reasons = new LinkedHashMap<>();
        reasons.put("reason", 1L);
        final SearchShardStatistics stats = new SearchShardStatistics(5, 5, 0, 0, reasons);

        WorkerCommonUtils.recordShardFailuresIfAny("test-index", stats, new OpenSearchIndexProgressState(), metrics);

        verify(shardsFailedCounter, never()).increment(0);
    }
}
