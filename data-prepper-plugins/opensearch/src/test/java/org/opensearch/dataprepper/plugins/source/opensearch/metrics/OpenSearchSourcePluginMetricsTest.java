/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.source.opensearch.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Timer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.metrics.PluginMetrics;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class OpenSearchSourcePluginMetricsTest {

    @Mock
    private PluginMetrics pluginMetrics;

    private Counter searchRequestsFailedCounter;
    private Counter searchShardsFailedCounter;
    private Counter indicesCompletedWithFailuresCounter;

    private OpenSearchSourcePluginMetrics objectUnderTest;

    @BeforeEach
    void setup() {
        searchRequestsFailedCounter = mock(Counter.class);
        searchShardsFailedCounter = mock(Counter.class);
        indicesCompletedWithFailuresCounter = mock(Counter.class);

        when(pluginMetrics.counter("documentsProcessed")).thenReturn(mock(Counter.class));
        when(pluginMetrics.counter("indicesProcessed")).thenReturn(mock(Counter.class));
        when(pluginMetrics.counter("processingErrors")).thenReturn(mock(Counter.class));
        when(pluginMetrics.counter("credentialsChanged")).thenReturn(mock(Counter.class));
        when(pluginMetrics.counter("clientRefreshErrors")).thenReturn(mock(Counter.class));
        when(pluginMetrics.counter("searchRequestsFailed")).thenReturn(searchRequestsFailedCounter);
        when(pluginMetrics.counter("searchShardsFailed")).thenReturn(searchShardsFailedCounter);
        when(pluginMetrics.counter("indicesCompletedWithFailures")).thenReturn(indicesCompletedWithFailuresCounter);
        when(pluginMetrics.timer("indexProcessingTime")).thenReturn(mock(Timer.class));
        when(pluginMetrics.summary("bytesReceived")).thenReturn(mock(DistributionSummary.class));
        when(pluginMetrics.summary("bytesProcessed")).thenReturn(mock(DistributionSummary.class));

        objectUnderTest = OpenSearchSourcePluginMetrics.create(pluginMetrics);
    }

    @Test
    void getSearchRequestsFailedCounter_returns_initialized_counter() {
        assertThat(objectUnderTest.getSearchRequestsFailedCounter(), notNullValue());
        assertThat(objectUnderTest.getSearchRequestsFailedCounter(), sameInstance(searchRequestsFailedCounter));
    }

    @Test
    void getSearchShardsFailedCounter_returns_initialized_counter() {
        assertThat(objectUnderTest.getSearchShardsFailedCounter(), notNullValue());
        assertThat(objectUnderTest.getSearchShardsFailedCounter(), sameInstance(searchShardsFailedCounter));
    }

    @Test
    void getIndicesCompletedWithFailuresCounter_returns_initialized_counter() {
        assertThat(objectUnderTest.getIndicesCompletedWithFailuresCounter(), notNullValue());
        assertThat(objectUnderTest.getIndicesCompletedWithFailuresCounter(), sameInstance(indicesCompletedWithFailuresCounter));
    }
}
