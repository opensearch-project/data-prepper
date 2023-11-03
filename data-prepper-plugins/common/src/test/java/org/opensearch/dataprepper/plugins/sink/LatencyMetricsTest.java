/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink;

import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.event.EventHandle;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.ArgumentMatchers.any;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

import java.time.Instant;
import java.util.Map;
import java.util.HashMap;
import java.util.function.ToDoubleFunction;

class LatencyMetricsTest {

    private PluginMetrics pluginMetrics;
    private EventHandle eventHandle;
    private LatencyMetrics latencyMetrics;
    private Map<String, Double> metricsMap;

    public LatencyMetrics createObjectUnderTest() {
        return new LatencyMetrics(pluginMetrics);
    }

    @BeforeEach
    void setup() {
        pluginMetrics = mock(PluginMetrics.class);
        eventHandle = mock(EventHandle.class);
        metricsMap = new HashMap<>();
        when(eventHandle.getInternalOriginationTime()).thenReturn(Instant.now());
        doAnswer(a -> {
            ToDoubleFunction<?> func = (ToDoubleFunction<?>)a.getArgument(2);
            double value = func.applyAsDouble(a.getArgument(1));
            metricsMap.put(a.getArgument(0), value);
            return null;
        }).when(pluginMetrics).gauge(any(),any(),any());
        latencyMetrics = createObjectUnderTest();
    }

    @Test
    public void testInternalOriginationTime() {
        latencyMetrics.update(eventHandle);
        verify(pluginMetrics, times(3)).gauge(any(), any(), any());
        assertThat(metricsMap.get(LatencyMetrics.INTERNAL_LATENCY_MIN_MS), greaterThanOrEqualTo(0.0));
        assertThat(metricsMap.get(LatencyMetrics.INTERNAL_LATENCY_MAX_MS), greaterThanOrEqualTo(0.0));
        assertThat(metricsMap.get(LatencyMetrics.INTERNAL_LATENCY_AVG_MS), greaterThanOrEqualTo(0.0));
    }

    @Test
    public void testExternalOriginationTime() {
        when(eventHandle.getExternalOriginationTime()).thenReturn(Instant.now());
        latencyMetrics.update(eventHandle);
        verify(pluginMetrics, times(6)).gauge(any(), any(), any());
        assertThat(metricsMap.get(LatencyMetrics.INTERNAL_LATENCY_MIN_MS), greaterThanOrEqualTo(0.0));
        assertThat(metricsMap.get(LatencyMetrics.INTERNAL_LATENCY_MAX_MS), greaterThanOrEqualTo(0.0));
        assertThat(metricsMap.get(LatencyMetrics.INTERNAL_LATENCY_AVG_MS), greaterThanOrEqualTo(0.0));
        assertThat(metricsMap.get(LatencyMetrics.EXTERNAL_LATENCY_MIN_MS), greaterThanOrEqualTo(0.0));
        assertThat(metricsMap.get(LatencyMetrics.EXTERNAL_LATENCY_MAX_MS), greaterThanOrEqualTo(0.0));
        assertThat(metricsMap.get(LatencyMetrics.EXTERNAL_LATENCY_AVG_MS), greaterThanOrEqualTo(0.0));
    }
}


