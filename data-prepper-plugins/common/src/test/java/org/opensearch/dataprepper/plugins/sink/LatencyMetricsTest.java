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
import static org.mockito.ArgumentMatchers.any;

import java.time.Instant;

class LatencyMetricsTest {

    private PluginMetrics pluginMetrics;
    private EventHandle eventHandle;
    private LatencyMetrics latencyMetrics;

    public LatencyMetrics createObjectUnderTest() {
        return new LatencyMetrics(pluginMetrics);
    }

    @BeforeEach
    void setup() {
        pluginMetrics = mock(PluginMetrics.class);
        eventHandle = mock(EventHandle.class);
        when(eventHandle.getInternalOriginationTime()).thenReturn(Instant.now());
        latencyMetrics = createObjectUnderTest();
    }

    @Test
    public void testInternalOriginationTime() {
        latencyMetrics.update(eventHandle);
        verify(pluginMetrics, times(3)).gauge(any(), any(), any());
    }

    @Test
    public void testExternalOriginationTime() {
        when(eventHandle.getExternalOriginationTime()).thenReturn(Instant.now());
        latencyMetrics.update(eventHandle);
        verify(pluginMetrics, times(6)).gauge(any(), any(), any());
    }
}


