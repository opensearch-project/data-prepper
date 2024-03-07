/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.sink;

import io.micrometer.core.instrument.Timer;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.event.EventHandle;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

import java.time.Instant;
import java.util.concurrent.TimeUnit;

class SinkLatencyMetricsTest {

    private PluginMetrics pluginMetrics;
    private EventHandle eventHandle;
    private SinkLatencyMetrics latencyMetrics;
    private Timer internalLatencyTimer;
    private Timer externalLatencyTimer;

    public SinkLatencyMetrics createObjectUnderTest() {
        return new SinkLatencyMetrics(pluginMetrics);
    }

    @BeforeEach
    void setup() {
        pluginMetrics = mock(PluginMetrics.class);
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        internalLatencyTimer = Timer
              .builder("internalLatency")
              .register(registry);
        externalLatencyTimer = Timer
              .builder("externalLatency")
              .register(registry);
        when(pluginMetrics.timer(SinkLatencyMetrics.INTERNAL_LATENCY)).thenReturn(internalLatencyTimer);
        when(pluginMetrics.timer(SinkLatencyMetrics.EXTERNAL_LATENCY)).thenReturn(externalLatencyTimer);
        eventHandle = mock(EventHandle.class);
        when(eventHandle.getInternalOriginationTime()).thenReturn(Instant.now());
        latencyMetrics = createObjectUnderTest();
    }

    @Test
    public void testInternalOriginationTime() {
        latencyMetrics.update(eventHandle);
        assertThat(internalLatencyTimer.count(), equalTo(1L));
    }

    @Test
    public void testExternalOriginationTime() {
        when(eventHandle.getExternalOriginationTime()).thenReturn(Instant.now().minusMillis(10));
        latencyMetrics.update(eventHandle);
        assertThat(internalLatencyTimer.count(), equalTo(1L));
        assertThat(externalLatencyTimer.count(), equalTo(1L));
        assertThat(externalLatencyTimer.max(TimeUnit.MILLISECONDS), greaterThanOrEqualTo(10.0));
    }
}


