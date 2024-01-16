/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.sink;

import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.event.EventHandle;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.micrometer.core.instrument.DistributionSummary;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

import java.time.Instant;

class SinkLatencyMetricsTest {

    private PluginMetrics pluginMetrics;
    private EventHandle eventHandle;
    private SinkLatencyMetrics latencyMetrics;
    private DistributionSummary internalLatencySummary;
    private DistributionSummary externalLatencySummary;

    public SinkLatencyMetrics createObjectUnderTest() {
        return new SinkLatencyMetrics(pluginMetrics);
    }

    @BeforeEach
    void setup() {
        pluginMetrics = mock(PluginMetrics.class);
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        internalLatencySummary = DistributionSummary
              .builder("internalLatency")
              .baseUnit("milliseconds")
              .register(registry);
        externalLatencySummary = DistributionSummary
              .builder("externalLatency")
              .baseUnit("milliseconds")
              .register(registry);
        when(pluginMetrics.summary(SinkLatencyMetrics.INTERNAL_LATENCY)).thenReturn(internalLatencySummary);
        when(pluginMetrics.summary(SinkLatencyMetrics.EXTERNAL_LATENCY)).thenReturn(externalLatencySummary);
        eventHandle = mock(EventHandle.class);
        when(eventHandle.getInternalOriginationTime()).thenReturn(Instant.now());
        latencyMetrics = createObjectUnderTest();
    }

    @Test
    public void testInternalOriginationTime() {
        latencyMetrics.update(eventHandle);
        assertThat(internalLatencySummary.count(), equalTo(1L));
    }

    @Test
    public void testExternalOriginationTime() {
        when(eventHandle.getExternalOriginationTime()).thenReturn(Instant.now().minusMillis(10));
        latencyMetrics.update(eventHandle);
        assertThat(internalLatencySummary.count(), equalTo(1L));
        assertThat(externalLatencySummary.count(), equalTo(1L));
        assertThat(externalLatencySummary.max(), greaterThanOrEqualTo(10.0));
    }
}


