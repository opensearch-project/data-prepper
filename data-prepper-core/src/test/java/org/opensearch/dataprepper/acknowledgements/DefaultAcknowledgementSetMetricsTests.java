/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.acknowledgements;

import org.opensearch.dataprepper.metrics.PluginMetrics;
import io.micrometer.core.instrument.Counter;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.mock;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.junit.jupiter.api.BeforeEach;
import org.mockito.junit.jupiter.MockitoExtension;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(MockitoExtension.class)
public class DefaultAcknowledgementSetMetricsTests {
    @Mock
    private Counter createdCounter;
    @Mock
    private Counter completedCounter;
    @Mock
    private Counter expiredCounter;
    @Mock
    private Counter invalidAcquiresCounter;
    @Mock
    private Counter invalidReleasesCounter;
    private DefaultAcknowledgementSetMetrics metrics;

    @Mock
    PluginMetrics pluginMetrics;
    
    @BeforeEach
    public void setup() {
        pluginMetrics = mock(PluginMetrics.class);
        when(pluginMetrics.counter(DefaultAcknowledgementSetMetrics.CREATED_METRIC_NAME)).thenReturn(createdCounter);
        when(pluginMetrics.counter(DefaultAcknowledgementSetMetrics.COMPLETED_METRIC_NAME)).thenReturn(completedCounter);
        when(pluginMetrics.counter(DefaultAcknowledgementSetMetrics.EXPIRED_METRIC_NAME)).thenReturn(expiredCounter);
        when(pluginMetrics.counter(DefaultAcknowledgementSetMetrics.INVALID_ACQUIRES_METRIC_NAME)).thenReturn(invalidAcquiresCounter);
        when(pluginMetrics.counter(DefaultAcknowledgementSetMetrics.INVALID_RELEASES_METRIC_NAME)).thenReturn(invalidReleasesCounter);
    }

    public DefaultAcknowledgementSetMetrics createObjectUnderTest() {
        return new DefaultAcknowledgementSetMetrics(pluginMetrics);
    }

    @Test
    public void testCounters() {
        metrics = createObjectUnderTest();
        metrics.increment(DefaultAcknowledgementSetMetrics.CREATED_METRIC_NAME);
        verify(createdCounter, times(1)).increment();
        metrics.increment(DefaultAcknowledgementSetMetrics.COMPLETED_METRIC_NAME);
        verify(completedCounter, times(1)).increment();
        metrics.increment(DefaultAcknowledgementSetMetrics.EXPIRED_METRIC_NAME);
        verify(expiredCounter, times(1)).increment();
        metrics.increment(DefaultAcknowledgementSetMetrics.INVALID_ACQUIRES_METRIC_NAME);
        verify(invalidAcquiresCounter, times(1)).increment();
        metrics.increment(DefaultAcknowledgementSetMetrics.INVALID_RELEASES_METRIC_NAME);
        verify(invalidReleasesCounter, times(1)).increment();
    }
}

