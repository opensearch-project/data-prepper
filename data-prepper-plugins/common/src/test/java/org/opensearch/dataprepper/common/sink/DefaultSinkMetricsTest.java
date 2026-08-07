/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.common.sink;

import io.micrometer.core.instrument.Timer;
import org.apache.commons.lang3.RandomStringUtils;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.opensearch.dataprepper.metrics.PluginMetrics;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;
import org.mockito.Mock;

import java.util.concurrent.TimeUnit;

public class DefaultSinkMetricsTest {
    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private Counter requestsSucceededCounter;
    @Mock
    private Counter requestsFailedCounter;
    @Mock
    private Counter eventsSucceededCounter;
    @Mock
    private Counter eventsFailedCounter;
    @Mock
    private Counter eventsDroppedCounter;
    @Mock
    private Counter sinkRetriesCounter;
    @Mock
    private Timer requestLatency;
    @Mock
    private DistributionSummary requestSize;
    @Mock
    private DistributionSummary eventSize;

    private String eventName;

    @BeforeEach
    void setUp() {
        pluginMetrics = mock(PluginMetrics.class);
        eventName = RandomStringUtils.randomNumeric(5);
        requestsSucceededCounter = mock(Counter.class);
        requestsFailedCounter = mock(Counter.class);
        eventsSucceededCounter = mock(Counter.class);
        eventsFailedCounter = mock(Counter.class);
        eventsDroppedCounter = mock(Counter.class);
        sinkRetriesCounter = mock(Counter.class);
        requestLatency = mock(Timer.class);
        requestSize = mock(DistributionSummary.class);
        eventSize = mock(DistributionSummary.class);
        when(pluginMetrics.counter(eq(DefaultSinkMetrics.SINK_REQUESTS_SUCCEEDED))).thenReturn(requestsSucceededCounter);
        when(pluginMetrics.counter(eq(DefaultSinkMetrics.SINK_REQUESTS_FAILED))).thenReturn(requestsFailedCounter);
        when(pluginMetrics.counter(eq(DefaultSinkMetrics.SINK_RETRIES))).thenReturn(sinkRetriesCounter);
        when(pluginMetrics.timer(eq(DefaultSinkMetrics.SINK_REQUEST_LATENCY))).thenReturn(requestLatency);
        when(pluginMetrics.summary(eq("sink"+eventName+"Size"))).thenReturn(eventSize);
        when(pluginMetrics.summary(eq(DefaultSinkMetrics.SINK_REQUEST_SIZE))).thenReturn(requestSize);
        when(pluginMetrics.counter(eq("sink"+eventName+"sSucceeded"))).thenReturn(eventsSucceededCounter);
        when(pluginMetrics.counter(eq("sink"+eventName+"sFailed"))).thenReturn(eventsFailedCounter);
        when(pluginMetrics.counter(eq("sink"+eventName+"sDropped"))).thenReturn(eventsDroppedCounter);
    }

    private DefaultSinkMetrics createObjectUnderTest() {
        return new DefaultSinkMetrics(pluginMetrics, eventName);
    }

    @Test
    void test_sink_metrics() {
        DefaultSinkMetrics defaultSinkMetrics = createObjectUnderTest();
        defaultSinkMetrics.incrementRequestsSuccessCounter(1);
        verify(requestsSucceededCounter).increment(1);
        defaultSinkMetrics.incrementRequestsFailedCounter(1);
        verify(requestsFailedCounter).increment(1);
        defaultSinkMetrics.incrementEventsSuccessCounter(1);
        verify(eventsSucceededCounter).increment(1);
        defaultSinkMetrics.incrementEventsFailedCounter(1);
        verify(eventsFailedCounter).increment(1);
        defaultSinkMetrics.incrementEventsDroppedCounter(1);
        verify(eventsDroppedCounter).increment(1);
        defaultSinkMetrics.incrementRetries(1);
        verify(sinkRetriesCounter).increment(1);
        defaultSinkMetrics.recordRequestSize(1.0);
        verify(requestSize).record(1.0);
        defaultSinkMetrics.recordEventSize(1.0);
        verify(eventSize).record(1.0);
    }

    @ParameterizedTest
    @EnumSource(value = TimeUnit.class, names = {"SECONDS", "MILLISECONDS", "MICROSECONDS", "NANOSECONDS"})
    void recordRequestLatency_with_TimeUnit_calls(final TimeUnit timeUnit) {
        createObjectUnderTest().recordRequestLatency(314, timeUnit);
        verify(requestLatency).record(314, timeUnit);
    }

    @Test
    void recordRequestLatency_with_double_calls_using_NANOSECONDS() {
        createObjectUnderTest().recordRequestLatency(102.0);
        verify(requestLatency).record(102, TimeUnit.NANOSECONDS);
    }
}
