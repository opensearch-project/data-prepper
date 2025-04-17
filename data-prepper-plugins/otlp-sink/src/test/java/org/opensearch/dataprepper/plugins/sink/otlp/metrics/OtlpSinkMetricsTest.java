/*
 *  Copyright OpenSearch Contributors
 *  SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.otlp.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Timer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.metrics.PluginMetrics;

import java.time.Duration;

import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class OtlpSinkMetricsTest {

    private PluginMetrics pluginMetrics;
    private Counter counterMock;
    private Timer timerMock;
    private DistributionSummary summaryMock;
    private OtlpSinkMetrics sinkMetrics;

    @BeforeEach
    void setUp() {
        pluginMetrics = mock(PluginMetrics.class);
        counterMock = mock(Counter.class);
        summaryMock = mock(DistributionSummary.class);
        timerMock = mock(Timer.class);

        when(pluginMetrics.counter(anyString())).thenReturn(counterMock);
        when(pluginMetrics.summary(anyString())).thenReturn(summaryMock);
        when(pluginMetrics.timer(anyString())).thenReturn(timerMock);

        sinkMetrics = new OtlpSinkMetrics(pluginMetrics);
    }

    @Test
    void testIncrementRecordsIn() {
        sinkMetrics.incrementRecordsIn(3);
        verify(counterMock).increment(3);
    }

    @Test
    void testIncrementRecordsOut() {
        sinkMetrics.incrementRecordsOut(2);
        verify(counterMock).increment(2);
    }

    @Test
    void testIncrementDroppedRecords() {
        sinkMetrics.incrementDroppedRecords(1);
        verify(counterMock).increment(1);
    }

    @Test
    void testIncrementErrorsCount() {
        sinkMetrics.incrementErrorsCount();
        verify(counterMock).increment(1);
    }

    @Test
    void testIncrementPayloadSize() {
        sinkMetrics.incrementPayloadSize(1024);
        verify(summaryMock).record(1024);
    }

    @Test
    void testRecordDeliveryLatency() {
        sinkMetrics.recordDeliveryLatency(150);
        verify(timerMock).record(Duration.ofMillis(150));
    }

    @Test
    void testRecordHttpLatency() {
        sinkMetrics.recordHttpLatency(150);
        verify(timerMock).record(Duration.ofMillis(150));
    }

    @Test
    void testIncrementRetriesCount() {
        sinkMetrics.incrementRetriesCount();
        verify(counterMock).increment(1);
    }

    @Test
    void testIncrementRejectedSpansCount() {
        sinkMetrics.incrementRejectedSpansCount(5);
        verify(counterMock).increment(5);
    }

    @Test
    void testRecordResponseCode() {
        sinkMetrics.recordResponseCode(200);
        verify(pluginMetrics).counter("http_2xx_responses");
    }
}