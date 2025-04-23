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
import org.opensearch.dataprepper.model.configuration.PluginSetting;

import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class OtlpSinkMetricsTest {

    private PluginMetrics pluginMetrics;
    private PluginSetting pluginSetting;
    private Counter counterMock;
    private DistributionSummary summaryMock;
    private Timer timerMock;
    private OtlpSinkMetrics sinkMetrics;

    @BeforeEach
    void setUp() {
        pluginMetrics = mock(PluginMetrics.class);
        pluginSetting = mock(PluginSetting.class);
        counterMock = mock(Counter.class);
        summaryMock = mock(DistributionSummary.class);
        timerMock = mock(Timer.class);

        when(pluginMetrics.counter(anyString())).thenReturn(counterMock);
        when(pluginSetting.getPipelineName()).thenReturn("otlp_pipeline");
        when(pluginSetting.getName()).thenReturn("otlp");

        sinkMetrics = new OtlpSinkMetrics(pluginMetrics, pluginSetting);
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
    void testIncrementErrorsCount() {
        sinkMetrics.incrementErrorsCount();
        verify(counterMock).increment(1);
    }

    @Test
    void testIncrementPayloadSize() {
        sinkMetrics.incrementPayloadSize(1024);
        // Cannot verify summaryMock as DistributionSummary is built statically inside constructor
    }

    @Test
    void testIncrementPayloadGzipSize() {
        sinkMetrics.incrementPayloadGzipSize(2048);
        // Cannot verify summaryMock without injecting mock
    }

    @Test
    void testRecordDeliveryLatency() {
        sinkMetrics.recordDeliveryLatency(150);
        // Would require refactoring to inject mock Timer for verification
    }

    @Test
    void testRecordHttpLatency() {
        sinkMetrics.recordHttpLatency(100);
        // Would require refactoring to inject mock Timer for verification
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
    void testRecordResponseCode_5xx() {
        sinkMetrics.recordResponseCode(503);
        verify(pluginMetrics).counter("http_5xx_responses");
    }

    @Test
    void testRecordResponseCode_4xx() {
        sinkMetrics.recordResponseCode(404);
        verify(pluginMetrics).counter("http_4xx_responses");
    }

    @Test
    void testRecordResponseCode_2xx() {
        sinkMetrics.recordResponseCode(200);
        verify(pluginMetrics).counter("http_2xx_responses");
    }
}