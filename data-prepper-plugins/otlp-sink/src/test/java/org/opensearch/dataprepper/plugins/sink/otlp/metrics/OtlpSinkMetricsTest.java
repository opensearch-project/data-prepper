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
import org.mockito.ArgumentCaptor;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.configuration.PluginSetting;

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.function.ToDoubleFunction;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
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

        // basic pluginSetting behavior
        when(pluginSetting.getPipelineName()).thenReturn("testPipeline");
        when(pluginSetting.getName()).thenReturn("otlp");

        // stub out all counter() calls to return the same Counter mock
        counterMock = mock(Counter.class);
        when(pluginMetrics.counter(anyString())).thenReturn(counterMock);

        summaryMock = mock(DistributionSummary.class);
        timerMock = mock(Timer.class);


        sinkMetrics = new OtlpSinkMetrics(pluginMetrics, pluginSetting);
    }

    @Test
    void testIncrementRecordsOut() {
        sinkMetrics.incrementRecordsOut(7);
        verify(pluginMetrics).counter("recordsOut");
        verify(counterMock).increment(7.0);
    }

    @Test
    void testIncrementErrorsCount() {
        sinkMetrics.incrementErrorsCount();
        verify(pluginMetrics).counter("errorsCount");
        verify(counterMock).increment(1.0);
    }

    @Test
    void testIncrementRejectedRecordsCount() {
        sinkMetrics.incrementRejectedRecordsCount(5);
        verify(pluginMetrics).counter("rejectedRecordsCount");
        verify(counterMock).increment(5.0);
    }

    @Test
    void testIncrementFailedRecordsCount() {
        sinkMetrics.incrementFailedRecordsCount(5);
        verify(pluginMetrics).counter("failedRecordsCount");
        verify(counterMock).increment(5.0);
    }

    @Test
    void testRecordResponseCodeCounters() {
        // 5xx
        sinkMetrics.recordResponseCode(503);
        verify(pluginMetrics).counter("http5xxResponses");
        verify(counterMock).increment();

        // 4xx
        sinkMetrics.recordResponseCode(404);
        verify(pluginMetrics).counter("http4xxResponses");
        // total increments called twice so far
        verify(counterMock, times(2)).increment();

        // 2xx
        sinkMetrics.recordResponseCode(200);
        verify(pluginMetrics).counter("http2xxResponses");
        verify(counterMock, times(3)).increment();
    }

    @Test
    void testRegisterQueueGauges() {
        final BlockingQueue<String> queue = new ArrayBlockingQueue<>(10);
        sinkMetrics.registerQueueGauges(queue);

        // we expect two gauges registered: queueSize and queueCapacity
        verify(pluginMetrics).gauge(eq("queueSize"), eq(queue), any());
        verify(pluginMetrics).gauge(eq("queueCapacity"), eq(queue), any());
    }

    @Test
    @SuppressWarnings("unchecked")
    void testQueueCapacityGaugeFunction() {
        final ArrayBlockingQueue<String> queue = new ArrayBlockingQueue<>(10);
        // put 3 elements so size=3, remainingCapacity=7
        queue.add("one");
        queue.add("two");
        queue.add("three");

        // register the gauges
        sinkMetrics.registerQueueGauges(queue);

        // capture the ToDoubleFunction passed to gauge(...)
        @SuppressWarnings("rawtypes") final ArgumentCaptor<ToDoubleFunction> captor = ArgumentCaptor.forClass(ToDoubleFunction.class);
        verify(pluginMetrics).gauge(eq("queueCapacity"), eq(queue), captor.capture());

        // apply it: remainingCapacity + size == 7 + 3 == 10
        final ToDoubleFunction<ArrayBlockingQueue<String>> func = captor.getValue();
        assertEquals(10.0, func.applyAsDouble(queue), 0.0);
    }

    @Test
    void testIncrementPayloadSize_delegatesToSummary() throws Exception {
        injectField("payloadSize", summaryMock);

        sinkMetrics.incrementPayloadSize(123L);

        verify(summaryMock).record(123L);
    }

    @Test
    void testIncrementPayloadGzipSize_delegatesToSummary() throws Exception {
        injectField("payloadGzipSize", summaryMock);

        sinkMetrics.incrementPayloadGzipSize(77L);

        verify(summaryMock).record(77L);
    }

    @Test
    void testRecordHttpLatency_delegatesToTimer() throws Exception {
        injectField("httpLatency", timerMock);

        sinkMetrics.recordHttpLatency(250L);

        verify(timerMock).record(Duration.ofMillis(250L));
    }

    private void injectField(final String fieldName, final Object mock) throws Exception {
        final Field f = OtlpSinkMetrics.class.getDeclaredField(fieldName);
        f.setAccessible(true);
        f.set(sinkMetrics, mock);
    }
}
