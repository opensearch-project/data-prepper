/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.sink.otlp;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.log.Log;
import org.opensearch.dataprepper.model.metric.Metric;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.trace.Span;
import org.opensearch.dataprepper.plugins.sink.otlp.buffer.OtlpSinkBuffer;
import org.opensearch.dataprepper.plugins.sink.otlp.configuration.OtlpSinkConfig;
import software.amazon.awssdk.regions.Region;

import java.lang.reflect.Field;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class OtlpSinkTest {
    private OtlpSink target;
    private OtlpSinkBuffer mockTraceBuffer;
    private OtlpSinkBuffer mockMetricBuffer;
    private OtlpSinkBuffer mockLogBuffer;
    private OtlpSinkConfig mockConfig;
    private PluginMetrics mockMetrics;
    private PluginSetting mockSetting;
    private AwsCredentialsSupplier mockAwsCredSupplier;

    @BeforeEach
    void setUp() throws Exception {
        mockAwsCredSupplier = mock(AwsCredentialsSupplier.class);
        mockConfig = mock(OtlpSinkConfig.class);
        when(mockConfig.getAwsRegion()).thenReturn(Region.of("us-west-2"));
        when(mockConfig.getEndpoint()).thenReturn("https://localhost/v1/traces");
        when(mockConfig.getMaxEvents()).thenReturn(10);
        when(mockConfig.getMaxBatchSize()).thenReturn(1_000_000L);
        when(mockConfig.getFlushTimeoutMillis()).thenReturn(100L);
        when(mockConfig.getMaxRetries()).thenReturn(2);

        mockMetrics = mock(PluginMetrics.class);
        mockSetting = mock(PluginSetting.class);
        when(mockSetting.getPipelineName()).thenReturn("pipeline");
        when(mockSetting.getName()).thenReturn("otlp");

        target = new OtlpSink(mockAwsCredSupplier, mockConfig, mockMetrics, mockSetting);

        // Replace the buffers map with mocks
        mockTraceBuffer = mock(OtlpSinkBuffer.class);
        mockMetricBuffer = mock(OtlpSinkBuffer.class);
        mockLogBuffer = mock(OtlpSinkBuffer.class);

        final Map<OtlpSignalType, OtlpSinkBuffer> mockBuffers = new EnumMap<>(OtlpSignalType.class);
        mockBuffers.put(OtlpSignalType.TRACE, mockTraceBuffer);
        mockBuffers.put(OtlpSignalType.METRIC, mockMetricBuffer);
        mockBuffers.put(OtlpSignalType.LOG, mockLogBuffer);

        final Field buffersField = OtlpSink.class.getDeclaredField("buffersBySignalType");
        buffersField.setAccessible(true);
        buffersField.set(target, mockBuffers);
    }

    @Test
    void testInitialize_startsBuffer() {
        target.initialize();

        verify(mockTraceBuffer).start();
        verify(mockMetricBuffer).start();
        verify(mockLogBuffer).start();
    }

    @Test
    void testOutput_addsEverySpanRecordToBuffer() {
        @SuppressWarnings("unchecked") final Record<Event> r1 = mock(Record.class);
        @SuppressWarnings("unchecked") final Record<Event> r2 = mock(Record.class);
        
        final Span span1 = mock(Span.class);
        final Span span2 = mock(Span.class);
        when(r1.getData()).thenReturn(span1);
        when(r2.getData()).thenReturn(span2);

        target.output(List.of(r1, r2));

        verify(mockTraceBuffer).add(r1);
        verify(mockTraceBuffer).add(r2);
    }

    @Test
    void testOutput_addsMetricRecordsToBuffer() {
        @SuppressWarnings("unchecked") final Record<Event> r1 = mock(Record.class);
        @SuppressWarnings("unchecked") final Record<Event> r2 = mock(Record.class);
        
        final Metric metric1 = mock(Metric.class);
        final Metric metric2 = mock(Metric.class);
        when(r1.getData()).thenReturn(metric1);
        when(r2.getData()).thenReturn(metric2);

        target.output(List.of(r1, r2));

        verify(mockMetricBuffer).add(r1);
        verify(mockMetricBuffer).add(r2);
    }

    @Test
    void testOutput_addsLogRecordsToBuffer() {
        @SuppressWarnings("unchecked") final Record<Event> r1 = mock(Record.class);
        @SuppressWarnings("unchecked") final Record<Event> r2 = mock(Record.class);
        
        final Log log1 = mock(Log.class);
        final Log log2 = mock(Log.class);
        when(r1.getData()).thenReturn(log1);
        when(r2.getData()).thenReturn(log2);

        target.output(List.of(r1, r2));

        verify(mockLogBuffer).add(r1);
        verify(mockLogBuffer).add(r2);
    }

    @Test
    void testOutput_addsMixedRecordsToBuffer() {
        @SuppressWarnings("unchecked") final Record<Event> spanRecord = mock(Record.class);
        @SuppressWarnings("unchecked") final Record<Event> metricRecord = mock(Record.class);
        @SuppressWarnings("unchecked") final Record<Event> logRecord = mock(Record.class);
        
        final Span span = mock(Span.class);
        final Metric metric = mock(Metric.class);
        final Log log = mock(Log.class);
        
        when(spanRecord.getData()).thenReturn(span);
        when(metricRecord.getData()).thenReturn(metric);
        when(logRecord.getData()).thenReturn(log);

        target.output(List.of(spanRecord, metricRecord, logRecord));

        verify(mockTraceBuffer).add(spanRecord);
        verify(mockMetricBuffer).add(metricRecord);
        verify(mockLogBuffer).add(logRecord);
    }

    @Test
    void testIsReady_returnsTrueOnlyAfterInitialization() {
        when(mockTraceBuffer.isRunning()).thenReturn(true);
        when(mockMetricBuffer.isRunning()).thenReturn(true);
        when(mockLogBuffer.isRunning()).thenReturn(true);

        assertFalse(target.isReady());

        target.initialize();
        assertTrue(target.isReady());

        when(mockTraceBuffer.isRunning()).thenReturn(false);
        assertFalse(target.isReady());
    }

    @Test
    void testShutdown_stopsBuffer() {
        target.shutdown();

        verify(mockTraceBuffer).stop();
        verify(mockMetricBuffer).stop();
        verify(mockLogBuffer).stop();
    }

    @Test
    void testConstructor_doesNotThrow() {
        assertDoesNotThrow(() -> new OtlpSink(mockAwsCredSupplier, mockConfig, mockMetrics, mockSetting));
    }
}
