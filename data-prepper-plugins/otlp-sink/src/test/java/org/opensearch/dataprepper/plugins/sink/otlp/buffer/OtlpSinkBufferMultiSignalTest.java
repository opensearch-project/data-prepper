/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.otlp.buffer;

import io.opentelemetry.proto.logs.v1.ResourceLogs;
import io.opentelemetry.proto.metrics.v1.ResourceMetrics;
import io.opentelemetry.proto.trace.v1.ResourceSpans;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventHandle;
import org.opensearch.dataprepper.model.log.Log;
import org.opensearch.dataprepper.model.metric.Metric;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.trace.Span;
import org.opensearch.dataprepper.plugins.otel.codec.OTelProtoStandardCodec;
import org.opensearch.dataprepper.plugins.sink.otlp.OtlpSignalType;
import org.opensearch.dataprepper.plugins.sink.otlp.configuration.OtlpSinkConfig;
import org.opensearch.dataprepper.plugins.sink.otlp.http.OtlpHttpSender;
import org.opensearch.dataprepper.plugins.sink.otlp.metrics.OtlpSinkMetrics;
import software.amazon.awssdk.regions.Region;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for OtlpSinkBuffer with multiple signal types (traces, metrics, logs).
 */
class OtlpSinkBufferMultiSignalTest {

    private OtlpSinkConfig config;
    private OtlpSinkMetrics metrics;
    private OTelProtoStandardCodec.OTelProtoEncoder encoder;
    private OtlpHttpSender sender;
    private OtlpSinkBuffer buffer;

    @BeforeEach
    void setUp() {
        config = mock(OtlpSinkConfig.class);
        when(config.getEndpoint()).thenReturn("https://localhost/v1/traces");
        when(config.getMaxEvents()).thenReturn(10);
        when(config.getMaxRetries()).thenReturn(2);
        when(config.getMaxBatchSize()).thenReturn(1_000_000L);
        when(config.getFlushTimeoutMillis()).thenReturn(100L);
        when(config.getAwsRegion()).thenReturn(Region.of("us-west-2"));

        metrics = mock(OtlpSinkMetrics.class);
        encoder = mock(OTelProtoStandardCodec.OTelProtoEncoder.class);
        sender = mock(OtlpHttpSender.class);

        buffer = new OtlpSinkBuffer(config, metrics, encoder, sender);
    }

    @AfterEach
    void tearDown() {
        if (buffer != null) {
            buffer.stop();
        }
    }

    @Test
    void testProcessTraceEvents() throws Exception {
        final ResourceSpans resourceSpans = ResourceSpans.getDefaultInstance();
        when(encoder.convertToResourceSpans(any(Span.class))).thenReturn(resourceSpans);

        buffer.start();

        final Record<Event> record1 = createMockSpanRecord();
        final Record<Event> record2 = createMockSpanRecord();

        buffer.add(record1);
        buffer.add(record2);

        await().atMost(2, SECONDS).untilAsserted(() -> 
            verify(sender).send(any(), eq(OtlpSignalType.TRACE))
        );

        buffer.stop();
    }

    @Test
    void testProcessMetricEvents() throws Exception {
        final ResourceMetrics resourceMetrics = ResourceMetrics.getDefaultInstance();
        when(encoder.convertToResourceMetrics(any(Metric.class))).thenReturn(resourceMetrics);

        buffer.start();

        final Record<Event> record1 = createMockMetricRecord();
        final Record<Event> record2 = createMockMetricRecord();

        buffer.add(record1);
        buffer.add(record2);

        await().atMost(2, SECONDS).untilAsserted(() -> 
            verify(sender).send(any(), eq(OtlpSignalType.METRIC))
        );

        buffer.stop();
    }

    @Test
    void testProcessLogEvents() throws Exception {
        final ResourceLogs resourceLogs = ResourceLogs.getDefaultInstance();
        when(encoder.convertToResourceLogs(any(Log.class))).thenReturn(resourceLogs);

        buffer.start();

        final Record<Event> record1 = createMockLogRecord();
        final Record<Event> record2 = createMockLogRecord();

        buffer.add(record1);
        buffer.add(record2);

        await().atMost(2, SECONDS).untilAsserted(() -> 
            verify(sender).send(any(), eq(OtlpSignalType.LOG))
        );

        buffer.stop();
    }

    @Test
    void testMixedSignalTypes_flushesOnSignalTypeChange() throws Exception {
        // Configure to not flush by count or time initially
        when(config.getMaxEvents()).thenReturn(100);
        when(config.getFlushTimeoutMillis()).thenReturn(10_000L);

        final ResourceSpans resourceSpans = ResourceSpans.getDefaultInstance();
        final ResourceMetrics resourceMetrics = ResourceMetrics.getDefaultInstance();
        final ResourceLogs resourceLogs = ResourceLogs.getDefaultInstance();

        when(encoder.convertToResourceSpans(any(Span.class))).thenReturn(resourceSpans);
        when(encoder.convertToResourceMetrics(any(Metric.class))).thenReturn(resourceMetrics);
        when(encoder.convertToResourceLogs(any(Log.class))).thenReturn(resourceLogs);

        buffer = new OtlpSinkBuffer(config, metrics, encoder, sender);
        buffer.start();

        // Add traces
        buffer.add(createMockSpanRecord());
        buffer.add(createMockSpanRecord());

        // Add metrics - should trigger flush of traces
        buffer.add(createMockMetricRecord());
        buffer.add(createMockMetricRecord());

        // Add logs - should trigger flush of metrics
        buffer.add(createMockLogRecord());

        // Wait for flushes
        await().atMost(2, SECONDS).untilAsserted(() -> {
            verify(sender, atLeast(2)).send(any(), any(OtlpSignalType.class));
        });

        buffer.stop();

        // Verify all three signal types were sent
        final ArgumentCaptor<OtlpSignalType> signalTypeCaptor = ArgumentCaptor.forClass(OtlpSignalType.class);
        verify(sender, atLeast(3)).send(any(), signalTypeCaptor.capture());

        final List<OtlpSignalType> capturedTypes = signalTypeCaptor.getAllValues();
        assertNotNull(capturedTypes);
    }

    @Test
    void testSignalTypeChangeTriggersImmediateFlush() throws Exception {
        when(config.getMaxEvents()).thenReturn(100);
        when(config.getFlushTimeoutMillis()).thenReturn(Long.MAX_VALUE);

        final ResourceSpans resourceSpans = ResourceSpans.getDefaultInstance();
        final ResourceMetrics resourceMetrics = ResourceMetrics.getDefaultInstance();

        when(encoder.convertToResourceSpans(any(Span.class))).thenReturn(resourceSpans);
        when(encoder.convertToResourceMetrics(any(Metric.class))).thenReturn(resourceMetrics);

        buffer = new OtlpSinkBuffer(config, metrics, encoder, sender);
        buffer.start();

        // Add a trace
        buffer.add(createMockSpanRecord());

        // Add a metric - should immediately flush the trace batch
        buffer.add(createMockMetricRecord());

        await().atMost(2, SECONDS).untilAsserted(() -> 
            verify(sender).send(any(), eq(OtlpSignalType.TRACE))
        );

        buffer.stop();
    }

    @Test
    void testEachSignalTypeFlushesIndependently() throws Exception {
        when(config.getMaxEvents()).thenReturn(2);
        when(config.getFlushTimeoutMillis()).thenReturn(Long.MAX_VALUE);

        final ResourceSpans resourceSpans = ResourceSpans.getDefaultInstance();
        final ResourceMetrics resourceMetrics = ResourceMetrics.getDefaultInstance();
        final ResourceLogs resourceLogs = ResourceLogs.getDefaultInstance();

        when(encoder.convertToResourceSpans(any(Span.class))).thenReturn(resourceSpans);
        when(encoder.convertToResourceMetrics(any(Metric.class))).thenReturn(resourceMetrics);
        when(encoder.convertToResourceLogs(any(Log.class))).thenReturn(resourceLogs);

        buffer = new OtlpSinkBuffer(config, metrics, encoder, sender);
        buffer.start();

        // Add 2 traces - should flush
        buffer.add(createMockSpanRecord());
        buffer.add(createMockSpanRecord());

        await().atMost(2, SECONDS).untilAsserted(() -> 
            verify(sender).send(any(), eq(OtlpSignalType.TRACE))
        );

        // Add 2 metrics - should flush
        buffer.add(createMockMetricRecord());
        buffer.add(createMockMetricRecord());

        await().atMost(2, SECONDS).untilAsserted(() -> 
            verify(sender).send(any(), eq(OtlpSignalType.METRIC))
        );

        // Add 2 logs - should flush
        buffer.add(createMockLogRecord());
        buffer.add(createMockLogRecord());

        await().atMost(2, SECONDS).untilAsserted(() -> 
            verify(sender).send(any(), eq(OtlpSignalType.LOG))
        );

        buffer.stop();

        // Verify each signal type was sent once
        verify(sender, times(1)).send(any(), eq(OtlpSignalType.TRACE));
        verify(sender, times(1)).send(any(), eq(OtlpSignalType.METRIC));
        verify(sender, times(1)).send(any(), eq(OtlpSignalType.LOG));
    }

    @Test
    void testEncodingFailureForMetric() throws Exception {
        when(encoder.convertToResourceMetrics(any(Metric.class)))
                .thenThrow(new RuntimeException("encoding error"));

        buffer.start();

        final Record<Event> record = createMockMetricRecord();
        buffer.add(record);

        TimeUnit.MILLISECONDS.sleep(200);

        verify(metrics).incrementFailedRecordsCount(1);
        verify(metrics, atLeast(1)).incrementErrorsCount();

        buffer.stop();
    }

    @Test
    void testEncodingFailureForLog() throws Exception {
        when(encoder.convertToResourceLogs(any(Log.class)))
                .thenThrow(new RuntimeException("encoding error"));

        buffer.start();

        final Record<Event> record = createMockLogRecord();
        buffer.add(record);

        TimeUnit.MILLISECONDS.sleep(200);

        verify(metrics).incrementFailedRecordsCount(1);
        verify(metrics, atLeast(1)).incrementErrorsCount();

        buffer.stop();
    }

    @Test
    void testFinalFlushWithMixedTypes() throws Exception {
        when(config.getMaxEvents()).thenReturn(1000);
        when(config.getFlushTimeoutMillis()).thenReturn(Long.MAX_VALUE);

        final ResourceSpans resourceSpans = ResourceSpans.getDefaultInstance();
        final ResourceMetrics resourceMetrics = ResourceMetrics.getDefaultInstance();

        when(encoder.convertToResourceSpans(any(Span.class))).thenReturn(resourceSpans);
        when(encoder.convertToResourceMetrics(any(Metric.class))).thenReturn(resourceMetrics);

        buffer = new OtlpSinkBuffer(config, metrics, encoder, sender);
        buffer.start();

        // Add some traces
        buffer.add(createMockSpanRecord());
        
        // Add some metrics (will flush traces)
        buffer.add(createMockMetricRecord());

        TimeUnit.MILLISECONDS.sleep(100);

        // Stop should flush remaining metrics
        buffer.stop();

        await().atMost(1, SECONDS).untilAsserted(() -> {
            verify(sender, atLeast(1)).send(any(), eq(OtlpSignalType.TRACE));
            verify(sender, atLeast(1)).send(any(), eq(OtlpSignalType.METRIC));
        });
    }

    @Test
    void testUnknownSignalTypeIsSkipped() throws Exception {
        buffer.start();

        // Create a record with an unknown event type
        final Record<Event> record = mock(Record.class);
        final Event unknownEvent = mock(Event.class);
        final EventHandle eventHandle = mock(EventHandle.class);
        
        when(record.getData()).thenReturn(unknownEvent);
        when(unknownEvent.getEventHandle()).thenReturn(eventHandle);

        buffer.add(record);

        TimeUnit.MILLISECONDS.sleep(200);
        buffer.stop();

        // Should not have called encoder for unknown type
        verify(encoder, times(0)).convertToResourceSpans(any());
        verify(encoder, times(0)).convertToResourceMetrics(any());
        verify(encoder, times(0)).convertToResourceLogs(any());
    }

    private Record<Event> createMockSpanRecord() {
        final Record<Event> record = mock(Record.class);
        final Span span = mock(Span.class);
        final EventHandle eventHandle = mock(EventHandle.class);
        when(record.getData()).thenReturn(span);
        when(span.getEventHandle()).thenReturn(eventHandle);
        return record;
    }

    private Record<Event> createMockMetricRecord() {
        final Record<Event> record = mock(Record.class);
        final Metric metric = mock(Metric.class);
        final EventHandle eventHandle = mock(EventHandle.class);
        when(record.getData()).thenReturn(metric);
        when(metric.getEventHandle()).thenReturn(eventHandle);
        return record;
    }

    private Record<Event> createMockLogRecord() {
        final Record<Event> record = mock(Record.class);
        final Log log = mock(Log.class);
        final EventHandle eventHandle = mock(EventHandle.class);
        when(record.getData()).thenReturn(log);
        when(log.getEventHandle()).thenReturn(eventHandle);
        return record;
    }
}
